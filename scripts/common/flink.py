"""A thin Flink SQL statement API over the Confluent Cloud REST endpoint, plus the
canonical lab-build list used by `reset --with-labs`.

Used to: check the model exists at deploy time (`SHOW MODELS`), rebuild the lab
statements from `sql/`, and drop them in `reset`.
"""

from __future__ import annotations

import time
from pathlib import Path

import requests

TERMINAL_PHASES = {"COMPLETED", "FAILED", "STOPPED"}
RUNNING = "RUNNING"

# The lab statements, in dependency order, each a file under sql/. These solution
# files are authored in Phases 3-4; `reset --with-labs` submits whatever exists and
# reports any that are missing. (service_health feeds the agents, which feed the
# inserts into remediation_decisions.)
LAB_SQL = (
    ("2.4_service_health.sql", "service_health"),
    ("4.2_release_agent.sql", "release_agent"),
    ("4.2_capacity_agent.sql", "capacity_agent"),
    ("4.3_remediation_decisions.sql", "remediation_decisions"),
    ("4.3_insert_release_agent.sql", "insert release_agent"),
    ("4.3_insert_capacity_agent.sql", "insert capacity_agent"),
)


class FlinkSession:
    """Submit statements and read their status against one developer's compute pool."""

    def __init__(self, card: dict[str, str]):
        try:
            self.rest = card["AIOPS_FLINK_REST_ENDPOINT"].rstrip("/")
            self.org = card["AIOPS_ORGANIZATION_ID"]
            self.env = card["AIOPS_ENVIRONMENT_ID"]
            self.pool = card["AIOPS_COMPUTE_POOL_ID"]
            self.catalog = card["AIOPS_CATALOG"]
            self.database = card["AIOPS_DATABASE"]
            self.auth = (card["AIOPS_FLINK_API_KEY"], card["AIOPS_FLINK_API_SECRET"])
        except KeyError as e:
            raise SystemExit(f"Credential card is missing {e}.") from e
        self.base = f"{self.rest}/sql/v1/organizations/{self.org}/environments/{self.env}/statements"
        self._seq = 0

    def _name(self, label: str = "aiops") -> str:
        self._seq += 1
        return f"{label}-{int(time.time() * 1000) % 10**9}-{self._seq}"

    def submit(self, sql: str, label: str = "aiops") -> str:
        name = self._name(label)
        body = {
            "name": name,
            "spec": {
                "statement": sql,
                "compute_pool_id": self.pool,
                "properties": {"sql.current-catalog": self.catalog, "sql.current-database": self.database},
            },
        }
        r = requests.post(self.base, json=body, auth=self.auth, timeout=30)
        if r.status_code >= 400:
            raise RuntimeError(f"submit failed ({r.status_code}): {r.text[:600]}")
        return name

    def wait(self, name: str, timeout: int = 180) -> dict:
        deadline = time.time() + timeout
        st: dict = {}
        while time.time() < deadline:
            st = requests.get(f"{self.base}/{name}", auth=self.auth, timeout=30).json()
            if st.get("status", {}).get("phase") in TERMINAL_PHASES | {RUNNING}:
                return st
            time.sleep(2)
        return st

    def results(self, name: str, max_rows: int = 50, timeout: int = 60, idle_pages: int = 6):
        url = f"{self.base}/{name}/results"
        seen, empty = 0, 0
        deadline = time.time() + timeout
        while seen < max_rows and time.time() < deadline:
            page = requests.get(url, auth=self.auth, timeout=30).json()
            data = (page.get("results") or {}).get("data") or []
            if data:
                empty = 0
                for item in data:
                    yield item.get("row")
                    seen += 1
                    if seen >= max_rows:
                        return
            else:
                empty += 1
                if seen > 0 and empty >= idle_pages:
                    return
            nxt = (page.get("metadata") or {}).get("next")
            if not nxt:
                return
            url = nxt
            time.sleep(0.5 if data else 1.0)

    def list_statements(self) -> list[dict]:
        out: list[dict] = []
        url = f"{self.base}?page_size=100"
        while url:
            page = requests.get(url, auth=self.auth, timeout=30).json()
            out.extend(page.get("data") or [])
            url = (page.get("metadata") or {}).get("next") or ""
        return out

    def stop(self, name: str) -> None:
        try:
            requests.delete(f"{self.base}/{name}", auth=self.auth, timeout=30)
        except requests.RequestException:
            pass


def model_exists(session: FlinkSession, model_name: str) -> bool:
    """True if `SHOW MODELS` lists `model_name`."""
    name = session.submit("SHOW MODELS;", label="aiops-check")
    status = session.wait(name, timeout=60)
    found = False
    if status.get("status", {}).get("phase") in {RUNNING, "COMPLETED"}:
        for row in session.results(name, max_rows=200, timeout=30):
            if row and model_name in [str(c) for c in row]:
                found = True
                break
    session.stop(name)
    return found


def list_tables(session: FlinkSession) -> set[str]:
    """Every table name `SHOW TABLES` returns in the session's catalog.database.

    Raises RuntimeError if the statement submit fails or never reaches RUNNING/COMPLETED
    — the caller is expected to catch and surface the error rather than treat it as
    "no tables exist" (D-020).
    """
    name = session.submit("SHOW TABLES;", label="aiops-check")
    status = session.wait(name, timeout=60)
    phase = status.get("status", {}).get("phase")
    if phase not in {RUNNING, "COMPLETED"}:
        session.stop(name)
        raise RuntimeError(f"SHOW TABLES did not reach RUNNING/COMPLETED (phase={phase}).")
    tables: set[str] = set()
    for row in session.results(name, max_rows=500, timeout=30):
        if row:
            tables.add(str(row[0]))
    session.stop(name)
    return tables


def create_lab_objects(card: dict[str, str], root: Path) -> bool:
    """Submit the canonical lab SQL from sql/, in order, waiting for each to come up.

    Returns True on success. Missing solution files (not yet authored) are reported
    and treated as a failure so the caller doesn't claim a half-built pipeline.
    """
    session = FlinkSession(card)
    sql_dir = Path(root) / "sql"
    missing = [fn for fn, _ in LAB_SQL if not (sql_dir / fn).is_file()]
    if missing:
        print(f"  Cannot rebuild labs: these solution files don't exist yet: {', '.join(missing)}")
        return False
    for filename, creates in LAB_SQL:
        sql = (sql_dir / filename).read_text().strip().rstrip(";").strip()
        try:
            name = session.submit(sql, label="aiops-lab")
        except RuntimeError as e:
            print(f"  {creates}: submit failed — {e}")
            return False
        phase = session.wait(name, timeout=180).get("status", {}).get("phase")
        if phase in (RUNNING, "COMPLETED"):
            print(f"  {creates}: {phase}  ({filename})")
            continue
        print(f"  {creates}: {phase}  ({filename})")
        return False
    return True
