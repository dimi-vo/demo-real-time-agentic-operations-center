"""A thin Flink SQL statement API over the Confluent Cloud REST endpoint.

Used to: check the tables and the model exist at deploy time (`SHOW TABLES`,
`SHOW MODELS`), and stop statements and drop the agents in `reset` and `reset-health`.
"""

from __future__ import annotations

import time

import requests

TERMINAL_PHASES = {"COMPLETED", "FAILED", "STOPPED"}
RUNNING = "RUNNING"


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


def still_running(session: FlinkSession, names: list[str]) -> list[str]:
    """The statements in `names` that are still listed and not in a terminal phase.

    A statement that is no longer listed (deleted) or has reached a terminal phase counts as gone."""
    wanted = set(names)
    running = []
    for st in session.list_statements():
        name = st.get("name", "")
        if name in wanted and st.get("status", {}).get("phase", "") not in TERMINAL_PHASES:
            running.append(name)
    return running


def wait_until_gone(session: FlinkSession, names: list[str], timeout: float = 60, interval: float = 2) -> list[str]:
    """Poll until none of `names` is running any more; returns those still running at the timeout."""
    deadline = time.time() + timeout
    remaining = still_running(session, names)
    while remaining and time.time() < deadline:
        time.sleep(interval)
        remaining = still_running(session, remaining)
    return remaining
