# Break 2a — Platform-team Terraform module

Phase 2 is larger than one break's worth (Terraform module + full developer tooling +
the generator's Kafka path). Per the brief's "propose splitting it with an extra
break", I've split it:

- **2a (this break):** the platform-team Terraform module — complete and apply-ready.
  You can provision a test developer now, which takes real minutes, **while I build 2b**.
- **2b (next):** `deploy` / `reset` / `destroy` / `setup-rtce` and the generator's
  Kafka path, verified against the environment you apply in 2a.

## 1. Summary

Built `platform-team/`, adapted from the reference and using the Phase 1 source-table
DDL:

- **`modules/developer/`** — per developer: a fresh `ADVANCED` environment, a direct
  `User:` EnvironmentAdmin binding, a Standard single-zone `RIVER-OOPS-DEMO-CLUSTER`,
  a `RIVER-OOPS-DEMO-pool` compute pool, and three Flink statements: `service_requests`,
  `service_deployments`, `llm_textgen_model` (`anthropic.claude-sonnet-4-6`). No seed,
  no `driver_race_history`, no service accounts or API keys.
- **Root** `main.tf` / `variables.tf` / `outputs.tf` / `providers.tf` / `versions.tf`
  — key-owner lookup, per-developer name sanitizer (twin of the Python one coming in
  2b), `handout` and `validation` outputs, `terraform.tfvars.example`.
- **`tests/*.tftest.hcl`** — 9 mocked `terraform test` checks (naming, validations,
  the developer stack, the two tables + model, outputs).
- **`README.md`** — the platform-team guide (steps, the AWS-only RTCE/Lightning region
  note, teardown).
- **`tests/test_platform_team_sync.py`** — fails if the `statements.tf` table DDL drifts
  from `sql/source_*.sql` (the single source of truth).

**Cumulative state.** Phase 0 (plan) → Phase 1 (DDL + offline generator) → Phase 2a
(Terraform module). No developer tooling or Kafka path yet. Commit: this commit.

## 2. ⚠️ Deviations and workarounds

- **DEVIATION D-006 (open)** — `service_deployments` uses `cleanup-policy = delete` +
  24h retention (not compaction). Confirmed supported by you at Break 1; you'll see the
  `CREATE TABLE` succeed when you apply (step below).

No workarounds.

## 3. How to test it yourself

**Verified by me** (ran them): `terraform fmt -check -recursive platform-team` (clean),
`terraform init` + `terraform test` in `platform-team/` (**9 passed, 0 failed**),
`uv run pytest` (**59 passed**, incl. the DDL-drift and schema-vs-DDL checks),
`uv run ruff check` (clean). `terraform validate` passes.

To re-run the offline checks:
```bash
cd platform-team && terraform init && terraform test        # 9 passed
cd .. && uv run pytest tests/ -q                             # drift + schema checks
```

**Only you can do this** (it creates real cloud resources and costs money — I never run
Terraform): see section 4.

## 4. Commands you need to run

Provision **one test developer** (ideally your own email, so you can also rehearse 2b):

```bash
cd platform-team
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars: set confluent_cloud_api_key/secret to an OrganizationAdmin
#   Global key, and developer_emails = ["you@yourcompany.com"]; optionally region.
terraform init
terraform plan -out=terraform.tfplan      # expect: Plan: 9 to add, 0 to change, 0 to destroy
terraform apply terraform.tfplan
rm terraform.tfplan
terraform output -raw handout              # your environment name + ID + Console URL
```

Expected: ~a few minutes (mostly the cluster). Afterwards,
`confluent flink statement list --environment <env-id> --cloud aws --region <region>`
shows the three `tf-…` statements `COMPLETED`, and the Console shows `service_requests`
and `service_deployments` topics (empty, with registered schemas) and
`SHOW MODELS` lists `llm_textgen_model`. Please paste back the `handout` output and
confirm the three statements completed — that's what I need to wire up and verify 2b.

Failure looks like: a plan error (bad key, non-user email) before anything is created,
or a statement stuck `FAILED`/`PENDING` (re-run `plan -out`/`apply`/`rm`).

## 5. Decisions and assumptions

No new decisions this sub-phase. D-006 (delete retention) is now exercised by the
`service_deployments` `CREATE TABLE` — confirm it reaches `COMPLETED` when you apply.

## 6. Questions for me

- Region for the test developer? Default `us-east-1` (on the RTCE/Lightning list). If
  your org prefers another AWS region, make sure it's on the list in README step 5.

## 7. Next phase (2b)

The developer tooling (`deploy --track pre-provisioned`, `reset`, `destroy`,
`setup-rtce`) and the generator's Kafka path (`opsgen/kafka_io.py`: Schema Registry
lookup + key/value serialization — raw string key for `service_requests`, Avro-encoded
key for `service_deployments`). Then Break 2b verifies, against the environment you
apply above: `uv run deploy` mints keys + writes the card; `uv run ops-sim` produces to
both topics at the expected rates; a Flink `SELECT` returns typed rows; the 5-second
re-emits and deploys show up in `service_deployments`; and `uv run reset` behaves.
I can start 2b immediately — I don't need your apply to finish to write it, only to
verify it.
