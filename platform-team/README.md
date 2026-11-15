# Platform team guide: Self-Healing IT workshop

Read this if you are the platform team provisioning one Confluent Cloud environment
per developer for the **Confluent Intelligence "Self-Healing IT"** workshop. It is
everything you need to prepare, run and clean up the environments.

## What this does

This Terraform module builds **each developer's workshop stack**:

- **a new, empty environment**, named after their email (see [Environment names](#environment-names)), with the Stream Governance Advanced package, and a role binding that makes the developer **EnvironmentAdmin** on it through their own user account;
- **a Standard, single-zone Kafka cluster**, `RIVER-OOPS-DEMO-CLUSTER`, on AWS in `region` (default `us-east-1`, the same for everyone). The environment's Schema Registry comes up with it;
- **a Flink compute pool**, `RIVER-OOPS-DEMO-pool`, that scales on demand up to `flink_max_cfu` CFUs (default 10), sized for the statements that run the whole workshop (the `service_health` `INSERT`, two agent `INSERT`s, and ad-hoc queries);
- **the workshop's tables and AI model:** three source tables, `service_metrics` (append-only, one metrics sample per service per second), `service_deployments` (upsert, keyed by `service_name`) and `service_incidents` (the simulator's answer key), all empty until the developer runs `uv run ops-sim`; two output tables, `service_health` and `remediation_decisions`, which the developer fills with `INSERT` statements in the labs (so the developer never writes a `CREATE TABLE`, and the ops wall can read all four of its topics from the start); and `llm_textgen_model` on the Confluent-hosted `anthropic.claude-sonnet-4-6` (`csp_hosted_model`).

There is **no static/seed table**: unlike some workshops, this one builds all of its
data live from the generator, so there is nothing to seed and nothing to count.

**It creates no service accounts and no API keys.** Your only key is the
OrganizationAdmin Global key you run this module with. It is the provider credential
and signs every Flink statement, which runs as the key's owner.

**One environment per person, instructors included,** and no spares: an environment is
only useful to the person bound to it, and each one runs a Kafka cluster. Someone who
joins late is added in minutes ([step 9](#9-add-everyone-else)).

On the developer side, each person signs in to the Console (through SSO, if you use
it), runs `confluent login`, then `uv run deploy --track pre-provisioned` from the
workshop repository, with no Terraform. The deploy mints a Global, a Kafka and a
Schema Registry key for their own account and writes them to a local credential card.
It creates nothing else in the environment. The full developer steps are in the
[pre-provisioned walkthrough](../docs/tracks/PRE-PROVISIONED.md).

## Your steps

| When | Step |
|---|---|
| A week or more before | [1. Install the tools and sign in](#1-install-the-tools-and-sign-in) |
| | [2. Get an OrganizationAdmin Global API key](#2-get-an-organizationadmin-global-api-key) |
| | [3. Check that every developer is a user in your organization](#3-check-that-every-developer-is-a-user-in-your-organization) |
| | [4. Check your quotas](#4-check-your-quotas) |
| | [5. Check the region](#5-check-the-region) |
| | [6. Check networking and access](#6-check-networking-and-access) |
| A few days before | [7. Create the instructors' environments](#7-create-the-instructors-environments) |
| | [8. Rehearse the developer flow](#8-rehearse-the-developer-flow) |
| Shortly before | [9. Add everyone else](#9-add-everyone-else) |
| | [10. Validate the environments](#10-validate-the-environments) |
| | [11. Developer access](#11-developer-access) |
| During | [When a developer comes to you](#when-a-developer-comes-to-you) |
| Afterwards | [12. Tear down](#12-tear-down) |

The clusters bill from the moment they're created, so apply everyone's stacks shortly
before the workshop; the instructors' go first, for the rehearsal.

## 1. Install the tools and sign in

You need:

- **Terraform 1.3 or later** (`terraform version`). `terraform init` downloads `confluentinc/confluent` (pinned to `2.94.0`), `hashicorp/http` (`3.6.3`), `hashicorp/time` (`0.14.2`) and `hashicorp/random`. The optional tests in `tests/` need Terraform 1.7 or later.
- **The Confluent CLI** (`confluent version`), for the checks below.

Sign in to the CLI as an OrganizationAdmin:

```bash
confluent login
```

## 2. Get an OrganizationAdmin Global API key

The module runs with one **Global** API key owned by an `OrganizationAdmin`.

```bash
confluent api-key list --resource global --current-user   # reuse one if you have its secret
confluent api-key create --resource global                # or create one (secret shown once)
confluent iam rbac role-binding list --principal User:<owner-id> --role OrganizationAdmin
```

A user can own at most 2 Global API keys. One row from the last command means the
owner is an `OrganizationAdmin`.

## 3. Check that every developer is a user in your organization

The module looks each email up and **never invites anyone:** an email that isn't a
user fails the plan.

```bash
confluent iam user list
```

Every developer's email must be in the `Email` column, exactly as you'll write it in
[step 7](#7-create-the-instructors-environments). If someone is missing:

- **With SSO,** ask them to sign in to [Confluent Cloud](https://confluent.cloud) once.
- **Without SSO,** `confluent iam user invitation create <email>` and have them accept it.

## 4. Check your quotas

With N people (instructors included), check there's room for N environments and
clusters:

```bash
confluent service-quota list organization --quota-code iam.max_environments.per_org
confluent service-quota list organization --quota-code iam.max_kafka_clusters.per_org
confluent service-quota list organization --quota-code kafka.max_kafka_creation.per_day
```

For each, `Applied Limit` minus `Usage` must be at least N. Leave headroom on
`kafka.max_kafka_creation.per_day` in case you replace a stack the same day. Raise a
quota through Confluent Support if needed.

## 5. Check the region

Everyone's cluster and compute pool go in the one `region` (default `us-east-1`):

```bash
confluent flink region list --cloud aws
confluent rtce region list
```

- **Flink:** the first command must list your region.
- **Real-Time Context Engine and Lightning Tables** (sections 5–7 of the workshop) are **AWS-only and in a subset of regions.** If `confluent rtce region list` doesn't list your region, developers skip those sections. As of late 2026 the supported AWS regions are roughly: `us-east-1`, `us-east-2`, `us-west-2`, `ap-northeast-1`, `ap-northeast-2`, `ap-southeast-1`, `ap-southeast-2`, `ap-south-1`, `eu-central-1`, `eu-west-1`, `eu-west-2`.
- **The Confluent-hosted model** `anthropic.claude-sonnet-4-6` (or the `csp_hosted_model` you set) must be offered there. No CLI command shows it; confirm with your Confluent account team, and the rehearsal ([step 8](#8-rehearse-the-developer-flow)) confirms it in practice.

## 6. Check networking and access

- **No IP filter blocks the developers' networks** from Kafka, Flink, Schema Registry or the management API.
- **Outbound 443 and 9092** from the developers' network: the generator runs on each laptop and produces to Kafka directly.
- **Public networking only.** Each cluster is a Standard cluster on public endpoints.
- **Keep the module's direct role bindings.** Each developer must be `EnvironmentAdmin` *directly* on their user, not through an SSO group mapping — API keys don't carry group-mapped permissions, so a group-only developer can't deploy.
- **New, empty environments only.** The module always creates fresh environments; don't swap in existing ones.

## 7. Create the instructors' environments

Run everything from this folder.

```bash
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars`:

- `confluent_cloud_api_key` / `confluent_cloud_api_secret`: the Global key from [step 2](#2-get-an-organizationadmin-global-api-key) (or export `TF_VAR_confluent_cloud_api_key` / `TF_VAR_confluent_cloud_api_secret` and delete the lines).
- `developer_emails`: for now, only the instructors', as `confluent iam user list` shows them.
- Optionally `region`, `flink_max_cfu`, `csp_hosted_model`.

Then:

```bash
terraform init
terraform plan -out=terraform.tfplan
terraform apply terraform.tfplan
rm terraform.tfplan
```

**`terraform plan`** ends with `Plan: N to add, 0 to change, 0 to destroy.`, where **N
is 12 per person**:

1. A random name suffix
2. One environment
3. One role binding for the user
4. One Standard cluster
5. One Flink compute pool
6. A 60-second pause
7. Flink statement: create the `service_metrics` table
8. Flink statement: create the `service_deployments` table
9. Flink statement: create the `service_incidents` table
10. Flink statement: create the `service_health` table
11. Flink statement: create the `remediation_decisions` table
12. Flink statement: create the `llm_textgen_model` model

It fails before creating anything if your key/secret is wrong, an email isn't a user,
the same person appears twice, or an email's local part has nothing to name an
environment after.

**`rm terraform.tfplan`:** the saved plan contains your API secret in plain text.

Then check:

```bash
terraform plan                       # must report: No changes.
terraform output -raw handout        # each person's environment name, ID and Console URL
confluent flink statement list --environment <env-id> --cloud aws --region <region>
```

All six statements (named `tf-…`) should be `COMPLETED`.

### If the apply stops

On `Statement with name "tf-…" already exists`, or a statement that failed with
"technical difficulties on our end", re-run the same `plan -out` / `apply` / `rm`
commands. Both are transient; the statements are `IF NOT EXISTS`, so a second run only
adds what's missing.

## 8. Rehearse the developer flow

Have an instructor run the whole developer flow in their own environment a few days
ahead: the [pre-provisioned walkthrough](../docs/tracks/PRE-PROVISIONED.md)'s deploy,
generator, pipeline and agents.

The agents call a Confluent-hosted model. **Confirm with your Confluent account team
how that inference is billed** in your organization. Sections 5–7 (Real-Time Context
Engine, Lightning Queries) depend on the region check in [step 5](#5-check-the-region).

## 9. Add everyone else

Add everyone's emails to `developer_emails`, then re-run:

```bash
terraform plan -out=terraform.tfplan
terraform apply terraform.tfplan
rm terraform.tfplan
```

Check the plan says `0 to change, 0 to destroy`, with **12 to add per new person**.
Each stack belongs to its exact email string, so **removing an email destroys that
person's whole stack**, and **changing an entry (even its case) replaces it** with a
new, empty one.

## 10. Validate the environments

```bash
terraform output -raw validation
```

It prints one Console link per developer to their `service_metrics` topic's **schema**
tab. Open each: the topic must exist and have a registered schema (the topics stay
empty until the developer runs `uv run ops-sim`, so a registered schema — not
messages — is what proves the `CREATE TABLE` ran). If a topic is missing, its
statement didn't complete — follow [If the apply stops](#if-the-apply-stops).

## 11. Developer access

```bash
terraform output -raw handout
```

```text
EMAIL                   ENVIRONMENT          ENVIRONMENT ID  CONSOLE URL
jane.doe@example.com    jane-doe-7f3a_env    env-a1b2c3      https://confluent.cloud/environments/env-a1b2c3
john.smith@example.com  john-smith-c21e_env  env-d4e5f6      https://confluent.cloud/environments/env-d4e5f6
```

**Send each developer their own row,** with a link to the
[pre-provisioned walkthrough](../docs/tracks/PRE-PROVISIONED.md). They need the
environment name and the Console URL; the deploy finds the environment by itself.

## When a developer comes to you

| The developer sees | What to do |
|---|---|
| `Your platform team hasn't provisioned <name> (<id>) yet` | Their stack is incomplete, most likely a stopped apply. Re-run `plan -out` / `apply` / `rm` (see [If the apply stops](#if-the-apply-stops)). |
| `Your Global key sees no environment you're directly bound to.` | Their email is missing from `developer_emails`, or they signed in with another account. Check the [handout](#11-developer-access). |
| `✗ table <name> is missing — ask your platform team to re-run their apply` (or `✗ model llm_textgen_model …`), from the readiness check at the end of `uv run deploy`; `<name>` can be any of the five tables, including `service_health` and `remediation_decisions` | A statement didn't complete; re-run the apply. |

A developer who sees `Confluent Cloud refused a new Global key` already holds the 2
Global keys a user may own — they delete one under **API keys → My account**.

## 12. Tear down

1. **Each developer runs `uv run destroy`** — it deletes only the three API keys their deploy minted (Global last) and their local card; the environment stays for you.
2. **You run, from this folder:**

    ```bash
    terraform destroy
    ```

    It lists everything it will delete (statements, compute pools, clusters, role
    bindings, environments) and asks you to type `yes`.

If a developer can't run step 1, their Kafka/SR keys are useless once the cluster is
gone, but their Global key keeps any other roles they hold — have them delete it under
**API keys → My account**.

<details>
<summary>Reference</summary>

### Environment names

Each environment is named after the email's local part, plus a random 4-character
suffix and `_env`. Characters other than letters, digits, `_` and `-` become `-` (runs
collapse to one), the result is cut to 40 characters and `-`/`_` is trimmed from the
ends; case is kept. `John.Doe@example.com` → `John-Doe-c21e_env`. **Don't rename the
environments, cluster or pool** — the deploy matches them by name.

### Cost

- **Per developer, apply → destroy:** a Standard Kafka cluster and Flink compute (CFUs on demand, up to `flink_max_cfu`). Developers' own statements keep running — and can keep using compute — until you destroy their stack, even after they run `uv run destroy`. Plus the agents' AI model calls (they fire only on anomalies, so cost tracks incidents, not time).
- **Per environment, until destroyed:** Stream Governance Advanced (billed per environment, can't be downgraded).

### Terraform state

State is local by default (`terraform.tfstate` in this folder). **It holds your Global
key's secret in plain text** (in every statement's credentials and the key-owner
lookup) and your developers' user details and environment IDs. Protect it like the key,
or use an encrypted backend (add a `backend` block before the first `init`).

### Files

| File | Purpose |
|---|---|
| `versions.tf` / `providers.tf` | Version pins; the Confluent provider configured with your Global key |
| `variables.tf` | Key/secret, `developer_emails` + validations, `region`, `flink_max_cfu`, `csp_hosted_model` |
| `main.tf` | User + key-owner lookups, name suffixes, one `modules/developer` per email |
| `outputs.tf` | The `handout` table and the `validation` links |
| `modules/developer/` | One developer's stack: environment, binding, cluster, pool, the three source tables, the two output tables and the model (`statements.tf`) |
| `tests/` | Offline `terraform test` checks (every provider mocked) |

</details>
