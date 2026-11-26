# =============================================================================
# One developer's stack: their environment and EnvironmentAdmin binding, a
# Kafka cluster, a Flink compute pool, and the workshop's ten tables (five source
# tables, five output tables) and model (statements.tf), and with deploy_labs the labs'
# statements (D-069). The cluster and pool names
# are fixed, because the developer's deploy finds them by name inside the environment.
#
# No service accounts, no API keys: the platform team's one Global key signs
# every statement and runs as its owner (principal_id).
# =============================================================================

locals {
  name_prefix = "RIVER-OOPS-DEMO"
}

data "confluent_organization" "main" {}

# Always a fresh environment: a reused one may already have a Schema Registry in
# another region, and the first schema registration then fails.
resource "confluent_environment" "main" {
  display_name = var.environment_name

  stream_governance {
    package = "ADVANCED"
  }
}

# Bound to the user directly, never through an SSO group mapping: the
# developer's own API keys don't carry group-mapped permissions.
resource "confluent_role_binding" "developer_env_admin" {
  principal   = "User:${var.user_id}"
  role_name   = "EnvironmentAdmin"
  crn_pattern = confluent_environment.main.resource_name
}

resource "confluent_kafka_cluster" "main" {
  display_name = "${local.name_prefix}-CLUSTER"
  cloud        = "AWS"
  region       = var.region
  availability = "SINGLE_ZONE"

  standard {}

  environment {
    id = confluent_environment.main.id
  }
}

# Schema Registry comes up with the environment's first cluster, in that
# cluster's region, so look it up only once ours exists. The CREATE TABLEs
# register schemas in it, so the statements wait for it too.
data "confluent_schema_registry_cluster" "main" {
  environment {
    id = confluent_environment.main.id
  }

  depends_on = [confluent_kafka_cluster.main]
}

data "confluent_flink_region" "main" {
  cloud  = "AWS"
  region = var.region
}

resource "confluent_flink_compute_pool" "main" {
  display_name = "${local.name_prefix}-pool"
  cloud        = "AWS"
  region       = var.region
  max_cfu      = var.flink_max_cfu

  environment {
    id = confluent_environment.main.id
  }
}

# A statement sent 1 s after its cluster came up once went FAILED server-side,
# and the provider's retry then hit 409 "already exists". Every statement waits
# for this short pause (tests/developer_module.tftest.hcl).
resource "time_sleep" "cluster_settle" {
  create_duration = "60s"

  depends_on = [data.confluent_schema_registry_cluster.main]
}
