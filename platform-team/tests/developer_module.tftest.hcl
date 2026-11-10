# modules/developer on its own: what one developer's stack contains, and how its
# statements are signed. `terraform test` (Terraform >= 1.7 for mock_provider).
# Every provider is mocked, so nothing here talks to Confluent Cloud or sleeps.

mock_provider "confluent" {
  mock_resource "confluent_environment" {
    defaults = {
      id            = "env-abc123"
      resource_name = "crn://confluent.cloud/organization=mock-org/environment=env-abc123"
    }
  }

  mock_data "confluent_flink_region" {
    defaults = {
      rest_endpoint = "https://flink.eu-central-1.aws.confluent.cloud"
    }
  }
}

mock_provider "time" {}

variables {
  user_id                    = "u-dev01"
  environment_name           = "John-Doe-c21e_env"
  region                     = "eu-central-1"
  flink_max_cfu              = 20
  csp_hosted_model           = "anthropic.claude-sonnet-4-6"
  principal_id               = "u-abc123"
  confluent_cloud_api_key    = "MOCKGLOBALKEY"
  confluent_cloud_api_secret = "mock-global-secret"
}

run "environment_and_direct_binding" {
  module {
    source = "./modules/developer"
  }

  assert {
    condition = (
      confluent_environment.main.display_name == "John-Doe-c21e_env" &&
      confluent_environment.main.stream_governance[0].package == "ADVANCED"
    )
    error_message = "the environment should be named var.environment_name, with Stream Governance ADVANCED"
  }

  assert {
    condition = (
      confluent_role_binding.developer_env_admin.principal == "User:u-dev01" &&
      confluent_role_binding.developer_env_admin.role_name == "EnvironmentAdmin" &&
      confluent_role_binding.developer_env_admin.crn_pattern == confluent_environment.main.resource_name
    )
    error_message = "the developer should be a direct User: EnvironmentAdmin on their own environment"
  }
}

# The developer's deploy finds the cluster and pool by these names.
run "cluster_and_pool" {
  command = plan

  module {
    source = "./modules/developer"
  }

  assert {
    condition = (
      confluent_kafka_cluster.main.display_name == "RIVER-OOPS-DEMO-CLUSTER" &&
      confluent_kafka_cluster.main.cloud == "AWS" &&
      confluent_kafka_cluster.main.region == "eu-central-1" &&
      confluent_kafka_cluster.main.availability == "SINGLE_ZONE" &&
      length(confluent_kafka_cluster.main.standard) == 1
    )
    error_message = "expected a Standard single-zone RIVER-OOPS-DEMO-CLUSTER on AWS in var.region"
  }

  assert {
    condition = (
      confluent_flink_compute_pool.main.display_name == "RIVER-OOPS-DEMO-pool" &&
      confluent_flink_compute_pool.main.cloud == "AWS" &&
      confluent_flink_compute_pool.main.region == "eu-central-1" &&
      confluent_flink_compute_pool.main.max_cfu == 20
    )
    error_message = "expected a RIVER-OOPS-DEMO-pool on AWS in var.region with max_cfu from the variable"
  }
}

run "statements_signed_as_principal_in_the_developer_catalog" {
  module {
    source = "./modules/developer"
  }

  assert {
    condition = alltrue([
      for s in values(output.flink_statements) : s.principal == "u-abc123"
    ])
    error_message = "every Flink statement must run as the platform team's Global key owner (principal_id)"
  }

  assert {
    condition = alltrue([
      for s in values(output.flink_statements) :
      s.catalog == "John-Doe-c21e_env" && s.database == "RIVER-OOPS-DEMO-CLUSTER"
    ])
    error_message = "every statement must target the developer's own environment and cluster"
  }

  assert {
    condition = (
      contains(keys(output.flink_statements), "service_requests") &&
      contains(keys(output.flink_statements), "service_deployments") &&
      contains(keys(output.flink_statements), "llm_textgen_model")
    )
    error_message = "the module must create the two source tables and the model"
  }
}

run "the_two_source_tables_and_model_are_created" {
  command = plan

  module {
    source = "./modules/developer"
  }

  assert {
    condition     = can(regex("CREATE TABLE IF NOT EXISTS `service_requests`", confluent_flink_statement.create_service_requests_table.statement))
    error_message = "service_requests table statement missing"
  }

  assert {
    condition = (
      can(regex("CREATE TABLE IF NOT EXISTS `service_deployments`", confluent_flink_statement.create_service_deployments_table.statement)) &&
      can(regex("PRIMARY KEY \\(`service_name`\\) NOT ENFORCED", confluent_flink_statement.create_service_deployments_table.statement)) &&
      can(regex("'changelog.mode' = 'upsert'", confluent_flink_statement.create_service_deployments_table.statement)) &&
      can(regex("'kafka.cleanup-policy' = 'delete'", confluent_flink_statement.create_service_deployments_table.statement))
    )
    error_message = "service_deployments must be an upsert table keyed by service_name with delete cleanup"
  }

  assert {
    condition     = can(regex("CREATE MODEL IF NOT EXISTS .*`llm_textgen_model`.*'confluent.model' = 'anthropic.claude-sonnet-4-6'", confluent_flink_statement.llm_textgen_model.statement))
    error_message = "llm_textgen_model must be created on the configured CSP Hosted model"
  }
}
