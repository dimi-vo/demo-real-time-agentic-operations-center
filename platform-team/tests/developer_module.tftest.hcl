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
  deploy_labs                = false
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
      contains(keys(output.flink_statements), "service_metrics") &&
      contains(keys(output.flink_statements), "service_deployments") &&
      contains(keys(output.flink_statements), "service_incidents") &&
      contains(keys(output.flink_statements), "support_tickets") &&
      contains(keys(output.flink_statements), "ticket_answer_key") &&
      contains(keys(output.flink_statements), "cve_scans") &&
      contains(keys(output.flink_statements), "cve_answer_key") &&
      contains(keys(output.flink_statements), "service_error_rate") &&
      contains(keys(output.flink_statements), "service_health") &&
      contains(keys(output.flink_statements), "remediation_decisions") &&
      contains(keys(output.flink_statements), "p1_tickets") &&
      contains(keys(output.flink_statements), "ticket_triage") &&
      contains(keys(output.flink_statements), "llm_textgen_model")
    )
    error_message = "the module must create the seven source tables, the five output tables and the model"
  }

  assert {
    condition     = length(output.flink_statements) == 13
    error_message = "the module must create twelve tables and the model, and nothing else"
  }
}

# checkout's tickets and their answer key (D-068): append-only sources with service_incidents'
# options, no key columns, one bucket.
run "the_ticket_source_tables_are_created" {
  command = plan

  module {
    source = "./modules/developer"
  }

  assert {
    condition = alltrue([
      for s in [
        confluent_flink_statement.create_support_tickets_table,
        confluent_flink_statement.create_ticket_answer_key_table,
        ] : (
        can(regex("'changelog.mode' = 'append'", s.statement)) &&
        can(regex("'kafka.consumer.isolation-level' = 'read-uncommitted'", s.statement)) &&
        can(regex("'kafka.cleanup-policy' = 'delete'", s.statement)) &&
        can(regex("DISTRIBUTED INTO 1 BUCKETS", s.statement)) &&
        !can(regex("PRIMARY KEY", s.statement))
      )
    ])
    error_message = "support_tickets and ticket_answer_key must be append-only, read-uncommitted, delete-cleanup tables in one bucket with no key"
  }

  assert {
    condition = (
      can(regex("^CREATE TABLE IF NOT EXISTS `support_tickets`", confluent_flink_statement.create_support_tickets_table.statement)) &&
      can(regex("^CREATE TABLE IF NOT EXISTS `ticket_answer_key`", confluent_flink_statement.create_ticket_answer_key_table.statement))
    )
    error_message = "the two ticket source tables must be created"
  }
}

# The scanner's findings and their answer key (D-070): append-only sources with
# support_tickets' options and its 5 s watermark, no key columns, one bucket, after the
# tickets' two in the chain.
run "the_cve_source_tables_are_created" {
  command = plan

  module {
    source = "./modules/developer"
  }

  assert {
    condition = alltrue([
      for s in [
        confluent_flink_statement.create_cve_scans_table,
        confluent_flink_statement.create_cve_answer_key_table,
        ] : (
        can(regex("'changelog.mode' = 'append'", s.statement)) &&
        can(regex("'kafka.consumer.isolation-level' = 'read-uncommitted'", s.statement)) &&
        can(regex("'kafka.cleanup-policy' = 'delete'", s.statement)) &&
        can(regex("'kafka.retention.time' = '24 h'", s.statement)) &&
        can(regex("DISTRIBUTED INTO 1 BUCKETS", s.statement)) &&
        can(regex("WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND", s.statement)) &&
        !can(regex("PRIMARY KEY", s.statement))
      )
    ])
    error_message = "cve_scans and cve_answer_key must be append-only, read-uncommitted, delete-cleanup tables in one bucket with a 5 s watermark and no key"
  }

  assert {
    condition = (
      can(regex("^CREATE TABLE IF NOT EXISTS `cve_scans`", confluent_flink_statement.create_cve_scans_table.statement)) &&
      can(regex("`cvss_score` DOUBLE", confluent_flink_statement.create_cve_scans_table.statement)) &&
      can(regex("^CREATE TABLE IF NOT EXISTS `cve_answer_key`", confluent_flink_statement.create_cve_answer_key_table.statement))
    )
    error_message = "the two CVE source tables must be created, and cve_scans must have a DOUBLE cvss_score"
  }
}

# The outputs of labs 5.1 and 5.2 (D-068) take support_tickets' options: append-only and
# read-uncommitted, so the agent reading each one never waits for a Flink commit (D-063).
run "the_ticket_output_tables_are_created" {
  command = plan

  module {
    source = "./modules/developer"
  }

  assert {
    condition = alltrue([
      for s in [
        confluent_flink_statement.create_p1_tickets_table,
        confluent_flink_statement.create_ticket_triage_table,
        ] : (
        can(regex("'changelog.mode' = 'append'", s.statement)) &&
        can(regex("'kafka.consumer.isolation-level' = 'read-uncommitted'", s.statement)) &&
        can(regex("'kafka.cleanup-policy' = 'delete'", s.statement)) &&
        can(regex("'kafka.retention.time' = '24 h'", s.statement)) &&
        can(regex("DISTRIBUTED INTO 1 BUCKETS", s.statement)) &&
        can(regex("WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND", s.statement)) &&
        !can(regex("PRIMARY KEY", s.statement))
      )
    ])
    error_message = "p1_tickets and ticket_triage must be append-only, read-uncommitted, delete-cleanup tables in one bucket with a 5 s watermark and no key"
  }

  assert {
    condition = (
      can(regex("^CREATE TABLE IF NOT EXISTS `p1_tickets`", confluent_flink_statement.create_p1_tickets_table.statement)) &&
      can(regex("^CREATE TABLE IF NOT EXISTS `ticket_triage`", confluent_flink_statement.create_ticket_triage_table.statement)) &&
      can(regex("`severity` STRING NOT NULL", confluent_flink_statement.create_ticket_triage_table.statement))
    )
    error_message = "the two ticket output tables must be created, and ticket_triage must have a STRING NOT NULL severity"
  }

  assert {
    condition     = can(regex("`ticket_id` STRING COMMENT", confluent_flink_statement.create_remediation_decisions_table.statement))
    error_message = "remediation_decisions must have a nullable ticket_id for the ticket responder's decisions"
  }
}

run "the_source_tables_and_model_are_created" {
  command = plan

  module {
    source = "./modules/developer"
  }

  assert {
    condition     = can(regex("CREATE TABLE IF NOT EXISTS `service_metrics`", confluent_flink_statement.create_service_metrics_table.statement))
    error_message = "service_metrics table statement missing"
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

# The three lab output tables exist before the labs start: attendees only INSERT into them
# (D-050, D-067). All are append-only and readable before commit, like the source tables.
run "the_three_output_tables_are_created" {
  command = plan

  module {
    source = "./modules/developer"
  }

  assert {
    condition = (
      can(regex("^CREATE TABLE IF NOT EXISTS `service_error_rate`", confluent_flink_statement.create_service_error_rate_table.statement)) &&
      can(regex("`error_status` STRING NOT NULL", confluent_flink_statement.create_service_error_rate_table.statement)) &&
      can(regex("'changelog.mode' = 'append'", confluent_flink_statement.create_service_error_rate_table.statement)) &&
      can(regex("'kafka.consumer.isolation-level' = 'read-uncommitted'", confluent_flink_statement.create_service_error_rate_table.statement)) &&
      can(regex("'kafka.cleanup-policy' = 'delete'", confluent_flink_statement.create_service_error_rate_table.statement))
    )
    error_message = "service_error_rate must be an append-only, read-uncommitted table with delete cleanup and a STRING error_status"
  }

  assert {
    condition = (
      can(regex("^CREATE TABLE IF NOT EXISTS `service_health`", confluent_flink_statement.create_service_health_table.statement)) &&
      can(regex("'changelog.mode' = 'append'", confluent_flink_statement.create_service_health_table.statement)) &&
      can(regex("'kafka.consumer.isolation-level' = 'read-uncommitted'", confluent_flink_statement.create_service_health_table.statement)) &&
      can(regex("'kafka.cleanup-policy' = 'delete'", confluent_flink_statement.create_service_health_table.statement))
    )
    error_message = "service_health must be an append-only, read-uncommitted table with delete cleanup"
  }

  assert {
    condition = (
      can(regex("^CREATE TABLE IF NOT EXISTS `remediation_decisions`", confluent_flink_statement.create_remediation_decisions_table.statement)) &&
      can(regex("'changelog.mode' = 'append'", confluent_flink_statement.create_remediation_decisions_table.statement)) &&
      can(regex("'kafka.consumer.isolation-level' = 'read-uncommitted'", confluent_flink_statement.create_remediation_decisions_table.statement)) &&
      can(regex("'kafka.cleanup-policy' = 'delete'", confluent_flink_statement.create_remediation_decisions_table.statement))
    )
    error_message = "remediation_decisions must be an append-only, read-uncommitted table with delete cleanup"
  }

  assert {
    condition = alltrue([
      for s in [
        confluent_flink_statement.create_service_error_rate_table,
        confluent_flink_statement.create_service_health_table,
        confluent_flink_statement.create_remediation_decisions_table,
      ] : can(regex("DISTRIBUTED INTO 1 BUCKETS", s.statement)) && can(regex("'kafka.retention.time' = '24 h'", s.statement))
    ])
    error_message = "every output table must use one bucket and 24 h retention, like the source tables"
  }
}

# The labs' statements are for a demo environment only (D-069): attendees write them in the
# workshop, so without deploy_labs the module plans none of them.
run "no_lab_statements_without_deploy_labs" {
  command = plan

  module {
    source = "./modules/developer"
  }

  assert {
    condition = alltrue([
      for s in [
        confluent_flink_statement.insert_service_error_rate,
        confluent_flink_statement.insert_service_health,
        confluent_flink_statement.create_release_agent,
        confluent_flink_statement.create_capacity_agent,
        confluent_flink_statement.insert_release_agent_errors,
        confluent_flink_statement.insert_release_agent,
        confluent_flink_statement.insert_capacity_agent,
        confluent_flink_statement.insert_p1_tickets,
        confluent_flink_statement.create_ticket_classifier,
        confluent_flink_statement.insert_ticket_classifier,
        confluent_flink_statement.create_ticket_responder,
        confluent_flink_statement.insert_ticket_responder,
      ] : length(s) == 0
    ])
    error_message = "without deploy_labs, the module must plan none of the labs' statements"
  }
}

run "deploy_labs_runs_the_twelve_lab_statements_as_principal_in_the_developer_catalog" {
  variables {
    deploy_labs = true
  }

  module {
    source = "./modules/developer"
  }

  assert {
    condition     = length(output.flink_statements) == 25
    error_message = "with deploy_labs, the module must create the twelve tables, the model and the labs' 12 statements"
  }

  assert {
    condition = alltrue([
      for s in values(output.flink_statements) :
      s.principal == "u-abc123" && s.catalog == "John-Doe-c21e_env" && s.database == "RIVER-OOPS-DEMO-CLUSTER"
    ])
    error_message = "every lab statement must run as principal_id in the developer's own environment and cluster"
  }

  assert {
    condition = (
      can(regex("^INSERT INTO `service_error_rate`\n", confluent_flink_statement.insert_service_error_rate[0].statement)) &&
      can(regex("^INSERT INTO `service_health`\n", confluent_flink_statement.insert_service_health[0].statement)) &&
      can(regex("^INSERT INTO `remediation_decisions`\n", confluent_flink_statement.insert_release_agent_errors[0].statement)) &&
      can(regex("^INSERT INTO `remediation_decisions`\n", confluent_flink_statement.insert_release_agent[0].statement)) &&
      can(regex("^INSERT INTO `remediation_decisions`\n", confluent_flink_statement.insert_capacity_agent[0].statement)) &&
      can(regex("^INSERT INTO `p1_tickets`\n", confluent_flink_statement.insert_p1_tickets[0].statement)) &&
      can(regex("^INSERT INTO `ticket_triage`\n", confluent_flink_statement.insert_ticket_classifier[0].statement)) &&
      can(regex("^INSERT INTO `remediation_decisions`\n", confluent_flink_statement.insert_ticket_responder[0].statement))
    )
    error_message = "the eight lab INSERTs must write the output tables (heredoc indent stripped)"
  }

  assert {
    condition = alltrue([
      for pair in [
        ["release_agent", confluent_flink_statement.create_release_agent[0].statement],
        ["capacity_agent", confluent_flink_statement.create_capacity_agent[0].statement],
        ["ticket_classifier", confluent_flink_statement.create_ticket_classifier[0].statement],
        ["ticket_responder", confluent_flink_statement.create_ticket_responder[0].statement],
      ] : can(regex("^CREATE AGENT `${pair[0]}`\nUSING MODEL `llm_textgen_model`\nUSING PROMPT 'OUTPUT FORMAT", pair[1]))
    ])
    error_message = "the four agents must be created on llm_textgen_model, with their prompts unindented"
  }
}
