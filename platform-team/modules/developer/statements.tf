# The workshop's tables and model, created as the platform team: the five source tables
# (service_metrics, service_deployments, service_incidents, and checkout's support_tickets
# and ticket_answer_key, D-068), the five output tables the labs fill with INSERT
# statements (service_error_rate, service_health, remediation_decisions, and section 5's
# p1_tickets and ticket_triage, D-068), and the model: 11 statements. With main.tf's five
# resources and the root's name suffix, that is 17 resources per developer.
# Each statement is signed with the Global key and runs as that key's owner (principal_id).
#
# The DDL is all IF NOT EXISTS, so re-running apply is a no-op for anything already
# there (e.g. after the provider's double-POST 409 flake, or after terminal
# statements are auto-deleted at 30 days). The same property means a changed DDL never
# reaches an existing table: DROP TABLE it first, then apply again.
#
# The CREATE TABLE bodies are copied from the files in sql/ (source_*.sql and sink_*.sql),
# the single source of truth for every schema, because this folder must work when copied
# out of the repo. tests/test_platform_team_sync.py catches drift between these heredocs
# and the sql/ files.
#
# The statements are chained with depends_on in one line (D-037), in the order of this
# file: the source tables, then the output tables in the order the labs fill them, and the
# model last. tests/test_platform_team_sync.py checks the chain.

resource "confluent_flink_statement" "create_service_metrics_table" {
  organization { id = data.confluent_organization.main.id }
  environment { id = confluent_environment.main.id }
  compute_pool { id = confluent_flink_compute_pool.main.id }
  principal { id = var.principal_id }
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = var.confluent_cloud_api_key
    secret = var.confluent_cloud_api_secret
  }

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `service_metrics` (
      `event_time` TIMESTAMP(3) COMMENT 'End of the second this sample covers (tick + 999 ms); also the Kafka record timestamp',
      `service_name` STRING COMMENT 'Which River Shop service the sample describes (foreign key to service_deployments)',
      `traffic_count` INT COMMENT 'Requests served in that second',
      `latency_ms` DOUBLE COMMENT 'Mean latency of the requests in that second, in milliseconds; set even when traffic_count is 0',
      `error_count` INT COMMENT 'Failed requests in that second; lab 2.2 reads it for web-gateway',
      WATERMARK FOR `event_time` AS `event_time` - INTERVAL '1' SECOND
    )
    DISTRIBUTED INTO 1 BUCKETS
    WITH (
      'changelog.mode' = 'append',
      'connector' = 'confluent',
      'kafka.cleanup-policy' = 'delete',
      'kafka.consumer.isolation-level' = 'read-uncommitted',
      'kafka.message-timestamp-type' = 'create-time',
      'kafka.retention.time' = '24 h',
      'scan.startup.mode' = 'earliest-offset',
      'value.format' = 'avro-registry'
    );
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [time_sleep.cluster_settle]
}

resource "confluent_flink_statement" "create_service_deployments_table" {
  organization { id = data.confluent_organization.main.id }
  environment { id = confluent_environment.main.id }
  compute_pool { id = confluent_flink_compute_pool.main.id }
  principal { id = var.principal_id }
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = var.confluent_cloud_api_key
    secret = var.confluent_cloud_api_secret
  }

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `service_deployments` (
      `service_name` STRING COMMENT 'Service identifier (primary key)',
      `version` STRING COMMENT 'Currently running version (semver)',
      `previous_version` STRING COMMENT 'Version running before the current one; null until the first deploy',
      `deployed_at` TIMESTAMP(3) COMMENT 'When the current version went live',
      `last_change` STRING COMMENT 'INITIAL, DEPLOY or ROLLBACK (SCALE is reserved for a later project)',
      `desired_replicas` INT COMMENT 'Desired replica count (equal to ready_replicas in this phase)',
      `ready_replicas` INT COMMENT 'Ready replica count (fixed per service in this phase)',
      `tier` STRING COMMENT 'CRITICAL or STANDARD',
      `owner_team` STRING COMMENT 'Team that owns the service',
      `event_time` TIMESTAMP(3) COMMENT 'Emit time of this state record (changes on every re-emit)',
      WATERMARK FOR `event_time` AS `event_time` - INTERVAL '0.001' SECOND,
      PRIMARY KEY (`service_name`) NOT ENFORCED
    ) DISTRIBUTED BY (`service_name`) INTO 1 BUCKETS
    WITH (
      'changelog.mode' = 'upsert',
      'connector' = 'confluent',
      'kafka.cleanup-policy' = 'delete',
      'kafka.consumer.isolation-level' = 'read-uncommitted',
      'kafka.retention.time' = '24 h',
      'scan.startup.mode' = 'earliest-offset',
      'value.format' = 'avro-registry'
    );
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.create_service_metrics_table]
}

resource "confluent_flink_statement" "create_service_incidents_table" {
  organization { id = data.confluent_organization.main.id }
  environment { id = confluent_environment.main.id }
  compute_pool { id = confluent_flink_compute_pool.main.id }
  principal { id = var.principal_id }
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = var.confluent_cloud_api_key
    secret = var.confluent_cloud_api_secret
  }

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `service_incidents` (
      `event_time` TIMESTAMP(3) COMMENT 'When this record was emitted; also the Kafka record timestamp',
      `incident_id` STRING COMMENT 'Stable id for this incident across its start and end records',
      `service_name` STRING COMMENT 'Service the incident affects (foreign key to service_deployments)',
      `incident_type` STRING COMMENT 'BAD_DEPLOY or UPSTREAM_DROP on payments, BAD_DEPLOY_ERRORS on web-gateway',
      `started_at` TIMESTAMP(3) COMMENT 'Wall-clock time degradation began (for BAD_DEPLOY and BAD_DEPLOY_ERRORS, the same tick as the deploy)',
      `ended_at` TIMESTAMP(3) COMMENT 'Wall-clock time degradation ended; NULL on the start record, set on the end record',
      `expected_trigger` STRING COMMENT 'What should flag it: LATENCY_HIGH or TRAFFIC_LOW in service_health, ERROR_RATE_HIGH in service_error_rate',
      `expected_action` STRING COMMENT 'What the agent should pick: ROLLBACK or INVESTIGATE_UPSTREAM',
      WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
    )
    DISTRIBUTED INTO 1 BUCKETS
    WITH (
      'changelog.mode' = 'append',
      'connector' = 'confluent',
      'kafka.cleanup-policy' = 'delete',
      'kafka.consumer.isolation-level' = 'read-uncommitted',
      'kafka.message-timestamp-type' = 'create-time',
      'kafka.retention.time' = '24 h',
      'scan.startup.mode' = 'earliest-offset',
      'value.format' = 'avro-registry'
    );
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.create_service_deployments_table]
}

resource "confluent_flink_statement" "create_support_tickets_table" {
  organization { id = data.confluent_organization.main.id }
  environment { id = confluent_environment.main.id }
  compute_pool { id = confluent_flink_compute_pool.main.id }
  principal { id = var.principal_id }
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = var.confluent_cloud_api_key
    secret = var.confluent_cloud_api_secret
  }

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `support_tickets` (
      `event_time` TIMESTAMP(3) COMMENT 'When the ticket was created; also the Kafka record timestamp',
      `ticket_id` STRING COMMENT 'The ticket id in its source: ZD-48215 for Zendesk, INC0041872 for ServiceNow',
      `source` STRING COMMENT 'Where the ticket was filed: ZENDESK or SERVICENOW',
      `service_name` STRING COMMENT 'The service the ticket is about: always checkout',
      `priority` STRING COMMENT 'P1, P2 or P3, as the reporter set it; lab 5.1 routes the P1 tickets',
      `subject` STRING COMMENT 'The ticket subject line',
      `description` STRING COMMENT 'The ticket text: one to three sentences in English',
      WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
    )
    DISTRIBUTED INTO 1 BUCKETS
    WITH (
      'changelog.mode' = 'append',
      'connector' = 'confluent',
      'kafka.cleanup-policy' = 'delete',
      'kafka.consumer.isolation-level' = 'read-uncommitted',
      'kafka.message-timestamp-type' = 'create-time',
      'kafka.retention.time' = '24 h',
      'scan.startup.mode' = 'earliest-offset',
      'value.format' = 'avro-registry'
    );
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.create_service_incidents_table]
}

resource "confluent_flink_statement" "create_ticket_answer_key_table" {
  organization { id = data.confluent_organization.main.id }
  environment { id = confluent_environment.main.id }
  compute_pool { id = confluent_flink_compute_pool.main.id }
  principal { id = var.principal_id }
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = var.confluent_cloud_api_key
    secret = var.confluent_cloud_api_secret
  }

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `ticket_answer_key` (
      `event_time` TIMESTAMP(3) COMMENT 'When the ticket was created (its support_tickets event_time); also the Kafka record timestamp',
      `ticket_id` STRING COMMENT 'The ticket this answers for (support_tickets.ticket_id)',
      `true_severity` STRING COMMENT 'What the simulator meant: HIGH (many customers cannot buy or pay) or LOW; P2 and P3 are always LOW',
      `expected_action` STRING COMMENT 'For a HIGH ticket, what the responder should pick: ROLLBACK, SCALE_OUT or INVESTIGATE_UPSTREAM; NULL when LOW',
      WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
    )
    DISTRIBUTED INTO 1 BUCKETS
    WITH (
      'changelog.mode' = 'append',
      'connector' = 'confluent',
      'kafka.cleanup-policy' = 'delete',
      'kafka.consumer.isolation-level' = 'read-uncommitted',
      'kafka.message-timestamp-type' = 'create-time',
      'kafka.retention.time' = '24 h',
      'scan.startup.mode' = 'earliest-offset',
      'value.format' = 'avro-registry'
    );
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.create_support_tickets_table]
}

resource "confluent_flink_statement" "create_service_error_rate_table" {
  organization { id = data.confluent_organization.main.id }
  environment { id = confluent_environment.main.id }
  compute_pool { id = confluent_flink_compute_pool.main.id }
  principal { id = var.principal_id }
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = var.confluent_cloud_api_key
    secret = var.confluent_cloud_api_secret
  }

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `service_error_rate` (
      `service_name` STRING COMMENT 'River Shop service this window describes (web-gateway)',
      `window_start` TIMESTAMP(3) NOT NULL COMMENT 'Start of the 10-second tumbling window',
      `window_end` TIMESTAMP(3) NOT NULL COMMENT 'End of the window; with service_name, the row identity',
      `traffic_count` INT COMMENT 'Requests in the window (SUM of the per-second traffic_count)',
      `error_count` INT COMMENT 'Failed requests in the window (SUM of the per-second error_count)',
      `error_rate` DOUBLE COMMENT 'error_count / traffic_count, from 0 to 1; null when the window has no requests',
      `error_status` STRING NOT NULL COMMENT 'HIGH when errors are at least 25 percent of the requests, else NORMAL',
      `version` STRING COMMENT 'Version live at the end of the window (temporal join, LAST_VALUE)',
      `previous_version` STRING COMMENT 'Version before it; null until the first deploy',
      `last_change` STRING COMMENT 'INITIAL, DEPLOY or ROLLBACK',
      `seconds_since_deploy` INT COMMENT 'Seconds from the last deploy to window_end',
      `detected_at` TIMESTAMP_LTZ(3) NOT NULL COMMENT 'When the rule produced this verdict (CURRENT_TIMESTAMP, an instant)'
    )
    DISTRIBUTED INTO 1 BUCKETS
    WITH (
      'changelog.mode' = 'append',
      'connector' = 'confluent',
      'kafka.cleanup-policy' = 'delete',
      'kafka.consumer.isolation-level' = 'read-uncommitted',
      'kafka.retention.time' = '24 h',
      'scan.startup.mode' = 'earliest-offset',
      'value.format' = 'avro-registry'
    );
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.create_ticket_answer_key_table]
}

resource "confluent_flink_statement" "create_service_health_table" {
  organization { id = data.confluent_organization.main.id }
  environment { id = confluent_environment.main.id }
  compute_pool { id = confluent_flink_compute_pool.main.id }
  principal { id = var.principal_id }
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = var.confluent_cloud_api_key
    secret = var.confluent_cloud_api_secret
  }

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `service_health` (
      `service_name` STRING COMMENT 'River Shop service this window describes',
      `window_start` TIMESTAMP(3) NOT NULL COMMENT 'Start of the 10-second tumbling window',
      `window_end` TIMESTAMP(3) NOT NULL COMMENT 'End of the window; with service_name, the row identity',
      `traffic_count` INT COMMENT 'Requests in the window (SUM of the per-second traffic_count)',
      `avg_latency_ms` DOUBLE COMMENT 'Mean of the per-second latency_ms in the window',
      `version` STRING COMMENT 'Version live at the end of the window (temporal join, LAST_VALUE)',
      `previous_version` STRING COMMENT 'Version before it; null until the first deploy',
      `last_change` STRING COMMENT 'INITIAL, DEPLOY or ROLLBACK',
      `seconds_since_deploy` INT COMMENT 'Seconds from the last deploy to window_end',
      `ready_replicas` INT COMMENT 'Ready replicas at the end of the window',
      `tier` STRING COMMENT 'CRITICAL or STANDARD',
      `latency_status` STRING NOT NULL COMMENT 'WARMING_UP, HIGH or NORMAL',
      `expected_latency_ms` DOUBLE COMMENT 'Detector forecast for avg_latency_ms; null while warming up',
      `latency_upper_bound` DOUBLE COMMENT 'Upper bound of the latency band',
      `traffic_status` STRING NOT NULL COMMENT 'WARMING_UP, HIGH, LOW or NORMAL',
      `expected_traffic_count` DOUBLE COMMENT 'Detector forecast for traffic_count',
      `traffic_lower_bound` DOUBLE COMMENT 'Lower bound of the traffic band',
      `traffic_upper_bound` DOUBLE COMMENT 'Upper bound of the traffic band',
      `detected_at` TIMESTAMP_LTZ(3) NOT NULL COMMENT 'When the detector produced this verdict (CURRENT_TIMESTAMP, an instant)'
    )
    DISTRIBUTED INTO 1 BUCKETS
    WITH (
      'changelog.mode' = 'append',
      'connector' = 'confluent',
      'kafka.cleanup-policy' = 'delete',
      'kafka.consumer.isolation-level' = 'read-uncommitted',
      'kafka.retention.time' = '24 h',
      'scan.startup.mode' = 'earliest-offset',
      'value.format' = 'avro-registry'
    );
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.create_service_error_rate_table]
}

resource "confluent_flink_statement" "create_remediation_decisions_table" {
  organization { id = data.confluent_organization.main.id }
  environment { id = confluent_environment.main.id }
  compute_pool { id = confluent_flink_compute_pool.main.id }
  principal { id = var.principal_id }
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = var.confluent_cloud_api_key
    secret = var.confluent_cloud_api_secret
  }

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `remediation_decisions` (
      `window_end` TIMESTAMP(3) NOT NULL COMMENT 'End of the window (service_health or service_error_rate) the decision was made on, or the ticket event_time for a ticket decision',
      `service_name` STRING NOT NULL,
      `agent_name` STRING NOT NULL COMMENT 'release_agent, capacity_agent or ticket_responder',
      `trigger` STRING NOT NULL COMMENT 'LATENCY_HIGH, ERROR_RATE_HIGH, TRAFFIC_HIGH, TRAFFIC_LOW or HIGH_SEVERITY_TICKET',
      `action` STRING NOT NULL COMMENT 'ROLLBACK, SCALE_OUT (from ticket_responder only), INVESTIGATE_UPSTREAM, OBSERVE, or INVALID',
      `target` STRING COMMENT 'Version for ROLLBACK from the release agent, otherwise null',
      `reason` STRING COMMENT 'The agent explanation, one or two sentences',
      `raw_response` STRING COMMENT 'The full agent answer, kept for debugging when parsing returns null',
      `ticket_id` STRING COMMENT 'The ticket a ticket_responder decision is about; null for the metric agents'
    )
    DISTRIBUTED INTO 1 BUCKETS
    WITH (
      'changelog.mode' = 'append',
      'connector' = 'confluent',
      'kafka.cleanup-policy' = 'delete',
      'kafka.consumer.isolation-level' = 'read-uncommitted',
      'kafka.retention.time' = '24 h',
      'scan.startup.mode' = 'earliest-offset',
      'value.format' = 'avro-registry'
    );
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.create_service_health_table]
}

resource "confluent_flink_statement" "create_p1_tickets_table" {
  organization { id = data.confluent_organization.main.id }
  environment { id = confluent_environment.main.id }
  compute_pool { id = confluent_flink_compute_pool.main.id }
  principal { id = var.principal_id }
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = var.confluent_cloud_api_key
    secret = var.confluent_cloud_api_secret
  }

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `p1_tickets` (
      `event_time` TIMESTAMP(3) COMMENT 'When the ticket was created, copied from support_tickets',
      `ticket_id` STRING COMMENT 'The ticket id in its source: ZD-48215 for Zendesk, INC0041872 for ServiceNow',
      `source` STRING COMMENT 'Where the ticket was filed: ZENDESK or SERVICENOW',
      `service_name` STRING COMMENT 'The service the ticket is about: always checkout',
      `priority` STRING COMMENT 'Always P1: lab 5.1 copies only the P1 tickets, so a P2 or P3 here is a filter leak',
      `subject` STRING COMMENT 'The ticket subject line',
      `description` STRING COMMENT 'The ticket text: one to three sentences in English',
      WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
    )
    DISTRIBUTED INTO 1 BUCKETS
    WITH (
      'changelog.mode' = 'append',
      'connector' = 'confluent',
      'kafka.cleanup-policy' = 'delete',
      'kafka.consumer.isolation-level' = 'read-uncommitted',
      'kafka.message-timestamp-type' = 'create-time',
      'kafka.retention.time' = '24 h',
      'scan.startup.mode' = 'earliest-offset',
      'value.format' = 'avro-registry'
    );
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.create_remediation_decisions_table]
}

resource "confluent_flink_statement" "create_ticket_triage_table" {
  organization { id = data.confluent_organization.main.id }
  environment { id = confluent_environment.main.id }
  compute_pool { id = confluent_flink_compute_pool.main.id }
  principal { id = var.principal_id }
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = var.confluent_cloud_api_key
    secret = var.confluent_cloud_api_secret
  }

  statement = <<-EOT
    CREATE TABLE IF NOT EXISTS `ticket_triage` (
      `event_time` TIMESTAMP(3) COMMENT 'When the ticket was created, copied from p1_tickets',
      `ticket_id` STRING COMMENT 'The ticket id in its source: ZD-48215 for Zendesk, INC0041872 for ServiceNow',
      `source` STRING COMMENT 'Where the ticket was filed: ZENDESK or SERVICENOW',
      `service_name` STRING COMMENT 'The service the ticket is about: always checkout',
      `priority` STRING COMMENT 'Always P1, as in p1_tickets',
      `subject` STRING COMMENT 'The ticket subject line',
      `description` STRING COMMENT 'The ticket text: one to three sentences in English',
      `severity` STRING NOT NULL COMMENT 'The classifier verdict: HIGH or LOW, or INVALID when the call failed or the answer did not parse',
      `reason` STRING COMMENT 'The classifier explanation, one or two sentences; lab 5.3 passes it to the responder',
      `raw_response` STRING COMMENT 'The full classifier answer, kept for debugging when parsing returns null',
      WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
    )
    DISTRIBUTED INTO 1 BUCKETS
    WITH (
      'changelog.mode' = 'append',
      'connector' = 'confluent',
      'kafka.cleanup-policy' = 'delete',
      'kafka.consumer.isolation-level' = 'read-uncommitted',
      'kafka.message-timestamp-type' = 'create-time',
      'kafka.retention.time' = '24 h',
      'scan.startup.mode' = 'earliest-offset',
      'value.format' = 'avro-registry'
    );
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.create_p1_tickets_table]
}

# CSP Hosted Flink Model Inference only: Confluent runs the model, so there is no
# connection and no cloud key. The name llm_textgen_model is what the agent SQL
# (sections 4 and 5 of the walkthrough) references.
locals {
  llm_textgen_with = join(", ", [
    "'provider' = 'confluent'",
    "'task' = 'text_generation'",
    "'confluent.model' = '${var.csp_hosted_model}'",
    "'confluent.params.max_tokens' = '50000'",
  ])
}

resource "confluent_flink_statement" "llm_textgen_model" {
  organization { id = data.confluent_organization.main.id }
  environment { id = confluent_environment.main.id }
  compute_pool { id = confluent_flink_compute_pool.main.id }
  principal { id = var.principal_id }
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = var.confluent_cloud_api_key
    secret = var.confluent_cloud_api_secret
  }

  statement = "CREATE MODEL IF NOT EXISTS `${confluent_environment.main.display_name}`.`${confluent_kafka_cluster.main.display_name}`.`llm_textgen_model` INPUT (prompt STRING) OUTPUT (response STRING) WITH (${local.llm_textgen_with});"

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.create_ticket_triage_table]
}
