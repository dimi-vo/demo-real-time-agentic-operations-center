# The workshop's tables and model, created as the platform team: the seven source tables
# (service_metrics, service_deployments, service_incidents, checkout's support_tickets
# and ticket_answer_key, D-068, and the scanner's cve_scans and cve_answer_key, D-070),
# the seven output tables the labs fill with INSERT statements (service_error_rate,
# service_health, remediation_decisions, section 5's p1_tickets and ticket_triage, D-068,
# and section 6's critical_cves and patch_decisions, D-070), and the model: 15 statements.
# patch_decisions also gets its RTCE topic (D-070). With that, main.tf's five resources and
# the root's name suffix, that is 22 resources per developer. A demo environment
# (deploy_labs = true, D-069) also gets the labs' 17 statements, at the end of this file: 32
# statements, 39 resources.
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
# file: the source tables, then the output tables in the order the labs fill them, the
# model, and then the labs' statements in lab order. The RTCE topic hangs off the chain,
# after patch_decisions' table. tests/test_platform_team_sync.py checks the chain.

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

resource "confluent_flink_statement" "create_cve_scans_table" {
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
    CREATE TABLE IF NOT EXISTS `cve_scans` (
      `event_time` TIMESTAMP(3) COMMENT 'When the scanner reported the finding; also the Kafka record timestamp',
      `finding_id` STRING COMMENT 'The finding id: SCAN- and six digits, e.g. SCAN-000123',
      `repository` STRING COMMENT 'The repository: web-gateway, checkout, payments, ci-tools or storefront-legacy (archived)',
      `cve_id` STRING COMMENT 'The CVE, fictional: CVE-2026-4xxxx. The same CVE can be found in several repositories',
      `package` STRING COMMENT 'The vulnerable package, e.g. urllib3',
      `installed_version` STRING COMMENT 'The version the repository has',
      `fixed_version` STRING COMMENT 'The first version without the vulnerability',
      `cvss_score` DOUBLE COMMENT 'CVSS score, 0.0 to 10.0 with one decimal; critical at 9.0 or more (lab 6.1)',
      `finding` STRING COMMENT 'What the scanner said: where the package sits and whether the vulnerable code is used',
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

  depends_on = [confluent_flink_statement.create_ticket_answer_key_table]
}

resource "confluent_flink_statement" "create_cve_answer_key_table" {
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
    CREATE TABLE IF NOT EXISTS `cve_answer_key` (
      `event_time` TIMESTAMP(3) COMMENT 'When the finding was reported (its cve_scans event_time); also the Kafka record timestamp',
      `finding_id` STRING COMMENT 'The finding this answers for (cve_scans.finding_id)',
      `expected_action` STRING COMMENT 'For a critical finding (CVSS 9.0 or more), what the security agent should answer: PATCH (shipped in a deployed service and the vulnerable code is used) or DISMISS; NULL when not critical',
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

  depends_on = [confluent_flink_statement.create_cve_scans_table]
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

  depends_on = [confluent_flink_statement.create_cve_answer_key_table]
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

resource "confluent_flink_statement" "create_critical_cves_table" {
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
    CREATE TABLE IF NOT EXISTS `critical_cves` (
      `event_time` TIMESTAMP(3) COMMENT 'When the scanner reported the finding, copied from cve_scans',
      `finding_id` STRING COMMENT 'The finding id: SCAN- and six digits, e.g. SCAN-000123',
      `repository` STRING COMMENT 'The repository: web-gateway, checkout, payments, ci-tools or storefront-legacy (archived)',
      `cve_id` STRING COMMENT 'The CVE, fictional: CVE-2026-4xxxx. The same CVE can be found in several repositories',
      `package` STRING COMMENT 'The vulnerable package, e.g. urllib3',
      `installed_version` STRING COMMENT 'The version the repository has',
      `fixed_version` STRING COMMENT 'The first version without the vulnerability',
      `cvss_score` DOUBLE COMMENT 'CVSS score, always 9.0 or more: lab 6.1 copies only the critical findings, so a lower score here is a filter leak',
      `finding` STRING COMMENT 'What the scanner said: where the package sits and whether the vulnerable code is used',
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

  depends_on = [confluent_flink_statement.create_ticket_triage_table]
}

resource "confluent_flink_statement" "create_patch_decisions_table" {
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
    CREATE TABLE IF NOT EXISTS `patch_decisions` (
      `event_time` TIMESTAMP(3) COMMENT 'When the scanner reported the finding, copied from critical_cves',
      `finding_id` STRING COMMENT 'The finding id: SCAN- and six digits, e.g. SCAN-000123',
      `repository` STRING COMMENT 'The repository: web-gateway, checkout, payments, ci-tools or storefront-legacy (archived)',
      `cve_id` STRING COMMENT 'The CVE, fictional: CVE-2026-4xxxx',
      `cvss_score` DOUBLE COMMENT 'CVSS score, 9.0 or more, as in critical_cves',
      `action` STRING NOT NULL COMMENT 'The security agent decision: PATCH or DISMISS, or INVALID when the call failed or the answer did not parse',
      `reason` STRING COMMENT 'The security agent explanation, one or two sentences',
      `patch_status` STRING NOT NULL COMMENT 'PATCHED for a PATCH that came back with a commit, FAILED for a PATCH without one, N/A otherwise; worked out by lab 6.4, not the agent',
      `commit_sha` STRING COMMENT 'The commit the patch agent answered with, 7 hex digits; null unless parsed',
      `patch_started_at` TIMESTAMP(3) COMMENT 'When the patch agent started, from its reply; null unless parsed',
      `patch_finished_at` TIMESTAMP(3) COMMENT 'When the patch agent finished, from its reply; null unless parsed',
      `raw_response` STRING COMMENT 'The full security agent answer, kept for debugging when parsing returns null',
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

  depends_on = [confluent_flink_statement.create_critical_cves_table]
}

# The Real-Time Context Engine serves patch_decisions (D-070), so a coding agent can ask about
# the security agent's decisions over MCP. It needs the topic, which the CREATE TABLE above
# makes, so it waits for that statement; nothing waits for it, so it stays off the chain. It is on in every environment,
# so var.region must be one where RTCE is offered (variables.tf). remediation_decisions stays
# a Console step for attendees, as in the reference workshop.
resource "confluent_rtce_topic" "patch_decisions" {
  cloud       = confluent_kafka_cluster.main.cloud
  region      = confluent_kafka_cluster.main.region
  description = "The security agent decisions on critical CVE scan findings in River Shop repositories, one row per finding: PATCH, DISMISS or INVALID with a reason, and for a PATCH the patch_status, the commit the external patch agent answered with, and when the patch started and finished."
  topic_name  = "patch_decisions"

  environment {
    id = confluent_environment.main.id
  }

  kafka_cluster {
    id = confluent_kafka_cluster.main.id
  }

  depends_on = [confluent_flink_statement.create_patch_decisions_table]
}

# CSP Hosted Flink Model Inference only: Confluent runs the model, so there is no
# connection and no cloud key. The name llm_textgen_model is what the agent SQL
# (sections 4 to 6 of the walkthrough) references.
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

  depends_on = [confluent_flink_statement.create_patch_decisions_table]
}

# The labs' seventeen statements, for a demo environment only (D-069): off unless deploy_labs
# is true, because attendees write these statements themselves in the workshop. Each is copied
# from its sql/ file, from the statement's first line to its end (lab 6.2's file holds two,
# the connection and the tool, D-070), and tests/test_platform_team_sync.py checks the copies
# character for character. The one difference is lab 6.2's endpoint, patch_agent_url.
#
# Turn deploy_labs on only while the environment's topics are empty: in the apply that
# creates the environment, or right after `uv run reset`. The agent INSERTs (4.3, 5.2, 5.3
# and 6.4) read from the latest offset, while 2.2, 2.5, 5.1 and 6.1 replay their sources, and
# depends_on waits only until the statement before is RUNNING, not until it has caught up.
# Started over data that is already there, the agents would call the model for every
# replayed anomaly, P1 and critical finding.
#
# Every statement here is replaced when its SQL changes (the provider's statement argument
# forces a new resource): a replaced INSERT replays into its output topic, and a replaced
# CREATE AGENT, CREATE TOOL or CREATE CONNECTION fails while its object exists, since an
# agent, like a table, outlives the statement that created it. They have no IF NOT EXISTS
# here, as in sql/. So after `uv run reset`, which stops the INSERTs and drops the agents, the
# tool and the connection, re-apply with -replace on the five create_*_agent statements and
# lab 6.2's connection and tool: platform-team/README.md, "A demo environment".

# Lab 2.2 (sql/2.2_service_error_rate.sql): web-gateway's error-rate rule, written to service_error_rate.
resource "confluent_flink_statement" "insert_service_error_rate" {
  count = var.deploy_labs ? 1 : 0

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
    INSERT INTO `service_error_rate`
    WITH enriched AS (
      SELECT
        m.event_time,
        m.service_name,
        m.traffic_count,
        m.error_count,
        d.version,
        d.previous_version,
        d.deployed_at,
        d.last_change
      FROM `service_metrics` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ m
      JOIN `service_deployments` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
        FOR SYSTEM_TIME AS OF m.event_time AS d
        ON m.service_name = d.service_name
      WHERE m.service_name = 'web-gateway'
    ),
    windowed AS (
      SELECT
        window_start,
        window_end,
        service_name,
        SUM(traffic_count) AS traffic_count,
        SUM(error_count) AS error_count,
        LAST_VALUE(version) AS version,
        LAST_VALUE(previous_version) AS previous_version,
        LAST_VALUE(last_change) AS last_change,
        TIMESTAMPDIFF(SECOND, LAST_VALUE(deployed_at), window_end) AS seconds_since_deploy
      FROM TABLE(TUMBLE(TABLE enriched, DESCRIPTOR(event_time), INTERVAL '10' SECOND))
      GROUP BY window_start, window_end, service_name
    )
    SELECT
      service_name,
      window_start,
      window_end,
      traffic_count,
      error_count,
      CAST(error_count AS DOUBLE) / NULLIF(traffic_count, 0) AS error_rate,
      CASE
        WHEN traffic_count > 0 AND error_count >= 0.25 * traffic_count THEN 'HIGH'
        ELSE 'NORMAL'
      END AS error_status,
      version,
      previous_version,
      last_change,
      seconds_since_deploy,
      -- When this row's verdict was made: an instant (TIMESTAMP_LTZ), evaluated per row, as in
      -- service_health. Its gap to window_end is the real-time delay: the window has to close.
      CURRENT_TIMESTAMP AS detected_at
    FROM windowed;
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.llm_textgen_model]
}

# Lab 2.5 (sql/2.5_service_health.sql): payments' ARIMA detection, written to service_health.
resource "confluent_flink_statement" "insert_service_health" {
  count = var.deploy_labs ? 1 : 0

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
    INSERT INTO `service_health`
    WITH enriched AS (
      SELECT
        m.event_time,
        m.service_name,
        m.traffic_count,
        m.latency_ms,
        d.version,
        d.previous_version,
        d.deployed_at,
        d.last_change,
        d.ready_replicas,
        d.tier
      FROM `service_metrics` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ m
      JOIN `service_deployments` /*+ OPTIONS('scan.startup.mode'='earliest-offset') */
        FOR SYSTEM_TIME AS OF m.event_time AS d
        ON m.service_name = d.service_name
      WHERE m.service_name = 'payments'
    ),
    windowed AS (
      SELECT
        window_start,
        window_end,
        window_time,
        service_name,
        SUM(traffic_count) AS traffic_count,
        AVG(latency_ms) AS avg_latency_ms,
        LAST_VALUE(version) AS version,
        LAST_VALUE(previous_version) AS previous_version,
        LAST_VALUE(last_change) AS last_change,
        TIMESTAMPDIFF(SECOND, LAST_VALUE(deployed_at), window_end) AS seconds_since_deploy,
        LAST_VALUE(ready_replicas) AS ready_replicas,
        LAST_VALUE(tier) AS tier
      FROM TABLE(TUMBLE(TABLE enriched, DESCRIPTOR(event_time), INTERVAL '10' SECOND))
      GROUP BY window_start, window_end, window_time, service_name
    ),
    anomaly AS (
      SELECT
        *,
        ML_DETECT_ANOMALIES(
          avg_latency_ms, window_time,
          JSON_OBJECT('minTrainingSize' VALUE 12, 'maxTrainingSize' VALUE 360, 'confidencePercentage' VALUE 99.95, 'enableStl' VALUE FALSE, 'p' VALUE 1, 'd' VALUE 0, 'q' VALUE 0)
        ) OVER (PARTITION BY service_name ORDER BY window_time RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS latency_result,
        ML_DETECT_ANOMALIES(
          CAST(traffic_count AS DOUBLE), window_time,
          JSON_OBJECT('minTrainingSize' VALUE 12, 'maxTrainingSize' VALUE 360, 'confidencePercentage' VALUE 99.95, 'enableStl' VALUE FALSE, 'p' VALUE 1, 'd' VALUE 0, 'q' VALUE 0)
        ) OVER (PARTITION BY service_name ORDER BY window_time RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS traffic_result
      FROM windowed
    )
    SELECT
      service_name,
      window_start,
      window_end,
      traffic_count,
      avg_latency_ms,
      version,
      previous_version,
      last_change,
      seconds_since_deploy,
      ready_replicas,
      tier,
      CASE
        WHEN latency_result.is_anomaly IS NULL THEN 'WARMING_UP'
        WHEN latency_result.is_anomaly AND latency_result.actual_value > latency_result.upper_bound THEN 'HIGH'
        ELSE 'NORMAL'
      END AS latency_status,
      latency_result.forecast_value AS expected_latency_ms,
      latency_result.upper_bound AS latency_upper_bound,
      CASE
        WHEN traffic_result.is_anomaly IS NULL THEN 'WARMING_UP'
        WHEN traffic_result.is_anomaly AND traffic_result.actual_value > traffic_result.upper_bound THEN 'HIGH'
        WHEN traffic_result.is_anomaly AND traffic_result.actual_value < traffic_result.lower_bound THEN 'LOW'
        ELSE 'NORMAL'
      END AS traffic_status,
      traffic_result.forecast_value AS expected_traffic_count,
      traffic_result.lower_bound AS traffic_lower_bound,
      traffic_result.upper_bound AS traffic_upper_bound,
      -- When this row's verdict was made: an instant (TIMESTAMP_LTZ), evaluated per row. The
      -- workspace shows it in your time zone, while the window columns are UTC; put
      -- SET 'sql.local-time-zone' = 'UTC'; in front of the query to see both in UTC. Its gap to
      -- window_end is the real-time delay: the window has to close and the watermark pass.
      CURRENT_TIMESTAMP AS detected_at
    FROM anomaly;
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.insert_service_error_rate]
}

# Lab 4.2 (sql/4.2_release_agent.sql): the release agent.
resource "confluent_flink_statement" "create_release_agent" {
  count = var.deploy_labs ? 1 : 0

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
    CREATE AGENT `release_agent`
    USING MODEL `llm_textgen_model`
    USING PROMPT 'OUTPUT FORMAT — respond with exactly these 3 labeled lines, in this order. Plain text only: no markdown, no bold, no bullet points, no blank lines, nothing before or after.

    Action: [ROLLBACK | OBSERVE]
    Target: [the version to roll back to if ROLLBACK, otherwise N/A]
    Reason: [one or two sentences]

    Correct ROLLBACK example:
    Action: ROLLBACK
    Target: 2.9.4
    Reason: Payments latency is 893 ms against an expected 226 ms and the current version went live 10 seconds ago. Returning to 2.9.4 is the fastest safe fix.

    Correct ROLLBACK example:
    Action: ROLLBACK
    Target: 3.4.3
    Reason: Web-gateway failed 206 of 500 requests in this window, an error rate of 41.2 percent against a threshold of 25 percent, and the current version went live 10 seconds ago. Returning to 3.4.3 is the fastest safe fix.

    Correct OBSERVE example:
    Action: OBSERVE
    Target: N/A
    Reason: Payments latency is 251 ms against an upper bound of 248 ms and the last deploy went live 41 minutes ago, so there is no recent release to roll back. A single window this close to the band is more likely to be noise than a real regression.

    ---

    You are the release reliability engineer on call for River Shop, an online store running three microservices on Kubernetes. Each input below is one 10-second window of data for a single service that has just been flagged, and the Trigger line says how. LATENCY_HIGH: its average latency was flagged by a trained statistical detector (ML_DETECT_ANOMALIES, which compares the measured latency with a forecast and a confidence band learned from the recent history of this service), while its traffic is normal, so this is not a load problem. ERROR_RATE_HIGH: at least 25 percent of its requests in the window failed, a fixed threshold. Either way, decide what the on-call team should do about the release.

    DECISION FRAMEWORK — apply the rules in this order and stop at the first one that matches:
    1. ROLLBACK — only when ALL THREE hold: Last Change is DEPLOY, Seconds Since Deploy is less than 600, and Previous Version is not N/A. A deploy in the last ten minutes is the prime suspect for a latency regression or a burst of failing requests. Copy Previous Version into the Target line exactly as given.
    2. OBSERVE — everything rule 1 does not cover: there was no deploy, the deploy is older than 600 seconds, or there is no previous version to return to. With no recent release to blame, there is nothing to roll back, so keep watching.

    STRICT VALUES — the Action line must be exactly ROLLBACK or OBSERVE, with no extra words. The Target line is N/A unless the action is ROLLBACK. Use only the numbers given in the input; never invent a value, a version or a service name.'
    WITH (
      'handle_exception' = 'continue',
      'max_consecutive_failures' = '3',
      'request_timeout' = '60'
    );
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.insert_service_health]
}

# Lab 4.2 (sql/4.2_capacity_agent.sql): the capacity agent.
resource "confluent_flink_statement" "create_capacity_agent" {
  count = var.deploy_labs ? 1 : 0

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
    CREATE AGENT `capacity_agent`
    USING MODEL `llm_textgen_model`
    USING PROMPT 'OUTPUT FORMAT — respond with exactly these 3 labeled lines, in this order. Plain text only: no markdown, no bold, no bullet points, no blank lines, nothing before or after.

    Action: [INVESTIGATE_UPSTREAM | OBSERVE]
    Target: N/A
    Reason: [one or two sentences]

    Correct INVESTIGATE_UPSTREAM example:
    Action: INVESTIGATE_UPSTREAM
    Target: N/A
    Reason: Payments received 31 requests in this window against an expected 298, a collapse rather than a surge. Adding replicas cannot help when work is not arriving, so the caller has to be investigated.

    Correct OBSERVE example:
    Action: OBSERVE
    Target: N/A
    Reason: Payments is at 374 requests per window against an expected 301 and an upper bound of 368, a marginal overshoot. No surge is expected, so more traffic than forecast is noise or the rebound after a drop, and there is no capacity action to take.

    ---

    You are the capacity and reliability engineer on call for River Shop, an online store running three microservices on Kubernetes. Each input below is one 10-second window of health data for a single service whose request count has just been flagged by a trained statistical detector (ML_DETECT_ANOMALIES, which compares the measured count with a forecast and a confidence band learned from the recent history of this service). The service it watches is payments, which normally takes about 300 requests per window on 2 replicas. The Trigger line tells you the direction: TRAFFIC_HIGH means more requests than expected, TRAFFIC_LOW means fewer. No traffic surge is expected at River Shop: more requests than forecast are either noise or the rebound right after a drop, and there is no capacity action to take for them. Decide what the on-call team should do.

    DECISION FRAMEWORK — apply the rules in this order and stop at the first one that matches:
    1. INVESTIGATE_UPSTREAM — the trigger is TRAFFIC_LOW. A collapse in requests means work is not arriving, so the caller or the upstream dependency has to be investigated.
    2. OBSERVE — the trigger is TRAFFIC_HIGH. More requests than forecast are noise or the rebound after a drop, not a load problem, so keep watching.

    STRICT VALUES — the Action line must be exactly INVESTIGATE_UPSTREAM or OBSERVE, with no extra words. The Target line is always N/A. Use only the numbers given in the input; never invent a value.'
    WITH (
      'handle_exception' = 'continue',
      'max_consecutive_failures' = '3',
      'request_timeout' = '60'
    );
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.create_release_agent]
}

# Lab 4.3, leg 1 (sql/4.3_insert_release_agent_errors.sql): error-rate breaches to the release agent.
resource "confluent_flink_statement" "insert_release_agent_errors" {
  count = var.deploy_labs ? 1 : 0

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
    INSERT INTO `remediation_decisions`
    SELECT
      h.`window_end`,
      h.`service_name`,
      'release_agent',
      'ERROR_RATE_HIGH',
      CASE
        WHEN a.`agent_status` = 'SUCCESS'
          AND UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1)))
              IN ('ROLLBACK', 'OBSERVE')
          THEN UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1)))
        ELSE 'INVALID'
      END,
      NULLIF(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Target:\*{0,2}\s*([^\n]+)', 1)), 'N/A'),
      TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Reason:\*{0,2}\s*([\s\S]+?)$', 1)),
      a.`agent_response`,
      CAST(NULL AS STRING)
    FROM (
      SELECT *
      FROM `service_error_rate` /*+ OPTIONS('scan.startup.mode'='latest-offset') */
      WHERE `error_status` = 'HIGH'
    ) h,
    LATERAL TABLE(AI_RUN_AGENT(
      'release_agent',
      CONCAT(
        'SERVICE ', h.`service_name`, '\n',
        'Trigger: ERROR_RATE_HIGH\n',
        'Error Rate: ', CAST(ROUND(h.`error_rate` * 100, 1) AS STRING), ' %',
        '  Threshold: 25 %\n',
        'Errors: ', CAST(h.`error_count` AS STRING), ' of ', CAST(h.`traffic_count` AS STRING), ' requests\n',
        'Last Change: ', COALESCE(h.`last_change`, 'N/A'),
        '  Seconds Since Deploy: ', CAST(h.`seconds_since_deploy` AS STRING), '\n',
        'Previous Version: ', COALESCE(h.`previous_version`, 'N/A')
      )
    )) AS a(`agent_status`, `agent_response`);
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.create_capacity_agent]
}

# Lab 4.3, leg 2 (sql/4.3_insert_release_agent.sql): latency anomalies to the release agent.
resource "confluent_flink_statement" "insert_release_agent" {
  count = var.deploy_labs ? 1 : 0

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
    INSERT INTO `remediation_decisions`
    SELECT
      h.`window_end`,
      h.`service_name`,
      'release_agent',
      'LATENCY_HIGH',
      CASE
        WHEN a.`agent_status` = 'SUCCESS'
          AND UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1)))
              IN ('ROLLBACK', 'OBSERVE')
          THEN UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1)))
        ELSE 'INVALID'
      END,
      NULLIF(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Target:\*{0,2}\s*([^\n]+)', 1)), 'N/A'),
      TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Reason:\*{0,2}\s*([\s\S]+?)$', 1)),
      a.`agent_response`,
      CAST(NULL AS STRING)
    FROM (
      SELECT *
      FROM `service_health` /*+ OPTIONS('scan.startup.mode'='latest-offset') */
      WHERE `latency_status` = 'HIGH' AND `traffic_status` NOT IN ('HIGH', 'LOW')
    ) h,
    LATERAL TABLE(AI_RUN_AGENT(
      'release_agent',
      CONCAT(
        'SERVICE ', h.`service_name`, ' (', h.`tier`, ')\n',
        'Trigger: LATENCY_HIGH\n',
        'Average Latency: ', CAST(ROUND(h.`avg_latency_ms`, 1) AS STRING), ' ms',
        '  Expected: ', CAST(ROUND(h.`expected_latency_ms`, 1) AS STRING), ' ms',
        '  Expected Upper Bound: ', CAST(ROUND(h.`latency_upper_bound`, 1) AS STRING), ' ms\n',
        'Last Change: ', COALESCE(h.`last_change`, 'N/A'),
        '  Seconds Since Deploy: ', CAST(h.`seconds_since_deploy` AS STRING), '\n',
        'Previous Version: ', COALESCE(h.`previous_version`, 'N/A')
      )
    )) AS a(`agent_status`, `agent_response`);
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.insert_release_agent_errors]
}

# Lab 4.3, leg 3 (sql/4.3_insert_capacity_agent.sql): traffic anomalies to the capacity agent.
resource "confluent_flink_statement" "insert_capacity_agent" {
  count = var.deploy_labs ? 1 : 0

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
    INSERT INTO `remediation_decisions`
    SELECT
      h.`window_end`,
      h.`service_name`,
      'capacity_agent',
      CASE WHEN h.`traffic_status` = 'HIGH' THEN 'TRAFFIC_HIGH' ELSE 'TRAFFIC_LOW' END,
      CASE
        WHEN a.`agent_status` = 'SUCCESS'
          AND UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1)))
              IN ('INVESTIGATE_UPSTREAM', 'OBSERVE')
          THEN UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1)))
        ELSE 'INVALID'
      END,
      NULLIF(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Target:\*{0,2}\s*([^\n]+)', 1)), 'N/A'),
      TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Reason:\*{0,2}\s*([\s\S]+?)$', 1)),
      a.`agent_response`,
      CAST(NULL AS STRING)
    FROM (
      SELECT *
      FROM `service_health` /*+ OPTIONS('scan.startup.mode'='latest-offset') */
      WHERE `traffic_status` IN ('HIGH', 'LOW')
    ) h,
    LATERAL TABLE(AI_RUN_AGENT(
      'capacity_agent',
      CONCAT(
        'SERVICE ', h.`service_name`, ' (', h.`tier`, ')\n',
        'Trigger: ', CASE WHEN h.`traffic_status` = 'HIGH' THEN 'TRAFFIC_HIGH' ELSE 'TRAFFIC_LOW' END, '\n',
        'Traffic Count: ', CAST(h.`traffic_count` AS STRING),
        '  Expected Traffic Count: ', CAST(ROUND(h.`expected_traffic_count`, 0) AS STRING),
        '  Expected Band: ', CAST(ROUND(h.`traffic_lower_bound`, 0) AS STRING),
        ' to ', CAST(ROUND(h.`traffic_upper_bound`, 0) AS STRING)
      )
    )) AS a(`agent_status`, `agent_response`);
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.insert_release_agent]
}

# Lab 5.1 (sql/5.1_p1_tickets.sql): the P1 tickets, copied to p1_tickets.
resource "confluent_flink_statement" "insert_p1_tickets" {
  count = var.deploy_labs ? 1 : 0

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
    INSERT INTO `p1_tickets`
    SELECT `event_time`, `ticket_id`, `source`, `service_name`, `priority`, `subject`, `description`
    FROM `support_tickets`
    WHERE `priority` = 'P1';
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.insert_capacity_agent]
}

# Lab 5.2 (sql/5.2_ticket_classifier.sql): the ticket classifier.
resource "confluent_flink_statement" "create_ticket_classifier" {
  count = var.deploy_labs ? 1 : 0

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
    CREATE AGENT `ticket_classifier`
    USING MODEL `llm_textgen_model`
    USING PROMPT 'OUTPUT FORMAT — respond with exactly these 2 labeled lines, in this order. Plain text only: no markdown, no bold, no bullet points, no blank lines, nothing before or after.

    Severity: [HIGH | LOW]
    Reason: [one or two sentences]

    Correct HIGH example:
    TICKET INC0041861 from SERVICENOW
    Priority: P1
    Subject: Checkout crawling since the campaign email went out
    Description: Since the campaign email went out at 18:35, checkout takes over a minute to load for all customers, and most of them give up before the order goes through. We have 80 complaints about the slowness already.
    Answer:
    Severity: HIGH
    Reason: All customers have waited over a minute for checkout since 18:35 and most give up before the order goes through, so many customers cannot buy right now.

    Correct LOW example:
    TICKET ZD-48203 from ZENDESK
    Priority: P1
    Subject: Saved cards not loading for me
    Description: My saved cards do not show up at checkout, so I have to type my card number in every time. Is something wrong with my account?
    Answer:
    Severity: LOW
    Reason: One customer does not see their saved cards and can still pay by typing the card number, so checkout works.

    Correct LOW example:
    TICKET ZD-48209 from ZENDESK
    Priority: P1
    Subject: URGENT!!! DISCOUNT CODE NOT WORKING
    Description: My code SPRING10 does not apply on order RS-484018. I am losing money on every minute of this, fix it NOW.
    Answer:
    Severity: LOW
    Reason: One customer cannot apply a discount code to one order, and checkout works: they can still buy. The capitals and the urgent wording do not make it HIGH.

    ---

    You are the service desk triage engineer for River Shop, an online store, and you look after its checkout service. Each input below is one support ticket about checkout, from Zendesk or ServiceNow: its id and source, its priority, its subject and its description. Every ticket below was marked P1 by whoever filed it, and reporters over-prioritise: a customer who cannot use a discount code files a P1 as readily as a shop where nobody can pay. Decide how severe each ticket really is.

    DECISION FRAMEWORK — apply the rules in this order and stop at the first one that matches:
    1. HIGH — the ticket reports that many customers cannot complete checkout or pay, right now. It speaks for many customers (everyone, every order, every payment, a queue of callers), not only for the person who filed it.
    2. LOW — everything else: one customer, a question, a request, something cosmetic. A problem only the reporter has is LOW, even when they cannot buy. Capitals, exclamation marks, the word "urgent" or the P1 itself never make a ticket HIGH.

    STRICT VALUES — the Severity line must be exactly HIGH or LOW, with no extra words. Use only what the ticket says; never invent customers, numbers or causes.'
    WITH (
      'handle_exception' = 'continue',
      'max_consecutive_failures' = '3',
      'request_timeout' = '60'
    );
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.insert_p1_tickets]
}

# Lab 5.2 (sql/5.2_insert_ticket_classifier.sql): every P1 to the classifier, its verdict to ticket_triage.
resource "confluent_flink_statement" "insert_ticket_classifier" {
  count = var.deploy_labs ? 1 : 0

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
    INSERT INTO `ticket_triage`
    SELECT
      t.`event_time`, t.`ticket_id`, t.`source`, t.`service_name`, t.`priority`, t.`subject`, t.`description`,
      CASE
        WHEN a.`agent_status` = 'SUCCESS'
          AND UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Severity:\*{0,2}\s*([^\n]+)', 1)))
              IN ('HIGH', 'LOW')
          THEN UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Severity:\*{0,2}\s*([^\n]+)', 1)))
        ELSE 'INVALID'
      END,
      TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Reason:\*{0,2}\s*([\s\S]+?)$', 1)),
      a.`agent_response`
    FROM (
      SELECT *
      FROM `p1_tickets` /*+ OPTIONS('scan.startup.mode'='latest-offset') */
    ) t,
    LATERAL TABLE(AI_RUN_AGENT(
      'ticket_classifier',
      CONCAT(
        'TICKET ', t.`ticket_id`, ' from ', t.`source`, '\n',
        'Priority: ', t.`priority`, '\n',
        'Subject: ', COALESCE(t.`subject`, 'N/A'), '\n',
        'Description: ', COALESCE(t.`description`, 'N/A')
      )
    )) AS a(`agent_status`, `agent_response`);
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.create_ticket_classifier]
}

# Lab 5.3 (sql/5.3_ticket_responder.sql): the ticket responder.
resource "confluent_flink_statement" "create_ticket_responder" {
  count = var.deploy_labs ? 1 : 0

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
    CREATE AGENT `ticket_responder`
    USING MODEL `llm_textgen_model`
    USING PROMPT 'OUTPUT FORMAT — respond with exactly these 2 labeled lines, in this order. Plain text only: no markdown, no bold, no bullet points, no blank lines, nothing before or after.

    Action: [ROLLBACK | SCALE_OUT | INVESTIGATE_UPSTREAM]
    Reason: [one or two sentences]

    There is no Target line: there is never a version or a replica count to give.

    Correct ROLLBACK example:
    TICKET ZD-48211 from ZENDESK
    Priority: P1
    Subject: Blank checkout page after the release
    Description: Since the release at 09:15, the checkout page goes blank after the address step for everyone we have asked. 45 customers have called in the last half hour and none of them could place an order.
    Classifier said: Since 09:15 the checkout page goes blank for everyone, and none of the 45 customers who called could place an order, so many customers cannot buy right now.
    Answer:
    Action: ROLLBACK
    Reason: Checkout has gone blank for everyone since the release at 09:15, so the release is the cause. Rolling it back is the fastest fix.

    Correct SCALE_OUT example:
    TICKET INC0041864 from SERVICENOW
    Priority: P1
    Subject: Checkout times out during the flash sale
    Description: Since the sale started at 14:10, checkout spins for a minute and then times out, for every customer we have talked to.
    Classifier said: Checkout has timed out for every customer since 14:10, so many customers cannot buy right now.
    Answer:
    Action: SCALE_OUT
    Reason: Checkout has been timing out for every customer since the flash sale started at 14:10, so it cannot keep up with the load. More capacity is the fastest fix.

    Correct INVESTIGATE_UPSTREAM example:
    TICKET INC0041867 from SERVICENOW
    Priority: P1
    Subject: Payment provider rejecting every payment
    Description: Since 11:30, every payment attempt comes back from the payment provider with error E2210, whatever the card. Customers reach the last step and cannot pay.
    Classifier said: Every payment attempt has failed since 11:30 and customers cannot pay at the last step, so many customers cannot pay right now.
    Answer:
    Action: INVESTIGATE_UPSTREAM
    Reason: Every payment since 11:30 comes back from the payment provider with error E2210, whatever the card, so the fault is at the provider and not in checkout. The provider has to be investigated first.

    ---

    You are the incident commander for River Shop, an online store, and you own its checkout service. Each input below is one support ticket about checkout, from Zendesk or ServiceNow: its id and source, its priority, its subject, its description and what the classifier said. Each ticket below has already been classified high severity by the classifier, whose reason comes with it: many customers cannot complete checkout or pay, right now. Recommend the one action that fixes it fastest.

    DECISION FRAMEWORK — apply the rules in this order and stop at the first one that matches:
    1. ROLLBACK — the failure is tied to a release, an update, a deploy or a new version. Undoing the change is the fastest fix.
    2. SCALE_OUT — checkout is slow or timing out under heavy load: a sale, a campaign, a peak or a rush. More capacity is the fastest fix.
    3. INVESTIGATE_UPSTREAM — payments fail at a payment provider or a payment gateway. The fault is outside checkout, so the fix starts with the provider.

    STRICT VALUES — the Action line must be exactly ROLLBACK, SCALE_OUT or INVESTIGATE_UPSTREAM, with no extra words. Use only what the ticket and the classifier say; never invent a cause, a version or a number.'
    WITH (
      'handle_exception' = 'continue',
      'max_consecutive_failures' = '3',
      'request_timeout' = '60'
    );
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.insert_ticket_classifier]
}

# Lab 5.3 (sql/5.3_insert_ticket_responder.sql): every HIGH verdict to the responder, its answer to remediation_decisions.
resource "confluent_flink_statement" "insert_ticket_responder" {
  count = var.deploy_labs ? 1 : 0

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
    INSERT INTO `remediation_decisions`
    SELECT
      tr.`event_time`,
      tr.`service_name`,
      'ticket_responder',
      'HIGH_SEVERITY_TICKET',
      CASE
        WHEN a.`agent_status` = 'SUCCESS'
          AND UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1)))
              IN ('ROLLBACK', 'SCALE_OUT', 'INVESTIGATE_UPSTREAM')
          THEN UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1)))
        ELSE 'INVALID'
      END,
      CAST(NULL AS STRING),
      TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Reason:\*{0,2}\s*([\s\S]+?)$', 1)),
      a.`agent_response`,
      tr.`ticket_id`
    FROM (
      SELECT *
      FROM `ticket_triage` /*+ OPTIONS('scan.startup.mode'='latest-offset') */
      WHERE `severity` = 'HIGH'
    ) tr,
    LATERAL TABLE(AI_RUN_AGENT(
      'ticket_responder',
      CONCAT(
        'TICKET ', tr.`ticket_id`, ' from ', tr.`source`, '\n',
        'Priority: ', tr.`priority`, '\n',
        'Subject: ', COALESCE(tr.`subject`, 'N/A'), '\n',
        'Description: ', COALESCE(tr.`description`, 'N/A'), '\n',
        'Classifier said: ', COALESCE(tr.`reason`, 'N/A')
      )
    )) AS a(`agent_status`, `agent_response`);
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.create_ticket_responder]
}

# Lab 6.1 (sql/6.1_critical_cves.sql): the critical findings, copied to critical_cves.
resource "confluent_flink_statement" "insert_critical_cves" {
  count = var.deploy_labs ? 1 : 0

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
    INSERT INTO `critical_cves`
    SELECT `event_time`, `finding_id`, `repository`, `cve_id`, `package`, `installed_version`,
      `fixed_version`, `cvss_score`, `finding`
    FROM `cve_scans`
    WHERE `cvss_score` >= 9.0;
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.insert_ticket_responder]
}

# Lab 6.2 (sql/6.2_patch_agent_tool.sql), its first statement: the connection to the presenter's
# patch-agent. Its endpoint is patch_agent_url, the tunnel's URL, in place of the file's
# placeholder: the one place a copy differs from its file. The password is in the SQL text, as
# in the file, and so in the state and the Console: Flink requires a username and a password
# on an A2A connection, and patch-agent checks neither.
# A new patch_agent_url changes the statement, so Terraform replaces it, and CREATE CONNECTION
# fails while the connection exists (ALTER CONNECTION changes only credentials): start the demo
# over with the new URL (README, "A demo environment").
resource "confluent_flink_statement" "create_patch_agent_connection" {
  count = var.deploy_labs ? 1 : 0

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
    CREATE CONNECTION `patch_agent_connection`
    WITH (
      'type' = 'a2a',
      'endpoint' = '${var.patch_agent_url}',
      'username' = 'workshop',
      'password' = 'workshop'
    );
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  # A precondition, not a validation on patch_agent_url: a validation can read deploy_labs
  # only from Terraform 1.9.
  lifecycle {
    precondition {
      condition     = var.patch_agent_url != ""
      error_message = "deploy_labs needs patch_agent_url: the URL of the tunnel in front of the presenter's `uv run patch-agent`, which lab 6.2's connection points at (README, \"A demo environment\")."
    }
  }

  depends_on = [confluent_flink_statement.insert_critical_cves]
}

# Lab 6.2 (sql/6.2_patch_agent_tool.sql), its second statement: the patch agent as a tool.
resource "confluent_flink_statement" "create_patch_agent_tool" {
  count = var.deploy_labs ? 1 : 0

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
    CREATE TOOL `patch_agent_tool`
    USING CONNECTION `patch_agent_connection`
    WITH (
      'type' = 'a2a',
      'agent_card_path' = '/.well-known/agent-card.json',
      'request_timeout' = '30',
      'max_retries' = '0'
    );
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.create_patch_agent_connection]
}

# Lab 6.3 (sql/6.3_security_agent.sql): the security agent, which calls the tool on PATCH.
resource "confluent_flink_statement" "create_security_agent" {
  count = var.deploy_labs ? 1 : 0

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
    CREATE AGENT `security_agent`
    USING MODEL `llm_textgen_model`
    USING PROMPT 'OUTPUT FORMAT — respond with exactly these 3 labeled lines, in this order. Plain text only: no markdown, no bold, no bullet points, no blank lines, nothing before or after.

    Action: [PATCH | DISMISS]
    Reason: [one or two sentences]
    Patch: [the answer from patch_agent_tool, copied exactly | N/A]

    Correct PATCH example:
    FINDING SCAN-000142 in payments
    CVE: CVE-2026-46471 (CVSS 9.8)
    Package: jsonwebtoken 8.5.1, fixed in 9.0.0
    Scanner said: The deployed payments service loads jsonwebtoken 8.5.1 at runtime, and the scanner traced a call path from a public endpoint to jwt.verify, which accepts tokens signed with the none algorithm.
    You call patch_agent_tool with: PATCH payments CVE-2026-46471 jsonwebtoken 8.5.1 -> 9.0.0
    patch_agent_tool replies: Patched CVE-2026-46471 in payments: bumped jsonwebtoken 8.5.1 → 9.0.0 in commit e73f47b, started 2026-11-20T09:14:02.480Z, finished 2026-11-20T09:14:08.915Z
    Answer:
    Action: PATCH
    Reason: jsonwebtoken ships in the deployed payments service, and the scanner traced a call path from a public endpoint to the vulnerable jwt.verify, so the flaw can be reached in production.
    Patch: Patched CVE-2026-46471 in payments: bumped jsonwebtoken 8.5.1 → 9.0.0 in commit e73f47b, started 2026-11-20T09:14:02.480Z, finished 2026-11-20T09:14:08.915Z

    Correct DISMISS example:
    FINDING SCAN-000157 in checkout
    CVE: CVE-2026-42817 (CVSS 9.1)
    Package: jackson-databind 2.9.10.1, fixed in 2.9.10.8
    Scanner said: jackson-databind 2.9.10.1 comes into checkout only as a dev dependency of its unit tests, and the production build leaves it out.
    Answer:
    Action: DISMISS
    Reason: jackson-databind is only a dev dependency of the checkout unit tests, and the production build leaves it out, so the vulnerable code never runs in production.
    Patch: N/A

    Correct DISMISS example:
    FINDING SCAN-000163 in storefront-legacy
    CVE: CVE-2026-43390 (CVSS 9.4)
    Package: lodash 4.17.15, fixed in 4.17.21
    Scanner said: storefront-legacy is archived and read-only, and nothing has been deployed from it since 2024, but its lock file still pins lodash 4.17.15.
    Answer:
    Action: DISMISS
    Reason: storefront-legacy is archived and nothing has been deployed from it since 2024, so the lodash it pins runs nowhere.
    Patch: N/A

    Correct DISMISS example:
    FINDING SCAN-000171 in web-gateway
    CVE: CVE-2026-46471 (CVSS 9.6)
    Package: jsonwebtoken 8.5.1, fixed in 9.0.0
    Scanner said: web-gateway has jsonwebtoken 8.5.1 as a runtime dependency in production, but the scanner traced no code path that reaches jwt.verify, the function that accepts tokens signed with the none algorithm.
    Answer:
    Action: DISMISS
    Reason: jsonwebtoken ships with web-gateway in production, but the scanner traced no code path that reaches the vulnerable jwt.verify, so the flaw cannot be triggered. The critical score and the flaw alone do not decide it.
    Patch: N/A

    ---

    You are the application security engineer for River Shop, an online store. A dependency scanner reports known vulnerabilities (CVEs) in the code repositories of the shop. Each input below is one finding: its id and repository, the CVE and its CVSS score, the vulnerable package with its installed version and the version that fixes it, and what the scanner said about where the package sits. Decide whether the finding is a real risk, and when it is, have the patch agent fix it.

    DECISION FRAMEWORK — apply the rules in this order and stop at the first one that matches:
    1. PATCH — both of these hold: the vulnerable package ships in the runtime dependencies of a deployed service, and the scanner says the vulnerable code is used (it found calls to it, or a code path that reaches it).
    2. DISMISS — any of these holds: the package is only a development, test or build dependency; the repository is archived or not deployed; the scanner finds no call to the vulnerable code.
    The CVSS score is always critical here. It never decides on its own. Decide on what the scanner said.

    PATCH AGENT — patch_agent_tool is an external agent that patches the package and answers with its commit. Use it only for PATCH:
    - Call patch_agent_tool once, with exactly this line and nothing else, filled in from the finding: PATCH <repository> <cve_id> <package> <installed_version> -> <fixed_version>
    - Wait for its answer, then copy that answer exactly into the Patch line: every word, number and symbol, on one line.
    - If the call fails, write Patch: FAILED and a few words on why. Never write a commit the tool did not return.
    - For DISMISS, never call the tool, and write Patch: N/A.

    STRICT VALUES — the Action line must be exactly PATCH or DISMISS, with no extra words. Use only what the finding says; never invent a repository, a package, a version or a commit.'
    USING TOOLS `patch_agent_tool`
    WITH (
      'handle_exception' = 'continue',
      'max_consecutive_failures' = '3',
      'max_iterations' = '3',
      'request_timeout' = '60'
    );
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.create_patch_agent_tool]
}

# Lab 6.4 (sql/6.4_insert_security_agent.sql): every critical finding to the security agent, its
# decision to patch_decisions.
resource "confluent_flink_statement" "insert_security_agent" {
  count = var.deploy_labs ? 1 : 0

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
    INSERT INTO `patch_decisions`
    SELECT
      f.`event_time`, f.`finding_id`, f.`repository`, f.`cve_id`, f.`cvss_score`,
      CASE
        WHEN a.`agent_status` = 'SUCCESS'
          AND UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1)))
              IN ('PATCH', 'DISMISS')
          THEN UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1)))
        ELSE 'INVALID'
      END,
      TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Reason:\*{0,2}\s*([^\n]+)', 1)),
      CASE
        WHEN a.`agent_status` = 'SUCCESS'
          AND UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1))) = 'PATCH'
          AND REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Patch:\*{0,2}[^\n]*?in commit ([0-9a-f]{7})\b', 1) IS NOT NULL
          THEN 'PATCHED'
        WHEN a.`agent_status` = 'SUCCESS'
          AND UPPER(TRIM(REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Action:\*{0,2}\s*([^\n]+)', 1))) = 'PATCH'
          THEN 'FAILED'
        ELSE 'N/A'
      END,
      REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Patch:\*{0,2}[^\n]*?in commit ([0-9a-f]{7})\b', 1),
      TO_TIMESTAMP(
        REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Patch:\*{0,2}[^\n]*?started (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)', 1),
        'yyyy-MM-dd''T''HH:mm:ss.SSS''Z'''
      ),
      TO_TIMESTAMP(
        REGEXP_EXTRACT(a.`agent_response`, '\*{0,2}Patch:\*{0,2}[^\n]*?finished (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)', 1),
        'yyyy-MM-dd''T''HH:mm:ss.SSS''Z'''
      ),
      a.`agent_response`
    FROM (
      SELECT *
      FROM `critical_cves` /*+ OPTIONS('scan.startup.mode'='latest-offset') */
    ) f,
    LATERAL TABLE(AI_RUN_AGENT(
      'security_agent',
      CONCAT(
        'FINDING ', COALESCE(f.`finding_id`, 'N/A'), ' in ', COALESCE(f.`repository`, 'N/A'), '\n',
        'CVE: ', COALESCE(f.`cve_id`, 'N/A'), ' (CVSS ', CAST(f.`cvss_score` AS STRING), ')\n',
        'Package: ', COALESCE(f.`package`, 'N/A'), ' ', COALESCE(f.`installed_version`, 'N/A'),
        ', fixed in ', COALESCE(f.`fixed_version`, 'N/A'), '\n',
        'Scanner said: ', COALESCE(f.`finding`, 'N/A')
      )
    )) AS a(`agent_status`, `agent_response`);
  EOT

  properties = {
    "sql.current-catalog"  = confluent_environment.main.display_name
    "sql.current-database" = confluent_kafka_cluster.main.display_name
  }

  depends_on = [confluent_flink_statement.create_security_agent]
}
