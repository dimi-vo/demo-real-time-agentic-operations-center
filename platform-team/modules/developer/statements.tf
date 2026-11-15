# The workshop's tables and model, created as the platform team: the three source tables
# (service_metrics, service_deployments, service_incidents), the two output tables the
# labs fill with INSERT statements (service_health, remediation_decisions), and the model.
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
# file, and the model comes last.

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
      `error_count` INT COMMENT 'Failed requests in that second (display only)',
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
      WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND,
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
      `incident_type` STRING COMMENT 'BAD_DEPLOY, TRAFFIC_SURGE or UPSTREAM_DROP',
      `started_at` TIMESTAMP(3) COMMENT 'Wall-clock time degradation began (for BAD_DEPLOY, the same tick as the deploy)',
      `ended_at` TIMESTAMP(3) COMMENT 'Wall-clock time degradation ended; NULL on the start record, set on the end record',
      `expected_trigger` STRING COMMENT 'What service_health should flag: LATENCY_HIGH, TRAFFIC_HIGH or TRAFFIC_LOW',
      `expected_action` STRING COMMENT 'What the agent should pick: ROLLBACK, SCALE_OUT or INVESTIGATE_UPSTREAM',
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

  depends_on = [confluent_flink_statement.create_service_incidents_table]
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
      `window_end` TIMESTAMP(3) NOT NULL COMMENT 'End of the service_health window the decision was made on',
      `service_name` STRING NOT NULL,
      `agent_name` STRING NOT NULL COMMENT 'release_agent or capacity_agent',
      `trigger` STRING NOT NULL COMMENT 'LATENCY_HIGH, TRAFFIC_HIGH or TRAFFIC_LOW',
      `action` STRING NOT NULL COMMENT 'ROLLBACK, SCALE_OUT, INVESTIGATE_UPSTREAM, OBSERVE, or INVALID',
      `target` STRING COMMENT 'Version for ROLLBACK, replica count for SCALE_OUT, otherwise null',
      `reason` STRING COMMENT 'The agent explanation, one or two sentences',
      `raw_response` STRING COMMENT 'The full agent answer, kept for debugging when parsing returns null'
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

# CSP Hosted Flink Model Inference only: Confluent runs the model, so there is no
# connection and no cloud key. The name llm_textgen_model is what the agent SQL
# (section 4 of the walkthrough) references.
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

  depends_on = [confluent_flink_statement.create_remediation_decisions_table]
}
