# The workshop's two source tables and the model, created as the platform team:
# each statement is signed with the Global key and runs as that key's owner
# (principal_id).
#
# The DDL is all IF NOT EXISTS, so re-running apply is a no-op for anything already
# there (e.g. after the provider's double-POST 409 flake, or after terminal
# statements are auto-deleted at 30 days).
#
# The CREATE TABLE bodies are copied from sql/source_service_requests.sql and
# sql/source_service_deployments.sql, the single source of truth for both schemas,
# because this folder must work when copied out of the repo. tests/test_platform_team_sync.py
# catches drift between these heredocs and the sql/ files.

resource "confluent_flink_statement" "create_service_requests_table" {
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
    CREATE TABLE IF NOT EXISTS `service_requests` (
      `event_time` TIMESTAMP(3) COMMENT 'Wall-clock time of the request, within the current 1s tick; also the Kafka record timestamp',
      `service_name` STRING COMMENT 'Which River Shop service served the request (foreign key to service_deployments)',
      `instance_id` STRING COMMENT 'Serving instance: {service}-{version_hash}-{replica_suffix}, like a Kubernetes pod',
      `http_method` STRING COMMENT 'HTTP method of the route (GET, POST, DELETE)',
      `route` STRING COMMENT 'Templated route, e.g. GET /api/v1/products/{id}, so cardinality stays low',
      `status_code` INT COMMENT 'HTTP status code (2xx success, 4xx client error, 5xx server error)',
      `latency_ms` DOUBLE COMMENT 'Request latency in milliseconds',
      `response_bytes` INT COMMENT 'Response body size in bytes (~200 for error responses)',
      `trace_id` STRING COMMENT '32 random hex characters identifying the request trace',
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

  depends_on = [confluent_flink_statement.create_service_requests_table]
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
      `started_at` TIMESTAMP(3) COMMENT 'Wall-clock time degradation actually began (not the deploy or trigger moment)',
      `ended_at` TIMESTAMP(3) COMMENT 'Wall-clock time degradation ended; NULL on the start record, set on the end record',
      `expected_trigger` STRING COMMENT 'What service_health should flag: LATENCY_HIGH, TRAFFIC_HIGH or TRAFFIC_LOW',
      `expected_action` STRING COMMENT 'What the agent should pick: ROLLBACK, SCALE_OUT or INVESTIGATE_UPSTREAM',
      `good_version` STRING COMMENT 'For BAD_DEPLOY, the version a ROLLBACK should target; NULL otherwise',
      WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
    )
    DISTRIBUTED BY HASH(`service_name`) INTO 1 BUCKETS
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

  depends_on = [confluent_flink_statement.create_service_incidents_table]
}
