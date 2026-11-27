output "environment_id" {
  value = confluent_environment.main.id
}

output "environment_name" {
  value = confluent_environment.main.display_name
}

output "cluster_id" {
  value = confluent_kafka_cluster.main.id
}

locals {
  # The labs' statements that exist: all seventeen with deploy_labs, none without (D-069).
  lab_statements = {
    for key, list in {
      insert_service_error_rate     = confluent_flink_statement.insert_service_error_rate
      insert_service_health         = confluent_flink_statement.insert_service_health
      create_release_agent          = confluent_flink_statement.create_release_agent
      create_capacity_agent         = confluent_flink_statement.create_capacity_agent
      insert_release_agent_errors   = confluent_flink_statement.insert_release_agent_errors
      insert_release_agent          = confluent_flink_statement.insert_release_agent
      insert_capacity_agent         = confluent_flink_statement.insert_capacity_agent
      insert_p1_tickets             = confluent_flink_statement.insert_p1_tickets
      create_ticket_classifier      = confluent_flink_statement.create_ticket_classifier
      insert_ticket_classifier      = confluent_flink_statement.insert_ticket_classifier
      create_ticket_responder       = confluent_flink_statement.create_ticket_responder
      insert_ticket_responder       = confluent_flink_statement.insert_ticket_responder
      insert_critical_cves          = confluent_flink_statement.insert_critical_cves
      create_patch_agent_connection = confluent_flink_statement.create_patch_agent_connection
      create_patch_agent_tool       = confluent_flink_statement.create_patch_agent_tool
      create_security_agent         = confluent_flink_statement.create_security_agent
      insert_security_agent         = confluent_flink_statement.insert_security_agent
    } : key => list[0] if length(list) > 0
  }
}

# A test can't reach resources inside a module, so the root's tests read the
# statements' catalog and principal from here. The name is the provider's
# auto-generated one, as listed in the Console. The labs' statements are listed only
# when deploy_labs is on (D-069).
output "flink_statements" {
  value = {
    for key, s in merge({
      service_metrics       = confluent_flink_statement.create_service_metrics_table
      service_deployments   = confluent_flink_statement.create_service_deployments_table
      service_incidents     = confluent_flink_statement.create_service_incidents_table
      support_tickets       = confluent_flink_statement.create_support_tickets_table
      ticket_answer_key     = confluent_flink_statement.create_ticket_answer_key_table
      cve_scans             = confluent_flink_statement.create_cve_scans_table
      cve_answer_key        = confluent_flink_statement.create_cve_answer_key_table
      service_error_rate    = confluent_flink_statement.create_service_error_rate_table
      service_health        = confluent_flink_statement.create_service_health_table
      remediation_decisions = confluent_flink_statement.create_remediation_decisions_table
      p1_tickets            = confluent_flink_statement.create_p1_tickets_table
      ticket_triage         = confluent_flink_statement.create_ticket_triage_table
      critical_cves         = confluent_flink_statement.create_critical_cves_table
      patch_decisions       = confluent_flink_statement.create_patch_decisions_table
      llm_textgen_model     = confluent_flink_statement.llm_textgen_model
    }, local.lab_statements) :
    key => {
      name      = s.statement_name
      catalog   = s.properties["sql.current-catalog"]
      database  = s.properties["sql.current-database"]
      principal = s.principal[0].id
    }
  }
}

# The topics the Real-Time Context Engine serves (D-070), by topic, with the environment and
# cluster each is in, for the root's tests.
output "rtce_topics" {
  value = {
    for t in [confluent_rtce_topic.patch_decisions] :
    t.topic_name => {
      environment = t.environment[0].id
      cluster     = t.kafka_cluster[0].id
    }
  }
}
