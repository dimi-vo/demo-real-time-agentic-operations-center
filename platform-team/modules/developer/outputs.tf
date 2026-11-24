output "environment_id" {
  value = confluent_environment.main.id
}

output "environment_name" {
  value = confluent_environment.main.display_name
}

output "cluster_id" {
  value = confluent_kafka_cluster.main.id
}

# A test can't reach resources inside a module, so the root's tests read the
# statements' catalog and principal from here. The name is the provider's
# auto-generated one, as listed in the Console.
output "flink_statements" {
  value = {
    for key, s in {
      service_metrics       = confluent_flink_statement.create_service_metrics_table
      service_deployments   = confluent_flink_statement.create_service_deployments_table
      service_incidents     = confluent_flink_statement.create_service_incidents_table
      support_tickets       = confluent_flink_statement.create_support_tickets_table
      ticket_answer_key     = confluent_flink_statement.create_ticket_answer_key_table
      service_error_rate    = confluent_flink_statement.create_service_error_rate_table
      service_health        = confluent_flink_statement.create_service_health_table
      remediation_decisions = confluent_flink_statement.create_remediation_decisions_table
      p1_tickets            = confluent_flink_statement.create_p1_tickets_table
      ticket_triage         = confluent_flink_statement.create_ticket_triage_table
      llm_textgen_model     = confluent_flink_statement.llm_textgen_model
    } :
    key => {
      name      = s.statement_name
      catalog   = s.properties["sql.current-catalog"]
      database  = s.properties["sql.current-database"]
      principal = s.principal[0].id
    }
  }
}
