variable "user_id" {
  description = "The developer's user ID (u-...), bound directly as EnvironmentAdmin"
  type        = string
}

variable "environment_name" {
  description = "Environment display name (<sanitized local part>-<4 hex>_env); also the Flink catalog"
  type        = string
}

variable "region" {
  description = "AWS region for the Kafka cluster and Flink compute pool"
  type        = string
}

variable "flink_max_cfu" {
  description = "Autoscaling ceiling for the Flink compute pool"
  type        = number
}

variable "csp_hosted_model" {
  description = "CSP Hosted model behind llm_textgen_model"
  type        = string
}

variable "deploy_labs" {
  description = "Also run the labs' 17 statements (statements.tf, D-069)"
  type        = bool
}

variable "patch_agent_url" {
  description = "With deploy_labs, lab 6.2's connection endpoint: the tunnel's URL in front of the presenter's patch-agent (D-070)"
  type        = string
}

variable "principal_id" {
  description = "Owner of the platform team's Global key; every statement runs as it"
  type        = string
}

variable "confluent_cloud_api_key" {
  description = "The platform team's Global API key; signs every Flink statement"
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Secret for confluent_cloud_api_key"
  type        = string
  sensitive   = true
}
