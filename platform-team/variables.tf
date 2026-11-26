# =============================================================================
# Platform-team inputs. Copy terraform.tfvars.example to terraform.tfvars, or
# export TF_VAR_confluent_cloud_api_key / TF_VAR_confluent_cloud_api_secret.
# =============================================================================

variable "confluent_cloud_api_key" {
  description = "Global API key owned by an OrganizationAdmin. It is the provider credential and signs every Flink statement."
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Secret for confluent_cloud_api_key"
  type        = string
  sensitive   = true
}

variable "developer_emails" {
  description = "One email per person, instructors included. Each must already be a user in the org; this module never invites anyone."
  type        = list(string)

  validation {
    condition     = length(distinct([for e in var.developer_emails : lower(e)])) == length(var.developer_emails)
    error_message = "developer_emails lists someone twice (emails are compared case-insensitively): ${join(", ", distinct([for e in var.developer_emails : e if length([for o in var.developer_emails : o if lower(o) == lower(e)]) > 1]))}. Keep one entry per person."
  }

  # Both lines inline the sanitizing expression from local.sanitized in main.tf,
  # because validations can't read locals before Terraform 1.9. Keep all three
  # copies in step.
  validation {
    condition     = length([for e in var.developer_emails : e if trim(substr(replace(replace(split("@", e)[0], "/[^A-Za-z0-9_-]/", "-"), "/-+/", "-"), 0, 40), "-_") == ""]) == 0
    error_message = "These emails leave nothing to name an environment after once their local part is sanitized (it needs a letter or digit): ${join(", ", [for e in var.developer_emails : e if trim(substr(replace(replace(split("@", e)[0], "/[^A-Za-z0-9_-]/", "-"), "/-+/", "-"), 0, 40), "-_") == ""])}. Fix or remove them."
  }
}

variable "region" {
  description = "AWS region for every developer's Kafka cluster and Flink compute pool. One region for everyone. Must be an AWS region where the Real-Time Context Engine / Lightning Tables and the CSP Hosted model are available."
  type        = string
  default     = "us-east-1"
}

variable "flink_max_cfu" {
  description = "Autoscaling ceiling for each developer's Flink compute pool; CFUs are consumed on demand, not reserved. Sized for the eight long-running statements of sections 2 to 5 (the INSERTs of labs 2.2 and 2.5, the three agent legs of 4.3, and 5.1, 5.2 and 5.3) and ad-hoc queries. Section 6 adds two more, 6.1 and 6.4, whose use against 10 is not checked yet (D-070)."
  type        = number
  default     = 10
}

variable "csp_hosted_model" {
  description = "CSP Hosted model behind each developer's llm_textgen_model. Case-sensitive: the model service matches it exactly. Must be offered in var.region."
  type        = string
  default     = "anthropic.claude-sonnet-4-6"
}

variable "deploy_labs" {
  description = "Also run the labs' 17 statements (the INSERTs and agents of labs 2.2 to 6.4, and lab 6.2's connection and tool) in every environment of this apply, for a demo environment (D-069). Never in the workshop's apply: attendees write these statements themselves. Turn it on only while the topics are empty, in the apply that creates the environment or right after `uv run reset` (README, \"A demo environment\"). Needs patch_agent_url."
  type        = bool
  default     = false
}

variable "patch_agent_url" {
  description = "With deploy_labs, the URL of the tunnel in front of the presenter's `uv run patch-agent`: lab 6.2's connection endpoint, in place of its placeholder (D-070). A deploy_labs plan fails without it. Unused without deploy_labs."
  type        = string
  default     = ""

  # Flink adds the agent card's path (/.well-known/agent-card.json) to the endpoint.
  validation {
    condition     = var.patch_agent_url == "" || (can(regex("^https?://[^\\s']+$", var.patch_agent_url)) && !endswith(var.patch_agent_url, "/"))
    error_message = "patch_agent_url must be a URL with its scheme and no trailing slash, as the tunnel prints it (e.g. https://example.trycloudflare.com)."
  }
}
