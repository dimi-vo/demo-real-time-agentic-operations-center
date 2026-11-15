# Apply-time checks of the root: real random suffixes and the handout table.
# `terraform test` (Terraform >= 1.7 for mock_provider). Random runs locally, so the
# suffix is a genuine random_id; confluent, http and time are mocked.

mock_provider "confluent" {
  mock_data "confluent_user" {
    defaults = {
      id = "u-mock01"
    }
  }

  mock_resource "confluent_environment" {
    defaults = {
      id            = "env-mock01"
      resource_name = "crn://confluent.cloud/organization=mock-org/environment=env-mock01"
    }
  }

  mock_data "confluent_flink_region" {
    defaults = {
      rest_endpoint = "https://flink.us-east-1.aws.confluent.cloud"
    }
  }

  mock_resource "confluent_kafka_cluster" {
    defaults = {
      id = "lkc-mock01"
    }
  }
}

mock_provider "http" {
  mock_data "http" {
    defaults = {
      status_code   = 200
      response_body = jsonencode({ spec = { owner = { id = "u-abc123", kind = "User" } } })
    }
  }
}

mock_provider "time" {}

variables {
  confluent_cloud_api_key    = "mock-key"
  confluent_cloud_api_secret = "mock-secret"
  developer_emails           = ["jane.doe@example.com", "john.smith@example.com"]
}

run "env_names_have_suffix_and_env_suffix" {
  assert {
    condition     = can(regex("^jane-doe-[0-9a-f]{4}_env$", module.developer["jane.doe@example.com"].environment_name))
    error_message = "environment name should be <sanitized>-<4 hex>_env"
  }
}

run "handout_lists_every_developer" {
  assert {
    condition = (
      can(regex("jane.doe@example.com", output.handout)) &&
      can(regex("john.smith@example.com", output.handout)) &&
      can(regex("EMAIL", output.handout))
    )
    error_message = "the handout output must list every developer with a header row"
  }

  assert {
    condition     = can(regex("service_metrics/message-viewer", output.validation))
    error_message = "the validation output must link to each service_metrics topic"
  }
}
