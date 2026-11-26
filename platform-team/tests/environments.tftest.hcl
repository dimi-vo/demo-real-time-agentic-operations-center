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

# deploy_labs reaches every environment of the apply (D-069): the platform team keeps a demo
# apply to one email, and the workshop's apply leaves it off.
run "deploy_labs_reaches_every_environment" {
  variables {
    deploy_labs     = true
    patch_agent_url = "https://patch-agent.example.com"
  }

  assert {
    condition     = alltrue([for m in values(module.developer) : length(m.flink_statements) == 32])
    error_message = "with deploy_labs, every developer's stack must run the labs' 17 statements"
  }
}

# patch_agent_url is lab 6.2's endpoint (D-070): a URL with its scheme, which Flink adds the
# agent card's path to, so without a trailing slash.
run "patch_agent_url_needs_its_scheme" {
  command = plan

  variables {
    deploy_labs     = true
    patch_agent_url = "patch-agent.example.com"
  }

  expect_failures = [var.patch_agent_url]
}

run "patch_agent_url_has_no_trailing_slash" {
  command = plan

  variables {
    deploy_labs     = true
    patch_agent_url = "https://patch-agent.example.com/"
  }

  expect_failures = [var.patch_agent_url]
}

run "the_workshop_apply_runs_no_lab_statement" {
  assert {
    condition     = alltrue([for m in values(module.developer) : length(m.flink_statements) == 15])
    error_message = "by default, every developer's stack must run only the fourteen tables and the model"
  }
}

# Every environment serves patch_decisions over the Real-Time Context Engine (D-070), in its
# own environment and cluster, with deploy_labs or without.
run "every_environment_serves_patch_decisions_over_rtce" {
  assert {
    condition = alltrue([
      for m in values(module.developer) : (
        length(m.rtce_topics) == 1 &&
        m.rtce_topics.patch_decisions.environment == m.environment_id &&
        m.rtce_topics.patch_decisions.cluster == m.cluster_id
      )
    ])
    error_message = "every developer's environment must have one RTCE topic, patch_decisions, on its own cluster"
  }
}
