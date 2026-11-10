# Environment naming and the developer_emails validations.
# `terraform test` (Terraform >= 1.7 for mock_provider). confluent, http and time
# are mocked, so nothing here talks to Confluent Cloud or sleeps.

mock_provider "confluent" {
  mock_resource "confluent_environment" {
    defaults = {
      resource_name = "crn://confluent.cloud/organization=mock-org/environment=env-mock01"
    }
  }

  mock_data "confluent_flink_region" {
    defaults = {
      rest_endpoint = "https://flink.us-east-1.aws.confluent.cloud"
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
}

# Keep identical to SANITIZER_EXAMPLES in tests/test_preprovisioned_lookup.py (the
# Python twin, sanitize_local_part in scripts/preprovisioned/lookup.py).
run "sanitizer_examples" {
  command = plan

  variables {
    developer_emails = [
      "John.Doe@example.com",
      "jane_smith@example.com",
      "weird++name@example.com",
      "a.very.long.local.part.that.exceeds.forty.characters.indeed@example.com",
    ]
  }

  assert {
    condition     = local.sanitized["John.Doe@example.com"] == "John-Doe"
    error_message = "dots become hyphens, case preserved"
  }

  assert {
    condition     = local.sanitized["jane_smith@example.com"] == "jane_smith"
    error_message = "underscores are allowed"
  }

  assert {
    condition     = local.sanitized["weird++name@example.com"] == "weird-name"
    error_message = "runs of non-alphanumerics collapse to a single hyphen"
  }

  assert {
    condition     = length(local.sanitized["a.very.long.local.part.that.exceeds.forty.characters.indeed@example.com"]) <= 40
    error_message = "the stem is cut to 40 characters"
  }
}

run "rejects_duplicate_emails" {
  command = plan

  variables {
    developer_emails = ["dup@example.com", "DUP@example.com"]
  }

  expect_failures = [var.developer_emails]
}

run "rejects_emails_with_no_nameable_local_part" {
  command = plan

  variables {
    developer_emails = ["++@example.com"]
  }

  expect_failures = [var.developer_emails]
}
