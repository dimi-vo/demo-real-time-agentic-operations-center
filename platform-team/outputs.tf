locals {
  # Header row first, then one row per developer, sorted by email.
  handout_table = concat(
    [{ email = "EMAIL", name = "ENVIRONMENT", id = "ENVIRONMENT ID", url = "CONSOLE URL" }],
    [
      for email in sort(tolist(local.emails)) : {
        email = email
        name  = module.developer[email].environment_name
        id    = module.developer[email].environment_id
        url   = "https://confluent.cloud/environments/${module.developer[email].environment_id}"
      }
    ],
  )

  handout_widths = {
    for col in ["email", "name", "id"] :
    col => max([for row in local.handout_table : length(row[col])]...)
  }
}

# One padded line per row, so the columns line up as a table; print it with
# `terraform output -raw handout`. Send each developer their row; the deploy
# finds the environment by itself, so the ID is for the platform team's records.
output "handout" {
  description = "Email, environment name, environment ID and Console URL, one line per developer"
  value = join("", [
    for row in local.handout_table :
    format(
      "%-${local.handout_widths.email}s  %-${local.handout_widths.name}s  %-${local.handout_widths.id}s  %s\n",
      row.email, row.name, row.id, row.url,
    )
  ])
}

# README step's check: a heading, then one numbered Console link per developer to
# their service_metrics topic's schema. The topics are empty until the developer
# runs `uv run ops-sim`, but the registered schema proves the CREATE TABLE ran.
# Print it with `terraform output -raw validation`.
output "validation" {
  description = "A numbered Console link per developer to their service_metrics topic schema"
  value = join("", concat(
    ["Check in the Console that each environment has the ten tables and the model (the registered schema proves the CREATE TABLE ran; the topics stay empty until the developer runs ops-sim):\n"],
    [
      for i, email in sort(keys(module.developer)) : format(
        "\n%d. %s: %s (%s)\n   https://confluent.cloud/environments/%s/clusters/%s/topics/service_metrics/message-viewer?tab=schema\n",
        i + 1,
        email,
        module.developer[email].environment_name,
        module.developer[email].environment_id,
        module.developer[email].environment_id,
        module.developer[email].cluster_id,
      )
    ],
  ))
}
