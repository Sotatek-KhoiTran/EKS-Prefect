resource "prefect_block" "github_credentials" {
  count = var.create_prefect_github_credentials_block ? 1 : 0

  name      = var.prefect_github_credentials_block_name
  type_slug = "github-credentials"
  data = jsonencode({
    token = coalesce(var.github_token, "")
  })

  lifecycle {
    precondition {
      condition     = var.github_token != null && var.github_token != ""
      error_message = "github_token must be set when create_prefect_github_credentials_block is true."
    }
  }
}
