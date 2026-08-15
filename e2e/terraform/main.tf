# Unmodified databricks/databricks against this checkout.
# Host and PAT come from DATABRICKS_HOST / DATABRICKS_TOKEN — never
# token = "dev" (MiniLake's trap; the seeder will not mint that value).
terraform {
  required_version = ">= 1.8, < 2.0"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.126.0"
    }
  }
}

provider "databricks" {
  auth_type = "pat"
}

data "databricks_current_user" "me" {}

resource "databricks_notebook" "hello" {
  path     = "${data.databricks_current_user.me.home}/tf-hello"
  language = "PYTHON"
  source   = "${path.module}/hello.py"
}

resource "databricks_workspace_file" "job_py" {
  path   = "${data.databricks_current_user.me.home}/tf-job.py"
  source = "${path.module}/job.py"
}

resource "databricks_job" "hello" {
  name = "tf-hello"
  task {
    task_key = "run"
    spark_python_task {
      python_file = databricks_workspace_file.job_py.path
    }
  }
}

output "user_home" {
  value = data.databricks_current_user.me.home
}

output "notebook_path" {
  value = databricks_notebook.hello.path
}

output "workspace_file_path" {
  value = databricks_workspace_file.job_py.path
}

output "job_id" {
  value = databricks_job.hello.id
}
