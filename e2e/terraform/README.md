# Terraform stranger (`databricks/databricks`)

The official [`databricks/databricks`](https://registry.terraform.io/providers/databricks/databricks)
provider completing `terraform apply` against **this** checkout. Auth is the
seeded admin PAT via `DATABRICKS_HOST` + `DATABRICKS_TOKEN`. `token=dev` is
401 — that string is MiniLake's trap, not a credential this seeder mints.

```sh
python3 e2e/terraform/run.py
```

Needs Terraform on `PATH` and network (`terraform init` pulls the pinned
provider from the registry). Nothing in Terraform or the provider is patched.

This is the DAB pair — workspace files + a job — spoken by the Terraform
provider. It does not claim `databricks bundle deploy`, Photon, cluster VMs,
secret ACLs, or `databricks_permissions`.

Do not use `data.databricks_spark_version.latest` or
`data.databricks_node_type.smallest`: those filter for DBR SKUs this
emulator will not invent.

## What it witnesses

| claim | how |
|---|---|
| Identity — PAT | `data.databricks_current_user`; `token=dev` fails plan |
| Workspace files / notebooks | `databricks_notebook` (classic SOURCE/PYTHON) + `databricks_workspace_file` (raw bytes) |
| Jobs 2.2 — create | `databricks_job` with `spark_python_task` (create, not run-now) |
