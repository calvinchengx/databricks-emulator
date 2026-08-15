# databricks-emulator

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

A clean-room, local emulator of a **Databricks workspace**, built as a peer of
[fabric-emulator](https://github.com/calvinchengx/fabric-emulator) and the rest
of the [Azure emulator family](https://github.com/calvinchengx/azure-emulators).

The bet is the same one the family already runs: **terminate the public REST,
attach a real engine, refuse what you cannot compute.** A cluster create that
does not run Spark, a SQL warehouse that answers Photon with DuckDB and does
not say so, or a Permissions API that stores grants and always allows, are the
silent-success class this family does not ship.

This repository is the workspace host. It is not a Fabric feature.
[fabric-emulator](https://github.com/calvinchengx/fabric-emulator) already
terminates Fabric's `DatabricksNotebook` / `DatabricksSparkPython` activities
locally and refuses `dbfs:` / `/Workspace` / `/Repos` paths by name — because
remapping those to a lakehouse would invent a mapping nobody wrote. This
emulator is the host those paths resolve against.

> **No data plane yet.** The repository is founded. There is no binary, no
> image, and no witnessed claim. The first honest slice is Jobs 2.1 + workspace
> files + DBFS + one Spark attach + tokens from
> [entra-emulator](https://github.com/calvinchengx/entra-emulator) — enough that
> `databricks-sdk` and fabric-emulator's Databricks activities can point at the
> same host. See [docs/00-doctrine.md](docs/00-doctrine.md).

## Emulator family

| Repo | Role |
|---|---|
| [entra-emulator](https://github.com/calvinchengx/entra-emulator) | The STS. Issues every token |
| [azure-keyvault-emulator](https://github.com/calvinchengx/azure-keyvault-emulator) | Key Vault data plane |
| [arm-emulator](https://github.com/calvinchengx/arm-emulator) | ARM control plane + RBAC |
| [azure-apim-emulator](https://github.com/calvinchengx/azure-apim-emulator) | API Management |
| [fabric-emulator](https://github.com/calvinchengx/fabric-emulator) | Fabric control + data plane. A **consumer** of this workspace |
| **databricks-emulator** | Databricks workspace. A **consumer** of entra; a **peer** of fabric |

Composition into [azure-emulators](https://github.com/calvinchengx/azure-emulators)
comes after there is an image to pin.

## License

Apache-2.0. Clean-room: grounded solely in Databricks' public documentation and
OpenAPI (Workspace, Jobs, Unity Catalog), with Databricks' own SDK and CLI as
the conformance oracle — no Databricks Runtime source.
