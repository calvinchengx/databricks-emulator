# 07 — Workspace and files

Three stores, all file-backed, all PAT/OIDC gated. Bytes on disk are the
attach. A 200 that did not write a file is a lookalike.

| Store | Root | Wire |
|---|---|---|
| Classic workspace | `{dataDir}/workspace` | `/api/2.0/workspace/*` |
| Workspace files | the same root | `/api/2.0/workspace-files/` |
| DBFS / Files API | `{dataDir}/dbfs` | `/api/2.0/dbfs/*` and `/api/2.0/fs/*` |

Path traversal (`/../…`) is 400.

## Classic import — SOURCE/PYTHON

`POST /api/2.0/workspace/import` accepts:

- `format=SOURCE` `language=PYTHON` — a notebook
- `format=AUTO` — a Python notebook if the first non-empty line is
  `# Databricks notebook source`, otherwise a FILE
- `format=RAW` or `FILE` — a workspace file, any bytes

JUPYTER, SOURCE/SCALA, SOURCE/R, SOURCE/SQL are 501. That is the classic
surface this slice ships; it is not "every notebook language".

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

w = WorkspaceClient(host="http://127.0.0.1:8447", token=open("data/admin.pat").read().strip())
w.workspace.upload(
    "/Shared/etl.py",
    b"print(1)\n",
    overwrite=True,
    format=ImportFormat.SOURCE,
    language=Language.PYTHON,
)
print(w.workspace.download("/Shared/etl.py").read())
```

Witness: `ci:e2e-sdk`, `ci:e2e-terraform`.

## Workspace files — raw bytes

`POST`/`PUT /api/2.0/workspace-files/import-file/{path}` writes the request
body as a FILE. `GET /api/2.0/workspace-files/{path}` returns those bytes or
404. This is how a `.whl` or a lockfile lands; it is not a notebook import.

Witness: `ci:e2e-terraform`, `go:TestWorkspaceFilesRawBytesAnd404`.

## DBFS and the Files API

`/api/2.0/dbfs/put|read|get-status|list|mkdirs|delete|move` and the streaming
`create` / `add-block` / `close` handles. `read` is capped at 1 MiB, as
Databricks documents.

`/api/2.0/fs/files/` and `/api/2.0/fs/directories/` share the same
`data/dbfs/` root. A Files API PUT is a DBFS GET.

Jobs load `dbfs:/…` paths from this store. Fabric activities that pass
`dbfs:/jobs/chain.py` hit these bytes — see
[Family integration](14-family-integration.md).

Witness: `ci:e2e-sdk`.

## Also on this store

`get-status`, `list`, `mkdirs`, `delete` (recursive). Direct download
(`direct_download=true`) returns `application/octet-stream` instead of
base64 JSON.
