# Visa Semantic Agent

This project builds an AI-ready Semantic Data Model (SDM) using open-source components:
- dbt + MetricFlow for metrics
- Ontop for ontology/SPARQL
- Metabase for visualization
- MCP-based Python agent for natural-language queries

## Prerequisites

- Python 3.11+
- Access to a Postgres instance matching the connection details in `~/.dbt/profiles.yml`
- `dbt` profile pointing at the `poc_semantic` project

## Environment Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If you already have a virtual environment, activate it and install the pinned dependencies from `requirements.txt`.

## Build the dbt & MetricFlow Artifacts

Generate the dbt semantic manifest and materialize the base model:

```bash
.venv/bin/dbt parse --project-dir poc_semantic --profiles-dir ~/.dbt
.venv/bin/dbt run --project-dir poc_semantic --profiles-dir ~/.dbt --select base_transactions
.venv/bin/python patch_manifest.py  # adds metric aggregation metadata required by MetricFlow
```

If your profiles live in a different directory, set `DBT_PROFILES_DIR` before running the commands.

## Run the FastAPI Service

```bash
.venv/bin/uvicorn semantic_api:app --reload
```

You can override the project or profiles directory by exporting `DBT_PROJECT_DIR` and `DBT_PROFILES_DIR` before launching the service.

## Smoke Test the API

With Uvicorn running on the default port:

```bash
curl -X POST http://127.0.0.1:8000/query \
  -H "Content-Type: application/json" \
  -d '{"metrics":["gmv"], "group_by":["merchant_id"]}'
```

Expect a JSON object containing a `data` array of metric rows, or a helpful error if the warehouse connection fails.
