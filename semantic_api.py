from fastapi import FastAPI, HTTPException, Request
import json
import os
from pathlib import Path

from dbt.adapters.exceptions.connection import FailedToConnectError
from dbt.adapters.factory import get_adapter_by_type
from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import AdapterBackedSqlClient
from dbt_metricflow.cli.dbt_connectors.dbt_config_accessor import dbtProjectMetadata
from dbt_semantic_interfaces.parsing.dir_to_model import PydanticSemanticManifest
from metricflow.engine.metricflow_engine import MetricFlowEngine
from metricflow.engine.metricflow_engine import MetricFlowQueryRequest
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup

# ---------------------------------------------------------------------
# Load the dbt semantic manifest
# ---------------------------------------------------------------------

project_dir_env = os.getenv("DBT_PROJECT_DIR")
project_dir = Path(project_dir_env).expanduser().resolve() if project_dir_env else Path("poc_semantic").resolve()
manifest_path = project_dir / "target" / "semantic_manifest.json"

if not manifest_path.exists():
    raise FileNotFoundError(f"semantic_manifest.json not found at {manifest_path}. Run `dbt parse` first.")

with open(manifest_path) as f:
    manifest_dict = json.load(f)

manifest = PydanticSemanticManifest.parse_obj(manifest_dict)
lookup = SemanticManifestLookup(manifest)

# ---------------------------------------------------------------------
# Initialize dbt SQL adapter and MetricFlow engine
# ---------------------------------------------------------------------
profiles_dir_env = os.getenv("DBT_PROFILES_DIR")
profiles_dir = Path(profiles_dir_env).expanduser().resolve() if profiles_dir_env else (Path.home() / ".dbt").resolve()

project_metadata = dbtProjectMetadata.load_from_paths(profiles_dir, project_dir)
adapter = get_adapter_by_type(project_metadata.profile.credentials.type)
sql_client = AdapterBackedSqlClient(adapter)
engine = MetricFlowEngine(lookup, sql_client)

# ---------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------
app = FastAPI(title="Visa Semantic Layer API", version="1.0.0")


@app.get("/")
def root():
    """Health check endpoint."""
    return {"status": "ok", "message": "Visa Semantic API running"}

@app.post("/query")
async def query(request: Request):
    """Execute a MetricFlow query via the async engine API."""
    body = await request.json()
    metric_names = body["metrics"]
    group_by_input = body.get("group_by", [])

    available_group_bys = engine.list_group_bys(metric_names=metric_names)
    available_labels = []
    alias_map = {}

    for item in available_group_bys:
        label = getattr(item, "dunder_name", None) or getattr(item, "name", None)
        if not label:
            continue
        available_labels.append(label)
        alias_map.setdefault(label, label)
        alias_map.setdefault(label.replace("__", "."), label)
        alias_map.setdefault(label.split("__")[-1], label)

    resolved_group_bys = []
    unresolved: dict[str, list[str]] = {}

    for raw_name in group_by_input:
        if raw_name in alias_map:
            resolved_group_bys.append(alias_map[raw_name])
            continue

        candidates = [label for label in available_labels if label.endswith(f"__{raw_name}")]
        if len(candidates) == 1:
            resolved_group_bys.append(candidates[0])
        else:
            unresolved[raw_name] = candidates or available_labels

    if unresolved:
        detail = {
            "message": "Could not resolve one or more group_by items.",
            "unresolved": {name: sorted(options) for name, options in unresolved.items()},
        }
        raise HTTPException(status_code=400, detail=detail)

    try:
        request = MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=metric_names,
            group_by_names=resolved_group_bys,
        )
        result = engine.query(request)
    except FailedToConnectError as exc:  # pragma: no cover - surface db connectivity issues
        message = (
            "MetricFlow could not connect to the configured warehouse. "
            "Verify the DB in your dbt profile is reachable, then retry."
        )
        raise HTTPException(
            status_code=503,
            detail={"message": message, "error": str(exc)},
        ) from exc
    except Exception as exc:  # pragma: no cover - bubble up error details for API clients
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    table = result.result_df
    records = [dict(zip(table.column_names, row)) for row in table.rows]
    return {"data": records}
