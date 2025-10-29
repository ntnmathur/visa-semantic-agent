import asyncio
from dbt_metricflow.engine.metricflow_engine import MetricFlowEngine
from dbt_metricflow.model.semantic_manifest_lookup import SemanticManifestLookup
from dbt_metricflow.specs.metric_spec import MetricSpec
from dbt_metricflow.specs.dimension_spec import DimensionSpec
from dbt_metricflow.specs.time_dimension_spec import TimeDimensionSpec
from dbt_metricflow.model.dbt_semantic_manifest import DbtSemanticManifest

# 1️⃣  Load dbt semantic manifest from your dbt project
manifest = DbtSemanticManifest.from_dbt_project(
    project_path="poc_semantic",      # path to your dbt project folder
    profiles_dir="~/.dbt",            # where your profiles.yml lives
)

# 2️⃣  Build the lookup and engine
lookup = SemanticManifestLookup(manifest)
engine = MetricFlowEngine(lookup)

# 3️⃣  Define what we want to query (metric + dimensions)
metric_spec = MetricSpec(element_name="gmv")
merchant_dim = DimensionSpec(element_name="merchant_id")
time_dim = TimeDimensionSpec(element_name="txn_ts", time_granularity="day")

# 4️⃣  Run the query asynchronously
async def run_query():
    result = await engine.query_async(
        metric_specs=[metric_spec],
        group_by=[merchant_dim, time_dim],
    )
    print(result)

asyncio.run(run_query())
