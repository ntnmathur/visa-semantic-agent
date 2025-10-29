#!/usr/bin/env python3
"""
Patch the semantic_manifest.json to add metric_aggregation_params required by metricflow_semantics
"""
import json
from pathlib import Path

manifest_path = Path("poc_semantic/target/semantic_manifest.json")

with open(manifest_path) as f:
    manifest = json.load(f)

# Find the semantic model that measures belong to
measure_to_semantic_model = {}
for sm in manifest["semantic_models"]:
    for measure in sm["measures"]:
        measure_to_semantic_model[measure["name"]] = sm["name"]

# Get measure aggregation for each measure
measure_agg = {}
for sm in manifest["semantic_models"]:
    for measure in sm["measures"]:
        measure_agg[measure["name"]] = measure["agg"]

# Patch each simple metric to add metric_aggregation_params
for metric in manifest["metrics"]:
    if metric["type"] == "simple":
        measure_name = metric["type_params"]["measure"]["name"]
        semantic_model_name = measure_to_semantic_model.get(measure_name)
        agg = measure_agg.get(measure_name, "sum")
        
        if semantic_model_name:
            metric["type_params"]["metric_aggregation_params"] = {
                "semantic_model": semantic_model_name,
                "use_latest_partition": None,
                "agg": agg
            }

# Write back
with open(manifest_path, "w") as f:
    json.dump(manifest, f, indent=2)

print(f"✓ Patched {manifest_path}")
