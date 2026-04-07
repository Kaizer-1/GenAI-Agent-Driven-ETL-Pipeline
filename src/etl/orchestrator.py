"""
orchestrator.py — Executes an ordered transformation plan against data.
"""

from etl.transformations import (
    SchemaFlattener,
    TypeCaster,
    MissingValueHandler,
    StandardizationEngine,
    DataValidator,
    FeatureEngineering,
    SegmentationEngine,
)

import json
import copy


# ── Schema Inferencer (lightweight, lives here) ────────────────────────

def schema_inferencer(data: list[dict]) -> list[dict]:
    """Log inferred schema; pass data through unchanged."""
    if not data:
        return data
    sample = data[0]
    print("[schema_inferencer] Inferred schema:")
    for k, v in sample.items():
        print(f"  {k}: {type(v).__name__} (sample={repr(v)[:60]})")
    return data


# ── Thin wrappers so every tool is Callable[[list[dict]], list[dict]] ──

def schema_flattener(data: list[dict]) -> list[dict]:
    return SchemaFlattener().apply(data)

def type_caster(data: list[dict]) -> list[dict]:
    return TypeCaster().apply(data)

def missing_value_handler(data: list[dict]) -> list[dict]:
    return MissingValueHandler(numeric_strategy="mean").apply(data)

def standardization_engine(data: list[dict]) -> list[dict]:
    return StandardizationEngine().apply(data)

def data_validator(data: list[dict]) -> list[dict]:
    return DataValidator().apply(data)

def feature_engineering(data: list[dict]) -> list[dict]:
    return FeatureEngineering().apply(data)

def segmentation_engine(data: list[dict]) -> list[dict]:
    return SegmentationEngine().apply(data)


# ── Tool registry ──────────────────────────────────────────────────────

TOOLS: dict[str, callable] = {
    "schema_inferencer": schema_inferencer,
    "schema_flattener": schema_flattener,
    "type_caster": type_caster,
    "missing_value_handler": missing_value_handler,
    "standardization_engine": standardization_engine,
    "data_validator": data_validator,
    "feature_engineering": feature_engineering,
    "segmentation_engine": segmentation_engine,
}


# ── Pipeline runner ────────────────────────────────────────────────────

def run_pipeline(data: list[dict], plan: list[str]) -> list[dict]:
    """Execute each tool in plan order, passing data through sequentially."""
    data = copy.deepcopy(data)  # don't mutate caller's data

    for step in plan:
        tool = TOOLS.get(step)
        if tool is None:
            print(f"[orchestrator] WARNING: unknown tool '{step}', skipping")
            continue
        try:
            print(f"[orchestrator] Running: {step}")
            data = tool(data)
            print(f"[orchestrator]   → {len(data)} rows")
        except Exception as e:
            print(f"[orchestrator] ERROR in '{step}': {e}, skipping")

    return data