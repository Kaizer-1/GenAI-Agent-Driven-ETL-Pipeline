"""
agent.py — LLM-based planner that inspects sample data and returns an ordered tool plan.
"""
from google import genai
import os
from dotenv import load_dotenv

load_dotenv()

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
MODEL_NAME = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
import json
import re

VALID_TOOLS = [
    "schema_inferencer",
    "schema_flattener",
    "type_caster",
    "missing_value_handler",
    "standardization_engine",
    "data_validator",
    "feature_engineering",
    "segmentation_engine",
]

FALLBACK_PLAN = list(VALID_TOOLS)

MISSING_SENTINELS = {None, "", "NA", "null", "N/A", "na", "none"}


# ── helpers ─────────────────────────────────────────────────────────────

def _has_missing(data: list[dict]) -> bool:
    return any(
        v is None or (isinstance(v, str) and v.strip() in MISSING_SENTINELS)
        for row in data for v in row.values()
    )


def _has_nested_json(data: list[dict]) -> bool:
    for row in data:
        for v in row.values():
            if isinstance(v, dict):
                return True
            if isinstance(v, str):
                s = v.strip()
                if s.startswith("{") and s.endswith("}"):
                    return True
    return False


def _has_type_issues(data: list[dict]) -> bool:
    """True if numeric-looking fields are stored as strings."""
    for row in data:
        for k, v in row.items():
            if isinstance(v, str):
                try:
                    float(v)
                    return True
                except ValueError:
                    pass
    return False


def _has_invalid_values(data: list[dict]) -> bool:
    for row in data:
        age = row.get("age")
        try:
            if float(age) < 0 or float(age) > 120:
                return True
        except (TypeError, ValueError):
            pass
    return False


def _has_categorical_issues(data: list[dict]) -> bool:
    """Crude check: same column has values that differ only by case or known aliases."""
    for key in ("location", "city", "state"):
        vals = {str(r[key]).strip().lower() for r in data if key in r and r[key]}
        if len(vals) > len({v.title() for v in vals}):
            return True
        # known alias pairs
        aliases = {"blr", "bangalore", "bengaluru", "ca", "california"}
        if vals & aliases:
            return True
    return False


# ── prompt / LLM interface ──────────────────────────────────────────────

def build_prompt(sample_data: list[dict]) -> str:
    snapshot = json.dumps(sample_data[:5], indent=2, default=str)
    return f"""You are an ETL planning agent.

Given this sample data:
{snapshot}

Available tools (use ONLY these names):
{json.dumps(VALID_TOOLS)}

Rules:
- Always start with schema_inferencer.
- Include schema_flattener if any field contains nested JSON.
- Include type_caster if numeric values are stored as strings.
- Include missing_value_handler if there are missing/null/NA values.
- Include standardization_engine always.
- Include data_validator if values could be out of valid range.
- Include feature_engineering if age or purchase_value exists.
- Include segmentation_engine ONLY as the last step.

Return ONLY a JSON array of tool names in execution order. No explanation."""


def call_llm(prompt: str) -> str:
    """Call Gemini LLM for planning."""
    try:
        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=prompt,
        )

        text = response.text or ""

        return text

    except Exception as e:
        print(f"[agent] LLM call failed ({e}), using heuristic planner")
        return ""


def _parse_plan(raw: str) -> list[str] | None:
    """Extract a JSON list of tool names from LLM output."""
    try:
        match = re.search(r"\[.*\]", raw, re.DOTALL)
        if match:
            plan = json.loads(match.group())
            if isinstance(plan, list) and all(t in VALID_TOOLS for t in plan):
                return plan
    except (json.JSONDecodeError, TypeError):
        pass
    return None


# ── heuristic planner (no LLM needed) ──────────────────────────────────

def _heuristic_plan(data: list[dict]) -> list[str]:
    plan = ["schema_inferencer"]
    if _has_nested_json(data):
        plan.append("schema_flattener")
    if _has_type_issues(data):
        plan.append("type_caster")
    if _has_missing(data):
        plan.append("missing_value_handler")
    if _has_categorical_issues(data):
        plan.append("standardization_engine")
    if _has_invalid_values(data):
        plan.append("data_validator")
    plan.append("feature_engineering")
    plan.append("segmentation_engine")
    return plan


# ── public API ──────────────────────────────────────────────────────────

def plan_pipeline(sample_data: list[dict]) -> list[str]:
    """Analyze data → ask LLM (or heuristic) → return ordered tool list."""
    print("[agent] Analyzing sample data…")

    prompt = build_prompt(sample_data)
    raw = call_llm(prompt)

    if raw:
        plan = _parse_plan(raw)
        if plan:
            print(f"[agent] LLM plan: {plan}")
            return plan
        print("[agent] Could not parse LLM response, falling back to heuristic")

    plan = _heuristic_plan(sample_data)
    print(f"[agent] Heuristic plan: {plan}")
    return plan