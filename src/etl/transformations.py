"""
GenAI Agent-Driven ETL Pipeline — Transformation Tools (Updated)
"""

import json
import re
import statistics
import csv  # ← ADDED
from abc import ABC, abstractmethod
import os
from google import genai
from dotenv import load_dotenv

load_dotenv()

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

# ── Base Class ──────────────────────────────────────────────────────────

class Transformation(ABC):
    """Base interface for all transformation tools."""
    name: str = "base"
    description: str = ""

    @abstractmethod
    def apply(self, data: list[dict], schema: dict | None = None) -> list[dict]:
        pass


# ── CSV LOADER (ADDED) ─────────────────────────────────────────────────

def load_csv(path: str) -> list[dict]:
    with open(path, newline='', encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ── 1. Missing Value Handler ────────────────────────────────────────────

class MissingValueHandler(Transformation):
    name = "missing_value_handler"
    description = "Detects and fills missing/null values using configurable rules."

    MISSING = {None, "", "NA", "null", "N/A", "na", "none"}

    DEFAULT_RULES: dict = {
        "email": "unknown@example.com",
        "_numeric": 0,
        "_boolean": False,
        "_string": "",
    }

    def __init__(self, rules: dict | None = None, numeric_strategy: str = "mean"):
        self.rules = {**self.DEFAULT_RULES, **(rules or {})}
        self.numeric_strategy = numeric_strategy

    def _is_missing(self, v) -> bool:
        if v is None:
            return True
        if isinstance(v, str) and v.strip() in self.MISSING:
            return True
        return False

    def _guess_type(self, key: str, values: list):
        if key in self.rules:
            return "explicit"
        for v in values:
            if isinstance(v, bool):
                return "boolean"
            if isinstance(v, (int, float)):
                return "numeric"
        return "string"

    def apply(self, data: list[dict], schema: dict | None = None) -> list[dict]:
        if not data:
            return data

        keys = {k for row in data for k in row}

        # detect numeric columns using schema if available
        numeric_fields = set()
        if schema:
            numeric_fields = {k for k, v in schema.items() if v in (int, float)}

        col_values = {
            k: [r.get(k) for r in data if not self._is_missing(r.get(k))]
            for k in keys
        }

        fills: dict = {}
        for k in keys:
            if k in self.rules:
                fills[k] = self.rules[k]
                continue

            # PRIORITIZE schema-based typing
            if k in numeric_fields:
                nums = []
                for v in col_values[k]:
                    try:
                        nums.append(float(v))
                    except:
                        continue
                fills[k] = statistics.mean(nums) if nums and self.numeric_strategy == "mean" else self.rules["_numeric"]
                continue

            col_type = self._guess_type(k, col_values[k])

            if col_type == "numeric":
                nums = []
                for v in col_values[k]:
                    try:
                        nums.append(float(v))
                    except:
                        continue
                fills[k] = statistics.mean(nums) if nums and self.numeric_strategy == "mean" else self.rules["_numeric"]

            elif col_type == "boolean":
                fills[k] = self.rules["_boolean"]

            else:
                fills[k] = self.rules["_string"]

        # apply fills
        out = []
        for row in data:
            new_row = {}
            for k, v in row.items():
                if self._is_missing(v):
                    new_row[k] = fills.get(k, "")
                else:
                    new_row[k] = v
            out.append(new_row)

        return out


# ── 2. Data Validator ───────────────────────────────────────────────────

class DataValidator(Transformation):
    name = "data_validator"
    description = "Validates rows (age range, email format) and tags each with is_valid."

    EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

    def __init__(self, age_range: tuple = (0, 120), filter_invalid: bool = False):
        self.age_min, self.age_max = age_range
        self.filter_invalid = filter_invalid

    def _validate(self, row: dict) -> bool:
        if "age" in row:
            try:
                age = float(row["age"])
                if not (self.age_min <= age <= self.age_max):
                    return False
            except (ValueError, TypeError):
                return False
        if "email" in row:
            if not self.EMAIL_RE.match(str(row["email"])):
                return False
        return True

    def apply(self, data: list[dict], schema: dict | None = None) -> list[dict]:
        out = []
        for row in data:
            new_row = row.copy()
            new_row["is_valid"] = self._validate(new_row)
            if not self.filter_invalid or new_row["is_valid"]:
                out.append(new_row)
        return out


# ── 3. Type Caster ──────────────────────────────────────────────────────

class TypeCaster(Transformation):
    name = "type_caster"
    description = "Casts fields to target types (int, float, bool) safely."

    BOOL_TRUE = {"true", "1", "yes", "y"}

    DEFAULT_SCHEMA: dict = {
        "age": int,
        "purchase_value": float,
        "is_active": bool,
        "engagement_score": float
    }

    def __init__(self, schema: dict | None = None):
        self.schema = schema if schema is not None else self.DEFAULT_SCHEMA

    def _cast(self, value, target):
        try:
            # normalize empty strings BEFORE casting
            if value is None or (isinstance(value, str) and value.strip() == ""):
                return None

            if target is bool:
                if isinstance(value, bool):
                    return value
                return str(value).strip().lower() in self.BOOL_TRUE

            if target is int:
                return int(float(value))

            return target(value)

        except (ValueError, TypeError):
            return None
        
    def apply(self, data: list[dict], schema: dict | None = None) -> list[dict]:
        effective_schema = schema or self.schema
        out = []
        for row in data:
            new_row = row.copy()
            for field, target in effective_schema.items():
                if field in new_row:
                    new_row[field] = self._cast(new_row[field], target)
            out.append(new_row)
        return out


# ── 4. Standardization Engine ───────────────────────────────────────────

class StandardizationEngine(Transformation):
    name = "standardization_engine"
    description = "Uses Gemini LLM to normalize categorical values dynamically."

    def __init__(self, model_name: str | None = None):
        self.model_name = model_name or os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
        self.client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

    def _standardize_column(self, field: str, values: list[str]) -> dict:
        """
        Ask LLM to standardize categorical values.
        Returns mapping: original → standardized
        """

        prompt = f"""
You are a data cleaning assistant.

Standardize the following values for column '{field}'.

Rules:
- Normalize names (cities, states, categories, etc.)
- Fix abbreviations (e.g., blr → Bangalore, CA → California)
- Keep consistent capitalization
- Do NOT hallucinate new values
- Return ONLY valid JSON

Values:
{values}

Output:
{{ "original_value": "standardized_value" }}
"""

        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
            )

            text = response.text.strip()

            # Extract JSON safely
            json_match = re.search(r"\{.*\}", text, re.DOTALL)
            if json_match:
                parsed = json.loads(json_match.group())

                # Normalize keys to lowercase for matching
                return {k.lower(): v for k, v in parsed.items()}

        except Exception as e:
            print(f"[LLM Error - {field}] {e}")

        return {}

    def apply(self, data: list[dict], schema: dict | None = None) -> list[dict]:
        if not data:
            return data

        fields = set(k for row in data for k in row)
        mappings = {}

        for field in fields:
            values = list({
                str(row[field]).strip()
                for row in data
                if field in row and isinstance(row[field], str)
            })

            # ✅ Skip non-useful columns
            if len(values) < 3:
                continue

            # ✅ Skip obvious non-categorical fields
            if field.lower() in ["email", "customer_id", "signup_date", "last_active"]:
                continue

            print(f"[LLM] Standardizing field: {field}")

            mapping = self._standardize_column(field, values)

            if mapping:
                mappings[field] = mapping

        # ✅ Apply mappings
        out = []
        for row in data:
            new_row = row.copy()

            for field, lookup in mappings.items():
                if field in new_row and isinstance(new_row[field], str):
                    key = new_row[field].strip().lower()
                    new_row[field] = lookup.get(key, new_row[field])

            out.append(new_row)

        return out


# ── 5. Schema Flattener ─────────────────────────────────────────────────

class SchemaFlattener(Transformation):
    name = "schema_flattener"
    description = "Parses nested JSON string fields and flattens them into top-level keys."

    def __init__(self, fields: list[str] | None = None):
        self.fields = fields or ["preferences"]

    def apply(self, data: list[dict], schema: dict | None = None) -> list[dict]:
        out = []
        for row in data:
            new_row = row.copy()
            for field in self.fields:
                raw = new_row.get(field)
                if raw is None:
                    continue
                if isinstance(raw, str):
                    try:
                        raw = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                if isinstance(raw, dict):
                    for k, v in raw.items():
                        new_row[k] = v
                    new_row.pop(field, None)
            out.append(new_row)
        return out


# ── 6. Feature Engineering ──────────────────────────────────────────────

class FeatureEngineering(Transformation):
    name = "feature_engineering"
    description = "Derives age_group and engagement_level from existing fields."

    def __init__(self, engagement_thresholds: tuple = (0.3, 0.7)):  # CHANGED
        self.low, self.high = engagement_thresholds

    @staticmethod
    def _age_group(age) -> str:
        try:
            age = float(age)
        except (TypeError, ValueError):
            return "unknown"
        if age < 18:
            return "minor"
        if age <= 60:
            return "adult"
        return "senior"

    def _engagement(self, score) -> str:  # CHANGED (input = engagement_score)
        try:
            score = float(score)
        except (TypeError, ValueError):
            return "low"

        if score >= self.high:
            return "high"
        if score >= self.low:
            return "medium"
        return "low"

    def apply(self, data: list[dict], schema: dict | None = None) -> list[dict]:
        out = []
        for row in data:
            new_row = row.copy()
            new_row["age_group"] = self._age_group(new_row.get("age"))

            # CHANGED: use engagement_score instead of purchase_value
            new_row["engagement_level"] = self._engagement(
                new_row.get("engagement_score")
            )

            out.append(new_row)
        return out



# ── 7. Segmentation Engine ─────────────────────────────────────────────

class SegmentationEngine(Transformation):
    name = "segmentation_engine"
    description = "Assigns customer segments (premium / churn_risk / regular) via rules."

    def apply(self, data: list[dict], schema: dict | None = None) -> list[dict]:
        out = []
        for row in data:
            new_row = row.copy()

            try:
                pv = float(new_row.get("purchase_value", 0) or 0)
            except:
                pv = 0

            try:
                engagement = float(new_row.get("engagement_score", 0) or 0)  # ADDED
            except:
                engagement = 0

            active = new_row.get("is_active")
            if isinstance(active, str):
                active = active.strip().lower() in ("true", "1", "yes")

            # CHANGED: better segmentation logic (still simple)
            if pv >= 200 and engagement >= 0.7:
                new_row["segment"] = "premium"
            elif not active or engagement < 0.3:
                new_row["segment"] = "churn_risk"
            else:
                new_row["segment"] = "regular"

            out.append(new_row)
        return out


# ── Pipeline Executor ───────────────────────────────────────────────────

class TransformationPipeline:
    def __init__(self, transformations: list[Transformation] | None = None):
        self.transformations = transformations or []

    def run(self, data: list[dict], schema: dict | None = None) -> list[dict]:
        for t in self.transformations:
            print(f"[Pipeline] Running {t.name}")
            data = t.apply(data, schema)
        return data


# ── Example Usage (UPDATED TO CSV) ──────────────────────────────────────

if __name__ == "__main__":
    data = load_csv("data/raw/mock_data.csv")  # ← CSV ONLY

    pipeline = TransformationPipeline([
        SchemaFlattener(),
        TypeCaster(),
        MissingValueHandler(),
        StandardizationEngine(),
        DataValidator(filter_invalid=True),
        FeatureEngineering(),
        SegmentationEngine(),
    ])

    result = pipeline.run(data)

    print(json.dumps(result, indent=2))