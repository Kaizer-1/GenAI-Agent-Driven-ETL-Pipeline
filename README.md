# 🚀 GenAI Agent–Driven ETL Pipeline

## 🧠 Overview
This project demonstrates a **GenAI-powered ETL (Extract–Transform–Load) pipeline** where an intelligent agent dynamically plans, reasons, and executes data transformations.

Instead of static pipelines, a **Large Language Model (LLM)-driven agent**:
- Understands incoming data
- Decides required transformations
- Executes them in the correct order
- Handles inconsistencies and schema changes

This is a **proof-of-concept (PoC)** focused on intelligent design, not production scale.

---

## 🎯 Objective
Design a GenAI agent that can:
- Understand data schema
- Decide transformation steps
- Orchestrate ETL workflows
- Handle data quality issues and schema changes

---

## 🏗️ Architecture
        ┌──────────────────────┐
        │   Input Data (CSV)   │
        └─────────┬────────────┘
                  │
                  ▼
        ┌──────────────────────┐
        │   GenAI Agent        │
        │ (Planner + Reasoner) │
        └─────────┬────────────┘
                  │
        Generates Execution Plan
                  │
                  ▼
        ┌──────────────────────┐
        │   Orchestrator       │
        │ (Pipeline Runner)    │
        └─────────┬────────────┘
                  │
                  ▼
        ┌──────────────────────┐
        │ Transformation Tools │
        └─────────┬────────────┘
                  │
                  ▼
        ┌──────────────────────┐
        │   Output (SQLite DB) │
        └──────────────────────┘
        
---

## ⚙️ ETL Flow

### 1. Extract
- Load semi-structured CSV data
- May include:
  - Missing values
  - Nested JSON fields
  - Inconsistent formats

---

### 2. Transform (Agent-Planned)

The agent generates a dynamic pipeline like:

```python
[
  "schema_inferencer",
  "schema_flattener",
  "type_caster",
  "missing_value_handler",
  "standardization_engine",
  "data_validator",
  "feature_engineering",
  "segmentation_engine"
]
```

---

### 🔧 Transformation Modules

#### 1. Schema Inferencer (LLM)

* Detect column types
* Identify JSON fields
* Infer primary key

#### 2. Schema Flattener

* Converts nested JSON → flat structure

#### 3. Type Caster

* Converts:

  * age → int
  * purchase_value → float
  * is_active → boolean

#### 4. Missing Value Handler

* Handles null values in:

  * email
  * purchase_value
  * last_active

#### 5. Standardization Engine (LLM-assisted)

* Normalizes values:

  * "blr" → "Bangalore"

#### 6. Data Validator

* Detects invalid data:

  * age < 0 or > 120

#### 7. Feature Engineering

* Creates new features:

  * age → age_group
  * engagement → bucket

#### 8. Segmentation Engine (LLM-assisted)

* Classifies users:

  * premium
  * regular
  * churn_risk

---

### 3. Load

* Store processed data into SQLite database

---

## 🤖 Role of GenAI / LLM

### 1. Planning

* Analyzes input schema
* Decides required transformations
* Orders execution steps

### 2. Reasoning

* Detects inconsistencies
* Dynamically selects tools

### 3. Semantic Understanding

* Standardizes messy data
* Performs intelligent segmentation

---

## 📁 Project Structure
project/
│
├── data/
│   └── raw/
│       └── mock_data.csv
│
├── agent/
│   └── agent.py              # LLM-based planner
│
├── etl/
│   ├── orchestrator.py       # Executes pipeline
│   ├── transformations.py    # Transformation logic
│   └── loader.py             # Loads data to DB
│
├── output/
│   └── database.db
│
├── main.py                   # Entry point
└── README.md

---

## ▶️ How It Works

1. Load raw data
2. Agent inspects schema/sample
3. Agent generates transformation plan
4. Orchestrator executes steps
5. Clean data stored in database

---

## 🧪 Example Use Case

**Input:**

* Messy customer dataset
* Mixed types, missing values, nested JSON

**Output:**

* Clean, structured dataset
* Ready for analytics or ML

---

## ⚖️ Trade-offs & Limitations

### ✅ Pros

* Dynamic pipeline generation
* Handles schema variability
* Reduces manual coding

### ❌ Cons

* LLM latency
* Non-deterministic outputs
* Not optimized for large-scale pipelines

---

## 🔄 Adaptability

### Schema Changes

* Agent re-analyzes data
* Updates pipeline automatically

### Data Quality Issues

* Adds stricter validation & cleaning

### New Data Sources

* Generates new pipeline without code changes

---

## 🛠️ Tech Stack

* Python
* Pandas
* SQLite
* LLM (Gemini / OpenAI APIs)

---

## 📌 Future Improvements

* Persistent memory for agent
* Auto-evaluation of pipeline quality
* Parallel execution
* Integration with cloud data warehouses

---

## 🧾 Summary

This project shows how **GenAI + agents** can:

* Replace static ETL pipelines
* Enable intelligent, adaptive workflows
* Automate data engineering decisions

A foundational step toward **autonomous data systems**.
