import json
from agent.agent import plan_pipeline
from etl.orchestrator import run_pipeline
from etl.transformations import load_csv
from etl.sql_loader import load_to_sqlite


def main() -> None:
    print("=" * 60)
    print("GenAI Agent-Driven ETL Pipeline")
    print("=" * 60)

    # 0. Load data from CSV
    data = load_csv("data/raw/mock_data.csv")   # ← CHANGE HERE

    # 1. Agent plans the pipeline
    print("\n▶ Generating execution plan…")
    plan = plan_pipeline(data)
    print(f"\nPlan: {plan}\n")

    # 2. Orchestrator executes
    print("▶ Running pipeline…")
    final = run_pipeline(data, plan)

    # 3. Show results
    print("\n▶ Final transformed data:")
    for row in final:
        print(json.dumps(row, indent=2, default=str))
    
    # 4. Load into SQL
    print("\n▶ Loading into SQLite DB…")
    load_to_sqlite(final)


if __name__ == "__main__":
    main()