import sqlite3
from typing import List, Dict


def load_to_sqlite(data: List[Dict], db_path: str = "data/output.db", table_name: str = "customers"):
    if not data:
        print("[SQL Loader] No data to insert.")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 1. Infer schema from first row
    columns = data[0].keys()

    def infer_type(value):
        if isinstance(value, int):
            return "INTEGER"
        elif isinstance(value, float):
            return "REAL"
        elif isinstance(value, bool):
            return "BOOLEAN"
        else:
            return "TEXT"

    schema = {col: infer_type(data[0][col]) for col in columns}

    # 2. Create table
    cols_sql = ", ".join([f"{col} {dtype}" for col, dtype in schema.items()])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_sql});")

    # 3. Insert data
    placeholders = ", ".join(["?"] * len(columns))
    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

    for row in data:
        values = [row.get(col) for col in columns]
        cursor.execute(insert_sql, values)

    conn.commit()
    conn.close()

    print(f"[SQL Loader] Loaded {len(data)} rows into '{table_name}'")