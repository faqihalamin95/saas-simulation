from src.utils.db import get_engine
from sqlalchemy import text

SCHEMAS = ["raw"]

def init_db():
    engine = get_engine()

    # Safety confirmation before dropping schemas
    confirm = input("⚠️  This will DROP all schemas. Type 'yes' to continue: ")
    if confirm.lower() != "yes":
        print("Aborted.")
        return
    
    print(" Resetting database schemas...")

    with engine.begin() as conn:
        for schema in SCHEMAS:
            # 1. Reset schema: Beware, this will drop ALL tables and data in the schema!
            print(f"   - Dropping schema: {schema}")
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE;"))

            # 2. Create new schema
            print(f"   - Creating schema: {schema}")
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

    print(" Database reset completed. Schemas are ready for ingestion.")

if __name__ == "__main__":
    init_db()