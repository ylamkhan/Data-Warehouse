#!/usr/bin/env python3

import os
import psycopg2
from dotenv import load_dotenv

def connect_to_db():
    load_dotenv()
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
        )
        print("✅ The connection to the database was successful.")
        return conn
    except Exception as e:
        print("❌ ERROR IN CONNECTION WITH DB:", e)
        return None

def get_row_count(cursor):
    cursor.execute("SELECT COUNT(*) FROM customers;")
    return cursor.fetchone()[0]

def remove_duplicates():
    query = """
    DELETE FROM customers
    WHERE ctid IN (
        SELECT ctid FROM (
            SELECT ctid,
                event_time,
                event_type,
                product_id,
                LAG(event_time) OVER (
                    PARTITION BY event_type, product_id
                    ORDER BY event_time
                ) AS previous_event_time
            FROM customers
        ) sub
        WHERE previous_event_time IS NOT NULL
        AND EXTRACT(EPOCH FROM (event_time - previous_event_time)) <= 1
    );
    """

    conn = connect_to_db()
    cursor = conn.cursor()

    total_before = get_row_count(cursor)

    cursor.execute(query)
    conn.commit()

    total_after = get_row_count(cursor)
    removed = total_before - total_after

    print(f"Rows before: {total_before}")
    print(f"Rows after : {total_after}")
    print(f"Removed    : {removed} duplicate rows.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    remove_duplicates()
