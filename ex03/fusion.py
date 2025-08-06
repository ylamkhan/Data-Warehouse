#!/usr/bin/env python3
from tqdm import tqdm
import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

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

def create_table_item_inser_item():
    query_creation = """
        DROP TABLE IF EXISTS item;
        CREATE TABLE item (
            product_id NUMERIC,
            category_id NUMERIC,
            category_code TEXT,
            brand TEXT
        );
    """
    query_insert = """
        INSERT INTO item (product_id, category_id, category_code, brand)
        VALUES (%s, %s, %s, %s)
    """

    df = pd.read_csv("../item/item.csv")

    records = [
        (
            row['product_id'],
            row['category_id'],
            row['category_code'],
            row['brand']
        )
        for _, row in df.iterrows()
    ]

    conn = connect_to_db()
    cursor = conn.cursor()
    
    cursor.execute(query_creation)

    # 👇 Use tqdm for a nice progress bar
    for record in tqdm(records, desc="🛠 Inserting items"):
        cursor.execute(query_insert, record)

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Fusion complete. Data written to `item` table.")


def fuse_customers_and_items():
    query = """
    DROP TABLE IF EXISTS customers_fused;

    CREATE TABLE customers_fused AS
    SELECT
        c.event_time,
        c.event_type,
        c.product_id,
        c.price,
        c.user_id,
        c.user_session,
        i.category_id,
        i.category_code,
        i.brand
    FROM customers_table c
    LEFT JOIN items i ON c.product_id = i.product_id;
    """

    conn = connect_to_db()
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Fusion complete. Data written to `customers_fused`.")

if __name__ == "__main__":
    create_table_item_inser_item()
    fuse_customers_and_items()
