import os
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import pandas as pd
from tqdm import tqdm



def create_connect_db():
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

def table_exists(cursor):
    print("✅  check if table exeste!")
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'customers'
        );
    """)
    return cursor.fetchone()[0]

def create_table(cursor):
    print("✅  creaction table")
    cursor.execute("""
    CREATE TABLE customers (
        event_time TIMESTAMP,
        event_type TEXT,
        product_id BIGINT,
        price FLOAT,
        user_id BIGINT,
        user_session TEXT
    );
    """
    )



def insert_table(cursor, all_path):
    print(f"✅  Inserting data from {all_path}...")

    df = pd.read_csv(all_path)
    df['event_time'] = pd.to_datetime(df['event_time'], utc=True)  # Optional: for clean datetime

    # Add progress bar while building the records list
    records = [
        (
            row['event_time'],
            row['event_type'],
            row['product_id'],
            row['price'],
            row['user_id'],
            row['user_session']
        )
        for _, row in tqdm(df.iterrows(), total=len(df), desc="⏳ Inserting rows")
    ]

    insert_query = """
    INSERT INTO customers (event_time, event_type, product_id, price, user_id, user_session)
    VALUES (%s, %s, %s, %s, %s, %s);
    """
    execute_batch(cursor, insert_query, records, page_size=1000)
    print(f"✅ Inserted {len(records)} rows.")



def creation_table_and_insert(all_path, conn):
    try:
        cursor = conn.cursor()

        if not table_exists(cursor):
            create_table(cursor)

        insert_table(cursor, all_path)
        conn.commit()

    except Exception as e:
        print("❌ Error during table creation/insertion:", e)
        conn.rollback()

    finally:
        cursor.close()

        



def main():
    #creation the connection with postgres;
    conn = create_connect_db()
    if conn:
        path_folder = "../customer"
        for file in os.listdir(path_folder):
            all_path = os.path.join(path_folder, file)
            if os.path.isfile(all_path):
                creation_table_and_insert(all_path,conn)
        

if __name__ == "__main__":
    main()