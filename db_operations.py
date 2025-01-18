import psycopg2
from psycopg2.extras import execute_values
import os
import dotenv
import time
import random

dotenv.load_dotenv()

POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

# --------------------------------------
# 1) Create a helper to connect to DB
# --------------------------------------
def get_connection(
    dbname="postgres",
    user="postgres",
    password=POSTGRES_PASSWORD,
    host="localhost",
    port=5432,
):
    """
    Returns a new psycopg2 connection.
    Adjust default arguments for your Docker/Postgres configuration.
    """
    return psycopg2.connect(
        dbname=dbname, user=user, password=password, host=host, port=port
    )


# --------------------------------------
# 2) Setup function: creates tables
# --------------------------------------
def setup_database(conn):
    """
    Create all required tables if they do not exist.
    """
    create_tokens_table = """
    CREATE TABLE IF NOT EXISTS tokens (
        mint_address   TEXT PRIMARY KEY,
        decimals       INT,
        mint_to_usd    NUMERIC(38, 18)
    );
    """

    create_users_table = """
    CREATE TABLE IF NOT EXISTS users (
        user_address          TEXT NOT NULL,
        transaction_signature TEXT NOT NULL,
        profit                NUMERIC(38, 18),
        PRIMARY KEY (user_address, transaction_signature)
    );
    """

    create_transactions_table = """
    CREATE TABLE IF NOT EXISTS transactions (
        signature        TEXT PRIMARY KEY,
        slot_number      BIGINT,
        user_address     TEXT,
        block_time       TIMESTAMP,
        success          BOOLEAN,
        tokens_involved  TEXT[] NULL,
        token_arbitraged TEXT REFERENCES tokens(mint_address),
        profit           NUMERIC(38, 18),
        created_at       TIMESTAMP DEFAULT NOW()
    );
    """

    with conn.cursor() as cur:
        cur.execute(create_tokens_table)
        cur.execute(create_users_table)
        cur.execute(create_transactions_table)
    conn.commit()


def clear_database(conn):
    """
    Deletes all data from all tables in the database.
    WARNING: This is for development purposes only!
    """
    with conn.cursor() as cur:
        # Disable foreign key checks temporarily to avoid constraint issues
        cur.execute("SET CONSTRAINTS ALL DEFERRED")

        # Clear all tables
        cur.execute("TRUNCATE TABLE transactions, users, tokens CASCADE")

        # Re-enable foreign key checks
        cur.execute("SET CONSTRAINTS ALL IMMEDIATE")
    conn.commit()

    print("Database cleared successfully!")


# --------------------------------------
# 3) Batch insert functions
# --------------------------------------
def insert_tokens(conn, token_rows):
    """
    Insert multiple token records at once using execute_values.
    `token_rows` is a list of tuples:
        [
           (mint_address, decimals, mint_to_usd),
           ...
        ]
    """
    sql = """
    INSERT INTO tokens (mint_address, decimals, mint_to_usd)
    VALUES %s
    ON CONFLICT (mint_address) DO UPDATE
        SET mint_to_usd = EXCLUDED.mint_to_usd
    """
    # sleep for random amount of time between 0 and 1 second
    time.sleep(random.uniform(0, 0.1))

    with conn.cursor() as cur:
        execute_values(cur, sql, token_rows)
    conn.commit()


def insert_users(conn, user_rows):
    """
    Insert multiple user records at once.
    `user_rows` is a list of tuples:
        [
            (user_address, transaction_signature, profit),
            ...
        ]
    If there's a conflict, we can UPDATE the profit (for example).
    """
    sql = """
    INSERT INTO users (user_address, transaction_signature, profit)
    VALUES %s
    ON CONFLICT (user_address, transaction_signature) DO UPDATE
        SET profit = EXCLUDED.profit
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, user_rows)
    conn.commit()


def insert_transactions(conn, tx_rows):
    """
    Insert multiple transactions at once.
    `tx_rows` is a list of tuples corresponding to the columns in `transactions`:
        [
          (signature, slot_number, user_address, block_time, success,
           tokens_involved, token_arbitraged, profit, created_at),
          ...
        ]

    `tokens_involved` can be passed in Python as a list, psycopg2 can handle it if you
    convert to e.g. `list` or `tuple`. For correct array insertion, you may need to ensure
    it's recognized as a list of strings.
    """
    sql = """
    INSERT INTO transactions (
        signature, slot_number, user_address, block_time, success,
        tokens_involved, token_arbitraged, profit, created_at
    ) VALUES %s
    ON CONFLICT (signature) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, tx_rows)
    conn.commit()


# --------------------------------------
# 4) Query functions (analytics)
# --------------------------------------


def get_total_profit(conn):
    """
    Returns total profit excluding negative values and non-successful transactions.
    """
    sql = """
    SELECT COALESCE(SUM(profit), 0)
    FROM transactions
    WHERE success = TRUE
      AND profit >= 0
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        result = cur.fetchone()
        return result[0] if result else 0


def get_average_profit(conn):
    """
    Returns average profit excluding negative values and non-successful transactions.
    """
    sql = """
    SELECT COALESCE(AVG(profit), 0)
    FROM transactions
    WHERE success = TRUE
      AND profit >= 0
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        result = cur.fetchone()
        return result[0] if result else 0


def get_top_10_transactions(conn):
    """
    Top 10 largest profit transactions (excluding negative profits and non-success).
    Returns list of (signature, profit).
    """
    sql = """
    SELECT signature, profit
    FROM transactions
    WHERE success = TRUE
      AND profit >= 0
    ORDER BY profit DESC
    LIMIT 10
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def get_most_common_tokens_arbitraged(conn, limit=10):
    """
    Returns the most common tokens arbitraged (token_arbitraged),
    excluding negative profits.
    Returns list of (token_arbitraged, count).
    """
    sql = f"""
    SELECT token_arbitraged, COUNT(*) as cnt
    FROM transactions
    WHERE profit >= 0
    GROUP BY token_arbitraged
    ORDER BY cnt DESC
    LIMIT {limit}
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def get_top_10_users_by_profit(conn):
    """
    Top 10 users by total profit, excluding non-success transactions.
    Returns list of (user_address, total_profit).
    """
    sql = """
    SELECT user_address, COALESCE(SUM(profit), 0) AS total_profit
    FROM transactions
    WHERE success = TRUE
    GROUP BY user_address
    ORDER BY total_profit DESC
    LIMIT 10
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def get_success_rate(conn):
    """
    Returns the percentage of successful transactions as a decimal between 0 and 1.
    For example, 0.75 means 75% of transactions were successful.
    """
    sql = """
    SELECT
        CASE
            WHEN COUNT(*) = 0 THEN 0
            ELSE ROUND(
                COUNT(CASE WHEN success = TRUE THEN 1 END)::NUMERIC /
                COUNT(*)::NUMERIC,
                4
            )
        END as success_rate
    FROM transactions
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        result = cur.fetchone()
        return result[0] if result else 0


def main():
    # 2) Setup tables if not exists
    setup_database(conn)

    # 3) Insert some sample tokens
    tokens_to_insert = [
        ("So11111111111111111111111111111111111111112", 9, 23.45),  # example
        ("USDC1111111111111111111111111111111111111111", 6, 1.00),
    ]
    insert_tokens(conn, tokens_to_insert)

    # 4) Insert some sample transactions
    tx_rows = [
        (
            "sig_123",  # signature
            155008105,  # slot_number
            "user_abc",  # user_address
            "2025-02-18 12:34:56",  # block_time
            True,  # success
            [
                "So11111111111111111111111111111111111111112",
                "USDC1111111111111111111111111111111111111111",
            ],  # tokens_involved
            "So11111111111111111111111111111111111111112",  # token_arbitraged
            12.34,  # profit
            None,  # created_at -> None means default
        ),
        (
            "sig_456",  # signature
            155008106,
            "user_xyz",
            "2025-02-18 12:40:00",
            True,
            ["So11111111111111111111111111111111111111112"],
            "So11111111111111111111111111111111111111112",
            -5.00,  # negative profit example
            None,
        ),
    ]
    insert_transactions(conn, tx_rows)

    # 5) Insert some sample user entries
    # Each row: (user_address, transaction_signature, profit)
    user_rows = [("user_abc", "sig_123", 12.34), ("user_xyz", "sig_456", -5.00)]
    insert_users(conn, user_rows)

    # 6) Query examples
    print("Total profit:", get_total_profit(conn))
    print("Average profit:", get_average_profit(conn))
    print("Top 10 largest profit transactions:", get_top_10_transactions(conn))
    print(
        "Most common tokens arbitraged:",
        get_most_common_tokens_arbitraged(conn, limit=5),
    )
    print("Top 10 users by profit:", get_top_10_users_by_profit(conn))


if __name__ == "__main__":
    # 1) Get a connection
    conn = get_connection()

    # main()

    # clear_database(conn)

    conn.close()
