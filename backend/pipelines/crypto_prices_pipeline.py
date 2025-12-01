import os
import json
import time
import urllib.parse
import urllib.request
import psycopg2
from dotenv import load_dotenv

API_URL = "https://api.coingecko.com/api/v3/simple/price"
COINS = [
    "bitcoin", "ethereum", "solana", "cardano", "polkadot",
    "dogecoin", "litecoin", "tron", "chainlink", "monero"
]
CURRENCY = "usd"


def _pg_uri():
    uri = os.environ.get("AIVEN_PG_URI") or os.environ.get("DATABASE_URL")
    if uri and uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)
    return uri


def connect_db():
    uri = _pg_uri()
    if not uri:
        raise RuntimeError("PostgreSQL connection string not set (AIVEN_PG_URI or DATABASE_URL)")
    return psycopg2.connect(uri)


def fetch_prices():
    params = {
        "ids": ",".join(COINS),
        "vs_currencies": CURRENCY,
        "include_market_cap": "false",
        "include_24hr_vol": "false",
        "include_24hr_change": "false",
    }
    url = API_URL + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "DataJourney/1.0"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS crypto_prices (
                id SERIAL PRIMARY KEY,
                coin TEXT,
                currency TEXT,
                price NUMERIC,
                fetched_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )
        # Ensure a natural uniqueness to avoid duplicate snapshots per run
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_crypto_coin_time ON crypto_prices(coin, fetched_at)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_crypto_coin ON crypto_prices(coin)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_crypto_fetched_at ON crypto_prices(fetched_at)"
        )
    conn.commit()


def run_pipeline():
    data = fetch_prices()
    rows = []
    # Use a single timestamp for the snapshot
    # Let the DB set fetched_at via DEFAULT NOW()
    for coin in COINS:
        price = data.get(coin, {}).get(CURRENCY)
        if price is None:
            continue
        rows.append((coin, CURRENCY, float(price)))

    conn = connect_db()
    ensure_table(conn)
    # Replace previous snapshot to avoid repetition in the preview
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE crypto_prices")
    conn.commit()
    with conn.cursor() as cur:
        # Upsert: if the same coin is inserted in the same fetched_at moment, skip duplicates
        cur.executemany(
            """
            INSERT INTO crypto_prices (coin, currency, price)
            VALUES (%s,%s,%s)
            ON CONFLICT (coin, fetched_at) DO NOTHING
            """,
            rows
        )
    conn.commit()
    conn.close()
    return len(rows)


if __name__ == "__main__":
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", "config", "config.env"))
    print("Starting Crypto Prices pipeline...")
    try:
        inserted = run_pipeline()
        print(f"Inserted {inserted} crypto price rows")
    except Exception as e:
        print(f"Pipeline failed: {e}")
