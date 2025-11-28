# dags/stock_etl.py

import os
import logging
from datetime import datetime

import requests
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_env_var(name: str, required: bool = True, default=None):
    value = os.getenv(name, default)
    if required and not value:
        msg = f"Required environment variable {name} is not set"
        logger.error(msg)
        raise RuntimeError(msg)
    return value


def fetch_stock_data():
    """
    Fetch daily stock data from Alpha Vantage API.
    Returns a list of dict rows to insert into DB.
    """
    api_key = get_env_var("STOCK_API_KEY")
    symbol = get_env_var("STOCK_API_SYMBOL", required=False, default="IBM")
    base_url = get_env_var(
        "STOCK_API_BASE_URL",
        required=False,
        default="https://www.alphavantage.co/query",
    )

    params = {
    "function": "TIME_SERIES_DAILY",
    "symbol": symbol,
    "apikey": api_key,
    }


    try:
        logger.info("Requesting data from Alpha Vantage for symbol=%s", symbol)
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.exception("HTTP error while fetching stock data: %s", e)
        raise

    try:
        data = response.json()
    except ValueError as e:
        logger.exception("Failed to parse JSON response: %s", e)
        raise

    # Handle API error responses / missing data
    if "Error Message" in data:
        msg = f"API returned error: {data['Error Message']}"
        logger.error(msg)
        raise RuntimeError(msg)

    time_series = data.get("Time Series (Daily)")
    if not time_series:
        msg = "No 'Time Series (Daily)' key found in API response"
        logger.error(msg)
        raise RuntimeError(msg)

    rows = []
    for date_str, values in time_series.items():
        try:
            ts = datetime.strptime(date_str, "%Y-%m-%d").date()
            open_price = float(values.get("1. open")) if values.get("1. open") else None
            high = float(values.get("2. high")) if values.get("2. high") else None
            low = float(values.get("3. low")) if values.get("3. low") else None
            close = float(values.get("4. close")) if values.get("4. close") else None
            adj_close = (
                float(values.get("5. adjusted close"))
                if values.get("5. adjusted close")
                else None
            )
            volume = int(values.get("6. volume")) if values.get("6. volume") else None

            # Skip rows with totally missing core fields
            if all(v is None for v in (open_price, high, low, close, adj_close, volume)):
                logger.warning("Skipping date %s due to missing values", date_str)
                continue

            rows.append(
                {
                    "symbol": symbol,
                    "ts": ts,
                    "open": open_price,
                    "high": high,
                    "low": low,
                    "close": close,
                    "adjusted_close": adj_close,
                    "volume": volume,
                }
            )
        except Exception as e:
            # Log but continue with other days
            logger.exception("Error parsing data for date %s: %s", date_str, e)

    if not rows:
        raise RuntimeError("No valid rows parsed from API response")

    logger.info("Parsed %d rows of stock data for %s", len(rows), symbol)
    return rows


def update_postgres(rows):
    """
    Upsert rows into the stock_prices table.
    """
    host = get_env_var("POSTGRES_HOST")
    port = get_env_var("POSTGRES_PORT", required=False, default="5432")
    dbname = get_env_var("POSTGRES_DB")
    user = get_env_var("POSTGRES_USER")
    password = get_env_var("POSTGRES_PASSWORD")

    conn = None
    try:
        conn = psycopg2.connect(
            host=host, port=port, dbname=dbname, user=user, password=password
        )
        conn.autocommit = False

        insert_query = """
            INSERT INTO stock_prices (
                symbol, ts, open, high, low, close, adjusted_close, volume
            )
            VALUES %s
            ON CONFLICT (symbol, ts) DO UPDATE
            SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                adjusted_close = EXCLUDED.adjusted_close,
                volume = EXCLUDED.volume;
        """

        values = [
            (
                r["symbol"],
                r["ts"],
                r["open"],
                r["high"],
                r["low"],
                r["close"],
                r["adjusted_close"],
                r["volume"],
            )
            for r in rows
        ]

        with conn.cursor() as cur:
            execute_values(cur, insert_query, values)
        conn.commit()
        logger.info("Successfully upserted %d rows into stock_prices", len(rows))

    except Exception as e:
        if conn:
            conn.rollback()
        logger.exception("Error updating PostgreSQL: %s", e)
        raise
    finally:
        if conn:
            conn.close()


def run_stock_etl(**context):
    """
    Main function to be used in Airflow DAG.
    Wraps everything with robust error handling.
    """
    logger.info("Starting stock ETL job")
    try:
        rows = fetch_stock_data()
        update_postgres(rows)
        logger.info("Stock ETL job completed successfully")
    except Exception as e:
        logger.exception("Stock ETL job failed: %s", e)
        # Let Airflow handle retries, mark task as failed
        raise
