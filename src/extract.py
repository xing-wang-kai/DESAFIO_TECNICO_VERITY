import pandas as pd
from src.utils import sqlite_connection


def read_orders_raw(db_path, start_date: str, end_date: str) -> pd.DataFrame:
    query = """
        SELECT
            event_id,
            order_id,
            customer_id,
            status,
            amount,
            business_date,
            ingested_at,
            source_file
        FROM orders_raw
        WHERE date(business_date) BETWEEN date(?) AND date(?)
           OR date(ingested_at) BETWEEN date(?) AND date(?)
    """

    with sqlite_connection(db_path) as conn:
        df = pd.read_sql_query(
            query,
            conn,
            params=[start_date, end_date, start_date, end_date],
        )

    return df