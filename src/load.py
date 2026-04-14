import pandas as pd
from pathlib import Path
from src.utils import sqlite_connection


def ensure_target_table(db_path):
    ddl = """
    CREATE TABLE IF NOT EXISTS orders_analytics (
        business_date TEXT NOT NULL,
        status TEXT NOT NULL,
        orders_count INTEGER NOT NULL,
        customers_count INTEGER NOT NULL,
        total_amount REAL NOT NULL,
        PRIMARY KEY (business_date, status)
    )
    """
    with sqlite_connection(db_path) as conn:
        conn.execute(ddl)



def replace_window_in_sqlite(db_path, analytics_pdf: pd.DataFrame, start_date: str, end_date: str):
    """Substitui os dados na tabela orders_analytics para o período especificado por start_date e end_date, e insere os novos dados do analytics_pdf.
    
    Args:   
        db_path (str): Caminho para o banco de dados SQLite.
        analytics_pdf (pd.DataFrame): DataFrame contendo os dados analíticos a serem inseridos.
        start_date (str): Data de início do período a ser substituído, no formato 'YYYY-MM-DD'.
        end_date (str): Data de fim do período a ser substituído, no formato 'YYYY-MM-DD'.
    """
    ensure_target_table(db_path)

    with sqlite_connection(db_path) as conn:
        conn.execute(
            """
            DELETE FROM orders_analytics
            WHERE date(business_date) BETWEEN date(?) AND date(?)
            """,
            [start_date, end_date],
        )

        analytics_pdf.to_sql("orders_analytics", conn, if_exists="append", index=False)



def write_parquet(df, output_dir: Path):
    (
        df.write
        .mode("overwrite")
        .partitionBy("business_date")
        .parquet(str(output_dir))
    )