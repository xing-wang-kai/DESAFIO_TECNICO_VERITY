from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from src.config import SOURCE_DB, TARGET_DB, OUTPUT_DIR, LOOKBACK_DAYS, APP_NAME
from src.extract import read_orders_raw
from src.transform import get_spark, transform_orders
from src.load import replace_window_in_sqlite, write_parquet


def run_pipeline(**context):
    ds = context["ds"]  # data lógica da DAG, ex: 2026-03-16
    execution_date = datetime.strptime(ds, "%Y-%m-%d").date()
    start_date = execution_date - timedelta(days=LOOKBACK_DAYS)
    end_date = execution_date

    start_date_str = start_date.isoformat()
    end_date_str = end_date.isoformat()

    raw_pdf = read_orders_raw(
        db_path=SOURCE_DB,
        start_date=start_date_str,
        end_date=end_date_str,
    )

    if raw_pdf.empty:
        print(f"Nenhum dado encontrado entre {start_date_str} e {end_date_str}")
        return

    spark = get_spark(APP_NAME)

    try:
        latest_orders_df, analytics_df = transform_orders(
            spark=spark,
            input_rows=raw_pdf.to_dict(orient="records"),
        )

        analytics_pdf = analytics_df.toPandas()

        replace_window_in_sqlite(
            db_path=TARGET_DB,
            analytics_pdf=analytics_pdf,
            start_date=start_date_str,
            end_date=end_date_str,
        )

        write_parquet(latest_orders_df, OUTPUT_DIR)

        print("Pipeline executado com sucesso")
        print(analytics_pdf)

    finally:
        spark.stop()


with DAG(
    dag_id="orders_analytics_pipeline",
    start_date=days_ago(3),  ## ESSA DATA SERÁ O PROCESSAMENTO DOS START DATE TAMBÉM
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "challenge",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["challenge", "airflow", "pyspark"],
) as dag:

    process_orders = PythonOperator(
        task_id="process_orders_window",
        python_callable=run_pipeline,
        provide_context=True,
    )

    process_orders