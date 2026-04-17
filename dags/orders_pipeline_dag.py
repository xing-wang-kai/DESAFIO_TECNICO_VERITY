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
    
    ## - [1] DEFINE VALORES PADRÕES COMO : DATA DE INICIO, DATA DE FIM, DATA DA EXECUÇÃO..
    # ds = context["ds"]  # data lógica da DAG, ex: 2026-03-16
    ds = "2026-04-11"
    execution_date = datetime.strptime(ds, "%Y-%m-%d").date()
    start_date = execution_date - timedelta(days=LOOKBACK_DAYS)
    end_date = execution_date

    start_date_str = start_date.isoformat()
    end_date_str = end_date.isoformat()

    ## - [2] FAZ A LEITURA DOS DADOS COM PANDAS NO BANCO SQLLITE, FILTRANDO PELA JANELA DE TEMPO DEFINIDA
    raw_pdf = read_orders_raw(
        db_path=SOURCE_DB,
        start_date=start_date_str,
        end_date=end_date_str,
    )

    ## - [3] VERIFICA SE EXISTEM DADOS PARA O PERÍODO, SE NÃO EXISTIR, IMPRIME UMA MENSAGEM E ENCERRA A EXECUÇÃO
    if raw_pdf.empty:
        print(f"Nenhum dado encontrado entre {start_date_str} e {end_date_str}")
        return

    # - [4] INICIA A SESSÃO DO SPARK.
    spark = get_spark(APP_NAME)

    try:
        
        # - [5] TRANSFORMA OS DADOS DE PEDIDOS, APLICANDO AS REGRAS DE NEGÓCIO DEFINIDAS, E GERANDO DUAS VISÕES:
        #     - latest_orders_df: visão detalhada do estado mais recente de cada pedido por data de negócio.
        #     - analytics_df: visão analítica diária consolidada por status.                    
        
        latest_orders_df, analytics_df = transform_orders(
            spark=spark,
            input_rows=raw_pdf.to_dict(orient="records"),
        )

        # - [6] GRAVA OS RESULTADOS ANALÍTICOS NO BANCO SQLITE SUBSTITUINDO APENAS O PERÍODO PROCESSADO, E GRAVA A VISÃO DETALHADA EM PARQUET NO DISCO, ORGANIZADO POR DATA DE NEGÓCIO.
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