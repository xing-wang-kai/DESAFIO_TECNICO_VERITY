from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )


def transform_orders(spark: SparkSession, input_rows: list[dict]) -> DataFrame:
    """Aplica as regras de negócio para transformar os dados de pedidos, gerando duas visões:  
    - latest_orders_df: visão detalhada do estado mais recente de cada pedido por data de negócio.
    - analytics_df: visão analítica diária consolidada por status.
    """
    df = spark.createDataFrame(input_rows)

    df = (
        df.withColumn("business_date", F.to_date("business_date"))
          .withColumn("ingested_at", F.to_timestamp("ingested_at"))
          .withColumn("amount", F.col("amount").cast("double"))
    )

    # 1) remove duplicação exata por event_id mantendo o mais recente ingerido
    w_event = Window.partitionBy("event_id").orderBy(F.col("ingested_at").desc())
    df_dedup_event = (
        df.withColumn("rn_event", F.row_number().over(w_event))
          .filter(F.col("rn_event") == 1)
          .drop("rn_event")
    )

    # 2) consolida o estado mais recente por order_id + business_date
    w_order = Window.partitionBy("order_id", "business_date").orderBy(
        F.col("ingested_at").desc(),
        F.col("event_id").desc()
    )

    latest_orders = (
        df_dedup_event
        .withColumn("rn_order", F.row_number().over(w_order))
        .filter(F.col("rn_order") == 1)
        .drop("rn_order")
    )

    # visão analítica consolidada diária
    analytics = (
        latest_orders.groupBy("business_date", "status")
        .agg(
            F.countDistinct("order_id").alias("orders_count"),
            F.countDistinct("customer_id").alias("customers_count"),
            F.round(F.sum("amount"), 2).alias("total_amount")
        )
        .orderBy("business_date", "status")
    )

    return latest_orders, analytics