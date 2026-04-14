from src.transform import get_spark, transform_orders


def test_transform_orders_deduplicates_and_keeps_latest():
    spark = get_spark("test-job")

    rows = [
        {
            "event_id": 1,
            "order_id": 1001,
            "customer_id": 501,
            "status": "pending",
            "amount": 100.0,
            "business_date": "2026-03-15",
            "ingested_at": "2026-03-15 08:00:00",
            "source_file": "a",
        },
        {
            "event_id": 2,
            "order_id": 1001,
            "customer_id": 501,
            "status": "paid",
            "amount": 100.0,
            "business_date": "2026-03-15",
            "ingested_at": "2026-03-15 09:00:00",
            "source_file": "b",
        },
    ]

    latest_df, analytics_df = transform_orders(spark, rows)
    latest = latest_df.collect()

    assert len(latest) == 1
    assert latest[0]["status"] == "paid"

    spark.stop()