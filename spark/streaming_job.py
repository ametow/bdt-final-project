"""
Spark Structured Streaming job.

Pipeline:
  Kafka(trades) -> parse JSON -> join static symbols.csv from HDFS (Spark SQL)
  -> 1-minute windowed aggregations per symbol -> HBase via Thrift (happybase)

Two HBase tables are written via foreachBatch:
  * trade_agg     rowkey = "<symbol>#<window_end_ms>" (cf:avg_price, vwap,
                  volume, count, min_price, max_price, anomaly, name, category)
  * trade_latest  rowkey = "<symbol>" (cf:price, qty, trade_time)
"""

import os

import happybase
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "trades")
HBASE_HOST = os.environ.get("HBASE_HOST", "hbase")
HBASE_PORT = int(os.environ.get("HBASE_PORT", "9090"))
HDFS_SYMBOLS = os.environ.get(
    "HDFS_SYMBOLS", "hdfs://namenode:9000/seed/symbols.csv"
)

TRADE_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("qty", DoubleType()),
    StructField("trade_id", LongType()),
    StructField("trade_time", LongType()),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bdt-trade-stream")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
        .getOrCreate()
    )


def load_symbols(spark: SparkSession):
    """Static reference dataset from HDFS (bonus Part 5)."""
    return (
        spark.read.option("header", "true").csv(HDFS_SYMBOLS)
        .select("symbol", "name", "category")
    )


def write_batch_to_hbase(batch_df, batch_id):
    """Write a micro-batch of aggregations to HBase tables."""
    rows = batch_df.collect()
    if not rows:
        return

    conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT, autoconnect=True)
    try:
        agg = conn.table("trade_agg")
        latest = conn.table("trade_latest")

        with agg.batch(batch_size=200) as agg_batch, \
                latest.batch(batch_size=200) as latest_batch:
            for r in rows:
                window_end_ms = int(r["window_end"].timestamp() * 1000)
                rk = f"{r['symbol']}#{window_end_ms}".encode()
                agg_batch.put(rk, {
                    b"cf:symbol": str(r["symbol"]).encode(),
                    b"cf:window_start": str(int(r["window_start"].timestamp() * 1000)).encode(),
                    b"cf:window_end": str(window_end_ms).encode(),
                    b"cf:avg_price": str(r["avg_price"]).encode(),
                    b"cf:vwap": str(r["vwap"]).encode(),
                    b"cf:min_price": str(r["min_price"]).encode(),
                    b"cf:max_price": str(r["max_price"]).encode(),
                    b"cf:volume": str(r["volume"]).encode(),
                    b"cf:count": str(r["trade_count"]).encode(),
                    b"cf:anomaly": str(bool(r["anomaly"])).encode(),
                    b"cf:name": str(r["name"] or "").encode(),
                    b"cf:category": str(r["category"] or "").encode(),
                })
                latest_batch.put(str(r["symbol"]).encode(), {
                    b"cf:price": str(r["last_price"]).encode(),
                    b"cf:qty": str(r["last_qty"]).encode(),
                    b"cf:trade_time": str(window_end_ms).encode(),
                    b"cf:name": str(r["name"] or "").encode(),
                    b"cf:category": str(r["category"] or "").encode(),
                })
    finally:
        conn.close()


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    symbols_df = load_symbols(spark).cache()
    symbols_df.show(truncate=False)

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json")
        .select(F.from_json("json", TRADE_SCHEMA).alias("t"))
        .select("t.*")
        .withColumn("event_time", (F.col("trade_time") / 1000).cast("timestamp"))
        .withWatermark("event_time", "30 seconds")
    )

    # Spark SQL join with static reference (Part 5).
    enriched = parsed.join(symbols_df, on="symbol", how="left")

    agg = (
        enriched
        .groupBy(
            F.window("event_time", "1 minute"),
            F.col("symbol"),
            F.col("name"),
            F.col("category"),
        )
        .agg(
            F.avg("price").alias("avg_price"),
            (F.sum(F.col("price") * F.col("qty")) / F.sum("qty")).alias("vwap"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.sum("qty").alias("volume"),
            F.count(F.lit(1)).alias("trade_count"),
            F.stddev_pop("price").alias("stddev_price"),
            F.last("price").alias("last_price"),
            F.last("qty").alias("last_qty"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "symbol", "name", "category",
            "avg_price", "vwap", "min_price", "max_price",
            "volume", "trade_count", "last_price", "last_qty",
            (F.abs(F.col("last_price") - F.col("avg_price"))
             > (F.coalesce(F.col("stddev_price"), F.lit(0.0)) * F.lit(3.0))).alias("anomaly"),
        )
    )

    query = (
        agg.writeStream
        .outputMode("update")
        .option("checkpointLocation", "/tmp/spark-ckpt-trades")
        .foreachBatch(write_batch_to_hbase)
        .trigger(processingTime="15 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
