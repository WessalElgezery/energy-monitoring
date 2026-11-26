"""
Schema, JSON parsing, timestamp handling, and batch transformations.
"""
from pyspark.sql import functions as F, types as T
import os

APPLIANCES = ["ac_1", "kettle", "boiler", "fridge", "washing_machine"]

INPUT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType(), True),
    T.StructField("timestamp", T.StringType(), True),
    T.StructField("V", T.DoubleType(), True),
    T.StructField("A", T.DoubleType(), True),
    T.StructField("P_agg", T.DoubleType(), True),
    T.StructField("ac_1", T.DoubleType(), True),
    T.StructField("kettle", T.DoubleType(), True),
    T.StructField("boiler", T.DoubleType(), True),
    T.StructField("fridge", T.DoubleType(), True),
    T.StructField("washing_machine", T.DoubleType(), True),
    T.StructField("issues", T.IntegerType(), True),
])

def parse_and_transform(kafka_stream):
    json_text = kafka_stream.select(F.col("value").cast("string").alias("json"))
    parsed = json_text.select(F.from_json("json", INPUT_SCHEMA).alias("r")).select("r.*")
    parsed = parsed.withColumn("event_time", F.coalesce(
        F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss.SSS"),
        F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS"),
    ))
    # House 2 has 10-second sampling and timestamp is expected to be unique.
    parsed = parsed.withColumn(
        "event_id", F.coalesce("event_id", F.date_format("event_time", "yyyyMMddHHmmssSSS"))
    )
    # NiFi is the validator; this is a guard against unparseable timestamps.
    return parsed.filter(F.col("event_time").isNotNull() & F.col("event_id").isNotNull())

def transform_batch(batch_df):
    """Return DataFrames shaped for each of the five MySQL tables."""
    with_time = batch_df.withColumn("reading_date", F.to_date("event_time")) \
                        .withColumn("hour_of_day", F.hour("event_time"))
    house = batch_df.select(
        "event_id", "event_time", F.col("V").alias("voltage_v"),
        F.col("A").alias("current_a"), F.col("P_agg").alias("whole_house_power"),
        F.col("issues").cast("integer").alias("issues")
    )
    appliance_parts = [
        batch_df.select(
            F.concat(F.col("event_id"), F.lit("_"), F.lit(a)).alias("event_id"),
            F.col("event_time"),
            F.lit(a).alias("appliance_name"),
            F.col(a).alias("power"),
        )
        for a in APPLIANCES   # your loop variable and list name may differ
    ]
    appliance = appliance_parts[0]
    for part in appliance_parts[1:]:
        appliance = appliance.unionByName(part)

    long_parts = [with_time.select(
        "reading_date", "hour_of_day", F.lit(name).alias("appliance_name"),
        F.col(name).alias("power")
    ) for name in APPLIANCES]
    long_df = long_parts[0]
    for part in long_parts[1:]:
        long_df = long_df.unionByName(part)
    usage = long_df.groupBy("reading_date", "hour_of_day", "appliance_name").agg(
        F.coalesce(F.sum("power"), F.lit(0.0)).alias("sum_power"),
        F.max("power").alias("max_power"),        # <- put this line back
        F.min("power").alias("min_power"),
        F.count("power").alias("reading_count"),
        F.count("*").alias("total_rows"),
    )

    house_summary = with_time.groupBy("reading_date", "hour_of_day").agg(
        F.coalesce(F.sum("P_agg"), F.lit(0.0)).alias("power_sum"), F.count("P_agg").alias("power_count"),
        F.max("P_agg").alias("max_power"), F.min("P_agg").alias("min_power"),
        F.coalesce(F.sum("V"), F.lit(0.0)).alias("voltage_sum"), F.count("V").alias("voltage_count"),
        F.max("V").alias("max_voltage"), F.min("V").alias("min_voltage"),
        F.coalesce(F.sum("A"), F.lit(0.0)).alias("current_sum"), F.count("A").alias("current_count"),
        F.max("A").alias("max_current"), F.min("A").alias("min_current"),
        F.sum(F.when(F.col("issues") == 1, 1).otherwise(0)).alias("issues_count"),
        F.count("*").alias("total_rows")
    )
    alerts = build_alerts(batch_df)
    return {"house": house, "appliance": appliance, "usage_summary": usage,
            "house_summary": house_summary, "alerts": alerts}

def build_alerts(batch_df):
    high_power = float(os.getenv("ALERT_HIGH_POWER_W", "2500"))
    low_v = float(os.getenv("ALERT_LOW_VOLTAGE_V", "210"))
    high_v = float(os.getenv("ALERT_HIGH_VOLTAGE_V", "250"))
    eligible = batch_df.filter(
        (F.col("P_agg") >= F.lit(high_power)) |
        (F.col("V") < F.lit(low_v)) | (F.col("V") > F.lit(high_v))
    )
    return eligible.withColumn("alert_time", F.current_timestamp()).withColumn(
        "alert_type", F.when(F.col("P_agg") >= F.lit(high_power), "HIGH_HOUSE_POWER")
        .when(F.col("V") < F.lit(low_v), "LOW_VOLTAGE").otherwise("HIGH_VOLTAGE")
    ).withColumn("severity", F.lit("warning")).withColumn(
        "appliance_name", F.lit(None).cast("string")
    ).withColumn("measured_value", F.when(F.col("alert_type") == "HIGH_HOUSE_POWER", F.col("P_agg")).otherwise(F.col("V"))
    ).withColumn("threshold_value", F.when(F.col("alert_type") == "HIGH_HOUSE_POWER", F.lit(high_power))
        .when(F.col("alert_type") == "LOW_VOLTAGE", F.lit(low_v)).otherwise(F.lit(high_v))
    ).withColumn("alert_key", F.concat_ws("|", F.col("event_id"), F.col("alert_type"))
    ).withColumn("message", F.concat(F.col("alert_type"), F.lit(" detected for event "), F.col("event_id"))
    ).select("alert_key", "alert_time", F.col("event_time").alias("source_event_time"),
        "alert_type", "severity", "appliance_name", "measured_value",
        "threshold_value", "message")
