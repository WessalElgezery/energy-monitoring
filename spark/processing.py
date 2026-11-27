"""
Schema, JSON parsing, timestamp handling, and batch transformations.
"""
import os

from pyspark.sql import functions as F, types as T


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
    json_text = kafka_stream.select(
        F.col("value").cast("string").alias("json")
    )

    parsed = json_text.select(
        F.from_json("json", INPUT_SCHEMA).alias("r")
    ).select("r.*")

    parsed = parsed.withColumn(
        "event_time",
        F.coalesce(
            F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss"),
            F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss.SSS"),
            F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS"),
        )
    )

    # House 2 uses 10-second sampling; timestamp is expected to be unique.
    parsed = parsed.withColumn(
        "event_id",
        F.coalesce(
            F.col("event_id"),
            F.date_format("event_time", "yyyyMMddHHmmssSSS")
        )
    )

    # NiFi validates records; this is an additional Spark safety check.
    return parsed.filter(
        F.col("event_time").isNotNull() &
        F.col("event_id").isNotNull()
    )


def transform_batch(batch_df):
    """Return DataFrames shaped for the five MySQL tables."""

    with_time = (
        batch_df
        .withColumn("reading_date", F.to_date("event_time"))
        .withColumn("hour_of_day", F.hour("event_time"))
    )

    # 1. One house_readings row per original event.
    house = batch_df.select(
        "event_id",
        "event_time",
        F.col("V").alias("voltage_v"),
        F.col("A").alias("current_a"),
        F.col("P_agg").alias("whole_house_power"),
        F.col("issues").cast("integer").alias("issues")
    )

    # 2. Convert appliance columns into long format.
    appliance_parts = [
        batch_df.select(
            F.concat(
                F.col("event_id"), F.lit("_"), F.lit(name)
            ).alias("event_id"),
            F.col("event_time"),
            F.lit(name).alias("appliance_name"),
            F.col(name).alias("power")
        )
        for name in APPLIANCES
    ]

    appliance = appliance_parts[0]
    for part in appliance_parts[1:]:
        appliance = appliance.unionByName(part)

    # 3. Hourly appliance summaries.
    long_parts = [
        with_time.select(
            "reading_date",
            "hour_of_day",
            F.lit(name).alias("appliance_name"),
            F.col(name).alias("power")
        )
        for name in APPLIANCES
    ]

    long_df = long_parts[0]
    for part in long_parts[1:]:
        long_df = long_df.unionByName(part)

    usage = long_df.groupBy(
        "reading_date", "hour_of_day", "appliance_name"
    ).agg(
        F.sum("power").alias("sum_power"),
        F.max("power").alias("max_power"),
        F.min("power").alias("min_power"),
        F.count("power").alias("reading_count"),
        F.count("*").alias("total_rows")
    )

    # 4. Hourly whole-house summaries.
    # SUM stays NULL if every measurement in that group is NULL.
    house_summary = with_time.groupBy(
        "reading_date", "hour_of_day"
    ).agg(
        F.sum("P_agg").alias("power_sum"),
        F.count("P_agg").alias("power_count"),
        F.max("P_agg").alias("max_power"),
        F.min("P_agg").alias("min_power"),

        F.sum("V").alias("voltage_sum"),
        F.count("V").alias("voltage_count"),
        F.max("V").alias("max_voltage"),
        F.min("V").alias("min_voltage"),

        F.sum("A").alias("current_sum"),
        F.count("A").alias("current_count"),
        F.max("A").alias("max_current"),
        F.min("A").alias("min_current"),

        F.sum(
            F.when(F.col("issues") == 1, 1).otherwise(0)
        ).alias("issues_count"),

        F.count("*").alias("total_rows")
    )

    alerts = build_alerts(batch_df)

    return {
        "house": house,
        "appliance": appliance,
        "usage_summary": usage,
        "house_summary": house_summary,
        "alerts": alerts
    }


def build_alerts(batch_df):
    """Generate a separate alert for every triggered condition."""

    high_power = float(os.getenv("ALERT_HIGH_POWER_W", "2500"))
    low_v = float(os.getenv("ALERT_LOW_VOLTAGE_V", "210"))
    high_v = float(os.getenv("ALERT_HIGH_VOLTAGE_V", "250"))

    def make_alert(condition, alert_type, measured_column, threshold):
        return (
            batch_df.filter(condition)
            .select(
                F.concat_ws(
                    "|", F.col("event_id"), F.lit(alert_type)
                ).alias("alert_key"),
                F.current_timestamp().alias("alert_time"),
                F.col("event_time").alias("source_event_time"),
                F.lit(alert_type).alias("alert_type"),
                F.lit("warning").alias("severity"),
                F.lit(None).cast("string").alias("appliance_name"),
                F.col(measured_column).alias("measured_value"),
                F.lit(threshold).cast("double").alias("threshold_value"),
                F.concat(
                    F.lit(alert_type + " detected for event "),
                    F.col("event_id")
                ).alias("message")
            )
        )

    high_power_alerts = make_alert(
        F.col("P_agg") >= F.lit(high_power),
        "HIGH_HOUSE_POWER",
        "P_agg",
        high_power
    )

    low_voltage_alerts = make_alert(
        F.col("V") < F.lit(low_v),
        "LOW_VOLTAGE",
        "V",
        low_v
    )

    high_voltage_alerts = make_alert(
        F.col("V") > F.lit(high_v),
        "HIGH_VOLTAGE",
        "V",
        high_v
    )

    return (
        high_power_alerts
        .unionByName(low_voltage_alerts)
        .unionByName(high_voltage_alerts)
    )
