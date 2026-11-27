"""
MySQL writer for Spark microbatches.
Uses MySQL INSERT ... ON DUPLICATE KEY UPDATE for retry-safe writes.
Microbatches are collected to the driver for this local coursework project.
"""
import os
from importlib import import_module

from processing import transform_batch

USAGE_COLS = [
    "reading_date", "hour_of_day", "appliance_name", "sum_power",
    "max_power", "min_power", "reading_count", "total_rows"
]

HOUSE_SUMMARY_COLS = [
    "reading_date", "hour_of_day", "power_sum", "power_count",
    "max_power", "min_power", "voltage_sum", "voltage_count",
    "max_voltage", "min_voltage", "current_sum", "current_count",
    "max_current", "min_current", "issues_count", "total_rows"
]

try:
    mysql_connector = import_module("mysql.connector")
except ImportError as exc:
    mysql_connector = None
    MYSQL_IMPORT_ERROR = exc


def _rows(df):
    return [tuple(row) for row in df.collect()]


def write_microbatch_to_mysql(batch_df, batch_id, mysql_url=None,
                              user=None, password=None):
    if mysql_connector is None:
        raise RuntimeError(
            "mysql-connector-python is missing. Install it in the Spark environment."
        ) from MYSQL_IMPORT_ERROR

    if batch_df.isEmpty():
        return

    outputs = transform_batch(batch_df)

    host = os.getenv("MYSQL_HOST", "mysql")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    database = os.getenv("MYSQL_DATABASE", "energy_monitoring")
    user = user or os.getenv("MYSQL_USER", "root")
    password = password if password is not None else os.getenv("MYSQL_PASSWORD", "")

    conn = mysql_connector.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    try:
        cur = conn.cursor()

        # 1. Original house readings.
        house_rows = _rows(outputs["house"])
        if house_rows:
            cur.executemany("""
                INSERT INTO house_readings
                (event_id,event_time,voltage_v,current_a,whole_house_power,issues)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                event_time=VALUES(event_time),
                voltage_v=VALUES(voltage_v),
                current_a=VALUES(current_a),
                whole_house_power=VALUES(whole_house_power),
                issues=VALUES(issues)
            """, house_rows)

        # 2. Appliance readings in long format.
        appliance_rows = _rows(outputs["appliance"])
        if appliance_rows:
            cur.executemany("""
                INSERT INTO appliance_readings
                (event_id,event_time,appliance_name,power)
                VALUES (%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                event_time=VALUES(event_time),
                appliance_name=VALUES(appliance_name),
                power=VALUES(power)
            """, appliance_rows)

        # 3. Hourly appliance partial aggregates.
        usage_rows = [
            (int(batch_id), *row)
            for row in _rows(
                outputs["usage_summary"].select(*USAGE_COLS)
            )
        ]
        if usage_rows:
            cur.executemany("""
                INSERT INTO usage_summary
                (batch_id,reading_date,hour_of_day,appliance_name,sum_power,
                 max_power,min_power,reading_count,total_rows)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                sum_power=VALUES(sum_power),
                max_power=VALUES(max_power),
                min_power=VALUES(min_power),
                reading_count=VALUES(reading_count),
                total_rows=VALUES(total_rows)
            """, usage_rows)

        # 4. Hourly whole-house partial aggregates.
        house_summary_rows = [
            (int(batch_id), *row)
            for row in _rows(
                outputs["house_summary"].select(*HOUSE_SUMMARY_COLS)
            )
        ]
        if house_summary_rows:
            cur.executemany("""
                INSERT INTO house_summary
                (batch_id,reading_date,hour_of_day,power_sum,power_count,
                 max_power,min_power,voltage_sum,voltage_count,max_voltage,
                 min_voltage,current_sum,current_count,max_current,min_current,
                 issues_count,total_rows)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                power_sum=VALUES(power_sum),
                power_count=VALUES(power_count),
                max_power=VALUES(max_power),
                min_power=VALUES(min_power),
                voltage_sum=VALUES(voltage_sum),
                voltage_count=VALUES(voltage_count),
                max_voltage=VALUES(max_voltage),
                min_voltage=VALUES(min_voltage),
                current_sum=VALUES(current_sum),
                current_count=VALUES(current_count),
                max_current=VALUES(max_current),
                min_current=VALUES(min_current),
                issues_count=VALUES(issues_count),
                total_rows=VALUES(total_rows)
            """, house_summary_rows)

        # 5. Alerts, deduplicated by alert_key.
        alert_rows = _rows(outputs["alerts"])
        if alert_rows:
            cur.executemany("""
                INSERT INTO energy_alerts
                (alert_key,alert_time,source_event_time,alert_type,severity,
                 appliance_name,measured_value,threshold_value,message)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                alert_time=VALUES(alert_time),
                source_event_time=VALUES(source_event_time),
                alert_type=VALUES(alert_type),
                severity=VALUES(severity),
                appliance_name=VALUES(appliance_name),
                measured_value=VALUES(measured_value),
                threshold_value=VALUES(threshold_value),
                message=VALUES(message)
            """, alert_rows)

        conn.commit()
        cur.close()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()
