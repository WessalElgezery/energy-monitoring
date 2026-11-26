"""
MySQL writer for Spark microbatches.
Uses MySQL INSERT ... ON DUPLICATE KEY UPDATE for retry-safe writes.
For this local project, the microbatch is collected to the driver and written in
small executemany batches. For large production streams, use a distributed staging
and merge strategy instead.
"""
import os
from datetime import date, datetime
from importlib import import_module
from processing import transform_batch

try:
    mysql_connector = import_module("mysql.connector")
except ImportError as exc:
    mysql_connector = None
    MYSQL_IMPORT_ERROR = exc

def _clean_value(v):
    # Spark Row values are Python scalars/None. mysql-connector accepts these.
    if hasattr(v, "asDict"):
        return v.asDict(recursive=True)
    return v

def _rows(df):
    return [tuple(_clean_value(x) for x in row) for row in df.collect()]

def write_microbatch_to_mysql(batch_df, batch_id, mysql_url=None, user=None, password=None):
    # Build the destination payloads from the bounded microbatch.
    outputs = transform_batch(batch_df)
    # Avoid opening a DB transaction for empty data.
    if batch_df.isEmpty():
        return

    host = os.getenv("MYSQL_HOST", "mysql")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    database = os.getenv("MYSQL_DATABASE", "energy_monitoring")
    user = user or os.getenv("MYSQL_USER", "root")
    password = password if password is not None else os.getenv("MYSQL_PASSWORD", "")
    conn = mysql_connector.connect(host=host, port=port, database=database,
                                   user=user, password=password)
    try:
        cur = conn.cursor()
        # 1) House readings (event_id primary key).
        house_rows = _rows(outputs["house"])
        if house_rows:
            cur.executemany("""
                INSERT INTO house_readings
                (event_id,event_time,voltage_v,current_a,whole_house_power,issues)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                event_time=VALUES(event_time), voltage_v=VALUES(voltage_v),
                current_a=VALUES(current_a), whole_house_power=VALUES(whole_house_power),
                issues=VALUES(issues)
            """, house_rows)

        # 2) Appliance rows. IDs are event_id + appliance name to satisfy PK.
        appliance_rows = _rows(outputs["appliance"])
        if appliance_rows:
            cur.executemany("""
                INSERT INTO appliance_readings
                (event_id,event_time,appliance_name,power)
                VALUES (%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                event_time=VALUES(event_time), appliance_name=VALUES(appliance_name),
                power=VALUES(power)
            """, appliance_rows)

        # 3) Per-microbatch hourly appliance partial aggregates.
        usage_rows = [
            (int(batch_id), *row) for row in _rows(outputs["usage_summary"])
        ]
        if usage_rows:
            cur.executemany("""
                INSERT INTO usage_summary
                (batch_id,reading_date,hour_of_day,appliance_name,sum_power,max_power,
                 min_power,reading_count,total_rows)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                sum_power=VALUES(sum_power), max_power=VALUES(max_power),
                min_power=VALUES(min_power), reading_count=VALUES(reading_count),
                total_rows=VALUES(total_rows)
            """, usage_rows)

        # 4) Per-microbatch house partial aggregates.
        house_summary_rows = [
            (int(batch_id), *row) for row in _rows(outputs["house_summary"])
        ]
        if house_summary_rows:
            cur.executemany("""
                INSERT INTO house_summary
                (batch_id,reading_date,hour_of_day,power_sum,power_count,max_power,min_power,
                 voltage_sum,voltage_count,max_voltage,min_voltage,current_sum,current_count,
                 max_current,min_current,issues_count,total_rows)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                power_sum=VALUES(power_sum), power_count=VALUES(power_count),
                max_power=VALUES(max_power), min_power=VALUES(min_power),
                voltage_sum=VALUES(voltage_sum), voltage_count=VALUES(voltage_count),
                max_voltage=VALUES(max_voltage), min_voltage=VALUES(min_voltage),
                current_sum=VALUES(current_sum), current_count=VALUES(current_count),
                max_current=VALUES(max_current), min_current=VALUES(min_current),
                issues_count=VALUES(issues_count), total_rows=VALUES(total_rows)
            """, house_summary_rows)

        # 5) Alerts keyed by deterministic alert_key.
        alert_rows = _rows(outputs["alerts"])
        if alert_rows:
            cur.executemany("""
                INSERT INTO energy_alerts
                (alert_key,alert_time,source_event_time,alert_type,severity,appliance_name,
                 measured_value,threshold_value,message)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                alert_time=VALUES(alert_time), source_event_time=VALUES(source_event_time),
                alert_type=VALUES(alert_type), severity=VALUES(severity),
                appliance_name=VALUES(appliance_name), measured_value=VALUES(measured_value),
                threshold_value=VALUES(threshold_value), message=VALUES(message)
            """, alert_rows)
        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
