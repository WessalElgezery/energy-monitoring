-- energy monitoring system: database structure
-- five tables only (frozen project decision):
--   house_readings, appliance_readings, usage_summary, house_summary, energy_alerts
--
-- rules enforced here:
--   * measurement columns allow null (missing values are never replaced with zero)
--   * issues is stored as received (issues = 1 is still valid data)
--   * datetime (not timestamp) is used so values are never shifted by timezone conversion
--   * every table has a key that makes spark retries safe (upsert / duplicate-safe insert)
--
-- the database itself is created by the mysql container (MYSQL_DATABASE),
-- so this file does not create or select a database.

-- ---------------------------------------------------------------
-- 1. house_readings: one row per original csv record
-- ---------------------------------------------------------------
create table if not exists house_readings (
    event_id          varchar(100) not null,
    event_time        datetime     not null,
    voltage_v         double       null,
    current_a         double       null,
    whole_house_power double       null,
    issues            tinyint      null,
    primary key (event_id),
    index idx_house_readings_time (event_time)
) engine=InnoDB default charset=utf8mb4;

-- ---------------------------------------------------------------
-- 2. appliance_readings: long format, five rows per csv record
--    (ac_1, kettle, boiler, fridge, washing_machine)
-- ---------------------------------------------------------------
create table if not exists appliance_readings (
    event_id       varchar(150) not null,
    event_time     datetime     not null,
    appliance_name varchar(50)  not null,
    power          double       null,
    primary key (event_id),
    unique key uq_appliance_time_name (event_time, appliance_name),
    index idx_appliance_name (appliance_name)
) engine=InnoDB default charset=utf8mb4;

-- ---------------------------------------------------------------
-- 3. usage_summary: hourly appliance partial aggregates per spark batch
--    final averages must be computed as sum(sum_power) / sum(reading_count)
-- ---------------------------------------------------------------
create table if not exists usage_summary (
    id             bigint      not null auto_increment,
    batch_id       bigint      not null,
    reading_date   date        not null,
    hour_of_day    tinyint     not null,
    appliance_name varchar(50) not null,
    sum_power      double      null,
    max_power      double      null,
    min_power      double      null,
    reading_count  bigint      not null,
    total_rows     bigint      not null,
    primary key (id),
    unique key uq_usage_summary (batch_id, reading_date, hour_of_day, appliance_name),
    index idx_usage_summary_hour (reading_date, hour_of_day)
) engine=InnoDB default charset=utf8mb4;

-- ---------------------------------------------------------------
-- 4. house_summary: hourly whole-house partial aggregates per spark batch
--    each metric keeps its own count because missing-value patterns differ
-- ---------------------------------------------------------------
create table if not exists house_summary (
    id            bigint  not null auto_increment,
    batch_id      bigint  not null,
    reading_date  date    not null,
    hour_of_day   tinyint not null,
    power_sum     double  null,
    power_count   bigint  not null,
    max_power     double  null,
    min_power     double  null,
    voltage_sum   double  null,
    voltage_count bigint  not null,
    max_voltage   double  null,
    min_voltage   double  null,
    current_sum   double  null,
    current_count bigint  not null,
    max_current   double  null,
    min_current   double  null,
    issues_count  bigint  not null,
    total_rows    bigint  not null,
    primary key (id),
    unique key uq_house_summary (batch_id, reading_date, hour_of_day),
    index idx_house_summary_hour (reading_date, hour_of_day)
) engine=InnoDB default charset=utf8mb4;

-- ---------------------------------------------------------------
-- 5. energy_alerts: alerts raised by spark, deduplicated by alert_key
-- ---------------------------------------------------------------
create table if not exists energy_alerts (
    id                bigint       not null auto_increment,
    alert_key         varchar(150) not null,
    alert_time        datetime     not null,
    source_event_time datetime     null,
    alert_type        varchar(50)  not null,
    severity          varchar(20)  not null,
    appliance_name    varchar(50)  null,
    measured_value    double       null,
    threshold_value   double       null,
    message           text         not null,
    primary key (id),
    unique key uq_alert_key (alert_key),
    index idx_alerts_time (alert_time)
) engine=InnoDB default charset=utf8mb4;
