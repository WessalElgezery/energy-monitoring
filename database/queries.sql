-- energy monitoring system: sql analysis queries
--
-- how the data is stored:
--   spark writes partial hourly aggregates (sums and counts) per micro-batch.
--   one hour can be split across several batches, so every final figure here is
--   computed from sums and counts across batches:
--       average = sum(sum_power) / sum(reading_count)
--   never average the per-batch averages.
--
-- energy conversion:
--   one reading = power in watts held for 10 seconds
--   kwh = sum of readings * 10 / 3,600,000
--   missing (null) readings are not counted, so energy slightly understates
--   the true use in hours that have missing values.
--
-- null rules: missing readings stay null and are never treated as zero.
-- nullif(x, 0) is used in every division so an empty group gives null, not an error.


-- ===============================================================
-- 0. sanity checks (run these first after a full pipeline run)
-- ===============================================================

-- 0a. row counts vs expected (full run: 120,960 and 604,800)
select 'house_readings' as table_name, count(*) as row_count, 120960 as expected_rows
from house_readings
union all
select 'appliance_readings', count(*), 604800
from appliance_readings;

-- 0b. every timestamp must have exactly five appliance rows (no rows returned = good)
select event_time, count(*) as appliance_rows
from appliance_readings
group by event_time
having count(*) <> 5;

-- 0c. summaries must cover every raw row
--     house_summary total should equal house_readings; usage_summary should be five times that
select
    (select sum(total_rows) from house_summary)  as house_summary_rows,
    (select count(*) from house_readings)        as house_readings_rows,
    (select sum(total_rows) from usage_summary)  as usage_summary_rows,
    (select count(*) from appliance_readings)    as appliance_readings_rows;


-- ===============================================================
-- 1. hourly and daily consumption (whole house)
-- ===============================================================

-- 1a. hourly consumption
select
    reading_date,
    hour_of_day,
    sum(power_sum) / nullif(sum(power_count), 0) as avg_power_w,
    max(max_power)                               as peak_power_w,
    min(min_power)                               as lowest_power_w,
    sum(power_sum) * 10 / 3600000                as energy_kwh
from house_summary
group by reading_date, hour_of_day
order by reading_date, hour_of_day;

-- 1b. daily consumption
select
    reading_date,
    sum(power_sum) / nullif(sum(power_count), 0) as avg_power_w,
    sum(power_sum) * 10 / 3600000                as energy_kwh
from house_summary
group by reading_date
order by reading_date;


-- ===============================================================
-- 2. hourly consumption per appliance
-- ===============================================================

select
    reading_date,
    hour_of_day,
    appliance_name,
    sum(sum_power) / nullif(sum(reading_count), 0) as avg_power_w,
    max(max_power)                                 as peak_power_w,
    sum(sum_power) * 10 / 3600000                  as energy_kwh
from usage_summary
group by reading_date, hour_of_day, appliance_name
order by reading_date, hour_of_day, appliance_name;


-- ===============================================================
-- 3. appliance comparison
-- ===============================================================

-- total energy per appliance and its share of all appliance energy
select
    appliance_name,
    sum(sum_power) * 10 / 3600000                                          as total_kwh,
    round(100 * sum(sum_power) / nullif(sum(sum(sum_power)) over (), 0), 2) as share_pct,
    sum(sum_power) / nullif(sum(reading_count), 0)                         as avg_power_w,
    max(max_power)                                                         as peak_power_w
from usage_summary
group by appliance_name
order by total_kwh desc;


-- ===============================================================
-- 4. peak consumption periods
-- ===============================================================

-- 4a. top 10 busiest hours (whole house)
with hourly as (
    select
        reading_date,
        hour_of_day,
        sum(power_sum) / nullif(sum(power_count), 0) as avg_power_w
    from house_summary
    group by reading_date, hour_of_day
)
select reading_date, hour_of_day, avg_power_w
from hourly
order by avg_power_w desc
limit 10;

-- 4b. the peak hour of each day
with hourly as (
    select
        reading_date,
        hour_of_day,
        sum(power_sum) / nullif(sum(power_count), 0) as avg_power_w
    from house_summary
    group by reading_date, hour_of_day
),
ranked as (
    select
        reading_date,
        hour_of_day,
        avg_power_w,
        rank() over (partition by reading_date order by avg_power_w desc) as rnk
    from hourly
)
select reading_date, hour_of_day, avg_power_w
from ranked
where rnk = 1
order by reading_date;

-- 4c. typical load by hour of day (all days combined)
select
    hour_of_day,
    sum(power_sum) / nullif(sum(power_count), 0) as avg_power_w
from house_summary
group by hour_of_day
order by avg_power_w desc;

-- 4d. each appliance's busiest hour of day
with hourly as (
    select
        appliance_name,
        hour_of_day,
        sum(sum_power) / nullif(sum(reading_count), 0) as avg_power_w
    from usage_summary
    group by appliance_name, hour_of_day
),
ranked as (
    select
        appliance_name,
        hour_of_day,
        avg_power_w,
        rank() over (partition by appliance_name order by avg_power_w desc) as rnk
    from hourly
)
select appliance_name, hour_of_day, avg_power_w
from ranked
where rnk = 1
order by appliance_name;


-- ===============================================================
-- 5. missing measurements
-- ===============================================================

-- 5a. missing values in the whole-house measurements
select 'voltage' as measurement,
       sum(total_rows) - sum(voltage_count) as missing_rows,
       sum(total_rows)                      as total_rows,
       round(100 * (sum(total_rows) - sum(voltage_count)) / nullif(sum(total_rows), 0), 3) as missing_pct
from house_summary
union all
select 'current',
       sum(total_rows) - sum(current_count),
       sum(total_rows),
       round(100 * (sum(total_rows) - sum(current_count)) / nullif(sum(total_rows), 0), 3)
from house_summary
union all
select 'whole_house_power',
       sum(total_rows) - sum(power_count),
       sum(total_rows),
       round(100 * (sum(total_rows) - sum(power_count)) / nullif(sum(total_rows), 0), 3)
from house_summary;

-- 5b. missing values per appliance
select
    appliance_name,
    sum(total_rows) - sum(reading_count) as missing_rows,
    sum(total_rows)                      as total_rows,
    round(100 * (sum(total_rows) - sum(reading_count)) / nullif(sum(total_rows), 0), 3) as missing_pct
from usage_summary
group by appliance_name
order by missing_rows desc;

-- 5c. hours with the most missing appliance readings
select
    reading_date,
    hour_of_day,
    appliance_name,
    sum(total_rows) - sum(reading_count) as missing_rows
from usage_summary
group by reading_date, hour_of_day, appliance_name
having sum(total_rows) - sum(reading_count) > 0
order by missing_rows desc
limit 20;

-- 5d. cross-check against the raw tables (null counts)
select
    count(*) - count(voltage_v)         as missing_voltage,
    count(*) - count(current_a)         as missing_current,
    count(*) - count(whole_house_power) as missing_power
from house_readings;


-- ===============================================================
-- 6. original issue flags (issues = 1 is still valid data)
-- ===============================================================

-- 6a. overall
select
    sum(issues_count) as issue_rows,
    sum(total_rows)   as total_rows,
    round(100 * sum(issues_count) / nullif(sum(total_rows), 0), 3) as issue_pct
from house_summary;

-- 6b. per day
select
    reading_date,
    sum(issues_count) as issue_rows,
    round(100 * sum(issues_count) / nullif(sum(total_rows), 0), 3) as issue_pct
from house_summary
group by reading_date
order by reading_date;

-- 6c. hours that contain issues
select
    reading_date,
    hour_of_day,
    sum(issues_count) as issue_rows
from house_summary
group by reading_date, hour_of_day
having sum(issues_count) > 0
order by issue_rows desc;

-- 6d. cross-check against the raw table
select issues, count(*) as rows_count
from house_readings
group by issues
order by issues;


-- ===============================================================
-- 7. house level vs appliance level
-- ===============================================================

-- 7a. hour by hour: whole house vs the five tracked appliances
--     untracked_kwh is energy used by things that are not one of the five appliances
with house as (
    select reading_date, hour_of_day,
           sum(power_sum) * 10 / 3600000 as house_kwh
    from house_summary
    group by reading_date, hour_of_day
),
appliances as (
    select reading_date, hour_of_day,
           sum(sum_power) * 10 / 3600000 as appliance_kwh
    from usage_summary
    group by reading_date, hour_of_day
)
select
    h.reading_date,
    h.hour_of_day,
    h.house_kwh,
    a.appliance_kwh,
    h.house_kwh - a.appliance_kwh as untracked_kwh,
    round(100 * a.appliance_kwh / nullif(h.house_kwh, 0), 1) as tracked_pct
from house h
join appliances a
  on a.reading_date = h.reading_date
 and a.hour_of_day  = h.hour_of_day
order by h.reading_date, h.hour_of_day;

-- 7b. overall totals
select
    (select sum(power_sum) * 10 / 3600000 from house_summary) as house_kwh,
    (select sum(sum_power) * 10 / 3600000 from usage_summary) as appliances_kwh;
