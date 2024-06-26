##
## Copyright (c) 2023-2024 by Dribia Data Research.
## This file is part of project Brainer,
## and is released under the MIT License Agreement.
## See the LICENSE file for more information.
##

{% set delay_time_limit = var('delay_time_limit') %}

WITH service_stops AS(
  SELECT
    service_id,
    SUM(
        CASE
            WHEN ((is_stop_executed_from_ontime=1) AND (is_stop_reserved=1)) THEN 1
            ELSE 0
        END
    ) / NULLIF(SUM(is_stop_reserved), 0) AS num_stops_with_reservations_executed_from_ontime,
    SUM(
        CASE
            WHEN ((is_stop_executed_from_boardings=1) AND (is_stop_reserved=1)) THEN 1
            ELSE 0
        END
    ) / NULLIF(SUM(is_stop_reserved), 0) AS num_stops_with_reservations_executed_from_boardings,
    -- If is_stop_on_time is null, then we consider that stop is not on time
    AVG(CASE WHEN is_stop_on_time IS NULL THEN 0 ELSE is_stop_on_time END) AS num_stops_on_time,
    SUM(
        CASE
            WHEN ((is_pickup_stop=1) AND (is_stop_reserved=1)) THEN 1
            ELSE 0
        END
    ) / NULLIF(SUM(is_pickup_stop), 0) AS num_pickup_stops_with_reservations,
    SUM(
        CASE
            WHEN ((is_pickup_stop=1) AND (has_stop_boardings=1)) THEN 1
            ELSE 0
        END
    ) / NULLIF(SUM(is_pickup_stop), 0) AS num_pickup_stops_with_boardings,
    --AVG(case when has_stop_boardings is null then null else has_stop_boardings end) as num_stops_with_boardings,
    -- If num_boardings_in_place is null, then we consider there is no boarding in place
    AVG(num_boardings_in_place) AS num_boardings_in_place,
--     AVG(CASE WHEN num_boardings_in_place IS NULL THEN 0 ELSE num_boardings_in_place END) AS num_boardings_in_place,
    MAX(CASE WHEN stop_type_id = 1 THEN is_stop_on_time END) AS did_service_start_on_time,
    MAX(CASE WHEN stop_type_id = 3 THEN is_stop_on_time END) AS did_service_end_on_time,
    FROM {{ ref("dbt_service_stops") }}
    GROUP BY service_id
)

-- main query
SELECT
    ser.id AS service_id,
    ser.country_id,
    ser.timestamp,
    --DATETIME_DIFF(ser.departure_timestamp, ser.reported_departure_timestamp, MINUTE) as mins_play_before_start,
    CASE WHEN DATETIME_DIFF(ser.departure_timestamp, ser.reported_departure_timestamp, MINUTE) BETWEEN 10 AND 60 THEN 1 ELSE 0 END AS is_play_10_mins_before_start,
    --timestamp_diff(ser.departure_timestamp, ser.assignation_modified, minute) as minutes_since_last_assignation_modification,
    CASE WHEN timestamp_diff(ser.departure_timestamp, ser.assignation_modified, MINUTE) BETWEEN {{ var('assignation_min_time') }} AND 60 THEN 0 ELSE 1 END AS is_assignation_not_modified_1_hour_before_start,
    ser_stop.num_stops_on_time as num_stops_on_time_per_service,
    CASE WHEN ser.assigned_vehicle IS NULL THEN 0 ELSE 1 END AS is_vehicle_assigned,
    CASE WHEN ser.assigned_driver IS NULL THEN 0 ELSE 1 END AS is_driver_assigned,
    CASE WHEN ser.holiday_description IS NULL THEN 1 ELSE 0 END AS is_no_holiday,
    CASE WHEN ser.traffic_control_incident_code LIKE '%{{ var ("incident_driver_bad_behavior") }}%' THEN 0 ELSE 1 END AS has_no_incident_driver_bad_behavior,
    CASE WHEN ser.traffic_control_incident_code LIKE '%{{ var ("incident_bus_broke_down") }}%' THEN 0 ELSE 1 END AS has_no_incident_bus_broke_down,
    CASE WHEN ser.traffic_control_incident_code LIKE '%{{ var ("incident_no_play_unresponsive") }}%' THEN 0 ELSE 1 END AS has_no_incident_no_play_unresponsive,
    CASE WHEN ser.traffic_control_incident_code LIKE '%{{ var ("incident_delayed_15") }}%' THEN 0 ELSE 1 END AS has_no_incident_delayed_15,
    CASE WHEN ser.traffic_control_incident_code LIKE '%{{ var ("incident_vehicle_in_bad_condition") }}%' THEN 0 ELSE 1 END AS has_no_incident_vehicle_in_bad_condition,
    --CASE WHEN JSON_EXTRACT_SCALAR(status_info, '$.readings')="true" then 1 else 0 end as has_boardings,
    CASE WHEN JSON_EXTRACT_SCALAR(status_info, '$.tracking')="true" THEN 1 ELSE 0 END AS is_track_recorded,
    CASE WHEN (ser.status=-60 AND ser.error_type IS NOT NULL) OR (ser.status=-40) OR (ser.status=-50 AND ser.error_type IS NOT NULL) THEN 0 ELSE 1 END AS is_play_pressed,
    CASE WHEN JSON_EXTRACT_SCALAR(status_info, '$.play_service')="true" THEN 1 ELSE 0 END AS is_play_pressed_from_status_info,
    ser_stop.did_service_start_on_time,
    ser_stop.did_service_end_on_time,
    ser_stop.num_pickup_stops_with_reservations AS num_pickup_stops_with_reservations_per_service,
    ser_stop.num_pickup_stops_with_boardings AS num_pickup_stops_with_boardings_per_service,
    ser_stop.num_stops_with_reservations_executed_from_ontime AS num_pickup_stops_with_reservations_executed_from_ontime_per_service,
    CASE
        WHEN ser_stop.num_stops_with_reservations_executed_from_ontime IS NULL THEN NULL
        WHEN ser_stop.num_stops_with_reservations_executed_from_ontime<1 THEN 0
        WHEN ser_stop.num_stops_with_reservations_executed_from_ontime=1 THEN 1
    END AS has_reserved_stops_executed,
    ser_stop.num_stops_with_reservations_executed_from_boardings AS num_pickup_stops_with_reservations_executed_from_boardings_per_service,
    ser_stop.num_boardings_in_place AS num_pickup_stops_with_boardings_in_place_per_service,
    --
    ser.route_id,
    ser.assigned_driver as driver_id,
    ser.assigned_operator as operator_id,
    CASE
        WHEN EXTRACT(MONTH FROM ser.timestamp) IN (12, 1, 2) THEN 'winter'
        WHEN EXTRACT(MONTH FROM ser.timestamp) IN (3, 4, 5) THEN 'spring'
        WHEN EXTRACT(MONTH FROM ser.timestamp) IN (6, 7, 8) THEN 'summer'
        WHEN EXTRACT(MONTH FROM ser.timestamp) IN (9, 10, 11) THEN 'autumn'
    END AS season_of_year,
    EXTRACT(MONTH FROM timestamp) AS month_of_year,
    EXTRACT(DAYOFWEEK FROM timestamp) AS day_of_week,
    EXTRACT(HOUR FROM departure_timestamp) AS hour_of_day,
    (
    SELECT
        AVG(CASE WHEN (sub_ser.status=-60 AND sub_ser.error_type IS NOT NULL) OR (sub_ser.status=-40) OR (sub_ser.status=-50 AND sub_ser.error_type IS NOT NULL) THEN 0 ELSE 1 END)
    FROM
        {{ ref("dbt_service") }} AS sub_ser
    WHERE
        sub_ser.assigned_driver = ser.assigned_driver
        AND DATE_DIFF(ser.timestamp, sub_ser.timestamp, DAY) <= {{ var("rolling_window_past_days") }}
        AND DATE_DIFF(ser.timestamp, sub_ser.timestamp, DAY)>0
    ) AS perc_pressed_play_driver,
    (
    SELECT
        COUNT(*)
    FROM
        {{ ref("dbt_service") }} AS sub_ser
    WHERE
        sub_ser.assigned_driver = ser.assigned_driver
        AND DATE_DIFF(ser.timestamp, sub_ser.timestamp, DAY)>0
    ) AS num_past_services_driver,
        (
    SELECT
        AVG(CASE WHEN is_stop_on_time=1 THEN 1 ELSE 0 END)
    FROM
        {{ ref("dbt_service_stops") }} AS ss
    WHERE
        ss.driver_id = ser.assigned_driver
        AND DATE_DIFF(ser.timestamp, ss.departure_time_expected, DAY) <= {{ var("rolling_window_past_days") }}
        AND DATE_DIFF(ser.timestamp, ss.departure_time_expected, DAY)>0
    ) AS perc_stop_on_time_driver,
    (
    SELECT
        SAFE_DIVIDE(SUM(CASE WHEN is_stop_reserved=1 and is_stop_executed_from_ontime=1 then 1 else 0 end), SUM(CASE WHEN is_stop_reserved=1 then 1 else 0 end))
    FROM
        {{ ref("dbt_service_stops") }} AS ss
    WHERE
        ss.driver_id = ser.assigned_driver
        AND DATE_DIFF(ser.timestamp, ss.departure_time_expected, DAY) <= {{ var("rolling_window_past_days") }}
        AND DATE_DIFF(ser.timestamp, ss.departure_time_expected, DAY)>0
    ) AS perc_reserved_stop_executed_from_on_time_driver,
    (
    SELECT
        SAFE_DIVIDE(SUM(CASE WHEN is_stop_reserved=1 and is_stop_executed_from_ontime=1 then 1 else 0 end), SUM(CASE WHEN is_stop_reserved=1 then 1 else 0 end))
    FROM
        {{ ref("dbt_service_stops") }} AS ss
    WHERE
        ss.route_id = ser.route_id
        AND DATE_DIFF(ser.timestamp, ss.departure_time_expected, DAY)>0
    ) AS perc_reserved_stop_executed_from_on_time_route,
FROM {{ ref("dbt_service") }} AS ser
LEFT JOIN service_stops AS ser_stop ON ser.id = ser_stop.service_id
