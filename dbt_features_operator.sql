##
## Copyright (c) 2023-2024 by Dribia Data Research.
## This file is part of project Brainer,
## and is released under the MIT License Agreement.
## See the LICENSE file for more information.
##

with services_w_assignation AS (
  SELECT
    ser.id AS service_id,
    ser.route_id,
    ser.provider AS operator_id,
    ser.country_id,
    CASE WHEN ser.assigned_vehicle IS NOT NULL THEN 1 ELSE 0 END AS is_vehicle_assigned,
    CASE WHEN ser.assigned_driver IS NOT NULL THEN 1 ELSE 0 END AS is_driver_assigned,
    CASE WHEN ser.traffic_control_incident_code LIKE '%{{ var ("incident_service_not_performed") }}%' THEN 0 ELSE 1 END AS no_incident_service_not_performed,
    CASE WHEN ser.traffic_control_incident_code LIKE '%{{ var ("incident_bus_broke_down") }}%' THEN 0 ELSE 1 END AS no_incident_bus_broke_down,
    CASE WHEN ser.traffic_control_incident_code LIKE '%{{ var ("incident_no_play_unresponsive") }}%' THEN 0 ELSE 1 END AS no_incident_no_play_unresponsive,
    CASE WHEN ser.traffic_control_incident_code LIKE '%{{ var ("incident_vehicle_in_bad_condition") }}%' THEN 0 ELSE 1 END AS no_incident_vehicle_in_bad_condition,
    ser.assigned_driver AS driver_id,
    CASE WHEN timestamp_diff(
      ser.departure_timestamp, ser.assignation_modified,
      minute
    ) BETWEEN {{ var('assignation_min_time') }} AND 60 THEN 0 ELSE 1 END AS is_assignation_not_modified_1_hour_before_start,
    ser.departure_timestamp,
  FROM
    {{ ref("dbt_service") }} AS ser
),

distinct_drivers_route AS (
  SELECT
    operator_id,
    route_id,
    country_id,
    COUNT (DISTINCT driver_id) AS drivers_route,
  FROM
    services_w_assignation
  WHERE
    departure_timestamp >= DATE_ADD(CURRENT_DATE, INTERVAL -2 MONTH)
  GROUP BY
    operator_id,
    route_id,
    country_id
),

distinct_drivers_sum AS (
  SELECT
    operator_id,
    country_id,
    AVG (
      CASE WHEN drivers_route > {{ var('max_distinct_drivers_per_route') }} THEN 0 ELSE 1 END
    ) AS num_routes_driver_assignation_continuous_per_operator,
  FROM
    distinct_drivers_route
  GROUP BY
    operator_id,
    country_id
),
--
-- score_drivers AS (
--   SELECT
--     operator_id,
--     AVG(score) AS average_drivers_performance_per_operator
--   FROM {{ ref("dbt_features_driver") }}
--   GROUP BY
--     operator_id
-- ),

features_assignation AS (
  SELECT
    operator_id,
    country_id,
    AVG(is_vehicle_assigned) AS num_services_vehicle_assigned_per_operator,
    AVG(is_driver_assigned) AS num_services_driver_assigned_per_operator,
    AVG(is_assignation_not_modified_1_hour_before_start) AS num_services_assignation_not_modified_1_hour_before_start_per_operator,
    AVG(no_incident_service_not_performed) AS num_services_without_incident_service_not_performed_per_operator,
    AVG(no_incident_bus_broke_down) AS num_services_without_incident_bus_broke_down_per_operator,
    AVG(no_incident_no_play_unresponsive) AS num_services_without_incident_no_play_unresponsive_per_operator,
    AVG(no_incident_vehicle_in_bad_condition) AS num_services_without_incident_vehicle_in_bad_condition_per_operator
  FROM
    services_w_assignation
  GROUP BY
    operator_id,
    country_id
)

SELECT
  fa.*,
  num_routes_driver_assignation_continuous_per_operator,
--   average_drivers_performance_per_operator,
FROM
  features_assignation AS fa
  LEFT JOIN distinct_drivers_sum AS dds ON fa.operator_id = dds.operator_id
--   LEFT JOIN score_drivers ON fa.operator_id = score_drivers.operator_id
WHERE
  fa.operator_id IS NOT NULL
