##
## Copyright (c) 2023-2024 by Dribia Data Research.
## This file is part of project Brainer,
## and is released under the MIT License Agreement.
## See the LICENSE file for more information.
##

SELECT
  ser.assigned_driver AS driver_id,
  ser.country_id,
  ser.assigned_operator AS operator_id,
  AVG(fs.is_play_pressed) AS num_play_pressed_per_driver,
  AVG(fs.is_play_10_mins_before_start) AS num_play_10_mins_before_start_per_driver,
  AVG(CASE WHEN (ser.traffic_control_incident_code LIKE '%{{ var("incident_driver_bad_behavior") }}%') THEN 0 ELSE 1 END) AS num_services_without_driver_incident_bad_behavior_per_driver,
  AVG(fs.num_pickup_stops_with_reservations_executed_from_ontime_per_service) AS num_pickup_stops_with_reservations_executed_from_ontime_per_driver,
  AVG(fs.num_pickup_stops_with_reservations_executed_from_boardings_per_service) AS num_pickup_stops_with_reservations_executed_from_boardings_per_driver,
  AVG(fs.num_pickup_stops_with_boardings_per_service) AS num_pickup_stops_with_boardings_per_driver,
  AVG(fs.num_pickup_stops_with_boardings_in_place_per_service) AS num_pickup_stops_with_boardings_in_place_per_driver,
  AVG(fs.did_service_start_on_time) AS num_services_started_on_time_per_driver,
FROM
  {{ ref("dbt_features_service") }} AS fs
LEFT JOIN {{ ref ("dbt_service") }} AS ser ON ser.id = fs.service_id
WHERE assigned_driver IS NOT NULL
GROUP BY
  driver_id,
  country_id,
  operator_id
