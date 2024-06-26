##
## Copyright (c) 2023-2024 by Dribia Data Research.
## This file is part of project Brainer,
## and is released under the MIT License Agreement.
## See the LICENSE file for more information.
##

SELECT
    route_stop_id,
    country_id,
    AVG(num_boardings_in_place) AS num_stops_boardings_in_place_per_stop,
    SUM(
        CASE
            WHEN ((is_stop_executed_from_ontime=1) AND (is_stop_reserved=1)) THEN 1
            ELSE 0
        END
    ) / NULLIF(SUM(is_stop_reserved), 0) AS num_stops_with_reservations_executed_from_ontime_per_stop,
    SUM(
        CASE
            WHEN ((is_stop_executed_from_boardings=1) AND (is_stop_reserved=1)) THEN 1
            ELSE 0
        END
    ) / NULLIF(SUM(is_stop_reserved), 0) AS num_stops_with_reservations_executed_from_boardings_per_stop,
    --safe_divide(SUM(is_stop_executed_from_ontime), SUM(is_stop_reserved)) as num_stops_with_reservations_executed_from_ontime_per_stop,
    --safe_divide(SUM(is_stop_executed_from_boardings), SUM(is_stop_reserved)) as num_stops_with_reservations_executed_from_boardings_per_stop,
    AVG(is_stop_on_time) AS num_stops_on_time_per_stop,
    AVG(is_stop_reserved) AS num_stops_with_reservations_per_stop,
    AVG(has_stop_boardings) AS num_stops_with_boardings_per_stop,
FROM
    {{ ref("dbt_service_stops") }}
GROUP BY
    route_stop_id, country_id
