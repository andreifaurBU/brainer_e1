##
## Copyright (c) 2023-2024 by Dribia Data Research.
## This file is part of project Brainer,
## and is released under the MIT License Agreement.
## See the LICENSE file for more information.
##

{% set delay_time_limit = var('delay_time_limit') %}

WITH ontime_drivers AS(
    SELECT
        ot.id as ontime_id,
        ot.route_stop_id,
        ot.stop_type_id,
        ot.service_id,
        ser.track_id,
        ser.route_id,
        ser.country_id AS country_id,
        ser.assigned_driver as driver_id,
        ot.mode,
        ot.arrivaltime AS arrival_time_actual,
        ot.stop_arrival_hour AS arrival_time_expected,
        ot.departuretime AS departure_time_actual,
        ot.stop_departure_hour AS departure_time_expected,
        TIMESTAMP_DIFF(ot.arrivaltime, ot.stop_departure_hour, Minute) AS mins_delay,
        IFNULL(TIMESTAMP_DIFF(ot.departuretime, ot.stop_departure_hour, Minute),0) AS mins_to_departure,
        CASE WHEN ot.bypassed IS FALSE THEN 1 WHEN ot.bypassed IS NULL THEN NULL ELSE 0 END AS is_stop_executed_from_ontime,
        CASE
            WHEN ot.stop_type_id IN (1, 2) THEN 1
            ELSE 0
        END AS is_pickup_stop
    FROM
        {{ ref("dbt_ontime") }} AS ot
    LEFT JOIN {{ ref("dbt_service") }} AS ser ON ot.service_id = ser.id
),

passenger_expected AS (
    SELECT
        service_id,
        reservation_route_stop_id,
        COUNT(*) AS passengers_expected
    FROM
        {{ ref("dbt_passenger_list") }}
    WHERE
        reservation_route_stop_id IS NOT NULL
    GROUP BY
        service_id,
        reservation_route_stop_id
),

passenger_boarded AS (
    SELECT
        service_id,
        boarding_route_stop_id,
        COUNT(*) AS passengers_boarded,
        AVG(
         CASE
            WHEN boarding_distance_to_stop IS NULL THEN NULL
            WHEN boarding_distance_to_stop <= {{ var('max_distance_to_stop') }} THEN 1
            ELSE 0 end
         ) AS num_boardings_in_place
    FROM
        {{ ref("dbt_passenger_list") }}
    WHERE
        boarding_route_stop_id IS NOT NULL
    GROUP BY
        service_id,
        boarding_route_stop_id
),

ontime_drivers_passengers AS (
    SELECT
        otd.*,
        e.passengers_expected,
        b.passengers_boarded,
        b.num_boardings_in_place,
    FROM
        ontime_drivers AS otd
  LEFT JOIN passenger_expected AS e ON otd.service_id=e.service_id AND otd.route_stop_id=e.reservation_route_stop_id
  LEFT JOIN passenger_boarded AS b ON otd.service_id=b.service_id AND otd.route_stop_id=b.boarding_route_stop_id
)

--  main query
SELECT
    ontime_drivers_passengers.*,
    CASE
        WHEN
            stop_type_id in (1, 2, 3) AND mins_delay IS NULL
        THEN NULL
        WHEN
            stop_type_id in (1, 2)
                AND ({% for country, delay_time in delay_time_limit.items() %}
                    (country_id={{country}} and mins_delay >= {{delay_time}}) OR
                    {% if loop.last %}mins_to_departure < 0{% endif %}
                    {% endfor %}
                )
        THEN 0
        WHEN
            stop_type_id in (1, 2)
                AND ({% for country, delay_time in delay_time_limit.items() %}
                    (country_id={{country}} and mins_delay < {{delay_time}}) OR
                    {% if loop.last %}mins_to_departure >= 0{% endif %}
                    {% endfor %}
            )
        THEN 1
        WHEN
            stop_type_id = 3
                AND ({% for country, delay_time in delay_time_limit.items() %}
                    (country_id={{country}} and mins_delay >= {{delay_time}})
                    {% if not loop.last %} OR {% endif %}
                    {% endfor %}
                )
        THEN 0
        WHEN
            stop_type_id = 3
                AND ({% for country, delay_time in delay_time_limit.items() %}
                    (country_id={{country}} and mins_delay < {{delay_time}})
                    {% if not loop.last %} OR {% endif %}
                    {% endfor %}
            )
        THEN 1
    END AS is_stop_on_time,
    CASE WHEN passengers_expected>0 THEN 1 ELSE 0 END is_stop_reserved,
    CASE WHEN passengers_boarded>0 THEN 1 ELSE 0 END AS has_stop_boardings,
    CASE WHEN num_boardings_in_place IS NULL THEN NULL WHEN num_boardings_in_place>0 THEN 1 ELSE 0 END AS is_stop_executed_from_boardings
FROM
    ontime_drivers_passengers
