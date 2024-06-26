##
## Copyright (c) 2023-2024 by Dribia Data Research.
## This file is part of project Brainer,
## and is released under the MIT License Agreement.
## See the LICENSE file for more information.
##

{% set delay_time_limit = var('delay_time_limit') %}

WITH service_arrival_departure AS (
  SELECT
      DISTINCT service_id,
      FIRST_VALUE(departure_time_expected) OVER (PARTITION BY service_id ORDER BY stop_type_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS service_departure_time_expected,
      FIRST_VALUE(departure_time_actual) OVER (PARTITION BY service_id ORDER BY stop_type_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS service_departure_time_actual,
      LAST_VALUE(arrival_time_expected) OVER (PARTITION BY service_id ORDER BY stop_type_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS service_arrival_time_expected,
      LAST_VALUE(arrival_time_actual) OVER (PARTITION BY service_id ORDER BY stop_type_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS service_arrival_time_actual
  FROM
      {{ref("dbt_service_stops")}}
  WHERE
      stop_type_id IN (1,3) -- (1-origin pick-up stop, 3-destination drop-off stop)
)

SELECT
    ser.route_id,
    ser.country_id,
    SUM(
        CASE
            WHEN ((ser_stop.is_stop_executed_from_ontime=1) AND (ser_stop.is_stop_reserved=1)) THEN 1
            ELSE 0
        END
    ) / NULLIF(SUM(ser_stop.is_stop_reserved), 0) AS num_pickup_stops_with_reservations_executed_from_ontime_per_route,
    SUM(
        CASE
            WHEN ((ser_stop.is_stop_executed_from_boardings=1) AND (ser_stop.is_stop_reserved=1)) THEN 1
            ELSE 0
        END
    ) / NULLIF(SUM(ser_stop.is_stop_reserved), 0) AS num_pickup_stops_with_reservations_executed_from_boardings_per_route,
    -- safe_divide(SUM(is_stop_executed_from_ontime), SUM(is_stop_reserved)) as num_pickup_stops_with_reservations_executed_from_ontime_per_route,
    -- safe_divide(SUM(is_stop_executed_from_boardings), SUM(is_stop_reserved)) as num_pickup_stops_with_reservations_executed_from_boardings_per_route,
    AVG(is_stop_on_time) AS num_stops_on_time_per_route,
    SUM(
        CASE
            WHEN ((ser_stop.is_pickup_stop=1) AND (ser_stop.is_stop_reserved=1)) THEN 1
            ELSE 0
        END
    ) / NULLIF(SUM(ser_stop.is_pickup_stop), 0) AS num_pickup_stops_with_reservations_per_route,
    SUM(
        CASE
            WHEN ((ser_stop.is_pickup_stop=1) AND (ser_stop.has_stop_boardings=1)) THEN 1
            ELSE 0
        END
    ) / NULLIF(SUM(ser_stop.is_pickup_stop), 0) AS num_pickup_stops_with_boardings_per_route,
    SUM(
        CASE
            WHEN ((ser_stop.is_pickup_stop=1) AND (ser_stop.has_stop_boardings=1) AND (ser_stop.is_stop_executed_from_boardings=1)) THEN 1
            ELSE 0
        END
    ) / NULLIF(SUM(ser_stop.is_pickup_stop), 0) AS num_pickup_stops_with_boardings_in_place_per_route,
    AVG(SAFE_DIVIDE(ser.passengers_expected, ser.pax)) AS route_expected_occupancy,
    AVG(
        CASE WHEN sad.service_departure_time_actual IS NULL THEN NULL WHEN sad.service_departure_time_expected IS NULL THEN NULL
        WHEN (
        {% for country, delay_time in delay_time_limit.items() %}
         ser.country_id={{country}} and DATETIME_DIFF(sad.service_departure_time_actual, sad.service_departure_time_expected, MINUTE) <= {{delay_time}}
         {% if not loop.last %}OR{% endif %}
        {% endfor %}
        )
        THEN 0
        ELSE 1 END
    ) AS num_did_service_start_on_time,
    AVG(
        CASE WHEN sad.service_arrival_time_actual IS NULL THEN NULL WHEN sad.service_arrival_time_expected IS NULL THEN NULL
        WHEN (
            {% for country, delay_time in delay_time_limit.items() %}
             ser.country_id={{country}} and DATETIME_DIFF(sad.service_arrival_time_actual, sad.service_arrival_time_expected, MINUTE) <= {{delay_time}}
             {% if not loop.last %}OR{% endif %}
            {% endfor %}
            )
        THEN 0
        ELSE 1 END
    ) AS num_did_service_end_on_time,
FROM
    {{ ref("dbt_service_stops") }} AS ser_stop
LEFT JOIN {{ ref("dbt_service") }} AS ser ON ser_stop.service_id = ser.id
LEFT JOIN service_arrival_departure AS sad ON ser.id = sad.service_id
GROUP BY
    ser.route_id, ser.country_id
