##
## Copyright (c) 2023-2024 by Dribia Data Research.
## This file is part of project Brainer,
## and is released under the MIT License Agreement.
## See the LICENSE file for more information.
##

WITH ontime_w_rtimes AS (
    SELECT
        ot.*,
        ser.route_id,
        rs.end_timestamp,
        ser.departure_timestamp,
--         CASE
--             WHEN ROW_NUMBER() OVER (
--                 PARTITION BY service_id
--             ORDER BY
--                 end_timestamp
--             )= 1
--                 THEN departure_timestamp
--             ELSE TIMESTAMP_ADD(
--                 departure_timestamp,
--                 INTERVAL TIMESTAMP_DIFF(
--                     end_timestamp,
--                     MIN(end_timestamp) OVER (
--                         PARTITION BY service_id
--                     ),
--                     SECOND
--                 ) SECOND
--             )
--         END AS route_stop_departure_hour,
        -- route_stop_departure_hour: is the theoretical departure hour for a stop.
        -- It has many nulls until July 2023, therefore we try to reconstruct it.
        -- The CASE statement is used to conditionally calculate the value of the route_stop_departure_hour.
        -- It checks if the current route_stop (i.e.: row) is the first stop within the partition defined by service_id when ordered by end_timestamp.
        -- If it is the first route_stop, it assigns the value of departure_timestamp from services table to route_stop_departure_hour.
        -- Otherwise, it calculates the time difference between route_stops and adjusts the departure_timestamp based on this difference.
--         TIMESTAMP_DIFF(
--             end_timestamp,
--             MIN(end_timestamp) OVER (
--                 PARTITION BY service_id
--             ),
--             HOUR
--         ) AS time_diff_r,
        -- time_diff_r: time difference, in hours, between the first stop of a service and the current stop.
--         CASE
--             WHEN ROW_NUMBER() OVER (
--                 PARTITION BY service_id
--             ORDER BY
--                 end_timestamp
--             )= 1
--                 THEN departure_timestamp
--             ELSE departure_timestamp + INTERVAL seconds_until_departure SECOND
--         END AS track_stop_departure_hour,
        -- track_stop_departure_hour: is the theoretical departure hour for a stop.
        -- It has many nulls until July 2023, therefore we try to reconstruct it.
        -- We apply the same logic as in route_stop_departure_hour, but now we know the difference between stops,
        -- in seconds, from the column seconds_until_departure.
        ts.seconds_until_departure,
        ts.id AS track_stop_id,
        CASE WHEN mode = "track" THEN ts.stop_id ELSE rs.id END AS route_stop_id,
        -- route_stop_id: we use the stop_id of the route_stop or track_stop depending on the mode
        CASE WHEN mode = "track" THEN ts.stop_type_id ELSE rs.stop_type_id END AS stop_type_id
        -- stop_type_id: we use the stop_type_id of the route_stop or track_stop depending on the mode
    FROM
        {{ source("busup", "ontime") }} AS ot
    INNER JOIN {{ ref("dbt_service") }} AS ser ON ot.service_id = ser.id
    LEFT JOIN {{ source("busup", "route_stops") }} AS rs ON ot.stop_id = rs.id
    LEFT JOIN {{ source("busup", "track_stops") }} AS ts ON ot.stop_id = ts.id
    ORDER BY
        ot.service_id,
        ser.route_id,
        rs.end_timestamp
)
-- correct stop_arrival_hour and stop_departure_hour
SELECT
    id,
    service_id,
    route_id,
    mode,
    route_stop_id,
    track_stop_id,
    stop_type_id,
    arrivaltime,
    departuretime,
    status,
    bypassed,
    arrivallat,
    arrivallng,
    arrivaldistance,
    departurelat,
    departurelng,
    departuredistance,
    DATETIME(DATE(departure_timestamp), TIME(stop_arrival_hour)) AS stop_arrival_hour,
    DATETIME(DATE(departure_timestamp), TIME(stop_departure_hour)) AS stop_departure_hour,
--     DATETIME(DATE(service_timestamp), TIME(stop_arrival_hour)) AS stop_arrival_hour,
    -- stop_arrival_hour has to be datimetime.
--   CASE WHEN (mode="route" AND stop_departure_hour IS NOT NULL) THEN DATETIME(DATE(route_stop_departure_hour), TIME(stop_departure_hour))
--      WHEN (mode="track" AND stop_departure_hour IS NOT NULL) THEN DATETIME(DATE(track_stop_departure_hour), TIME(stop_departure_hour))
--      WHEN (mode="route" AND stop_departure_hour IS NULL) THEN route_stop_departure_hour
--      WHEN (mode="route" AND time_diff_r>24) THEN NULL
--      WHEN (mode="track" AND stop_departure_hour IS NULL) THEN track_stop_departure_hour
--      END AS stop_departure_hour,
    -- stop_departure_hour has many nulls until July 2023.
    -- If it is null we use the reconstructed (datetime) value, otherwise we use the actual value.
    created_at,
    updated_at
FROM
    ontime_w_rtimes
WHERE
    route_stop_id IS NOT NULL
AND
    stop_type_id IN (1, 2, 3)
ORDER BY
    service_id,
    end_timestamp
