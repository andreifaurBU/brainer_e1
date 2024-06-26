##
## Copyright (c) 2023-2024 by Dribia Data Research.
## This file is part of project Brainer,
## and is released under the MIT License Agreement.
## See the LICENSE file for more information.
##

{% set valid_countries = var('valid_countries') %}
{% set valid_future_service_status = var('valid_future_service_status') %}

-- passengers expected in the future service


-- get approximate coordinates of a service first stop
WITH routes_coord AS (
SELECT
    route_id,
    AVG(lat) AS service_approx_lat,
    AVG(lng) AS service_approx_lng,
FROM
    {{ source ("busup", "route_stops") }} as route_stops
GROUP BY
    route_id
),

city_polygon AS (
SELECT
    city,
    -- point(lng,lat)
    ST_MakePolygon(ST_MakeLine(
      ARRAY[
        ST_GEOGPOINT(west, north),
        ST_GEOGPOINT(west, south),
        ST_GEOGPOINT(east, south),
        ST_GEOGPOINT(east, north),
        ST_GEOGPOINT(west, north)
      ]
    )) AS polygon_geometry
    FROM {{ ref("city_coordinates") }}
),

first_stop_nearest_polygon AS (
SELECT
    p.route_id,
    ARRAY_AGG(
        STRUCT(
            poly.city AS city,
            ST_DISTANCE(ST_GEOGPOINT(p.service_approx_lng, p.service_approx_lat), poly.polygon_geometry) as distance
        )
      ORDER BY ST_DISTANCE(ST_GEOGPOINT(p.service_approx_lng, p.service_approx_lat), poly.polygon_geometry) LIMIT 1
    )[OFFSET(0)] AS nearest_polygon
FROM
    routes_coord AS p
CROSS JOIN
    city_polygon AS poly
GROUP BY
    p.route_id, p.service_approx_lng, p.service_approx_lat
),

-- main query
future_service as (
    SELECT
    ser.*,
    sa.driver as assigned_driver,
    sa.vehicle as assigned_vehicle,
    sa.provider as assigned_operator,
    routes.circular as route_circular,
    sites.country_id,
    countries.name as country_name,
    sa.modified as assignation_modified,
    routes_coord.service_approx_lat,
    routes_coord.service_approx_lng,
    fsnp.nearest_polygon.city as nearest_city,
    hol.description as holiday_description,
    FROM
    {{ source ("busup", "services") }} AS ser
    INNER JOIN {{ source ("busup", "routes") }} AS routes ON routes.id = ser.route_id
    INNER JOIN {{ source ("busup", "sites") }} AS sites ON sites.id = routes.primary_site
    INNER JOIN {{ source ("busup", "countries") }} AS countries ON countries.id = sites.country_id
    LEFT JOIN {{ source ("busup", "service_assignation") }} AS sa ON ser.id = sa.service_id
    LEFT JOIN routes_coord ON routes_coord.route_id = ser.route_id
    LEFT JOIN first_stop_nearest_polygon AS fsnp on fsnp.route_id= ser.route_id
    LEFT JOIN {{ ref("holiday") }} AS hol ON hol.city = fsnp.nearest_polygon.city AND hol.date = EXTRACT (DATE FROM ser.timestamp)
    WHERE
    ser.timestamp >= "{{ var('start_date') }}"
    AND routes.circular IS FALSE
    AND ser.timestamp > CURRENT_DATETIME()
    AND ({% for status in valid_future_service_status %}
    ser.status = {{status}}
    {% if not loop.last %} OR {% endif %}
    {% endfor %}
    )
    AND ({% for country in valid_countries %}
    sites.country_id = {{country}}
    {% if not loop.last %} OR {% endif %}
    {% endfor %}
    )
)

-- main query
SELECT
    ser.id AS service_id,
    ser.country_id,
    ser.timestamp,
    CASE
        WHEN EXTRACT(MONTH FROM ser.timestamp) IN (12, 1, 2) THEN 'winter'
        WHEN EXTRACT(MONTH FROM ser.timestamp) IN (3, 4, 5) THEN 'spring'
        WHEN EXTRACT(MONTH FROM ser.timestamp) IN (6, 7, 8) THEN 'summer'
        WHEN EXTRACT(MONTH FROM ser.timestamp) IN (9, 10, 11) THEN 'autumn'
    END AS season_of_year,
    ser.route_id,
    EXTRACT(DAYOFWEEK FROM timestamp) AS day_of_week,
    EXTRACT(MONTH FROM timestamp) AS month_of_year,
    EXTRACT(HOUR FROM departure_timestamp) AS hour_of_day,
    assigned_driver as driver_id,
    assigned_operator as operator_id,
    (
    SELECT
        AVG(CASE WHEN (sub_ser.status=-60 AND sub_ser.error_type IS NOT NULL) OR (sub_ser.status=-40) OR (sub_ser.status=-50 AND sub_ser.error_type IS NOT NULL) THEN 0 ELSE 1 END)
    FROM
        {{ ref("dbt_service") }}  AS sub_ser
    WHERE
        sub_ser.assigned_driver = ser.assigned_driver
        -- CURRENT_TIMESTAMP or ser.timestamp?
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
        SAFE_DIVIDE(
                SUM(CASE WHEN is_stop_reserved=1 and is_stop_executed_from_ontime=1 then 1 else 0 end),
                SUM(CASE WHEN is_stop_reserved=1 then 1 else 0 end)
        )
    FROM
        {{ ref("dbt_service_stops") }} AS ss
    WHERE
        ss.driver_id = ser.assigned_driver
        AND DATE_DIFF(ser.timestamp, ss.departure_time_expected, DAY) <= {{ var("rolling_window_past_days") }}
        AND DATE_DIFF(ser.timestamp, ss.departure_time_expected, DAY)>0
    ) AS perc_reserved_stop_executed_from_on_time_driver,
    (
    SELECT
        SAFE_DIVIDE(
                SUM(CASE WHEN is_stop_reserved=1 and is_stop_executed_from_ontime=1 then 1 else 0 end),
                SUM(CASE WHEN is_stop_reserved=1 then 1 else 0 end)
        )
    FROM
        {{ ref("dbt_service_stops") }} AS ss
    WHERE
        ss.route_id = ser.route_id
        AND DATE_DIFF(ser.timestamp, ss.departure_time_expected, DAY)>0
    ) AS perc_reserved_stop_executed_from_on_time_route,
FROM
    future_service AS ser
WHERE
    assigned_driver IS NOT NULL
