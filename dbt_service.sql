##
## Copyright (c) 2023-2024 by Dribia Data Research.
## This file is part of project Brainer,
## and is released under the MIT License Agreement.
## See the LICENSE file for more information.
##
{% set valid_countries = var('valid_countries') %}
{% set valid_past_service_status = var('valid_past_service_status') %}

-- collect in a string all incidents, separated by comma, of a service
WITH service_grouped_incidents AS (
SELECT
    service_id,
    array_to_string(array_agg(distinct CAST(tci.traffic_control_incident_code as string)), ',') AS traffic_control_incident_code,
    array_to_string(array_agg(distinct tcic.message), ',') AS incident_message,
FROM
    {{ source ("busup", "traffic_control_incidents") }} AS tci
LEFT JOIN
   {{ source ("busup", "traffic_control_incidents_codes") }} AS tcic
ON
    tcic.id = tci.traffic_control_incident_code
WHERE
    tci.deleted_at IS NULL
--      AND tci.traffic_control_incident_code != {{ var("incident_to_exclude") }}
GROUP BY
    service_id
),

-- passengers expected in the service
passengers_expected AS (
SELECT
    service_id,
    COUNT(*) AS passengers_expected
FROM
    {{ ref ("dbt_passenger_list") }}
WHERE
    service_id IS NOT NULL
GROUP BY
    service_id
),

-- passengers boarded in the service
passengers_boarded AS (
SELECT
    service_id,
    COUNT(*) AS passengers_boarded
FROM
    {{ source("busup", "ticket_consumptions") }}
WHERE
    service_id IS NOT NULL AND ticket_consumptions.error IS FALSE
GROUP BY
    service_id
),

-- get approximate coordinates of a service first stop
routes_coord AS (
SELECT
    route_id,
--     MIN(start_timestamp) AS first_stop_timestamp,
--     ARRAY_AGG(id ORDER BY start_timestamp LIMIT 1)[OFFSET(0)] AS first_stop_id,
--     ARRAY_AGG(lat ORDER BY start_timestamp LIMIT 1)[OFFSET(0)] AS first_stop_lat,
--     ARRAY_AGG(lng ORDER BY start_timestamp LIMIT 1)[OFFSET(0)] AS first_stop_lng,
--     ARRAY_AGG(lat)[OFFSET(0)] AS service_approx_lat,
--     ARRAY_AGG(lng)[OFFSET(0)] AS service_approx_lng,
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
)

-- main query
SELECT
    ser.*,
    sa.driver as assigned_driver,
    sa.vehicle as assigned_vehicle,
    sa.provider as assigned_operator,
    routes.circular as route_circular,
    sites.country_id,
    countries.name as country_name,
    sa.modified as assignation_modified,
    sgi.traffic_control_incident_code,
    sgi.incident_message,
    pe.passengers_expected,
    pb.passengers_boarded,
    routes_coord.service_approx_lat,
    routes_coord.service_approx_lng,
    fsnp.nearest_polygon.city as nearest_city,
    hol.description as holiday_description,
FROM
    {{ source("busup", "services") }} AS ser
INNER JOIN {{ source("busup", "routes") }} AS routes ON routes.id = ser.route_id
INNER JOIN {{ source("busup", "sites") }} AS sites ON sites.id = routes.primary_site
INNER JOIN {{ source("busup", "countries") }} AS countries ON countries.id = sites.country_id
LEFT JOIN {{ source ("busup", "service_assignation") }} AS sa ON ser.id = sa.service_id
LEFT JOIN service_grouped_incidents AS sgi ON ser.id = sgi.service_id
LEFT JOIN passengers_expected AS pe ON ser.id = pe.service_id
LEFT JOIN passengers_boarded AS pb ON ser.id = pb.service_id
LEFT JOIN routes_coord ON routes_coord.route_id = ser.route_id
LEFT JOIN first_stop_nearest_polygon AS fsnp on fsnp.route_id= ser.route_id
LEFT JOIN {{ ref("holiday") }} AS hol ON hol.city = fsnp.nearest_polygon.city AND hol.date = EXTRACT(DATE FROM ser.timestamp)
WHERE
    ser.timestamp >= "{{ var('start_date') }}"
    AND routes.circular IS FALSE
    AND({% for status in valid_past_service_status %}
    ser.status = {{status}}
    {% if not loop.last %}OR{% endif %}
    {% endfor %}
    )
    AND({% for country in valid_countries %}
    sites.country_id = {{country}}
    {% if not loop.last %}OR{% endif %}
    {% endfor %}
    )
    AND (
        sgi.traffic_control_incident_code IS NULL
        OR sgi.traffic_control_incident_code NOT LIKE '%{{ var("incident_to_exclude") }}%'
    )
