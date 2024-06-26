##
## Copyright (c) 2023-2024 by Dribia Data Research.
## This file is part of project Brainer,
## and is released under the MIT License Agreement.
## See the LICENSE file for more information.
##

{% set valid_countries = var('valid_countries') %}
{% set valid_past_service_status = var('valid_past_service_status') %}

-- select services to be excluded due to incident major force (11)
WITH service_to_exclude_due_to_incident AS (
SELECT
    service_id
FROM
    {{ source ("busup", "traffic_control_incidents") }} AS tci
LEFT JOIN
   {{ source ("busup", "traffic_control_incidents_codes") }} AS tcic
ON
    tcic.id = tci.traffic_control_incident_code
WHERE
    tci.deleted_at IS NULL
    AND tci.traffic_control_incident_code = {{ var("incident_to_exclude") }}
),

-- select only ticket consumptions with no errors
valid_ticket_consumptions AS (
    SELECT
        *
    FROM
        {{ source("busup", "ticket_consumptions") }}
    WHERE
        ticket_consumptions.error IS FALSE
)

-- main query
SELECT
    ROW_NUMBER() OVER (ORDER BY bookings.id, ser.id, ser.timestamp) AS reservation_id,
    -- Assign a reservation_id to each row ordering over bookings and services
    -- between bookings start_timestamp and end_timestamp.
    bookings.id AS booking_id,
    ser.id AS service_id,
    bookings.route_id AS route_id,
    bookings.route_stop_id AS reservation_route_stop_id,
    bookings.status AS booking_status,
    ser.status AS service_status,
    valid_ticket_consumptions.id AS boarding_id,
    valid_ticket_consumptions.timestamp AS boarding_timestamp,
    valid_ticket_consumptions.route_stop_id AS boarding_route_stop_id,
    valid_ticket_consumptions.distance_route_stop AS boarding_distance_to_stop,
FROM
    {{ source("busup", "bookings") }} AS bookings
INNER JOIN {{ source("busup", "services") }} as ser ON bookings.route_id = ser.route_id
INNER JOIN {{ source("busup", "routes") }} as routes ON routes.id = ser.route_id
INNER JOIN {{ source("busup", "sites") }} AS sites ON sites.id = routes.primary_site
LEFT JOIN valid_ticket_consumptions ON ser.id = valid_ticket_consumptions.service_id AND bookings.id = valid_ticket_consumptions.booking_id
WHERE
    ser.timestamp >= "{{ var('start_date') }}"
    AND routes.circular IS FALSE
    AND({% for status in valid_past_service_status %}
    ser.status = {{status}}
    {% if not loop.last %}OR{% endif %}
    {% endfor %}
    )
    AND ser.timestamp BETWEEN bookings.start_timestamp AND bookings.end_timestamp
    AND({% for country in valid_countries %}
    sites.country_id = {{country}}
    {% if not loop.last %}OR{% endif %}
    {% endfor %}
    )
    AND ser.id NOT IN (SELECT * FROM service_to_exclude_due_to_incident)
ORDER BY
    reservation_id
