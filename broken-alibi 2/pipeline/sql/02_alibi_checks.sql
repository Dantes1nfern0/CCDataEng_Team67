-- Alibi checks. Each alibi is stored as data in ref.alibi_claims (reference/alibi_claims.csv),
-- and each claim type has one rule below. Generic rules (6 to 9) run for everyone.
-- Every finding carries the event_id and raw_event_hash of the record that backs it.
--
-- Severity:  CONTRADICTS = the data says the opposite of the claim
--            SUSPICIOUS  = doesn't fit the claim, or needs explaining
--            INFO        = context that supports or frames the claim
--            NO_ALIBI    = person was there with no alibi on record

CREATE OR REPLACE TABLE curated.alibi_findings AS
WITH
t AS (
    SELECT * FROM curated.employee_activity_timeline
    WHERE event_timestamp BETWEEN getvariable('window_start') - INTERVAL 1 HOUR
                              AND getvariable('window_end')   + INTERVAL 1 HOUR
),
claims AS (SELECT * FROM ref.alibi_claims),
people AS (SELECT DISTINCT employee_id FROM claims),
loc    AS (SELECT * FROM ref.locations),
building_exits AS (
    SELECT t.* FROM t JOIN loc ON loc.code = t.location
    WHERE loc.is_building_entrance AND t.event_type IN ('BADGE_OUT', 'PARKING_EXIT')
),
-- Each badge IN paired with the next badge OUT at the same door.
badge_visits AS (
    SELECT * FROM (
        SELECT employee_id, location, zone, event_type,
               event_timestamp                 AS in_at,
               event_id                        AS in_event_id,
               raw_event_hash                  AS in_hash,
               lead(event_timestamp) OVER w    AS out_at,
               lead(event_type)      OVER w    AS next_type
        FROM t WHERE event_source = 'badge'
        WINDOW w AS (PARTITION BY employee_id, location ORDER BY event_timestamp)
    ) WHERE event_type = 'BADGE_IN' AND next_type = 'BADGE_OUT'
),
badge_device_tol AS (
    SELECT tolerance_seconds FROM curated.clock_tolerance WHERE source_a = 'badge' AND source_b = 'device'
),

-- 1. "Left before X": anything recorded after X contradicts it.
left_before AS (
    SELECT c.employee_id, 'LEFT_BEFORE' AS check_name, 'CONTRADICTS' AS severity,
           t.event_timestamp AS evidence_at,
           format('Said they left before {}, but at {}: {}.',
                  strftime(c.claim_time, '%H:%M'), strftime(t.event_timestamp, '%H:%M:%S'), t.description) AS finding,
           t.event_id, t.raw_event_hash
    FROM claims c JOIN t ON t.employee_id = c.employee_id AND t.event_timestamp > c.claim_time
    WHERE c.claim_type = 'LEFT_BEFORE'
),

-- 2. "Left around X": expect a building exit within claim_value minutes of X.
last_seen AS (
    SELECT * FROM t QUALIFY row_number() OVER (PARTITION BY employee_id ORDER BY event_timestamp DESC) = 1
),
left_around AS (
    SELECT c.employee_id, 'LEFT_AROUND' AS check_name, 'SUSPICIOUS' AS severity,
           l.event_timestamp AS evidence_at,
           format('Said they left the building around {}, but no building exit (lobby, side door or parking) '
                  || 'is recorded between {} and {}. Last record: {} at {}.',
                  strftime(c.claim_time, '%H:%M'),
                  strftime(c.claim_time - to_minutes(CAST(c.claim_value AS INT)), '%H:%M'),
                  strftime(c.claim_time + to_minutes(CAST(c.claim_value AS INT)), '%H:%M'),
                  l.description, strftime(l.event_timestamp, '%H:%M:%S')) AS finding,
           l.event_id, l.raw_event_hash
    FROM claims c JOIN last_seen l USING (employee_id)
    WHERE c.claim_type = 'LEFT_AROUND'
      AND NOT EXISTS (
          SELECT 1 FROM building_exits e
          WHERE e.employee_id = c.employee_id
            AND e.event_timestamp BETWEEN c.claim_time - to_minutes(CAST(c.claim_value AS INT))
                                      AND c.claim_time + to_minutes(CAST(c.claim_value AS INT)))
),

-- 3. "I was on floor N": a known different floor contradicts it; entering a restricted zone on it is flagged.
on_floor AS (
    SELECT c.employee_id, 'ON_FLOOR' AS check_name,
           CASE WHEN t.floor <> c.claim_value THEN 'CONTRADICTS' ELSE 'SUSPICIOUS' END AS severity,
           t.event_timestamp AS evidence_at,
           CASE WHEN t.floor <> c.claim_value
                THEN format('Said they were on floor {}, but at {}: {} (floor {}).',
                            c.claim_value, strftime(t.event_timestamp, '%H:%M:%S'), t.description, t.floor)
                ELSE format('Said they were on floor {}. The floor matches, but the door is the restricted {} ({} at {}).',
                            c.claim_value, t.zone, t.description, strftime(t.event_timestamp, '%H:%M:%S'))
           END AS finding,
           t.event_id, t.raw_event_hash
    FROM claims c
    JOIN t   ON t.employee_id = c.employee_id
    JOIN loc ON loc.code = t.location
    WHERE c.claim_type = 'ON_FLOOR' AND t.floor IS NOT NULL
      AND (t.floor <> c.claim_value OR (loc.restricted AND t.event_type = 'BADGE_IN'))
),

-- 4. "I was in zone Z": report how long the badge puts them there.
in_zone AS (
    SELECT c.employee_id, 'IN_ZONE' AS check_name, 'INFO' AS severity, v.in_at AS evidence_at,
           format('Badge puts them in the {} from {} to {}.',
                  v.zone, strftime(v.in_at, '%H:%M:%S'), strftime(v.out_at, '%H:%M:%S')) AS finding,
           v.in_event_id AS event_id, v.in_hash AS raw_event_hash
    FROM claims c JOIN badge_visits v ON v.employee_id = c.employee_id AND v.zone = c.claim_value
    WHERE c.claim_type = 'IN_ZONE'
),

-- 5. "In and out": a visit longer than claim_value minutes stretches it.
max_visit AS (
    SELECT c.employee_id, 'MAX_VISIT_MINUTES' AS check_name, 'SUSPICIOUS' AS severity, v.in_at AS evidence_at,
           format('Said they were in and out, but this {} visit lasted {} minutes ({} to {}).',
                  v.zone, CAST(round(date_diff('second', v.in_at, v.out_at) / 60.0) AS INT),
                  strftime(v.in_at, '%H:%M:%S'), strftime(v.out_at, '%H:%M:%S')) AS finding,
           v.in_event_id AS event_id, v.in_hash AS raw_event_hash
    FROM claims c JOIN badge_visits v ON v.employee_id = c.employee_id
    WHERE c.claim_type = 'MAX_VISIT_MINUTES'
      AND date_diff('second', v.in_at, v.out_at) > CAST(c.claim_value AS INT) * 60
),

-- 6. Device activity before the person's first badge-in of the night.
first_badge_in AS (
    SELECT t.employee_id, min(t.event_timestamp) AS first_in, arg_min(t.zone, t.event_timestamp) AS first_zone,
           arg_min(loc.is_building_entrance, t.event_timestamp) AS first_is_entrance
    FROM t JOIN loc ON loc.code = t.location
    WHERE t.event_type = 'BADGE_IN' GROUP BY t.employee_id
),
device_before_badge AS (
    SELECT d.employee_id, 'DEVICE_BEFORE_BADGE_IN' AS check_name, 'SUSPICIOUS' AS severity,
           d.event_timestamp AS evidence_at,
           CASE WHEN b.first_in IS NULL
                THEN format('{} at {}, but no badge-in for them is in the data.',
                            d.description, strftime(d.event_timestamp, '%H:%M:%S'))
                ELSE format('{} at {}, {}m {}s before their first badge-in ({} at {}). {}',
                            d.description, strftime(d.event_timestamp, '%H:%M:%S'),
                            date_diff('second', d.event_timestamp, b.first_in) // 60,
                            date_diff('second', d.event_timestamp, b.first_in) % 60,
                            b.first_zone, strftime(b.first_in, '%H:%M:%S'),
                            CASE WHEN NOT b.first_is_entrance
                                 THEN 'That first badge is an inside door, so their building entry is missing from the data.'
                                 WHEN date_diff('second', d.event_timestamp, b.first_in) <= tol.tolerance_seconds
                                 THEN 'Close enough that clock drift could explain it.'
                                 ELSE 'Too far apart for normal clock drift: either they got in without badging, or someone else used their machine.'
                            END)
           END AS finding,
           d.event_id, d.raw_event_hash
    FROM t d
    JOIN people USING (employee_id)
    LEFT JOIN first_badge_in b USING (employee_id)
    CROSS JOIN badge_device_tol tol
    WHERE d.event_source = 'device' AND (b.first_in IS NULL OR d.event_timestamp < b.first_in)
),

-- 7. The car left the garage before the badge left the room.
exit_before_badge_out AS (
    SELECT e.employee_id, 'EXIT_BEFORE_BADGE_OUT' AS check_name, 'SUSPICIOUS' AS severity,
           e.parking_exit_at AS evidence_at,
           format('Their car left the garage at {}, {}m {}s before their badge left {} at {}. '
                  || 'Either the clocks disagree or someone else used the badge or the parking pass.',
                  strftime(e.parking_exit_at, '%H:%M:%S'), e.seconds_out_of_order // 60, e.seconds_out_of_order % 60,
                  e.badge_zone, strftime(e.badge_out_at, '%H:%M:%S')) AS finding,
           e.parking_event_id AS event_id, t.raw_event_hash
    FROM curated.clock_offset_evidence e
    JOIN people USING (employee_id)
    JOIN t ON t.event_id = e.parking_event_id
),

-- 8. Device still active after the person's most recent badge record was an OUT.
badge_only AS (SELECT * FROM t WHERE event_source = 'badge'),
device_after_badge_out AS (
    SELECT d.employee_id, 'DEVICE_AFTER_BADGE_OUT' AS check_name, 'SUSPICIOUS' AS severity,
           d.event_timestamp AS evidence_at,
           format('{} at {}, {} minutes after they badged out of the {} at {}. No badge record shows where they were.',
                  d.description, strftime(d.event_timestamp, '%H:%M:%S'),
                  date_diff('minute', b.event_timestamp, d.event_timestamp),
                  b.zone, strftime(b.event_timestamp, '%H:%M:%S')) AS finding,
           d.event_id, d.raw_event_hash
    FROM t d
    ASOF JOIN badge_only b ON d.employee_id = b.employee_id AND d.event_timestamp >= b.event_timestamp
    WHERE d.event_source = 'device' AND b.event_type = 'BADGE_OUT'
),

-- 9. Sensitive actions: finance files, external email, USB devices.
sensitive AS (
    SELECT t.employee_id, 'SENSITIVE_ACTIVITY' AS check_name, 'SUSPICIOUS' AS severity,
           t.event_timestamp AS evidence_at,
           format('{} at {}.', t.description, strftime(t.event_timestamp, '%H:%M:%S')) AS finding,
           t.event_id, t.raw_event_hash
    FROM t JOIN people USING (employee_id)
    WHERE (t.event_type = 'FILE_ACCESS' AND json_extract_string(t.details, '$.path') LIKE '/finance/%')
       OR (t.event_type = 'EMAIL_SENT'  AND json_extract_string(t.details, '$.to') = 'external')
       OR  t.event_type = 'USB_INSERTED'
),

-- 10. In the building with no alibi.
no_alibi AS (
    SELECT c.employee_id, 'NO_ALIBI' AS check_name, 'NO_ALIBI' AS severity,
           min(t.event_timestamp) AS evidence_at,
           format('No alibi on record. {} records place them in or near the building between {} and {}.',
                  count(t.event_id), strftime(min(t.event_timestamp), '%H:%M'), strftime(max(t.event_timestamp), '%H:%M')) AS finding,
           NULL AS event_id, NULL AS raw_event_hash
    FROM claims c LEFT JOIN t USING (employee_id)
    WHERE c.claim_type = 'NO_ALIBI'
    GROUP BY c.employee_id
),

all_findings AS (
    SELECT * FROM left_before
    UNION ALL BY NAME SELECT * FROM left_around
    UNION ALL BY NAME SELECT * FROM on_floor
    UNION ALL BY NAME SELECT * FROM in_zone
    UNION ALL BY NAME SELECT * FROM max_visit
    UNION ALL BY NAME SELECT * FROM device_before_badge
    UNION ALL BY NAME SELECT * FROM exit_before_badge_out
    UNION ALL BY NAME SELECT * FROM device_after_badge_out
    UNION ALL BY NAME SELECT * FROM sensitive
    UNION ALL BY NAME SELECT * FROM no_alibi
)
SELECT f.employee_id,
       (SELECT any_value(alibi_text) FROM claims c WHERE c.employee_id = f.employee_id) AS alibi_text,
       f.check_name, f.severity, f.evidence_at, f.finding, f.event_id, f.raw_event_hash
FROM all_findings f
ORDER BY f.employee_id, f.evidence_at NULLS FIRST, f.check_name;
