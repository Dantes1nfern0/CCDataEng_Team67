-- Clock confidence: which events could be in a different order than their timestamps say?
--
-- Ordering rule: a person badges out of a room before their car leaves the garage.
-- When the parking exit comes first, the badge and parking clocks disagree (or someone
-- else used the badge or the pass). The largest gap we see becomes the tolerance for
-- that pair of systems. Pairs with no evidence get a default tolerance.

CREATE OR REPLACE TABLE curated.clock_offset_evidence AS
SELECT p.employee_id,
       p.event_timestamp                                       AS parking_exit_at,
       b.event_timestamp                                       AS badge_out_at,
       b.zone                                                  AS badge_zone,
       date_diff('second', p.event_timestamp, b.event_timestamp) AS seconds_out_of_order,
       p.event_id                                              AS parking_event_id,
       b.event_id                                              AS badge_event_id
FROM curated.employee_activity_timeline p
JOIN curated.employee_activity_timeline b
  ON b.employee_id = p.employee_id
 AND b.event_type = 'BADGE_OUT'
 AND b.event_timestamp > p.event_timestamp
 AND b.event_timestamp <= p.event_timestamp + INTERVAL 30 MINUTE
WHERE p.event_type = 'PARKING_EXIT';

CREATE OR REPLACE TABLE curated.clock_tolerance AS
WITH pairs(source_a, source_b) AS (VALUES ('badge', 'device'), ('badge', 'txn'), ('device', 'txn')),
     ev AS (SELECT max(seconds_out_of_order) AS worst, count(*) AS n FROM curated.clock_offset_evidence)
SELECT source_a, source_b,
       CASE WHEN (source_a, source_b) = ('badge', 'txn') AND ev.n > 0
            THEN greatest(ev.worst, getvariable('default_tolerance'))
            ELSE getvariable('default_tolerance') END AS tolerance_seconds,
       CASE WHEN (source_a, source_b) = ('badge', 'txn') AND ev.n > 0
            THEN format('largest badge-vs-parking ordering gap seen across {} pairs', ev.n)
            ELSE 'no ordering evidence in the data; default assumption' END AS basis
FROM pairs, ev;

-- Any two events for the same person, from different systems, closer together than
-- that pair's tolerance, may really have happened in either order.
CREATE OR REPLACE TEMP TABLE _uncertain AS
SELECT a.event_id,
       string_agg(format('{} {} at {} ({}s apart, tolerance {}s)',
                         b.event_source, b.event_type, strftime(b.event_timestamp, '%H:%M:%S'),
                         abs(date_diff('second', a.event_timestamp, b.event_timestamp)), c.tolerance_seconds),
                  '; ' ORDER BY b.event_timestamp) AS note
FROM curated.employee_activity_timeline a
JOIN curated.employee_activity_timeline b
  ON b.employee_id = a.employee_id AND b.event_source <> a.event_source
JOIN curated.clock_tolerance c
  ON c.source_a = least(a.event_source, b.event_source)
 AND c.source_b = greatest(a.event_source, b.event_source)
WHERE abs(date_diff('second', a.event_timestamp, b.event_timestamp)) <= c.tolerance_seconds
GROUP BY a.event_id;

UPDATE curated.employee_activity_timeline AS t
SET clock_confidence = 'low', clock_note = 'Order uncertain vs ' || u.note
FROM _uncertain u
WHERE t.event_id = u.event_id;
