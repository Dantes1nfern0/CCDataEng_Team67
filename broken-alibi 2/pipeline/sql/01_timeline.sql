-- The investigation question: what happened between 10 PM and midnight on Aug 14, 2026 (UTC)?
-- One query over one table. We also pull 15 minutes either side of the window,
-- because a 00:03 exit decides whether "left around midnight" holds up.

SELECT strftime(event_timestamp, '%Y-%m-%d %H:%M:%S')            AS time_utc,
       CASE WHEN event_timestamp BETWEEN getvariable('window_start') AND getvariable('window_end')
            THEN 'in window' ELSE 'just outside' END            AS window_position,
       employee_id,
       event_source,
       event_type,
       description,
       zone,
       location,
       clock_confidence,
       clock_note,
       raw_event_hash,
       event_id
FROM curated.employee_activity_timeline
WHERE event_timestamp BETWEEN getvariable('window_start') - to_minutes(getvariable('margin_minutes'))
                          AND getvariable('window_end')   + to_minutes(getvariable('margin_minutes'))
  AND employee_id IN ('EMP-0047', 'EMP-0031', 'EMP-0092', 'EMP-0011')
ORDER BY event_timestamp, employee_id;
