# Broken Alibi: investigation output

Window: 2026-08-14 22:00 to 2026-08-15 00:00 UTC, plus 15 minutes either side. All times below are UTC, as stamped by the source systems.

## Chain of custody

- Source files vs published hashes: badge_access.csv OK, transactions.csv OK, device_logs.log OK
- Raw zone files vs hashes taken at ingestion: badge_access_2026-08-14.csv OK, segment-00001.log OK, changes-00000001-00000005.jsonl OK
- Rows in this answer re-hashed against raw: 20 of 20 OK
- Audit log hash chain: OK (25 entries, chain intact)
- **Overall: every check passed**

## Timeline

| Time (UTC) | Who | Source | What happened | Clock | Verified |
|---|---|---|---|---|---|
| 21:47:03 | EMP-0092 | badge | Badge IN at Lobby (1F) *(outside window)* | high | OK |
| 22:03:19 | EMP-0092 | txn | Paid $4.75 at Cafeteria | high | OK |
| 22:14:58 | EMP-0092 | badge | Badge OUT at Lobby (1F) | high | OK |
| 22:31:12 | EMP-0047 | badge | Badge IN at Executive suite (3F) | low | OK |
| 22:33:00 | EMP-0047 | device | Laptop login (success) | low | OK |
| 22:38:44 | EMP-0047 | txn | Paid $3.50 at Vending 3F | low | OK |
| 22:41:22 | EMP-0031 | device | Workstation read file /finance/audit_2025.xlsx | high | OK |
| 22:44:07 | EMP-0031 | badge | Badge IN at Server room | high | OK |
| 22:59:31 | EMP-0031 | txn | Car left through Parking exit B | low | OK |
| 23:07:44 | EMP-0031 | badge | Badge OUT at Server room | low | OK |
| 23:12:04 | EMP-0047 | device | Laptop sent an email to an external address | high | OK |
| 23:22:56 | EMP-0047 | badge | Badge OUT at Executive suite (3F) | high | OK |
| 23:41:29 | EMP-0092 | badge | Badge IN at Lobby (1F) | low | OK |
| 23:44:00 | EMP-0092 | txn | Paid $2.25 at Lobby vending | low | OK |
| 23:44:17 | EMP-0011 | device | USB device plugged into workstation | high | OK |
| 23:52:07 | EMP-0011 | badge | Badge IN at Stairwell B | high | OK |
| 23:58:02 | EMP-0047 | device | Laptop logout (success) | high | OK |
| 00:02:47 | EMP-0011 | txn | Car left through Parking exit B *(outside window)* | low | OK |
| 00:03:14 | EMP-0011 | badge | Badge OUT at Stairwell B *(outside window)* | low | OK |
| 00:11:33 | EMP-0092 | badge | Badge OUT at Lobby (1F) *(outside window)* | high | OK |

Clock = low means another system recorded an event for the same person close enough that the two could really have happened in the other order.

## Alibis

### EMP-0011: No alibi, and the activity needs explaining

> (no alibi on record)

- **NO_ALIBI** (NO_ALIBI): No alibi on record. 4 records place them in or near the building between 23:44 and 00:03.
- **SUSPICIOUS** (DEVICE_BEFORE_BADGE_IN): USB device plugged into workstation at 23:44:17, 7m 50s before their first badge-in (Stairwell B at 23:52:07). Too far apart for normal clock drift: either they got in without badging, or someone else used their machine.
- **SUSPICIOUS** (SENSITIVE_ACTIVITY): USB device plugged into workstation at 23:44:17.
- **SUSPICIOUS** (EXIT_BEFORE_BADGE_OUT): Their car left the garage at 00:02:47, 0m 27s before their badge left Stairwell B at 00:03:14. Either the clocks disagree or someone else used the badge or the parking pass.

### EMP-0031: Contradicted by the data

> I was in the server room fixing a batch job. Left before 11 PM.

- **CONTRADICTS** (LEFT_BEFORE): Said they left before 23:00, but at 23:07:44: Badge OUT at Server room.
- **SUSPICIOUS** (DEVICE_BEFORE_BADGE_IN): Workstation read file /finance/audit_2025.xlsx at 22:41:22, 2m 45s before their first badge-in (Server room at 22:44:07). That first badge is an inside door, so their building entry is missing from the data.
- **SUSPICIOUS** (SENSITIVE_ACTIVITY): Workstation read file /finance/audit_2025.xlsx at 22:41:22.
- **SUSPICIOUS** (EXIT_BEFORE_BADGE_OUT): Their car left the garage at 22:59:31, 8m 13s before their badge left Server room at 23:07:44. Either the clocks disagree or someone else used the badge or the parking pass.
- **INFO** (IN_ZONE): Badge puts them in the Server room from 22:44:07 to 23:07:44.

### EMP-0047: Doesn't fully hold up

> I was on the 3rd floor working late on the audit files. Left the building around midnight.

- **SUSPICIOUS** (ON_FLOOR): Said they were on floor 3. The floor matches, but the door is the restricted Executive suite (Badge IN at Executive suite (3F) at 22:31:12).
- **SUSPICIOUS** (SENSITIVE_ACTIVITY): Laptop sent an email to an external address at 23:12:04.
- **SUSPICIOUS** (DEVICE_AFTER_BADGE_OUT): Laptop logout (success) at 23:58:02, 36 minutes after they badged out of the Executive suite at 23:22:56. No badge record shows where they were.
- **SUSPICIOUS** (LEFT_AROUND): Said they left the building around 00:00, but no building exit (lobby, side door or parking) is recorded between 23:30 and 00:30. Last record: Laptop logout (success) at 23:58:02.

### EMP-0092: Doesn't fully hold up

> I came back to grab something from my desk after dinner. I was in and out.

- **SUSPICIOUS** (MAX_VISIT_MINUTES): Said they were in and out, but this Lobby visit lasted 28 minutes (21:47:03 to 22:14:58).
- **SUSPICIOUS** (MAX_VISIT_MINUTES): Said they were in and out, but this Lobby visit lasted 30 minutes (23:41:29 to 00:11:33).

## Run summary

- Ingested this run: badge 0, device 0, txn 0
- Curated rows: 20, quarantined: 0, duplicates dropped: 0
