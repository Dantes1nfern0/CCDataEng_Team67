import os
from pathlib import Path

PIPELINE_DIR = Path(__file__).resolve().parent.parent
SOURCE_DIR = Path(os.environ.get("BLACKWOOD_SOURCE_DIR", PIPELINE_DIR.parent / "data" / "raw"))
OUTPUT_DIR = Path(os.environ.get("BLACKWOOD_OUTPUT_DIR", PIPELINE_DIR / "output"))
REFERENCE_DIR = PIPELINE_DIR / "reference"
SQL_DIR = PIPELINE_DIR / "sql"

# The investigation window. The data is stamped Z (UTC) and we read the brief's
# "10 PM to midnight on Aug 14" as UTC. If it means Eastern time (UTC-4 in
# August), change these two lines to 2026-08-15 02:00 and 04:00.
WINDOW_START = "2026-08-14 22:00:00+00"
WINDOW_END = "2026-08-15 00:00:00+00"
# Show events this many minutes either side of the window, so a 00:02 exit is not cut off.
WINDOW_MARGIN_MINUTES = 15

# How far apart two clocks may be when we have no evidence about them.
# We only have ordering evidence for badge vs parking (see clean step).
DEFAULT_CLOCK_TOLERANCE_SECONDS = 120

# Badge exports land once a night. We assume the export for day D arrives at 02:00 UTC on D+1.
BADGE_EXPORT_HOUR_UTC = 2
