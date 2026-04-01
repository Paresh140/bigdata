#!/usr/bin/env python3
"""
=============================================================================
  CISC 5950 — Lab 2  |  Part 1A  |  Task 1.1
  Reducer: Aggregate Statistics & Output Cleaned Dataset
  NYC Parking Violations Dataset
=============================================================================
  Responsibilities:
    • Write cleaned CSV header then stream DATA rows directly to stdout
      (never buffer millions of rows in RAM)
    • Aggregate the tiny STAT counters emitted by each mapper
    • Print a formatted Data Quality Report at the very end
=============================================================================
"""

import sys

# ---------------------------------------------------------------------------
# Output CSV header  (original 40 columns + engineered hour_of_day)
# ---------------------------------------------------------------------------

HEADER = [
    "summons_number", "plate_id", "registration_state", "plate_type",
    "issue_date", "violation_code", "vehicle_body_type", "vehicle_make",
    "issuing_agency", "street_code1", "street_code2", "street_code3",
    "vehicle_expiration_date", "violation_location", "violation_precinct",
    "issuer_precinct", "issuer_code", "issuer_command", "issuer_squad",
    "violation_time", "time_first_observed", "violation_county",
    "violation_in_front_of_or", "house_number", "street_name",
    "intersecting_street", "date_first_observed", "law_section",
    "sub_division", "violation_legal_code", "days_parking_in_effect",
    "from_hours_in_effect", "to_hours_in_effect", "vehicle_color",
    "unregistered_vehicle", "vehicle_year", "meter_number",
    "feet_from_curb", "violation_post_code", "violation_description",
    "no_standing_or_stopping", "hydrant_violation",
    "double_parking_violation", "hour_of_day",
]

# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

BOX_W = 62

def _bar(fill='─', left='╠', right='╣'):
    return f"{left}{fill * (BOX_W - 2)}{right}"

def _row(label, value, pct=None):
    if pct is not None:
        right_part = f"{value:>12,}  ({pct:5.1f}%)"
    else:
        right_part = f"{value:>12,}"
    left_part = f"  {label}"
    gap = BOX_W - 2 - len(left_part) - len(right_part)
    return f"║{left_part}{' ' * max(gap, 1)}{right_part}║"

def _section(title):
    padded = f"  {title}  "
    side = (BOX_W - 2 - len(padded)) // 2
    return f"║{'═' * side}{padded}{'═' * (BOX_W - 2 - side - len(padded))}║"

def print_quality_report(total, valid, invalid_date, missing_crit,
                          color_corr, state_corr):
    pct = lambda n: (n / total * 100) if total else 0.0

    lines = [
        f"╔{'═' * (BOX_W - 2)}╗",
        f"║{'  TASK 1.1 — DATA QUALITY REPORT':^{BOX_W - 2}}║",
        f"║{'  NYC Parking Violations  ·  2024 – 2025':^{BOX_W - 2}}║",
        _bar('═', '╠', '╣'),
        _section("INPUT SUMMARY"),
        _bar('═', '╠', '╣'),
        _row("Total raw records read",          total),
        _row("Valid records (output)",           valid,        pct(valid)),
        _bar('═', '╠', '╣'),
        _section("RECORDS DISCARDED"),
        _bar('═', '╠', '╣'),
        _row("Outside 2024-2025 date range",    invalid_date, pct(invalid_date)),
        _row("Missing/invalid critical fields", missing_crit, pct(missing_crit)),
        _row("Total discarded",                 invalid_date + missing_crit,
             pct(invalid_date + missing_crit)),
        _bar('═', '╠', '╣'),
        _section("STANDARDIZATION CORRECTIONS"),
        _bar('═', '╠', '╣'),
        _row("Vehicle color corrections",       color_corr),
        _row("State code corrections",          state_corr),
        _row("Total corrections applied",       color_corr + state_corr),
        _bar('═', '╠', '╣'),
        _section("DATA RETENTION RATE"),
        _bar('═', '╠', '╣'),
        f"║  {'Retention':<30} {'█' * int(pct(valid) / 2):.<29} {pct(valid):5.1f}% ║",
        f"╚{'═' * (BOX_W - 2)}╝",
    ]

    sys.stdout.write("\n")
    for l in lines:
        # RPT prefix lets run.sh grep these lines reliably
        print(f"RPT{l}")
    sys.stdout.write("\n")


# ---------------------------------------------------------------------------
# Main reducer logic
# ---------------------------------------------------------------------------

def main():
    total        = 0
    valid        = 0
    invalid_date = 0
    missing_crit = 0
    color_corr   = 0
    state_corr   = 0

    # META lines: only ~6 per mapper — safe to hold in memory.
    # REC lines: potentially millions — stream straight to stdout, never buffer.
    meta_lines = []

    out = sys.stdout

    # Emit CSV header immediately
    out.write(','.join(HEADER) + '\n')

    # Single pass: stream REC rows, collect only META lines
    for line in sys.stdin:
        line = line.strip()
        if not line or '::' not in line:
            continue

        record_type, _, data = line.partition('::')

        if record_type == 'REC':
            # Write directly — zero memory accumulation
            out.write(data + '\n')

        elif record_type == 'META':
            # Tiny — ~6 lines per mapper
            meta_lines.append(data)

    out.flush()

    # Aggregate META counters (all REC rows already written above)
    for data in meta_lines:
        stat_key, _, raw_val = data.partition('=')
        try:
            val = int(raw_val)
        except ValueError:
            continue
        if   stat_key == 'TOTAL':             total        += val
        elif stat_key == 'VALID':             valid        += val
        elif stat_key == 'INVALID_DATE':      invalid_date += val
        elif stat_key == 'MISSING_CRITICAL':  missing_crit += val
        elif stat_key == 'COLOR_CORRECTION':  color_corr   += val
        elif stat_key == 'STATE_CORRECTION':  state_corr   += val

    # Emit quality report
    if total > 0:
        print_quality_report(total, valid, invalid_date, missing_crit,
                              color_corr, state_corr)


if __name__ == "__main__":
    main()
