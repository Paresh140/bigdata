#!/usr/bin/env python3
"""
=============================================================================
  CISC 5950 — Lab 2  |  Part 1A  |  Task 1.1
  Mapper: Data Quality Assessment & Cleaning
  NYC Parking Violations Dataset
=============================================================================
  Responsibilities:
    • Skip malformed / incomplete records
    • Validate issue_date is within 2024–2025
    • Standardize vehicle colors  (e.g. BLK → BLACK)
    • Standardize state codes     (e.g. ny  → NY)
    • Engineer hour_of_day from violation_time
    • Emit cleaned DATA rows + per-mapper STAT counters
=============================================================================
"""

import sys

# ---------------------------------------------------------------------------
# Lookup tables
# ---------------------------------------------------------------------------

COLOR_MAP = {
    # Black variants
    'BLK': 'BLACK', 'BLACK': 'BLACK', 'BK': 'BLACK', 'BL': 'BLACK',
    # White variants
    'WH': 'WHITE', 'WHITE': 'WHITE', 'WHT': 'WHITE',
    # Gray variants
    'GY': 'GRAY', 'GRAY': 'GRAY', 'GREY': 'GRAY', 'GR': 'GRAY',
    # Red variants
    'RD': 'RED', 'RED': 'RED',
    # Blue variants
    'BLUE': 'BLUE',
    # Green variants
    'GRN': 'GREEN', 'GREEN': 'GREEN',
    # Yellow variants
    'YW': 'YELLOW', 'YELLOW': 'YELLOW', 'YELL': 'YELLOW',
    # Tan variants
    'TN': 'TAN', 'TAN': 'TAN',
    # Brown variants
    'BR': 'BROWN', 'BROWN': 'BROWN',
    # Silver variants
    'SLV': 'SILVER', 'SILVER': 'SILVER', 'SILVE': 'SILVER',
}

STATE_MAP = {
    # New York
    'ny': 'NY', 'NY': 'NY', 'new york': 'NY', 'n.y.': 'NY',
    # New Jersey
    'nj': 'NJ', 'NJ': 'NJ', 'new jersey': 'NJ',
    # Connecticut
    'ct': 'CT', 'CT': 'CT', 'connecticut': 'CT',
    # Pennsylvania
    'pa': 'PA', 'PA': 'PA', 'pennsylvania': 'PA',
}

INVALID_PLATES = {'BLANKPLATE', 'BLANK', 'NOPLATE', 'MISSING', 'UNKNOWN', '0', '99', '00'}
INVALID_STATES = {'99', '00', '0', '', 'UNKNOWN'}

MIN_FIELDS = 36  # minimum columns expected in a valid row

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def parse_time_to_hour(time_str):
    """
    Convert violation_time string (e.g. '0823A', '1145P') to 24-hour integer.
    Returns -1 when the value cannot be parsed.
    """
    if not time_str or not time_str.strip():
        return -1
    try:
        t = time_str.strip().upper()
        if len(t) < 4:
            return -1
        hour = int(t[:2])
        ampm = next((c for c in t[2:] if c in ('A', 'P')), None)
        if ampm == 'P' and hour != 12:
            hour += 12
        elif ampm == 'A' and hour == 12:
            hour = 0
        return hour if 0 <= hour <= 23 else -1
    except Exception:
        return -1


def standardize_color(color):
    """Map raw color string to canonical uppercase color name."""
    if not color or not color.strip():
        return 'UNKNOWN'
    return COLOR_MAP.get(color.strip().upper(), color.strip().upper())


def standardize_state(state):
    """Map raw state string to canonical 2-letter uppercase code."""
    if not state or not state.strip():
        return 'UNKNOWN'
    cleaned = state.strip().lower()
    return STATE_MAP.get(cleaned, cleaned.upper())


def is_valid_date(date_str):
    """
    Return True only when issue_date falls within 2024–2025.
    Handles ISO format: '2024-07-20T00:00:00.000'
    """
    if not date_str or not date_str.strip():
        return False
    try:
        year = int(date_str.strip().strip('"').split('-')[0])
        return 2024 <= year <= 2025
    except Exception:
        return False


def is_valid_record(plate_id, violation_code, issue_date, reg_state):
    """
    Gate-keep the four critical fields that must be present and non-trivial.
    Returns False if the record should be discarded.
    """
    # Plate ID
    if not plate_id or not plate_id.strip() or plate_id.strip() == '""':
        return False
    if plate_id.strip().upper().strip('"') in INVALID_PLATES:
        return False
    # Violation code
    if not violation_code or not violation_code.strip() or violation_code.strip() == '""':
        return False
    # Issue date
    if not issue_date or not issue_date.strip() or issue_date.strip() == '""':
        return False
    # Registration state (optional field — only fail if explicitly invalid)
    if reg_state and reg_state.strip().upper().strip('"') in INVALID_STATES:
        return False
    return True


# ---------------------------------------------------------------------------
# Main mapper logic
# ---------------------------------------------------------------------------

def main():
    # ── Skip header line ────────────────────────────────────────────────────
    first_line = sys.stdin.readline().strip()
    if not first_line:
        return

    # ── Counters ────────────────────────────────────────────────────────────
    total              = 0
    valid              = 0
    invalid_date_cnt   = 0
    missing_crit_cnt   = 0
    color_corr_cnt     = 0
    state_corr_cnt     = 0

    # ── Process data lines ──────────────────────────────────────────────────
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        total += 1
        fields = [f.strip().strip('"') for f in line.split(',')]

        # ── Structural check ─────────────────────────────────────────────
        if len(fields) < MIN_FIELDS:
            missing_crit_cnt += 1
            continue

        try:
            plate_id           = fields[1]
            registration_state = fields[2]
            issue_date         = fields[4]
            violation_code     = fields[5]
            violation_time     = fields[19]
            vehicle_color      = fields[33]
        except IndexError:
            missing_crit_cnt += 1
            continue

        # ── Critical field validation ────────────────────────────────────
        if not is_valid_record(plate_id, violation_code, issue_date, registration_state):
            missing_crit_cnt += 1
            continue

        # ── Date range filter (2024–2025) ────────────────────────────────
        if not is_valid_date(issue_date):
            invalid_date_cnt += 1
            continue

        valid += 1

        # ── Color standardization ────────────────────────────────────────
        std_color = standardize_color(vehicle_color)
        if vehicle_color != std_color:
            color_corr_cnt += 1
            fields[33] = std_color

        # ── State standardization ────────────────────────────────────────
        std_state = standardize_state(registration_state)
        if registration_state != std_state:
            state_corr_cnt += 1
            fields[2] = std_state

        # ── Time feature engineering ─────────────────────────────────────
        hour = parse_time_to_hour(violation_time)

        # ── Emit cleaned row ─────────────────────────────────────────────
        print(f"REC::{','.join(fields + [str(hour)])}")

    # ── Emit per-mapper statistics ──────────────────────────────────────────
    print(f"META::TOTAL={total}")
    print(f"META::VALID={valid}")
    print(f"META::INVALID_DATE={invalid_date_cnt}")
    print(f"META::MISSING_CRITICAL={missing_crit_cnt}")
    print(f"META::COLOR_CORRECTION={color_corr_cnt}")
    print(f"META::STATE_CORRECTION={state_corr_cnt}")


if __name__ == "__main__":
    main()
