"""Shared date parsing for the messy created_at/updated_at audit columns.

Used ONLY for watermark computation during extraction (deciding what to pull
next run) — this is an extraction-time concern. Full business-date typing
(conversion_time, impression_time, etc.) happens in Silver SQL instead, per
the medallion principle that Bronze/extraction stays as close to "raw" as
possible. Reuses the same format list validated during Q1/Q2's forensic
analysis of this dataset (data guide's own "Parsing cheat-sheet").
"""

import re
from datetime import datetime, timezone

_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y",
    "%m/%d/%Y %H:%M:%S",
    "%d-%m-%Y %H:%M:%S",
    "%d-%m-%Y",
    "%Y-%m-%dT%H:%M:%S",
    "%b %d, %Y",
    "%d %b %Y",
]

_TZ_SUFFIX = re.compile(r"\s*(UTC|GMT|EST|EDT|PST|PDT|CST|CDT)$")
_TZ_OFFSET = re.compile(r"([+-]\d{2}:\d{2})$")  # colon required — see Q1's forensics note on why


def parse_dt(raw):
    """Best-effort parse of a messy date string to a naive UTC datetime.
    Returns None if unparseable (never raises) — the caller decides whether
    that's fatal (quarantine) or just "no watermark signal from this row".
    """
    if raw is None:
        return None
    s = raw.strip()
    if not s or s.upper() == "N/A":
        return None
    if re.match(r"^\d{13}$", s):
        return datetime.fromtimestamp(int(s) / 1000, tz=timezone.utc).replace(tzinfo=None)
    if re.match(r"^\d{10}$", s):
        return datetime.fromtimestamp(int(s), tz=timezone.utc).replace(tzinfo=None)
    s1 = _TZ_OFFSET.sub("", s)
    s1 = _TZ_SUFFIX.sub("", s1).rstrip("Z").strip()
    for fmt in _FORMATS:
        try:
            return datetime.strptime(s1, fmt)
        except ValueError:
            continue
    return None


def max_watermark(*raw_values):
    """Highest parseable timestamp among the given raw strings, or None."""
    parsed = [parse_dt(v) for v in raw_values]
    parsed = [p for p in parsed if p is not None]
    return max(parsed) if parsed else None
