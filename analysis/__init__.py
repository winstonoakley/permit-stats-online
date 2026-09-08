"""
Analysis pipeline for the permit tool's analytics events.

    python -m analysis report --since 2026-01-01 [--source prod|local]

Stages: pull (fetch and cache raw JSONL) -> transform (tidy tables, traffic
filtering) -> checks (fail loudly on bad data) -> metrics (one function per
question in ANALYTICS_NOTES.md) -> report (dated HTML under reports/).

Runtime deps live in requirements-analysis.txt, separate from the web app.
"""

SUPPORTED_SCHEMA_VERSION = 2

# The recreation.gov lottery for Enchantments overnight permits is open from
# February 15 through March 1. Traffic is expected to spike in this window.
LOTTERY_WINDOW = ((2, 15), (3, 1))   # (month, day) inclusive bounds

# Days are counted in the lottery's time zone, not UTC.
DEFAULT_TZ = "America/Los_Angeles"

KNOWN_EVENT_TYPES = ("page_view", "get_table", "heatmap_view", "error", "ad_view")


class DataQualityError(RuntimeError):
    """Raised when a data-quality check with severity 'error' fails."""
