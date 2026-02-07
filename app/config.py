"""
Central configuration for sponsor–event matching.
All values here are intended to be stored in a database table and loaded at runtime.
"""

from typing import Any, Dict, List

# ---------------------------------------------------------------------------
# Budget
# ---------------------------------------------------------------------------
BUDGET_NEAR_BOUNDARY_RATIO = 0.1  # 10% of range = "near boundary" partial match.

# ---------------------------------------------------------------------------
# Footfall
# ---------------------------------------------------------------------------
FOOTFALL_PARTIAL_MATCH_RATIO = 0.8  # Event footfall >= 80% of min = partial match.

# ---------------------------------------------------------------------------
# Default scoring weights (must sum to 100 if all criteria used)
# ---------------------------------------------------------------------------
DEFAULT_WEIGHTS = {
    "geo_weight": 40.0,
    "budget_weight": 25.0,
    "sponsorship_type_weight": 15.0,
    "event_type_weight": 10.0,
    "footfall_weight": 10.0,
    "event_format_weight": 0.0,
    "audience_type_weight": 0.0,
    # Primary (contribute to score only): age range, campaign period
    "age_range_weight": 0.0,
    "campaign_period_weight": 0.0,
    # Secondary: audience language
    "audience_language_weight": 0.0,
    # Tertiary: marketing channels, past experience (soft filter)
    "marketing_channels_weight": 0.0,
    "past_experience_weight": 0.0,
}

# ---------------------------------------------------------------------------
# Country name normalization (map API variants → canonical for matching)
# ---------------------------------------------------------------------------
COUNTRY_ALIASES: Dict[str, str] = {
    "uk": "united kingdom",
    "great britain": "united kingdom",
    "gb": "united kingdom",
    "usa": "united states",
    "us": "united states",
    "united states of america": "united states",
    "uae": "united arab emirates",
    "united arab emirates": "united arab emirates",
    "korea": "south korea",
    "republic of korea": "south korea",
    "dprk": "north korea",
    "democratic people's republic of korea": "north korea",
    "russia": "russian federation",
    "russian federation": "russian federation",
    "iran": "iran",
    "islamic republic of iran": "iran",
    "vietnam": "viet nam",
    "viet nam": "viet nam",
    "tanzania": "united republic of tanzania",
    "united republic of tanzania": "united republic of tanzania",
    "bolivia": "bolivia (plurinational state of)",
    "venezuela": "venezuela (bolivarian republic of)",
}

# ---------------------------------------------------------------------------
# Allowed values for optional match dimensions (for validation / UI)
# ---------------------------------------------------------------------------
EVENT_FORMAT_VALUES: List[str] = ["In-person", "Virtual", "Hybrid"]
AUDIENCE_TYPE_VALUES: List[str] = ["B2B", "B2C", "Both"]
# Audience language: empty/blank = universal
AUDIENCE_LANGUAGE_VALUES: List[str] = ["English", "Hindi", "Bilingual", "Spanish", "Other"]

# ---------------------------------------------------------------------------
# Export as a single dict for DB storage (key = config key, value = config value)
# Non-scalar values (dict/list) can be stored as JSON in the table.
# ---------------------------------------------------------------------------
def get_config_for_db() -> Dict[str, Any]:
    """Return config as a flat-ish dict suitable for storing in a config table."""
    return {
        "budget_near_boundary_ratio": BUDGET_NEAR_BOUNDARY_RATIO,
        "footfall_partial_match_ratio": FOOTFALL_PARTIAL_MATCH_RATIO,
        "default_weights": DEFAULT_WEIGHTS,
        "country_aliases": COUNTRY_ALIASES,
        "event_format_values": EVENT_FORMAT_VALUES,
        "audience_type_values": AUDIENCE_TYPE_VALUES,
        "audience_language_values": AUDIENCE_LANGUAGE_VALUES,
    }
