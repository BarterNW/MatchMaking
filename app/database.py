"""
BarterNow Database Module - PostgreSQL Implementation

This module provides database access for the BarterNow matchmaking system.
It strictly follows the schema defined in Database_information JSON files.

DESIGN PRINCIPLES:
- Schema-aligned: All tables/columns match JSON metadata exactly
- Geographic: String-based matching (city/state/country names, NO lat/lon)
- Read-mostly: ConfigDB is immutable; CoreDB writes only to matches/deals
- Explicit: All joins based on foreign_keys.json relationships
"""

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from typing import Dict, List, Optional, Any
import os
from contextlib import contextmanager
from dotenv import load_dotenv

# Loading env
load_dotenv()

# ============================================================================
# SECTION 1: CONNECTION & POOL MANAGEMENT
# ============================================================================

# Database connection string (from environment)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://barternow_user:password@localhost:5432/barternow_db"
)

# Connection pool (initialized once, lazy)
_pool: Optional[ConnectionPool] = None


def get_pool() -> ConnectionPool:
    """
    Get or create the connection pool.
    
    WHY: Connection pooling for performance. Avoids creating/destroying
         connections on every query.
    """
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            DATABASE_URL,
            min_size=2,
            max_size=10,
            timeout=30,
            kwargs={"row_factory": dict_row}  # Default to dict rows
        )
    return _pool


@contextmanager
def get_connection():
    """
    Get a connection from the pool (context manager).
    
    Usage:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT ...")
    
    WHY: Ensures connections are properly returned to pool.
    """
    pool = get_pool()
    with pool.connection() as conn:
        yield conn


def close_pool():
    """Close the connection pool (for app shutdown)."""
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


# ============================================================================
# SECTION 2: SCHEMA CONSTANTS (from JSON metadata)
# ============================================================================

class ConfigDB:
    """
    configdb schema - immutable reference tables.
    
    WHY: Centralize all configdb table names. Prevents typos, enables
         refactoring, makes schema clear.
    """
    SCHEMA = "barternow_configdb"
    
    # Geographic reference tables
    COUNTRIES = f"{SCHEMA}.countries"
    STATES = f"{SCHEMA}.states"
    CITIES = f"{SCHEMA}.cities"
    CITY_ALIASES = f"{SCHEMA}.city_aliases"
    CITY_TIERS = f"{SCHEMA}.city_tiers"
    GEOGRAPHIC_FOCUS_TYPES = f"{SCHEMA}.geographic_focus_types"
    
    # Audience reference tables
    AUDIENCE_AGE_BUCKETS = f"{SCHEMA}.audience_age_buckets"
    AUDIENCE_TYPES = f"{SCHEMA}.audience_types"
    
    # Event reference tables
    EVENT_CATEGORIES = f"{SCHEMA}.event_categories"
    EVENT_TYPES = f"{SCHEMA}.event_types"
    EVENT_SIZE_BUCKETS = f"{SCHEMA}.event_size_buckets"
    
    # Deliverable reference tables
    DELIVERABLE_TYPES = f"{SCHEMA}.deliverable_types"
    
    # Other reference tables
    INDUSTRIES = f"{SCHEMA}.industries"
    INTEREST_TAGS = f"{SCHEMA}.interest_tags"
    KPI_TYPES = f"{SCHEMA}.kpi_types"
    OBJECTIVE_TYPES = f"{SCHEMA}.objective_types"
    PLATFORM_TYPES = f"{SCHEMA}.platform_types"
    
    # Matching configuration tables
    MATCH_RULE_SETS = f"{SCHEMA}.match_rule_sets"
    MATCH_WEIGHT_SETS = f"{SCHEMA}.match_weight_sets"


class CoreDB:
    """
    coredb schema - business truth (events, brands, matching).
    
    WHY: Centralize all coredb table names. These tables are mutable
         and contain user-entered data.
    """
    SCHEMA = "barternow_coredb"
    
    # Core identity tables
    USERS = f"{SCHEMA}.users"
    ORGS = f"{SCHEMA}.orgs"
    ORG_MEMBERSHIPS = f"{SCHEMA}.org_memberships"
    
    # Brand tables
    BRAND_PROFILES = f"{SCHEMA}.brand_profiles"
    BRAND_TARGET_CITIES = f"{SCHEMA}.brand_target_cities"
    BRAND_TARGET_STATES = f"{SCHEMA}.brand_target_states"  # NEW (schema review)
    BRAND_TARGET_COUNTRIES = f"{SCHEMA}.brand_target_countries"  # NEW (schema review)
    BRAND_TARGET_AGE_BUCKETS = f"{SCHEMA}.brand_target_age_buckets"
    BRAND_TARGET_AUDIENCE_TYPES = f"{SCHEMA}.brand_target_audience_types"
    BRAND_TARGET_INTEREST_TAGS = f"{SCHEMA}.brand_target_interest_tags"
    BRAND_PREFERRED_CATEGORIES = f"{SCHEMA}.brand_preferred_categories"
    BRAND_DELIVERABLE_PREFERENCES = f"{SCHEMA}.brand_deliverable_preferences"
    
    # Event tables
    EVENT_PROFILES = f"{SCHEMA}.event_profiles"
    EVENT_CATEGORIES_MAP = f"{SCHEMA}.event_categories_map"
    EVENT_AUDIENCE_PROFILE = f"{SCHEMA}.event_audience_profile"
    EVENT_AGE_DISTRIBUTION = f"{SCHEMA}.event_age_distribution"
    EVENT_AUDIENCE_TYPES_MAP = f"{SCHEMA}.event_audience_types_map"
    EVENT_INTEREST_TAGS_MAP = f"{SCHEMA}.event_interest_tags_map"
    EVENT_DELIVERABLES_INVENTORY = f"{SCHEMA}.event_deliverables_inventory"
    EVENT_SPONSORSHIP_INVENTORY = f"{SCHEMA}.event_sponsorship_inventory"
    
    # Matching & deals tables
    MATCHES = f"{SCHEMA}.matches"
    SPONSORSHIP_DEALS = f"{SCHEMA}.sponsorship_deals"
    DEAL_DELIVERABLES = f"{SCHEMA}.deal_deliverables"
    DEAL_WORKSPACES = f"{SCHEMA}.deal_workspaces"
    DELIVERABLE_PROOFS = f"{SCHEMA}.deliverable_proofs"
    
    # Reporting tables
    ROI_REPORTS = f"{SCHEMA}.roi_reports"
    EVENT_CLOSURE_REPORTS = f"{SCHEMA}.event_closure_reports"


# ============================================================================
# SECTION 3: GEOGRAPHIC RESOLUTION (String-Based)
# ============================================================================

def resolve_city_geography(city_id: int) -> Optional[Dict[str, Any]]:
    """
    Resolve full geographic hierarchy for a city.
    
    Returns:
        {
            'city_id': int,
            'city_name': str,
            'state_id': int,
            'state_name': str,
            'country_id': int,
            'country_name': str,
            'city_tier': int
        }
    
    WHY: Matchmaking needs deterministic city → state → country resolution.
         String-based matching requires full hierarchy.
    """
    query = f"""
        SELECT 
            c.city_id,
            c.city_name,
            s.state_id,
            s.state_name,
            co.country_id,
            co.country_name,
            c.city_tier
        FROM {ConfigDB.CITIES} c
        LEFT JOIN {ConfigDB.STATES} s ON c.state_id = s.state_id
        LEFT JOIN {ConfigDB.COUNTRIES} co ON s.country_id = co.country_id
        WHERE c.city_id = %s AND c.is_active = true
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (city_id,))
            return cur.fetchone()


def get_cities_in_state(state_id: int) -> List[int]:
    """
    Get all city IDs in a given state.
    
    WHY: For state-level matching, need to check if event's city is in any
         of the brand's target states.
    """
    query = f"""
        SELECT city_id 
        FROM {ConfigDB.CITIES} 
        WHERE state_id = %s AND is_active = true
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (state_id,))
            return [row['city_id'] for row in cur.fetchall()]


def get_cities_in_country(country_id: int) -> List[int]:
    """
    Get all city IDs in a given country.
    
    WHY: For national-level matching, need to check if event's city is in any
         of the brand's target countries.
    """
    query = f"""
        SELECT c.city_id 
        FROM {ConfigDB.CITIES} c
        LEFT JOIN {ConfigDB.STATES} s ON c.state_id = s.state_id
        WHERE s.country_id = %s AND c.is_active = true
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (country_id,))
            return [row['city_id'] for row in cur.fetchall()]


# ============================================================================
# SECTION 4: BRAND DATA RETRIEVAL
# ============================================================================

def get_brand_profile(brand_org_id: int) -> Optional[Dict[str, Any]]:
    """
    Retrieve complete brand profile with all preferences.
    
    Returns dict with keys:
        - brand_profile_id, brand_org_id, brand_name
        - objective_primary, spend_per_event_min, spend_per_event_max
        - city_tier_preference, campaign_start, campaign_end
        - geographic_focus_type
        - default_match_weight_set_id, default_match_rule_set_id
        - target_cities: List[{city_id, city_name, state_name}]
        - target_states: List[{state_id, state_name}]  # NEW
        - target_countries: List[{country_id, country_name}]  # NEW
        - preferred_categories: List[{category_id, category_name}]
        - avoided_categories: List[{category_id, category_name}]
        - wanted_deliverables: List[{deliverable_type_id, deliverable_name}]
        - must_have_deliverables: List[{deliverable_type_id, deliverable_name}]
        - target_age_buckets: List[{age_bucket_id, bucket_label, min_age, max_age}]
        - target_audience_types: List[{audience_type_id, type_name}]
        - target_interest_tags: List[{interest_tag_id, tag_name}]
    
    WHY: Matching algorithm needs all brand criteria to score events.
         Single function call prevents N+1 queries.
    """
    # Join based on foreign_keys.json:
    # - brand_profiles.brand_org_id → orgs.org_id
    profile_query = f"""
        SELECT 
            bp.brand_profile_id,
            bp.brand_org_id,
            bp.objective_primary,
            bp.spend_per_event_min,
            bp.spend_per_event_max,
            bp.city_tier_preference,
            bp.campaign_start,
            bp.campaign_end,
            bp.geographic_focus_type,
            bp.default_match_weight_set_id,
            bp.default_match_rule_set_id,
            bp.notes,
            o.org_name as brand_name
        FROM {CoreDB.BRAND_PROFILES} bp
        JOIN {CoreDB.ORGS} o ON bp.brand_org_id = o.org_id
        WHERE bp.brand_org_id = %s
    """
    
    # Join based on foreign_keys.json:
    # - brand_target_cities.brand_org_id → orgs.org_id
    # - brand_target_cities.city_id → cities.city_id
    cities_query = f"""
        SELECT 
            btc.city_id,
            c.city_name,
            c.state_name
        FROM {CoreDB.BRAND_TARGET_CITIES} btc
        JOIN {ConfigDB.CITIES} c ON btc.city_id = c.city_id
        WHERE btc.brand_org_id = %s AND btc.is_active = true
    """
    
    # NEW: Join based on schema review additions
    # - brand_target_states.brand_org_id → orgs.org_id
    # - brand_target_states.state_id → states.state_id
    states_query = f"""
        SELECT 
            bts.state_id,
            s.state_name
        FROM {CoreDB.BRAND_TARGET_STATES} bts
        JOIN {ConfigDB.STATES} s ON bts.state_id = s.state_id
        WHERE bts.brand_org_id = %s AND bts.is_active = true
    """
    
    # NEW: Join based on schema review additions
    # - brand_target_countries.brand_org_id → orgs.org_id
    # - brand_target_countries.country_id → countries.country_id
    countries_query = f"""
        SELECT 
            btc.country_id,
            co.country_name
        FROM {CoreDB.BRAND_TARGET_COUNTRIES} btc
        JOIN {ConfigDB.COUNTRIES} co ON btc.country_id = co.country_id
        WHERE btc.brand_org_id = %s AND btc.is_active = true
    """
    
    # Join based on foreign_keys.json:
    # - brand_preferred_categories.brand_org_id → orgs.org_id
    # - brand_preferred_categories.category_id → event_categories.category_id
    categories_query = f"""
        SELECT 
            bpc.category_id,
            ec.category_name,
            bpc.preference_type
        FROM {CoreDB.BRAND_PREFERRED_CATEGORIES} bpc
        JOIN {ConfigDB.EVENT_CATEGORIES} ec ON bpc.category_id = ec.category_id
        WHERE bpc.brand_org_id = %s
    """
    
    # Join based on foreign_keys.json:
    # - brand_deliverable_preferences.brand_org_id → orgs.org_id
    # - brand_deliverable_preferences.deliverable_type_id → deliverable_types.deliverable_type_id
    deliverables_query = f"""
        SELECT 
            bdp.deliverable_type_id,
            dt.deliverable_name,
            bdp.preference_type
        FROM {CoreDB.BRAND_DELIVERABLE_PREFERENCES} bdp
        JOIN {ConfigDB.DELIVERABLE_TYPES} dt ON bdp.deliverable_type_id = dt.deliverable_type_id
        WHERE bdp.brand_org_id = %s
    """
    
    # Join based on foreign_keys.json:
    # - brand_target_age_buckets.brand_org_id → orgs.org_id
    # - brand_target_age_buckets.age_bucket_id → audience_age_buckets.age_bucket_id
    age_query = f"""
        SELECT 
            btab.age_bucket_id,
            aab.bucket_label,
            aab.min_age,
            aab.max_age
        FROM {CoreDB.BRAND_TARGET_AGE_BUCKETS} btab
        JOIN {ConfigDB.AUDIENCE_AGE_BUCKETS} aab ON btab.age_bucket_id = aab.age_bucket_id
        WHERE btab.brand_org_id = %s
    """
    
    # Join based on foreign_keys.json:
    # - brand_target_audience_types.brand_org_id → orgs.org_id
    # - brand_target_audience_types.audience_type_id → audience_types.audience_type_id
    audience_types_query = f"""
        SELECT 
            btat.audience_type_id,
            at.type_name
        FROM {CoreDB.BRAND_TARGET_AUDIENCE_TYPES} btat
        JOIN {ConfigDB.AUDIENCE_TYPES} at ON btat.audience_type_id = at.audience_type_id
        WHERE btat.brand_org_id = %s
    """
    
    # Join based on foreign_keys.json:
    # - brand_target_interest_tags.brand_org_id → orgs.org_id
    # - brand_target_interest_tags.interest_tag_id → interest_tags.interest_tag_id
    interest_query = f"""
        SELECT 
            btit.interest_tag_id,
            it.tag_name
        FROM {CoreDB.BRAND_TARGET_INTEREST_TAGS} btit
        JOIN {ConfigDB.INTEREST_TAGS} it ON btit.interest_tag_id = it.interest_tag_id
        WHERE btit.brand_org_id = %s
    """
    
    # Execute all queries in a single connection
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Get base profile
            cur.execute(profile_query, (brand_org_id,))
            profile = cur.fetchone()
            if not profile:
                return None
            
            # Get target cities
            cur.execute(cities_query, (brand_org_id,))
            profile['target_cities'] = cur.fetchall()
            
            # Get target states (NEW)
            cur.execute(states_query, (brand_org_id,))
            profile['target_states'] = cur.fetchall()
            
            # Get target countries (NEW)
            cur.execute(countries_query, (brand_org_id,))
            profile['target_countries'] = cur.fetchall()
            
            # Get categories (split by preference type)
            cur.execute(categories_query, (brand_org_id,))
            categories = cur.fetchall()
            profile['preferred_categories'] = [c for c in categories if c['preference_type'] == 'preferred']
            profile['avoided_categories'] = [c for c in categories if c['preference_type'] == 'avoid']
            
            # Get deliverables (split by preference type)
            cur.execute(deliverables_query, (brand_org_id,))
            deliverables = cur.fetchall()
            profile['wanted_deliverables'] = [d for d in deliverables if d['preference_type'] == 'wanted']
            profile['must_have_deliverables'] = [d for d in deliverables if d['preference_type'] == 'must_have']
            
            # Get age buckets
            cur.execute(age_query, (brand_org_id,))
            profile['target_age_buckets'] = cur.fetchall()
            
            # Get audience types
            cur.execute(audience_types_query, (brand_org_id,))
            profile['target_audience_types'] = cur.fetchall()
            
            # Get interest tags
            cur.execute(interest_query, (brand_org_id,))
            profile['target_interest_tags'] = cur.fetchall()
            
            return profile


def get_all_brand_orgs() -> List[int]:
    """
    Get all active brand organization IDs.
    
    WHY: For batch matching operations (match all brands against new event).
    """
    query = f"""
        SELECT org_id 
        FROM {CoreDB.ORGS}
        WHERE org_type = 'brand' AND is_active = true
        ORDER BY org_id
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return [row['org_id'] for row in cur.fetchall()]


def get_brands_list() -> List[Dict[str, Any]]:
    """
    Get minimal list of brands for API dropdown (id, sponsor_name, status).
    
    WHY: /api/sponsors needs lightweight list; frontend expects id, sponsor_name, status.
    """
    query = f"""
        SELECT org_id, org_name
        FROM {CoreDB.ORGS}
        WHERE org_type = 'brand' AND is_active = true
        ORDER BY org_id
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return [
                {"id": row["org_id"], "sponsor_name": row["org_name"] or "Unnamed", "status": "active"}
                for row in cur.fetchall()
            ]

def get_events_list() -> List[Dict[str, Any]]:
    """
    Get minimal list of event for API dropdown (id, event_name, status).
    
    WHY: /api/events needs lightweight list; frontend expects id, event_name, status.
    """
    query = f"""
        SELECT org_id, org_name
        FROM {CoreDB.ORGS}
        WHERE org_type = 'event' AND is_active = true
        ORDER BY org_id
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return [
                {"id": row["org_id"], "event_name": row["org_name"] or "Unnamed", "status": "active"}
                for row in cur.fetchall()
            ]


# ============================================================================
# SECTION 5: EVENT DATA RETRIEVAL
# ============================================================================

def get_event_profile(event_org_id: int) -> Optional[Dict[str, Any]]:
    """
    Retrieve complete event profile with all details.
    
    Returns dict with keys:
        - event_profile_id, event_org_id, event_org_name
        - event_name, event_type_id, event_type_name
        - city_id, city_name, state_name
        - venue_name, start_date, end_date, expected_audience_size
        - categories: List[{category_id, category_name}]
        - package_min, package_max (sponsorship budget)
        - deliverables_offered: List[{deliverable_type_id, deliverable_name, max_count}]
        - age_distribution: List[{age_bucket_id, bucket_label, min_age, max_age, percent}]
        - audience_types: List[{audience_type_id, type_name, weight}]
        - interest_tags: List[{interest_tag_id, tag_name, weight}]
    
    WHY: Matching algorithm needs all event details to score against brands.
    """
    # Join based on foreign_keys.json:
    # - event_profiles.event_org_id → orgs.org_id
    # - event_profiles.event_type_id → event_types.event_type_id
    # - event_profiles.city_id → cities.city_id
    profile_query = f"""
        SELECT 
            ep.event_profile_id,
            ep.event_org_id,
            ep.event_name,
            ep.event_type_id,
            et.event_type_name,
            ep.city_id,
            c.city_name,
            c.state_name,
            ep.venue_name,
            ep.start_date,
            ep.end_date,
            ep.expected_audience_size,
            o.org_name as event_org_name
        FROM {CoreDB.EVENT_PROFILES} ep
        JOIN {CoreDB.ORGS} o ON ep.event_org_id = o.org_id
        LEFT JOIN {ConfigDB.EVENT_TYPES} et ON ep.event_type_id = et.event_type_id
        LEFT JOIN {ConfigDB.CITIES} c ON ep.city_id = c.city_id
        WHERE ep.event_org_id = %s
    """
    
    # Join based on foreign_keys.json:
    # - event_categories_map.event_org_id → orgs.org_id
    # - event_categories_map.category_id → event_categories.category_id
    categories_query = f"""
        SELECT 
            ecm.category_id,
            ec.category_name
        FROM {CoreDB.EVENT_CATEGORIES_MAP} ecm
        JOIN {ConfigDB.EVENT_CATEGORIES} ec ON ecm.category_id = ec.category_id
        WHERE ecm.event_org_id = %s
    """
    
    # Join based on foreign_keys.json:
    # - event_sponsorship_inventory.event_org_id → orgs.org_id
    sponsorship_query = f"""
        SELECT 
            package_min,
            package_max
        FROM {CoreDB.EVENT_SPONSORSHIP_INVENTORY}
        WHERE event_org_id = %s
    """
    
    # Join based on foreign_keys.json:
    # - event_deliverables_inventory.event_org_id → orgs.org_id
    # - event_deliverables_inventory.deliverable_type_id → deliverable_types.deliverable_type_id
    deliverables_query = f"""
        SELECT 
            edi.deliverable_type_id,
            dt.deliverable_name,
            edi.max_count
        FROM {CoreDB.EVENT_DELIVERABLES_INVENTORY} edi
        JOIN {ConfigDB.DELIVERABLE_TYPES} dt ON edi.deliverable_type_id = dt.deliverable_type_id
        WHERE edi.event_org_id = %s
    """
    
    # Join based on foreign_keys.json:
    # - event_age_distribution.event_org_id → orgs.org_id
    # - event_age_distribution.age_bucket_id → audience_age_buckets.age_bucket_id
    age_query = f"""
        SELECT 
            ead.age_bucket_id,
            aab.bucket_label,
            aab.min_age,
            aab.max_age,
            ead.percent
        FROM {CoreDB.EVENT_AGE_DISTRIBUTION} ead
        JOIN {ConfigDB.AUDIENCE_AGE_BUCKETS} aab ON ead.age_bucket_id = aab.age_bucket_id
        WHERE ead.event_org_id = %s
    """
    
    # Join based on foreign_keys.json:
    # - event_audience_types_map.event_org_id → orgs.org_id
    # - event_audience_types_map.audience_type_id → audience_types.audience_type_id
    audience_types_query = f"""
        SELECT 
            eatm.audience_type_id,
            at.type_name,
            eatm.weight
        FROM {CoreDB.EVENT_AUDIENCE_TYPES_MAP} eatm
        JOIN {ConfigDB.AUDIENCE_TYPES} at ON eatm.audience_type_id = at.audience_type_id
        WHERE eatm.event_org_id = %s
    """
    
    # Join based on foreign_keys.json:
    # - event_interest_tags_map.event_org_id → orgs.org_id
    # - event_interest_tags_map.interest_tag_id → interest_tags.interest_tag_id
    interest_query = f"""
        SELECT 
            eitm.interest_tag_id,
            it.tag_name,
            eitm.weight
        FROM {CoreDB.EVENT_INTEREST_TAGS_MAP} eitm
        JOIN {ConfigDB.INTEREST_TAGS} it ON eitm.interest_tag_id = it.interest_tag_id
        WHERE eitm.event_org_id = %s
    """
    
    # Execute all queries in a single connection
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Get base profile
            cur.execute(profile_query, (event_org_id,))
            profile = cur.fetchone()
            if not profile:
                return None
            
            # Get categories
            cur.execute(categories_query, (event_org_id,))
            profile['categories'] = cur.fetchall()
            
            # Get sponsorship budget
            cur.execute(sponsorship_query, (event_org_id,))
            sponsorship = cur.fetchone()
            profile['package_min'] = sponsorship['package_min'] if sponsorship else None
            profile['package_max'] = sponsorship['package_max'] if sponsorship else None
            
            # Get deliverables
            cur.execute(deliverables_query, (event_org_id,))
            profile['deliverables_offered'] = cur.fetchall()
            
            # Get age distribution
            cur.execute(age_query, (event_org_id,))
            profile['age_distribution'] = cur.fetchall()
            
            # Get audience types
            cur.execute(audience_types_query, (event_org_id,))
            profile['audience_types'] = cur.fetchall()
            
            # Get interest tags
            cur.execute(interest_query, (event_org_id,))
            profile['interest_tags'] = cur.fetchall()
            
            return profile


def get_all_event_orgs() -> List[int]:
    """
    Get all active event organization IDs.
    
    WHY: For batch matching operations (match all events against new brand).
    """
    query = f"""
        SELECT org_id 
        FROM {CoreDB.ORGS}
        WHERE org_type = 'event' AND is_active = true
        ORDER BY org_id
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return [row['org_id'] for row in cur.fetchall()]


# ============================================================================
# SECTION 6: GEOGRAPHIC MATCHING LOGIC
# ============================================================================

def check_geographic_match(
    event_city_id: int,
    brand_profile: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Determine if event geography matches brand preferences.
    
    Matching logic (hierarchical, deterministic, string-based):
    1. LOCAL focus → match by city_name (event city_id in brand_target_cities)
    2. STATE focus → match by state_name (event state_id in brand_target_states)
    3. NATIONAL focus → match by country_name (event country_id in brand_target_countries)
    
    Args:
        event_city_id: City ID from event_profiles.city_id
        brand_profile: Dict from get_brand_profile() with target cities/states/countries
    
    Returns:
        {
            'matches': bool,
            'match_level': 'city' | 'state' | 'country' | None,
            'explanation': str
        }
    
    WHY: Replace distance-based filtering (lat/lon + haversine) with explicit
         string-based matching. Deterministic, reviewable, no approximation.
    """
    focus_type = brand_profile.get('geographic_focus_type', 'local')
    event_geo = resolve_city_geography(event_city_id)
    
    if not event_geo:
        return {
            'matches': False,
            'match_level': None,
            'explanation': 'Event city not found in geography database'
        }
    
    # LOCAL: Check if event's city_id is in brand's target_cities
    if focus_type == 'local':
        target_city_ids = [c['city_id'] for c in brand_profile.get('target_cities', [])]
        if event_city_id in target_city_ids:
            return {
                'matches': True,
                'match_level': 'city',
                'explanation': f"City match: {event_geo['city_name']} is in brand's target cities"
            }
        else:
            return {
                'matches': False,
                'match_level': None,
                'explanation': f"City {event_geo['city_name']} not in brand's target cities"
            }
    
    # STATE: Check if event's state_id is in brand's target_states
    elif focus_type == 'state':
        target_state_ids = [s['state_id'] for s in brand_profile.get('target_states', [])]
        event_state_id = event_geo.get('state_id')
        
        if event_state_id and event_state_id in target_state_ids:
            return {
                'matches': True,
                'match_level': 'state',
                'explanation': f"State match: {event_geo['state_name']} is in brand's target states"
            }
        else:
            return {
                'matches': False,
                'match_level': None,
                'explanation': f"State {event_geo.get('state_name', 'unknown')} not in brand's target states"
            }
    
    # NATIONAL: Check if event's country_id is in brand's target_countries
    elif focus_type == 'national':
        target_country_ids = [c['country_id'] for c in brand_profile.get('target_countries', [])]
        event_country_id = event_geo.get('country_id')
        
        if event_country_id and event_country_id in target_country_ids:
            return {
                'matches': True,
                'match_level': 'country',
                'explanation': f"Country match: {event_geo['country_name']} is in brand's target countries"
            }
        else:
            return {
                'matches': False,
                'match_level': None,
                'explanation': f"Country {event_geo.get('country_name', 'unknown')} not in brand's target countries"
            }
    
    # Unknown focus type
    return {
        'matches': False,
        'match_level': None,
        'explanation': f"Unknown geographic focus type: {focus_type}"
    }


# ============================================================================
# SECTION 7: MATCH CONFIGURATION (from configdb)
# ============================================================================

def get_match_weight_set(weight_set_id: int) -> Optional[Dict[str, float]]:
    """
    Load match weight configuration from configdb.match_weight_sets.
    
    Returns:
        {
            'category': float,
            'geo': float,
            'budget': float,
            'audience': float,
            'deliverables': float
        }
    
    WHY: Weights are config-driven (not hardcoded). Enables A/B testing,
         per-brand customization, and tuning without code changes.
    """
    query = f"""
        SELECT 
            weight_category,
            weight_geo,
            weight_budget,
            weight_audience,
            weight_deliverables
        FROM {ConfigDB.MATCH_WEIGHT_SETS}
        WHERE match_weight_set_id = %s AND is_active = true
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (weight_set_id,))
            row = cur.fetchone()
            if row:
                return {
                    'category': float(row['weight_category']),
                    'geo': float(row['weight_geo']),
                    'budget': float(row['weight_budget']),
                    'audience': float(row['weight_audience']),
                    'deliverables': float(row['weight_deliverables'])
                }
            return None


def get_match_rule_set(rule_set_id: int) -> Optional[Dict[str, Any]]:
    """
    Load match rule configuration from configdb.match_rule_sets.
    
    Returns: Full row from match_rule_sets table with all rule parameters
    
    WHY: Hard filters (must_have deliverables, date windows, budget overlap)
         are config-driven. Enables rule tuning without code deployment.
    """
    query = f"""
        SELECT *
        FROM {ConfigDB.MATCH_RULE_SETS}
        WHERE match_rule_set_id = %s AND is_active = true
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (rule_set_id,))
            row = cur.fetchone()
            if row:
                return {
                    'enforce_must_have_deliverables': row['enforce_must_have_deliverables'],
                    'enforce_city_filter': row['enforce_city_filter'], 
                    'enforce_date_window': row['enforce_date_window'],
                    'enforce_budget_overlap': row['enforce_budget_overlap'],
                    'min_budget_overlap_ratio': float(row['min_budget_overlap_ratio']),
                    'allowed_date_slack_days': float(row['allowed_date_slack_days']),
                    'min_audience_overlap_score': float(row['min_audience_overlap_score']),
                    'budget_near_boundary_ratio': float(row['budget_near_boundary_ratio']),
                    'footfall_partial_match_ratio': float(row['footfall_partial_match_ratio'])
                }
            return None
