"""
BarterNow PostgreSQL Database Seeding Script

This script:
1. READS reference data from configdb (countries, states, cities, event types, etc.)
   and only INSERTS weight_set_map and rule_set_map into configdb if missing.
2. INSERTS sample data into coredb only: brands (orgs + brand_profiles + preferences)
   and events (orgs + event_profiles + details).

IMPORTANT: Run this AFTER executing sql/matchmaking_schema_review.sql
ConfigDB reference data (except match weight/rule sets) must already exist in the database.
"""

from .database import get_connection, ConfigDB, CoreDB
from typing import Dict, List, Tuple
import sys


# # ============================================================================
# # SECTION 1: CONFIGDB REFERENCE DATA
# # ============================================================================

# def seed_countries() -> Dict[str, int]:
#     """
#     Seed countries reference table.
    
#     Returns: Dict mapping country_name -> country_id
#     """
#     countries_data = [
#         # (country_name, country_code)
#         ("India", "IN"),
#         ("United States", "US"),
#         ("United Kingdom", "GB"),
#         ("Canada", "CA"),
#         ("Australia", "AU"),
#         ("Singapore", "SG"),
#         ("Brazil", "BR"),
#         ("Pakistan", "PK"),
#     ]
    
#     country_map = {}
    
#     with get_connection() as conn:
#         with conn.cursor() as cur:
#             for country_name, country_code in countries_data:
#                 cur.execute(f"""
#                     INSERT INTO {ConfigDB.COUNTRIES} (country_name, country_code)
#                     VALUES (%s, %s)
#                     ON CONFLICT (country_name) DO UPDATE SET country_code = EXCLUDED.country_code
#                     RETURNING country_id
#                 """, (country_name, country_code))
#                 country_id = cur.fetchone()['country_id']
#                 country_map[country_name] = country_id
            
#             conn.commit()
    
#     print(f"✅ Seeded {len(countries_data)} countries")
#     return country_map


# def seed_states(country_map: Dict[str, int]) -> Dict[str, int]:
#     """
#     Seed states reference table.
    
#     Args:
#         country_map: Dict from seed_countries()
    
#     Returns: Dict mapping state_name -> state_id
#     """
#     # (state_name, country_name, state_code)
#     states_data = [
#         # India states
#         ("Maharashtra", "India", "MH"),
#         ("Karnataka", "India", "KA"),
#         ("Delhi", "India", "DL"),
#         ("Tamil Nadu", "India", "TN"),
#         ("Gujarat", "India", "GJ"),
#         ("Rajasthan", "India", "RJ"),
        
#         # US states
#         ("California", "United States", "CA"),
#         ("New York", "United States", "NY"),
#         ("Texas", "United States", "TX"),
#         ("Illinois", "United States", "IL"),
#         ("Florida", "United States", "FL"),
#         ("Nevada", "United States", "NV"),
        
#         # Brazil states
#         ("Rio de Janeiro", "Brazil", "RJ"),
#         ("São Paulo", "Brazil", "SP"),
        
#         # Pakistan provinces
#         ("Sindh", "Pakistan", "SD"),
        
#         # Canada provinces
#         ("Ontario", "Canada", "ON"),
#     ]
    
#     state_map = {}
    
#     with get_connection() as conn:
#         with conn.cursor() as cur:
#             for state_name, country_name, state_code in states_data:
#                 country_id = country_map.get(country_name)
#                 if not country_id:
#                     print(f"⚠️ Country '{country_name}' not found for state '{state_name}'")
#                     continue
                
#                 cur.execute(f"""
#                     INSERT INTO {ConfigDB.STATES} (state_name, country_id, state_code)
#                     VALUES (%s, %s, %s)
#                     ON CONFLICT (state_name, country_id) DO UPDATE SET state_code = EXCLUDED.state_code
#                     RETURNING state_id
#                 """, (state_name, country_id, state_code))
#                 state_id = cur.fetchone()['state_id']
#                 state_map[state_name] = state_id
            
#             conn.commit()
    
#     print(f"✅ Seeded {len(states_data)} states")
#     return state_map


# def seed_cities(state_map: Dict[str, int], country_map: Dict[str, int]) -> Dict[str, int]:
#     """
#     Seed cities reference table.
    
#     Args:
#         state_map: Dict from seed_states()
#         country_map: Dict from seed_countries()
    
#     Returns: Dict mapping city_name -> city_id
#     """
#     # (city_name, state_name, country_name, city_tier)
#     cities_data = [
#         # India cities
#         ("Mumbai", "Maharashtra", "India", 1),
#         ("Pune", "Maharashtra", "India", 1),
#         ("Bangalore", "Karnataka", "India", 1),
#         ("Delhi", "Delhi", "India", 1),
#         ("Chennai", "Tamil Nadu", "India", 1),
#         ("Ahmedabad", "Gujarat", "India", 1),
#         ("Jaipur", "Rajasthan", "India", 2),
        
#         # US cities
#         ("San Francisco", "California", "United States", 1),
#         ("Los Angeles", "California", "United States", 1),
#         ("San Jose", "California", "United States", 1),
#         ("Oakland", "California", "United States", 2),
#         ("San Diego", "California", "United States", 1),
#         ("New York", "New York", "United States", 1),
#         ("Austin", "Texas", "United States", 1),
#         ("Chicago", "Illinois", "United States", 1),
#         ("Evanston", "Illinois", "United States", 2),
#         ("Aurora", "Illinois", "United States", 2),
#         ("Las Vegas", "Nevada", "United States", 1),
        
#         # Brazil cities
#         ("Rio de Janeiro", "Rio de Janeiro", "Brazil", 1),
        
#         # Pakistan cities
#         ("Karachi", "Sindh", "Pakistan", 1),
        
#         # Canada cities
#         ("Toronto", "Ontario", "Canada", 1),
#     ]
    
#     city_map = {}
    
#     with get_connection() as conn:
#         with conn.cursor() as cur:
#             for city_name, state_name, country_name, city_tier in cities_data:
#                 state_id = state_map.get(state_name)
#                 country_id = country_map.get(country_name)
                
#                 if not state_id:
#                     print(f"⚠️ State '{state_name}' not found for city '{city_name}'")
#                     continue
                
#                 if not country_id:
#                     print(f"⚠️ Country '{country_name}' not found for city '{city_name}'")
#                     continue
                
#                 cur.execute(f"""
#                     INSERT INTO {ConfigDB.CITIES} (city_name, state_name, state_id, country_id, city_tier)
#                     VALUES (%s, %s, %s, %s, %s)
#                     ON CONFLICT (city_name, state_name) DO UPDATE 
#                     SET state_id = EXCLUDED.state_id, country_id = EXCLUDED.country_id
#                     RETURNING city_id
#                 """, (city_name, state_name, state_id, country_id, city_tier))
#                 city_id = cur.fetchone()['city_id']
#                 city_map[city_name] = city_id
            
#             conn.commit()
    
#     print(f"✅ Seeded {len(cities_data)} cities")
#     return city_map


# def seed_event_types() -> Dict[str, int]:
#     """Seed event types reference table."""
#     event_types_data = [
#         # (event_type_name, event_type_code)
#         ("Tech Conference", "TECH_CONF"),
#         ("Music Festival", "MUSIC_FEST"),
#         ("Sports Event", "SPORTS"),
#         ("Cultural Festival", "CULTURAL"),
#         ("Startup Event", "STARTUP"),
#         ("Hackathon", "HACKATHON"),
#         ("Community Event", "COMMUNITY"),
#         ("Charity Event", "CHARITY"),
#         ("Food Festival", "FOOD_FEST"),
#         ("Trade Show", "TRADE_SHOW"),
#     ]
    
#     type_map = {}
    
#     with get_connection() as conn:
#         with conn.cursor() as cur:
#             for type_name, type_code in event_types_data:
#                 cur.execute(f"""
#                     INSERT INTO {ConfigDB.EVENT_TYPES} (event_type_name, event_type_code)
#                     VALUES (%s, %s)
#                     ON CONFLICT (event_type_code) DO NOTHING
#                     RETURNING event_type_id
#                 """, (type_name, type_code))
#                 result = cur.fetchone()
#                 if result:
#                     type_map[type_name] = result['event_type_id']
#                 else:
#                     # Already exists, fetch it
#                     cur.execute(f"SELECT event_type_id FROM {ConfigDB.EVENT_TYPES} WHERE event_type_code = %s", (type_code,))
#                     type_map[type_name] = cur.fetchone()['event_type_id']
            
#             conn.commit()
    
#     print(f"✅ Seeded {len(event_types_data)} event types")
#     return type_map


# def seed_event_categories() -> Dict[str, int]:
#     """Seed event categories reference table."""
#     categories_data = [
#         # (category_name, category_code)
#         ("Technology", "TECH"),
#         ("Music", "MUSIC"),
#         ("Sports", "SPORTS"),
#         ("Food & Beverage", "FOOD"),
#         ("Cultural", "CULTURAL"),
#         ("Business", "BUSINESS"),
#         ("Education", "EDUCATION"),
#         ("Health & Wellness", "HEALTH"),
#         ("Entertainment", "ENTERTAINMENT"),
#         ("Community", "COMMUNITY"),
#     ]
    
#     category_map = {}
    
#     with get_connection() as conn:
#         with conn.cursor() as cur:
#             for cat_name, cat_code in categories_data:
#                 cur.execute(f"""
#                     INSERT INTO {ConfigDB.EVENT_CATEGORIES} (category_name, category_code)
#                     VALUES (%s, %s)
#                     ON CONFLICT (category_name) DO NOTHING
#                     RETURNING category_id
#                 """, (cat_name, cat_code))
#                 result = cur.fetchone()
#                 if result:
#                     category_map[cat_name] = result['category_id']
#                 else:
#                     cur.execute(f"SELECT category_id FROM {ConfigDB.EVENT_CATEGORIES} WHERE category_code = %s", (cat_code,))
#                     category_map[cat_name] = cur.fetchone()['category_id']
            
#             conn.commit()
    
#     print(f"✅ Seeded {len(categories_data)} event categories")
#     return category_map


# def seed_audience_age_buckets() -> Dict[str, int]:
#     """Seed audience age buckets reference table."""
#     age_buckets_data = [
#         # (bucket_label, bucket_code, min_age, max_age)
#         ("Under 18", "AGE_U18", None, 17),
#         ("18-24", "AGE_18_24", 18, 24),
#         ("25-34", "AGE_25_34", 25, 34),
#         ("35-44", "AGE_35_44", 35, 44),
#         ("45-54", "AGE_45_54", 45, 54),
#         ("55-64", "AGE_55_64", 55, 64),
#         ("65+", "AGE_65_PLUS", 65, None),
#         ("All Ages", "AGE_ALL", None, None),
#     ]
    
#     bucket_map = {}
    
#     with get_connection() as conn:
#         with conn.cursor() as cur:
#             for label, code, min_age, max_age in age_buckets_data:
#                 cur.execute(f"""
#                     INSERT INTO {ConfigDB.AUDIENCE_AGE_BUCKETS} (bucket_label, bucket_code, min_age, max_age)
#                     VALUES (%s, %s, %s, %s)
#                     ON CONFLICT (bucket_label) DO NOTHING
#                     RETURNING age_bucket_id
#                 """, (label, code, min_age, max_age))
#                 result = cur.fetchone()
#                 if result:
#                     bucket_map[label] = result['age_bucket_id']
#                 else:
#                     cur.execute(f"SELECT age_bucket_id FROM {ConfigDB.AUDIENCE_AGE_BUCKETS} WHERE bucket_code = %s", (code,))
#                     bucket_map[label] = cur.fetchone()['age_bucket_id']
            
#             conn.commit()
    
#     print(f"✅ Seeded {len(age_buckets_data)} age buckets")
#     return bucket_map


# def seed_audience_types() -> Dict[str, int]:
#     """Seed audience types reference table."""
#     audience_types_data = [
#         # (type_name, type_code)
#         ("B2B", "B2B"),
#         ("B2C", "B2C"),
#         ("Students", "STUDENTS"),
#         ("Professionals", "PROFESSIONALS"),
#         ("General Public", "GENERAL"),
#         ("Families", "FAMILIES"),
#     ]
    
#     type_map = {}
    
#     with get_connection() as conn:
#         with conn.cursor() as cur:
#             for type_name, type_code in audience_types_data:
#                 cur.execute(f"""
#                     INSERT INTO {ConfigDB.AUDIENCE_TYPES} (type_name, type_code)
#                     VALUES (%s, %s)
#                     ON CONFLICT (type_name) DO NOTHING
#                     RETURNING audience_type_id
#                 """, (type_name, type_code))
#                 result = cur.fetchone()
#                 if result:
#                     type_map[type_name] = result['audience_type_id']
#                 else:
#                     cur.execute(f"SELECT audience_type_id FROM {ConfigDB.AUDIENCE_TYPES} WHERE type_code = %s", (type_code,))
#                     type_map[type_name] = cur.fetchone()['audience_type_id']
            
#             conn.commit()
    
#     print(f"✅ Seeded {len(audience_types_data)} audience types")
#     return type_map


# def seed_interest_tags() -> Dict[str, int]:
#     """Seed interest tags reference table."""
#     interest_tags_data = [
#         # (tag_name, tag_code)
#         ("Technology", "TECH"),
#         ("Innovation", "INNOVATION"),
#         ("AI & Machine Learning", "AI_ML"),
#         ("Blockchain", "BLOCKCHAIN"),
#         ("Music", "MUSIC"),
#         ("Sports", "SPORTS"),
#         ("Fitness", "FITNESS"),
#         ("Food", "FOOD"),
#         ("Art", "ART"),
#         ("Culture", "CULTURE"),
#         ("Business", "BUSINESS"),
#         ("Networking", "NETWORKING"),
#         ("Startups", "STARTUPS"),
#         ("Entrepreneurship", "ENTREPRENEURSHIP"),
#         ("Community", "COMMUNITY"),
#         ("Charity", "CHARITY"),
#         ("Education", "EDUCATION"),
#         ("Sustainability", "SUSTAINABILITY"),
#     ]
    
#     tag_map = {}
    
#     with get_connection() as conn:
#         with conn.cursor() as cur:
#             for tag_name, tag_code in interest_tags_data:
#                 cur.execute(f"""
#                     INSERT INTO {ConfigDB.INTEREST_TAGS} (tag_name, tag_code)
#                     VALUES (%s, %s)
#                     ON CONFLICT (tag_name) DO NOTHING
#                     RETURNING interest_tag_id
#                 """, (tag_name, tag_code))
#                 result = cur.fetchone()
#                 if result:
#                     tag_map[tag_name] = result['interest_tag_id']
#                 else:
#                     cur.execute(f"SELECT interest_tag_id FROM {ConfigDB.INTEREST_TAGS} WHERE tag_code = %s", (tag_code,))
#                     tag_map[tag_name] = cur.fetchone()['interest_tag_id']
            
#             conn.commit()
    
#     print(f"✅ Seeded {len(interest_tags_data)} interest tags")
#     return tag_map


# def seed_deliverable_types() -> Dict[str, int]:
#     """Seed deliverable types reference table."""
#     deliverable_types_data = [
#         # (deliverable_name, deliverable_code, default_weight)
#         ("Stage Branding", "STAGE_BRAND", 0.80),
#         ("Booth Space", "BOOTH", 0.70),
#         ("Speaking Slot", "SPEAKING", 0.90),
#         ("Social Media Posts", "SOCIAL_POSTS", 0.60),
#         ("Email Marketing", "EMAIL", 0.50),
#         ("Event App Branding", "APP_BRAND", 0.65),
#         ("Banner Ads", "BANNER_ADS", 0.55),
#         ("Product Sampling", "SAMPLING", 0.75),
#         ("Branded Materials", "MATERIALS", 0.60),
#         ("VIP Access", "VIP_ACCESS", 0.85),
#     ]
    
#     deliv_map = {}
    
#     with get_connection() as conn:
#         with conn.cursor() as cur:
#             for deliv_name, deliv_code, weight in deliverable_types_data:
#                 cur.execute(f"""
#                     INSERT INTO {ConfigDB.DELIVERABLE_TYPES} (deliverable_name, deliverable_code, default_weight)
#                     VALUES (%s, %s, %s)
#                     ON CONFLICT (deliverable_name) DO NOTHING
#                     RETURNING deliverable_type_id
#                 """, (deliv_name, deliv_code, weight))
#                 result = cur.fetchone()
#                 if result:
#                     deliv_map[deliv_name] = result['deliverable_type_id']
#                 else:
#                     cur.execute(f"SELECT deliverable_type_id FROM {ConfigDB.DELIVERABLE_TYPES} WHERE deliverable_code = %s", (deliv_code,))
#                     deliv_map[deliv_name] = cur.fetchone()['deliverable_type_id']
            
#             conn.commit()
    
#     print(f"✅ Seeded {len(deliverable_types_data)} deliverable types")
#     return deliv_map


# ============================================================================
# LOAD CONFIGDB REFERENCE DATA (read-only; do not insert except weight/rule sets)
# ============================================================================

def load_configdb_reference_maps() -> Tuple[
    Dict[str, int], Dict[str, int], Dict[str, int],
    Dict[str, int], Dict[str, int], Dict[str, int],
    Dict[str, int], Dict[str, int], Dict[str, int]
]:
    """
    Load all configdb reference maps from the database (read-only).
    
    Returns:
        (country_map, state_map, city_map, event_type_map, category_map,
         deliverable_map, age_bucket_map, audience_type_map, interest_tag_map)
    Maps use name/label as key -> id as value (e.g. country_name -> country_id).
    """
    country_map: Dict[str, int] = {}
    state_map: Dict[str, int] = {}
    city_map: Dict[str, int] = {}
    event_type_map: Dict[str, int] = {}
    category_map: Dict[str, int] = {}
    deliverable_map: Dict[str, int] = {}
    age_bucket_map: Dict[str, int] = {}
    audience_type_map: Dict[str, int] = {}
    interest_tag_map: Dict[str, int] = {}

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT country_id, country_name FROM {ConfigDB.COUNTRIES}")
            for row in cur.fetchall():
                country_map[row["country_name"]] = row["country_id"]

            cur.execute(f"SELECT state_id, state_name FROM {ConfigDB.STATES}")
            for row in cur.fetchall():
                state_map[row["state_name"]] = row["state_id"]

            cur.execute(f"SELECT city_id, city_name FROM {ConfigDB.CITIES}")
            for row in cur.fetchall():
                city_map[row["city_name"]] = row["city_id"]

            cur.execute(f"SELECT event_type_id, event_type_name FROM {ConfigDB.EVENT_TYPES}")
            for row in cur.fetchall():
                event_type_map[row["event_type_name"]] = row["event_type_id"]

            cur.execute(f"SELECT category_id, category_name FROM {ConfigDB.EVENT_CATEGORIES}")
            for row in cur.fetchall():
                category_map[row["category_name"]] = row["category_id"]

            cur.execute(f"SELECT deliverable_type_id, deliverable_name FROM {ConfigDB.DELIVERABLE_TYPES}")
            for row in cur.fetchall():
                deliverable_map[row["deliverable_name"]] = row["deliverable_type_id"]

            cur.execute(f"SELECT age_bucket_id, bucket_label FROM {ConfigDB.AUDIENCE_AGE_BUCKETS}")
            for row in cur.fetchall():
                age_bucket_map[row["bucket_label"]] = row["age_bucket_id"]

            cur.execute(f"SELECT audience_type_id, type_name FROM {ConfigDB.AUDIENCE_TYPES}")
            for row in cur.fetchall():
                audience_type_map[row["type_name"]] = row["audience_type_id"]

            cur.execute(f"SELECT interest_tag_id, tag_name FROM {ConfigDB.INTEREST_TAGS}")
            for row in cur.fetchall():
                interest_tag_map[row["tag_name"]] = row["interest_tag_id"]

    print(f"✅ Loaded configdb reference maps: "
          f"countries={len(country_map)}, states={len(state_map)}, cities={len(city_map)}, "
          f"event_types={len(event_type_map)}, categories={len(category_map)}, "
          f"deliverables={len(deliverable_map)}, age_buckets={len(age_bucket_map)}, "
          f"audience_types={len(audience_type_map)}, interest_tags={len(interest_tag_map)}")
    return (
        country_map, state_map, city_map, event_type_map, category_map,
        deliverable_map, age_bucket_map, audience_type_map, interest_tag_map,
    )


def seed_match_weight_sets() -> Dict[str, int]:
    """Seed match weight sets reference table."""
    # (match_weight_set_id, set_name, set_code, weight_category, weight_geo, weight_budget, weight_audience, weight_deliverables)
    weight_sets_data = [
        (100, "Balanced", "BALANCED", 0.25, 0.20, 0.20, 0.20, 0.15),
        (101, "Geography First", "GEO_FIRST", 0.20, 0.40, 0.15, 0.15, 0.10),
        (102, "Budget First", "BUDGET_FIRST", 0.20, 0.15, 0.40, 0.15, 0.10),
        (103, "Audience First", "AUDIENCE_FIRST", 0.15, 0.15, 0.20, 0.40, 0.10),
        (104, "Conservative", "CONSERVATIVE", 0.25, 0.25, 0.25, 0.15, 0.10),
    ]
    
    weight_map = {}
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            for set_id, set_name, set_code, w_cat, w_geo, w_bud, w_aud, w_deliv in weight_sets_data:
                cur.execute(f"""
                    INSERT INTO {ConfigDB.MATCH_WEIGHT_SETS} 
                    (match_weight_set_id, set_name, set_code, weight_category, weight_geo, weight_budget, weight_audience, weight_deliverables)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (set_code) DO NOTHING
                    RETURNING match_weight_set_id
                """, (set_id, set_name, set_code, w_cat, w_geo, w_bud, w_aud, w_deliv))
                result = cur.fetchone()
                if result:
                    weight_map[set_name] = result['match_weight_set_id']
                else:
                    cur.execute(f"SELECT match_weight_set_id FROM {ConfigDB.MATCH_WEIGHT_SETS} WHERE set_code = %s", (set_code,))
                    weight_map[set_name] = cur.fetchone()['match_weight_set_id']
            
            conn.commit()
    
    print(f"✅ Seeded {len(weight_sets_data)} match weight sets")
    return weight_map


def seed_match_rule_sets() -> Dict[str, int]:
    """Seed match rule sets reference table."""
    # (match_rule_set_id, set_name, set_code, enforce_must_have_deliverables, enforce_city_filter,
    #  enforce_date_window, enforce_budget_overlap, min_budget_overlap_ratio,
    #  allowed_date_slack_days, min_audience_overlap_score)
    rule_sets_data = [
        (100, "Standard", "STANDARD", True, False, True, True, 0.10, 0, 0.30),
        (101, "Strict", "STRICT", True, True, True, True, 0.20, 0, 0.50),
        (102, "Relaxed", "RELAXED", False, False, False, False, 0.05, 30, 0.10),
        (103, "Geography Strict", "GEO_STRICT", True, True, False, True, 0.10, 14, 0.20),
    ]
    
    rule_map = {}
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            for rule_data in rule_sets_data:
                set_id, set_name, set_code = rule_data[0], rule_data[1], rule_data[2]
                cur.execute(f"""
                    INSERT INTO {ConfigDB.MATCH_RULE_SETS}
                    (match_rule_set_id, set_name, set_code, enforce_must_have_deliverables, enforce_city_filter,
                     enforce_date_window, enforce_budget_overlap, min_budget_overlap_ratio,
                     allowed_date_slack_days, min_audience_overlap_score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (set_code) DO NOTHING
                    RETURNING match_rule_set_id
                """, rule_data)
                result = cur.fetchone()
                if result:
                    rule_map[set_name] = result['match_rule_set_id']
                else:
                    cur.execute(f"SELECT match_rule_set_id FROM {ConfigDB.MATCH_RULE_SETS} WHERE set_code = %s", (set_code,))
                    rule_map[set_name] = cur.fetchone()['match_rule_set_id']
            
            conn.commit()
    
    print(f"✅ Seeded {len(rule_sets_data)} match rule sets")
    return rule_map


# ============================================================================
# SECTION 2: COREDB SAMPLE DATA - BRANDS
# ============================================================================

def seed_sample_brands(city_map: Dict[str, int], state_map: Dict[str, int], 
                       country_map: Dict[str, int], category_map: Dict[str, int],
                       deliverable_map: Dict[str, int], age_bucket_map: Dict[str, int],
                       audience_type_map: Dict[str, int], interest_tag_map: Dict[str, int],
                       weight_set_map: Dict[str, int], rule_set_map: Dict[str, int]) -> List[int]:
    """
    Seed sample brand organizations with profiles and preferences.
    
    Returns: List of brand_org_ids created
    """
    
    # Brand 1: TechCorp India (LOCAL focus - Mumbai)
    brand_1_org_id = 201
    
    # Brand 2: MusicCo California (STATE focus - California)
    brand_2_org_id = 202
    
    # Brand 3: GlobalBrand USA (NATIONAL focus - United States)
    brand_3_org_id = 203
    
    # Brand 4: LocalChicago (LOCAL focus - Chicago area)
    brand_4_org_id = 204
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            
            # ================================================================
            # BRAND 1: TechCorp India (LOCAL - Mumbai)
            # ================================================================
            
            # Create org (OVERRIDING SYSTEM VALUE required if org_id is IDENTITY GENERATED ALWAYS)
            cur.execute(f"""
                INSERT INTO {CoreDB.ORGS} (org_id, org_type, org_name, is_active)
                OVERRIDING SYSTEM VALUE
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (org_id) DO NOTHING
            """, (brand_1_org_id, 'brand', 'TechCorp India', True))
            
            # Create brand profile (LOCAL focus)
            cur.execute(f"""
                INSERT INTO {CoreDB.BRAND_PROFILES}
                (brand_org_id, objective_primary, spend_per_event_min, spend_per_event_max,
                 geographic_focus_type, campaign_start, campaign_end, default_match_weight_set_id, default_match_rule_set_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (brand_org_id) DO NOTHING
            """, (brand_1_org_id, 'awareness', 75000, 200000, 'local', 
                  '2025-01-01', '2025-12-31', 100, 100))
            
            # Add target cities (Mumbai, Pune)
            for city_name in ["Mumbai", "Pune"]:
                city_id = city_map.get(city_name)
                if city_id:
                    cur.execute(f"""
                        INSERT INTO {CoreDB.BRAND_TARGET_CITIES} (brand_org_id, city_id, is_active)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (brand_org_id, city_id) DO NOTHING
                    """, (brand_1_org_id, city_id, True))
            
            # Add preferred categories (Technology, Business)
            for cat_name in ["Technology", "Business"]:
                cat_id = category_map.get(cat_name)
                if cat_id:
                    cur.execute(f"""
                        INSERT INTO {CoreDB.BRAND_PREFERRED_CATEGORIES} 
                        (brand_org_id, category_id, preference_type)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (brand_org_id, category_id, preference_type) DO NOTHING
                    """, (brand_1_org_id, cat_id, 'preferred'))
            
            # Add wanted deliverables
            for deliv_name in ["Stage Branding", "Social Media Posts", "Speaking Slot"]:
                deliv_id = deliverable_map.get(deliv_name)
                if deliv_id:
                    cur.execute(f"""
                        INSERT INTO {CoreDB.BRAND_DELIVERABLE_PREFERENCES}
                        (brand_org_id, deliverable_type_id, preference_type)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (brand_org_id, deliverable_type_id, preference_type) DO NOTHING
                    """, (brand_1_org_id, deliv_id, 'wanted'))
            
            # Add target age buckets
            for age_label in ["25-34", "35-44"]:
                age_id = age_bucket_map.get(age_label)
                if age_id:
                    cur.execute(f"""
                        INSERT INTO {CoreDB.BRAND_TARGET_AGE_BUCKETS} (brand_org_id, age_bucket_id)
                        VALUES (%s, %s)
                        ON CONFLICT (brand_org_id, age_bucket_id) DO NOTHING
                    """, (brand_1_org_id, age_id))
            
            # Add target audience types
            for aud_type in ["B2B", "Professionals"]:
                aud_id = audience_type_map.get(aud_type)
                if aud_id:
                    cur.execute(f"""
                        INSERT INTO {CoreDB.BRAND_TARGET_AUDIENCE_TYPES} (brand_org_id, audience_type_id)
                        VALUES (%s, %s)
                        ON CONFLICT (brand_org_id, audience_type_id) DO NOTHING
                    """, (brand_1_org_id, aud_id))
            
            # Add target interest tags
            for tag_name in ["Technology", "Innovation", "AI & Machine Learning"]:
                tag_id = interest_tag_map.get(tag_name)
                if tag_id:
                    cur.execute(f"""
                        INSERT INTO {CoreDB.BRAND_TARGET_INTEREST_TAGS} (brand_org_id, interest_tag_id)
                        VALUES (%s, %s)
                        ON CONFLICT (brand_org_id, interest_tag_id) DO NOTHING
                    """, (brand_1_org_id, tag_id))
            
            # ================================================================
            # BRAND 2: MusicCo California (STATE - California)
            # ================================================================
            
            cur.execute(f"""
                INSERT INTO {CoreDB.ORGS} (org_id, org_type, org_name, is_active)
                OVERRIDING SYSTEM VALUE
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (org_id) DO NOTHING
            """, (brand_2_org_id, 'brand', 'MusicCo California', True))
            
            cur.execute(f"""
                INSERT INTO {CoreDB.BRAND_PROFILES}
                (brand_org_id, objective_primary, spend_per_event_min, spend_per_event_max,
                 geographic_focus_type, campaign_start, campaign_end, default_match_weight_set_id, default_match_rule_set_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (brand_org_id) DO NOTHING
            """, (brand_2_org_id, 'awareness', 25000, 100000, 'state',
                  '2025-06-01', '2025-08-31', 101, 101))
            
            # Add target states (California)
            california_state_id = state_map.get("California")
            if california_state_id:
                cur.execute(f"""
                    INSERT INTO {CoreDB.BRAND_TARGET_STATES} (brand_org_id, state_id, is_active)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (brand_org_id, state_id) DO NOTHING
                """, (brand_2_org_id, california_state_id, True))
            
            # Add preferred categories
            for cat_name in ["Music", "Entertainment", "Cultural"]:
                cat_id = category_map.get(cat_name)
                if cat_id:
                    cur.execute(f"""
                        INSERT INTO {CoreDB.BRAND_PREFERRED_CATEGORIES}
                        (brand_org_id, category_id, preference_type)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (brand_org_id, category_id, preference_type) DO NOTHING
                    """, (brand_2_org_id, cat_id, 'preferred'))
            
            # Add wanted deliverables
            for deliv_name in ["Social Media Posts", "Event App Branding"]:
                deliv_id = deliverable_map.get(deliv_name)
                if deliv_id:
                    cur.execute(f"""
                        INSERT INTO {CoreDB.BRAND_DELIVERABLE_PREFERENCES}
                        (brand_org_id, deliverable_type_id, preference_type)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (brand_org_id, deliverable_type_id, preference_type) DO NOTHING
                    """, (brand_2_org_id, deliv_id, 'wanted'))
            
            # ================================================================
            # BRAND 3: GlobalBrand USA (NATIONAL - United States)
            # ================================================================
            
            cur.execute(f"""
                INSERT INTO {CoreDB.ORGS} (org_id, org_type, org_name, is_active)
                OVERRIDING SYSTEM VALUE
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (org_id) DO NOTHING
            """, (brand_3_org_id, 'brand', 'GlobalBrand USA', True))
            
            cur.execute(f"""
                INSERT INTO {CoreDB.BRAND_PROFILES}
                (brand_org_id, objective_primary, spend_per_event_min, spend_per_event_max,
                 geographic_focus_type, campaign_start, campaign_end, default_match_weight_set_id, default_match_rule_set_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (brand_org_id) DO NOTHING
            """, (brand_3_org_id, 'awareness', 50000, 250000, 'national',
                  '2025-03-01', '2025-11-30', 102, 102))
            
            # Add target countries (United States)
            us_country_id = country_map.get("United States")
            if us_country_id:
                cur.execute(f"""
                    INSERT INTO {CoreDB.BRAND_TARGET_COUNTRIES} (brand_org_id, country_id, is_active)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (brand_org_id, country_id) DO NOTHING
                """, (brand_3_org_id, us_country_id, True))
            
            # Add preferred categories
            for cat_name in ["Technology", "Business"]:
                cat_id = category_map.get(cat_name)
                if cat_id:
                    cur.execute(f"""
                        INSERT INTO {CoreDB.BRAND_PREFERRED_CATEGORIES}
                        (brand_org_id, category_id, preference_type)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (brand_org_id, category_id, preference_type) DO NOTHING
                    """, (brand_3_org_id, cat_id, 'preferred'))
            
            # Add must-have deliverables (strict requirement)
            for deliv_name in ["Stage Branding", "Social Media Posts"]:
                deliv_id = deliverable_map.get(deliv_name)
                if deliv_id:
                    cur.execute(f"""
                        INSERT INTO {CoreDB.BRAND_DELIVERABLE_PREFERENCES}
                        (brand_org_id, deliverable_type_id, preference_type)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (brand_org_id, deliverable_type_id, preference_type) DO NOTHING
                    """, (brand_3_org_id, deliv_id, 'must_have'))
            
            # ================================================================
            # BRAND 4: LocalChicago (LOCAL - Chicago area)
            # ================================================================
            
            cur.execute(f"""
                INSERT INTO {CoreDB.ORGS} (org_id, org_type, org_name, is_active)
                OVERRIDING SYSTEM VALUE
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (org_id) DO NOTHING
            """, (brand_4_org_id, 'brand', 'LocalChicago Community Fund', True))
            
            cur.execute(f"""
                INSERT INTO {CoreDB.BRAND_PROFILES}
                (brand_org_id, objective_primary, spend_per_event_min, spend_per_event_max,
                 geographic_focus_type, campaign_start, campaign_end, default_match_weight_set_id, default_match_rule_set_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (brand_org_id) DO NOTHING
            """, (brand_4_org_id, 'awareness', 5000, 25000, 'local',
                  None, None, 103, 103))
            
            # Add target cities (Chicago, Evanston, Aurora)
            for city_name in ["Chicago", "Evanston", "Aurora"]:
                city_id = city_map.get(city_name)
                if city_id:
                    cur.execute(f"""
                        INSERT INTO {CoreDB.BRAND_TARGET_CITIES} (brand_org_id, city_id, is_active)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (brand_org_id, city_id) DO NOTHING
                    """, (brand_4_org_id, city_id, True))
            
            # Add preferred categories
            for cat_name in ["Community", "Cultural"]:
                cat_id = category_map.get(cat_name)
                if cat_id:
                    cur.execute(f"""
                        INSERT INTO {CoreDB.BRAND_PREFERRED_CATEGORIES}
                        (brand_org_id, category_id, preference_type)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (brand_org_id, category_id, preference_type) DO NOTHING
                    """, (brand_4_org_id, cat_id, 'preferred'))
            
            conn.commit()
    
    brand_ids = [brand_1_org_id, brand_2_org_id, brand_3_org_id, brand_4_org_id]
    print(f"✅ Seeded {len(brand_ids)} sample brands")
    return brand_ids


# ============================================================================
# SECTION 3: COREDB SAMPLE DATA - EVENTS
# ============================================================================

def seed_sample_events(city_map: Dict[str, int], event_type_map: Dict[str, int],
                       category_map: Dict[str, int], deliverable_map: Dict[str, int],
                       age_bucket_map: Dict[str, int], audience_type_map: Dict[str, int],
                       interest_tag_map: Dict[str, int]) -> List[int]:
    """
    Seed sample event organizations with profiles and details.
    
    Returns: List of event_org_ids created
    """
    
    events_to_create = [
        {
            'org_id': 101,
            'org_name': 'Mumbai Tech Summit 2025',
            'event_name': 'Mumbai Tech Summit 2025',
            'city_name': 'Mumbai',
            'event_type': 'Tech Conference',
            'start_date': '2025-06-15',
            'end_date': '2025-06-17',
            'expected_audience_size': 5000,
            'package_min': 100000,
            'package_max': 200000,
            'categories': ['Technology', 'Business'],
            'deliverables': [('Stage Branding', 5), ('Social Media Posts', 20), ('Speaking Slot', 3)],
            'age_buckets': [('25-34', 40), ('35-44', 35), ('18-24', 25)],
            'audience_types': [('B2B', 1.0), ('Professionals', 0.8)],
            'interest_tags': [('Technology', 1.0), ('Innovation', 0.9), ('AI & Machine Learning', 0.7)]
        },
        {
            'org_id': 102,
            'org_name': 'Surat Startup Expo',
            'event_name': 'Surat Expo 2025',
            'city_name': 'Surat',
            'event_type': 'Startup Event',
            'start_date': '2025-04-10',
            'end_date': '2025-04-12',
            'expected_audience_size': 3000,
            'package_min': 60000,
            'package_max': 150000,
            'categories': ['Technology', 'Business'],
            'deliverables': [('Stage Branding', 3), ('Booth Space', 10), ('Social Media Posts', 15)],
            'age_buckets': [('25-34', 50), ('18-24', 30), ('35-44', 20)],
            'audience_types': [('B2B', 1.0), ('Professionals', 0.7)],
            'interest_tags': [('Startups', 1.0), ('Entrepreneurship', 0.9), ('Technology', 0.8)]
        },
        {
            'org_id': 103,
            'org_name': 'Gurgaon Music Festival',
            'event_name': 'Gurgaon Summer Music Fest',
            'city_name': 'Gurgaon',
            'event_type': 'Music Festival',
            'start_date': '2025-07-20',
            'end_date': '2025-07-22',
            'expected_audience_size': 15000,
            'package_min': 40000,
            'package_max': 120000,
            'categories': ['Music', 'Entertainment'],
            'deliverables': [('Stage Branding', 2), ('Social Media Posts', 30), ('Banner Ads', 10)],
            'age_buckets': [('18-24', 45), ('25-34', 35), ('35-44', 20)],
            'audience_types': [('B2C', 1.0), ('General Public', 0.9)],
            'interest_tags': [('Music', 1.0), ('Entertainment', 0.8)]
        },
        {
            'org_id': 104,
            'org_name': 'Chennai Community Fair',
            'event_name': 'Chennai Community Health Fair',
            'city_name': 'Chennai',
            'event_type': 'Community Event',
            'start_date': '2025-09-10',
            'end_date': '2025-09-10',
            'expected_audience_size': 800,
            'package_min': 8000,
            'package_max': 20000,
            'categories': ['Community', 'Health & Wellness'],
            'deliverables': [('Booth Space', 5), ('Branded Materials', 1000)],
            'age_buckets': [('35-44', 30), ('45-54', 25), ('25-34', 20), ('55-64', 15), ('18-24', 10)],
            'audience_types': [('B2C', 1.0), ('Families', 0.8)],
            'interest_tags': [('Community', 1.0), ('Education', 0.6)]
        },
        {
            'org_id': 105,
            'org_name': 'Ghaziabad Tech Conference',
            'event_name': 'Ghaziabad AI Summit 2025',
            'city_name': 'Ghaziabad',
            'event_type': 'Tech Conference',
            'start_date': '2025-05-05',
            'end_date': '2025-05-07',
            'expected_audience_size': 4000,
            'package_min': 80000,
            'package_max': 200000,
            'categories': ['Technology'],
            'deliverables': [('Stage Branding', 4), ('Speaking Slot', 5), ('Social Media Posts', 25)],
            'age_buckets': [('25-34', 45), ('35-44', 35), ('18-24', 20)],
            'audience_types': [('B2B', 1.0), ('Professionals', 0.9)],
            'interest_tags': [('AI & Machine Learning', 1.0), ('Technology', 0.9), ('Innovation', 0.8)]
        },
    ]
    
    event_org_ids = []
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            for event_data in events_to_create:
                org_id = event_data['org_id']
                
                # Create org
                cur.execute(f"""
                    INSERT INTO {CoreDB.ORGS} (org_id, org_type, org_name, is_active)
                    OVERRIDING SYSTEM VALUE
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (org_id) DO NOTHING
                    RETURNING org_id
                """, (org_id, 'event', event_data['org_name'], True))
                
                result = cur.fetchone()
                if not result:
                    # Already exists, fetch it
                    cur.execute(f"SELECT org_id FROM {CoreDB.ORGS} WHERE org_id = %s", (org_id,))
                    result = cur.fetchone()
                
                event_org_id = result['org_id']
                event_org_ids.append(event_org_id)
                
                # Get city_id
                city_id = city_map.get(event_data['city_name'])
                if not city_id:
                    print(f"⚠️ City '{event_data['city_name']}' not found for event '{event_data['event_name']}'")
                    continue
                
                # Get event_type_id
                event_type_id = event_type_map.get(event_data['event_type'])
                
                # Create event profile
                cur.execute(f"""
                    INSERT INTO {CoreDB.EVENT_PROFILES}
                    (event_org_id, event_name, event_type_id, city_id, start_date, end_date, expected_audience_size)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_org_id) DO NOTHING
                """, (event_org_id, event_data['event_name'], event_type_id, city_id,
                      event_data['start_date'], event_data['end_date'], event_data['expected_audience_size']))
                
                # Add categories
                for cat_name in event_data.get('categories', []):
                    cat_id = category_map.get(cat_name)
                    if cat_id:
                        cur.execute(f"""
                            INSERT INTO {CoreDB.EVENT_CATEGORIES_MAP} (event_org_id, category_id)
                            VALUES (%s, %s)
                            ON CONFLICT (event_org_id, category_id) DO NOTHING
                        """, (event_org_id, cat_id))
                
                # Add sponsorship inventory
                cur.execute(f"""
                    INSERT INTO {CoreDB.EVENT_SPONSORSHIP_INVENTORY} (event_org_id, package_min, package_max)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (event_org_id) DO NOTHING
                """, (event_org_id, event_data['package_min'], event_data['package_max']))
                
                # Add deliverables inventory
                for deliv_name, max_count in event_data.get('deliverables', []):
                    deliv_id = deliverable_map.get(deliv_name)
                    if deliv_id:
                        cur.execute(f"""
                            INSERT INTO {CoreDB.EVENT_DELIVERABLES_INVENTORY}
                            (event_org_id, deliverable_type_id, max_count)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (event_org_id, deliverable_type_id) DO NOTHING
                        """, (event_org_id, deliv_id, max_count))
                
                # Add age distribution
                for age_label, percent in event_data.get('age_buckets', []):
                    age_id = age_bucket_map.get(age_label)
                    if age_id:
                        cur.execute(f"""
                            INSERT INTO {CoreDB.EVENT_AGE_DISTRIBUTION} (event_org_id, age_bucket_id, percent)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (event_org_id, age_bucket_id) DO NOTHING
                        """, (event_org_id, age_id, percent))
                
                # Add audience types
                for aud_type, weight in event_data.get('audience_types', []):
                    aud_id = audience_type_map.get(aud_type)
                    if aud_id:
                        cur.execute(f"""
                            INSERT INTO {CoreDB.EVENT_AUDIENCE_TYPES_MAP} (event_org_id, audience_type_id, weight)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (event_org_id, audience_type_id) DO NOTHING
                        """, (event_org_id, aud_id, weight))
                
                # Add interest tags
                for tag_name, weight in event_data.get('interest_tags', []):
                    tag_id = interest_tag_map.get(tag_name)
                    if tag_id:
                        cur.execute(f"""
                            INSERT INTO {CoreDB.EVENT_INTEREST_TAGS_MAP} (event_org_id, interest_tag_id, weight)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (event_org_id, interest_tag_id) DO NOTHING
                        """, (event_org_id, tag_id, weight))
            
            conn.commit()
    
    print(f"✅ Seeded {len(event_org_ids)} sample events")
    return event_org_ids


# ============================================================================
# MAIN SEEDING FUNCTION
# ============================================================================

def seed_database_postgres():
    """
    Main function to seed PostgreSQL database.
    
    Order:
    1. Load ConfigDB reference maps from DB (read-only; no insert except below).
    2. Seed ConfigDB match_weight_sets and match_rule_sets only (if missing).
    3. Seed CoreDB: sample brands (orgs → profiles → preferences), then events.
    
    ConfigDB reference data (countries, states, cities, types, etc.) must already exist.
    """
    print("=" * 80)
    print("BarterNow PostgreSQL Database Seeding")
    print("=" * 80)
    
    try:
        # Test connection first
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database()")
                db_name = cur.fetchone()['current_database']
                print(f"📊 Connected to database: {db_name}")
        
        print("\n🌍 Loading ConfigDB reference data (read-only)...")
        print("-" * 80)
        (
            country_map, state_map, city_map, event_type_map, category_map,
            deliverable_map, age_bucket_map, audience_type_map, interest_tag_map,
        ) = load_configdb_reference_maps()
        
        print("\n⚙️ Seeding ConfigDB: match weight sets & rule sets only...")
        print("-" * 80)
        weight_set_map = seed_match_weight_sets()
        rule_set_map = seed_match_rule_sets()
        
        print("\n👥 Seeding CoreDB sample data...")
        print("-" * 80)
        
        # Seed brands (depend on reference data from configdb)
        brand_ids = seed_sample_brands(
            city_map, state_map, country_map, category_map, deliverable_map,
            age_bucket_map, audience_type_map, interest_tag_map, weight_set_map, rule_set_map
        )
        
        # Seed events (depend on reference data from configdb)
        event_ids = seed_sample_events(
            city_map, event_type_map, category_map, deliverable_map,
            age_bucket_map, audience_type_map, interest_tag_map
        )
        
        print("\n" + "=" * 80)
        print("✅ Database seeding completed successfully!")
        print("=" * 80)
        print(f"📊 Summary (configdb loaded, coredb seeded):")
        print(f"   - ConfigDB refs: countries={len(country_map)}, states={len(state_map)}, cities={len(city_map)}, "
              f"event_types={len(event_type_map)}, categories={len(category_map)}")
        print(f"   - CoreDB: brands={len(brand_ids)}, events={len(event_ids)}")
        print("=" * 80)
        
        print("\n🧪 Test matching with:")
        print("   from app.matching import get_matches_for_brand")
        print(f"   result = get_matches_for_brand(brand_org_id={brand_ids[0]})")
        print("   print(result)")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error during seeding: {e}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return False


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def verify_seeding():
    """Verify that seeding was successful."""
    print("\n🔍 Verifying seeded data...")
    print("-" * 80)
    
    checks = [
        (f"SELECT COUNT(*) as cnt FROM {ConfigDB.COUNTRIES}", "Countries"),
        (f"SELECT COUNT(*) as cnt FROM {ConfigDB.STATES}", "States"),
        (f"SELECT COUNT(*) as cnt FROM {ConfigDB.CITIES}", "Cities"),
        (f"SELECT COUNT(*) as cnt FROM {ConfigDB.EVENT_TYPES}", "Event Types"),
        (f"SELECT COUNT(*) as cnt FROM {ConfigDB.EVENT_CATEGORIES}", "Event Categories"),
        (f"SELECT COUNT(*) as cnt FROM {CoreDB.ORGS} WHERE org_type = 'brand'", "Brands"),
        (f"SELECT COUNT(*) as cnt FROM {CoreDB.ORGS} WHERE org_type = 'event'", "Events"),
        (f"SELECT COUNT(*) as cnt FROM {CoreDB.BRAND_PROFILES}", "Brand Profiles"),
        (f"SELECT COUNT(*) as cnt FROM {CoreDB.EVENT_PROFILES}", "Event Profiles"),
    ]
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            for query, label in checks:
                cur.execute(query)
                count = cur.fetchone()['cnt']
                status = "✅" if count > 0 else "❌"
                print(f"{status} {label}: {count}")
    
    # Check geographic hierarchy
    print("\n🌍 Checking geographic hierarchy...")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT COUNT(*) as cnt 
                FROM {ConfigDB.CITIES} 
                WHERE state_id IS NULL OR country_id IS NULL
            """)
            incomplete = cur.fetchone()['cnt']
            
            if incomplete == 0:
                print(f"✅ All cities have complete geographic hierarchy")
            else:
                print(f"⚠️ {incomplete} cities missing state_id or country_id")
    
    # Check brand geographic preferences
    print("\n👥 Checking brand geographic preferences...")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT 
                    bp.geographic_focus_type,
                    COUNT(DISTINCT bp.brand_org_id) as brand_count,
                    COUNT(DISTINCT btc.brand_org_id) as brands_with_cities,
                    COUNT(DISTINCT bts.brand_org_id) as brands_with_states,
                    COUNT(DISTINCT btco.brand_org_id) as brands_with_countries
                FROM {CoreDB.BRAND_PROFILES} bp
                LEFT JOIN {CoreDB.BRAND_TARGET_CITIES} btc ON bp.brand_org_id = btc.brand_org_id
                LEFT JOIN {CoreDB.BRAND_TARGET_STATES} bts ON bp.brand_org_id = bts.brand_org_id
                LEFT JOIN {CoreDB.BRAND_TARGET_COUNTRIES} btco ON bp.brand_org_id = btco.brand_org_id
                GROUP BY bp.geographic_focus_type
                ORDER BY bp.geographic_focus_type
            """)
            
            for row in cur.fetchall():
                focus = row['geographic_focus_type']
                brand_count = row['brand_count']
                cities = row['brands_with_cities']
                states = row['brands_with_states']
                countries = row['brands_with_countries']
                
                print(f"   {focus}: {brand_count} brands (cities:{cities}, states:{states}, countries:{countries})")


def clear_sample_data():
    """
    Clear ONLY coredb sample data (brands, events, matches, deals).
    ConfigDB reference data is left unchanged.
    
    WARNING: This will delete all brands, events, matches, and deals in coredb.
             Use only in development/testing.
    """
    print("⚠️  Clearing sample data (coredb only; configdb unchanged)...")
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Delete in FK-safe order (children first); coredb only
            tables_to_clear = [
                CoreDB.MATCHES,
                CoreDB.EVENT_INTEREST_TAGS_MAP,
                CoreDB.EVENT_AUDIENCE_TYPES_MAP,
                CoreDB.EVENT_AGE_DISTRIBUTION,
                CoreDB.EVENT_DELIVERABLES_INVENTORY,
                CoreDB.EVENT_SPONSORSHIP_INVENTORY,
                CoreDB.EVENT_CATEGORIES_MAP,
                CoreDB.EVENT_PROFILES,
                CoreDB.BRAND_TARGET_INTEREST_TAGS,
                CoreDB.BRAND_TARGET_AUDIENCE_TYPES,
                CoreDB.BRAND_TARGET_AGE_BUCKETS,
                CoreDB.BRAND_DELIVERABLE_PREFERENCES,
                CoreDB.BRAND_PREFERRED_CATEGORIES,
                CoreDB.BRAND_TARGET_COUNTRIES,
                CoreDB.BRAND_TARGET_STATES,
                CoreDB.BRAND_TARGET_CITIES,
                CoreDB.BRAND_PROFILES,
                CoreDB.ORG_MEMBERSHIPS,
                CoreDB.ORGS,
            ]
            
            for table in tables_to_clear:
                cur.execute(f"DELETE FROM {table}")
            
            conn.commit()
    
    print("✅ Coredb sample data cleared")


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Main entry point for seeding."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Seed BarterNow PostgreSQL database')
    parser.add_argument('--clear', action='store_true', help='Clear existing sample data first')
    parser.add_argument('--verify', action='store_true', help='Only verify seeding (no changes)')
    args = parser.parse_args()
    
    if args.verify:
        verify_seeding()
        return
    
    if args.clear:
        clear_sample_data()
    
    # clear_sample_data()
    success = seed_database_postgres()
    
    if success:
        print("\n🔍 Running verification...")
        verify_seeding()
        sys.exit(0)
    else:
        print("\n❌ Seeding failed. Check errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
