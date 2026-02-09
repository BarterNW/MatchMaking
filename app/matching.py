"""
BarterNow Matching Module - Brand/Event Matching Logic

This module implements the matchmaking algorithm that scores events against
brand preferences. Uses string-based geographic matching (NO lat/lon).

DESIGN PRINCIPLES:
- Geographic: String-based hierarchical matching (city → state → country)
- Config-driven: Weights and rules from configdb tables
- Explainable: Each score includes human-readable explanation
"""

import json
from typing import Dict, List, Optional
from .database import (
    get_brand_profile,
    get_event_profile,
    get_all_event_orgs,
    check_geographic_match,
    get_match_weight_set,
    get_match_rule_set
)


# ============================================================================
# SCORING FUNCTIONS
# ============================================================================

def score_geography(
    event_city_id: int,
    brand_profile: Dict,
    weight: float
) -> Dict:
    """
    Score geography match using string-based hierarchical matching.
    
    CRITICAL: NO latitude/longitude, NO distance calculations.
    Uses check_geographic_match() which does:
    - LOCAL focus → city_id exact match
    - STATE focus → state_id exact match  
    - NATIONAL focus → country_id exact match
    
    WHY: Deterministic, reviewable, no approximation errors.
    """
    if not event_city_id:
        return {
            "weight": weight,
            "match_factor": 0.0,
            "contribution": 0.0,
            "explanation": "Event city not specified",
            "passed_hard_filter": False
        }
    
    geo_match = check_geographic_match(event_city_id, brand_profile)
    
    if geo_match['matches']:
        return {
            "weight": weight,
            "match_factor": 1.0,
            "contribution": weight * 1.0,
            "explanation": geo_match['explanation'],
            "passed_hard_filter": True,
            "match_level": geo_match['match_level']
        }
    else:
        return {
            "weight": weight,
            "match_factor": 0.0,
            "contribution": 0.0,
            "explanation": geo_match['explanation'],
            "passed_hard_filter": False
        }


def score_budget(
    event_funding_min: Optional[float],
    event_funding_max: Optional[float],
    sponsor_budget_min: Optional[float],
    sponsor_budget_max: Optional[float],
    weight: float,
    budget_near_boundary_ratio: float
) -> Dict:
    """Score budget match based on range overlap."""
    if event_funding_min is None or event_funding_max is None:
        return {
            "weight": weight,
            "match_factor": 0.5,
            "contribution": weight * 0.5,
            "explanation": "Event budget not specified"
        }
    
    # Convert Decimals to float (PostgreSQL numeric fields return Decimal)
    event_funding_min = float(event_funding_min)
    event_funding_max = float(event_funding_max)
    sponsor_budget_min = float(sponsor_budget_min)
    sponsor_budget_max = float(sponsor_budget_max)
    
    # Check if ranges overlap
    overlap_min = max(sponsor_budget_min, event_funding_min)
    overlap_max = min(sponsor_budget_max, event_funding_max)
    
    if overlap_min <= overlap_max:
        # Full match - ranges overlap
        match_factor = 1.0
        explanation = f"Budget ranges overlap: event needs ${event_funding_min:,.0f}-${event_funding_max:,.0f}, brand offer ${sponsor_budget_min:,.0f}-${sponsor_budget_max:,.0f}."
    else:
        # Check if near boundary (within configured ratio of range)
        sponsor_range = sponsor_budget_max - sponsor_budget_min
        event_range = event_funding_max - event_funding_min
        threshold = max(sponsor_range, event_range) * budget_near_boundary_ratio
        
        gap = min(
            abs(event_funding_max - sponsor_budget_min),
            abs(sponsor_budget_max - event_funding_min)
        )
        
        if gap <= threshold:
            match_factor = 0.5
            explanation = f"Budget ranges are close but don't overlap: event needs ${event_funding_min:,.0f}-${event_funding_max:,.0f}, bramd offer ${sponsor_budget_min:,.0f}-${sponsor_budget_max:,.0f}."
        else:
            match_factor = 0.0
            explanation = f"Budget ranges don't match: event needs ${event_funding_min:,.0f}-${event_funding_max:,.0f}, brand offer ${sponsor_budget_min:,.0f}-${sponsor_budget_max:,.0f}."
    
    contribution = weight * match_factor
    
    return {
        "weight": weight,
        "match_factor": match_factor,
        "contribution": contribution,
        "explanation": explanation
    }


def score_categories(
    event_categories: List[Dict],
    preferred_categories: List[Dict],
    avoided_categories: List[Dict],
    weight: float
) -> Dict:
    """
    Score category match.
    
    Logic:
    - If event category in preferred → full match
    - If event category in avoided → zero match
    - Otherwise → neutral (0.5)
    """
    if not event_categories:
        return {
            "weight": weight,
            "match_factor": 0.5,
            "contribution": weight * 0.5,
            "explanation": "Event categories not specified"
        }
    
    event_cat_ids = {c['category_id'] for c in event_categories}
    preferred_ids = {c['category_id'] for c in preferred_categories}
    avoided_ids = {c['category_id'] for c in avoided_categories}
    
    # Check if any event category is avoided
    if event_cat_ids & avoided_ids:
        avoided_names = [c['category_name'] for c in event_categories if c['category_id'] in avoided_ids]
        return {
            "weight": weight,
            "match_factor": 0.0,
            "contribution": 0.0,
            "explanation": f"Event has avoided categories: {', '.join(avoided_names)}"
        }
    
    # Check if any event category is preferred
    if event_cat_ids & preferred_ids:
        preferred_names = [c['category_name'] for c in event_categories if c['category_id'] in preferred_ids]
        return {
            "weight": weight,
            "match_factor": 1.0,
            "contribution": weight * 1.0,
            "explanation": f"Event has preferred categories: {', '.join(preferred_names)}"
        }
    
    # Neutral - not preferred but not avoided
    event_names = [c['category_name'] for c in event_categories]
    return {
        "weight": weight,
        "match_factor": 0.5,
        "contribution": weight * 0.5,
        "explanation": f"Event categories ({', '.join(event_names)}) are neutral (not preferred, not avoided)"
    }


def score_audience_overlap(
    event_age_distribution: List[Dict],
    event_audience_types: List[Dict],
    event_interest_tags: List[Dict],
    brand_age_buckets: List[Dict],
    brand_audience_types: List[Dict],
    brand_interest_tags: List[Dict],
    weight: float
) -> Dict:
    """
    Score audience overlap based on age, audience types, and interests.
    
    Logic:
    - Age: Check if any brand target age buckets overlap with event distribution
    - Audience types: Check if any brand target types match event types
    - Interests: Check if any brand target interests match event interests
    
    Returns weighted average of all three.
    """
    scores = []
    explanations = []
    
    # Age overlap
    if brand_age_buckets and event_age_distribution:
        brand_age_ids = {ab['age_bucket_id'] for ab in brand_age_buckets}
        event_age_ids = {ad['age_bucket_id'] for ad in event_age_distribution}
        overlap = brand_age_ids & event_age_ids
        
        if overlap:
            scores.append(1.0)
            explanations.append(f"Age buckets overlap ({len(overlap)} buckets)")
        else:
            scores.append(0.0)
            explanations.append("No age bucket overlap")
    
    # Audience types overlap
    if brand_audience_types and event_audience_types:
        brand_type_ids = {at['audience_type_id'] for at in brand_audience_types}
        event_type_ids = {at['audience_type_id'] for at in event_audience_types}
        overlap = brand_type_ids & event_type_ids
        
        if overlap:
            scores.append(1.0)
            explanations.append(f"Audience types match ({len(overlap)} types)")
        else:
            scores.append(0.0)
            explanations.append("No audience type overlap")
    
    # Interest tags overlap
    if brand_interest_tags and event_interest_tags:
        brand_tag_ids = {it['interest_tag_id'] for it in brand_interest_tags}
        event_tag_ids = {it['interest_tag_id'] for it in event_interest_tags}
        overlap = brand_tag_ids & event_tag_ids
        
        if overlap:
            scores.append(1.0)
            overlap_names = [it['tag_name'] for it in event_interest_tags if it['interest_tag_id'] in overlap]
            explanations.append(f"Interest tags match: {', '.join(overlap_names[:3])}")
        else:
            scores.append(0.0)
            explanations.append("No interest tag overlap")
    
    # Calculate average
    if scores:
        match_factor = sum(scores) / len(scores)
        explanation = "\n".join(explanations)
    else:
        match_factor = 0.5
        explanation = "Insufficient audience data to score"
    
    return {
        "weight": weight,
        "match_factor": match_factor,
        "contribution": weight * match_factor,
        "explanation": explanation
    }


def score_deliverables(
    event_deliverables: List[Dict],
    wanted_deliverables: List[Dict],
    must_have_deliverables: List[Dict],
    weight: float
) -> Dict:
    """
    Score deliverables match.
    
    Logic:
    - If brand has must_have deliverables, ALL must be offered by event (hard filter)
    - Otherwise, score based on percentage of wanted deliverables offered
    """
    if not event_deliverables:
        return {
            "weight": weight,
            "match_factor": 0.0,
            "contribution": 0.0,
            "explanation": "Event offers no deliverables",
            "passed_hard_filter": False
        }
    
    event_deliv_ids = {d['deliverable_type_id'] for d in event_deliverables}
    
    # Check must_have deliverables (hard filter)
    if must_have_deliverables:
        must_have_ids = {d['deliverable_type_id'] for d in must_have_deliverables}
        missing = must_have_ids - event_deliv_ids
        
        if missing:
            missing_names = [d['deliverable_name'] for d in must_have_deliverables 
                           if d['deliverable_type_id'] in missing]
            return {
                "weight": weight,
                "match_factor": 0.0,
                "contribution": 0.0,
                "explanation": f"Event missing must-have deliverables: {', '.join(missing_names)}",
                "passed_hard_filter": False
            }
    
    # Score based on wanted deliverables
    if wanted_deliverables:
        wanted_ids = {d['deliverable_type_id'] for d in wanted_deliverables}
        offered = wanted_ids & event_deliv_ids
        
        if offered:
            match_factor = len(offered) / len(wanted_ids)
            offered_names = [d['deliverable_name'] for d in event_deliverables 
                           if d['deliverable_type_id'] in offered]
            explanation = f"Event offers {len(offered)}/{len(wanted_ids)} wanted deliverables: {', '.join(offered_names)}"
        else:
            match_factor = 0.5
            explanation = "Event offers deliverables, but none are in wanted list"
    else:
        match_factor = 1.0
        explanation = "No specific deliverable preferences"
    
    return {
        "weight": weight,
        "match_factor": match_factor,
        "contribution": weight * match_factor,
        "explanation": explanation,
        "passed_hard_filter": True
    }


# ============================================================================
# MAIN MATCHING LOGIC FOR EVENT
# ============================================================================

def evaluate_event_for_brands(brand_org_id: int, event_org_id: int) -> Optional[Dict]:
    """
    Evaluate a single event against a brand's preferences.
    
    Returns:
        {
            'event_org_id': int,
            'event_name': str,
            'total_score': float,
            'max_score': float,
            'match_percentage': float,
            'breakdown': {...},
            'explanation': str
        }
        OR None if event doesn't pass hard filters
    
    WHY: Single function to score one brand-event pair. Used by batch matching.
    """
    # Get brand and event profiles
    brand = get_brand_profile(brand_org_id)
    event = get_event_profile(event_org_id)
    
    if not brand or not event:
        return None
    
    # Get match configuration (weights and rules)
    weight_set_id = brand.get('default_match_weight_set_id')
    rule_set_id = brand.get('default_match_rule_set_id')
    
    DEFAULT_WEIGHTS = {
        'category': 0.25,
        'geo': 0.20,
        'budget': 0.20,
        'audience': 0.20,
        'deliverables': 0.15
    }
    DEFAULT_RULES = {
        'enforce_must_have_deliverables': False,
        'enforce_city_filter': False,
        'enforce_date_window': False,
        'enforce_budget_overlap': False,
        'min_budget_overlap_ratio': 1.0,
        'allowed_date_slack_days': 0,
        'min_audience_overlap_score': 1.0,
        'budget_near_boundary_ratio': 0.1,
        'footfall_partial_match_ratio': 0.8
    }
    # Load weights (or use defaults when missing/inactive in configdb)
    weights = get_match_weight_set(weight_set_id) if weight_set_id else None
    if not weights:
        weights = DEFAULT_WEIGHTS
    # Load rules (or use defaults when missing/inactive in configdb)
    rules = get_match_rule_set(rule_set_id) if rule_set_id else None
    if not rules:
        rules = DEFAULT_RULES
    
    # Score geography (HARD FILTER)
    geo_score = score_geography(
        event.get('city_id'),
        brand,
        weights['geo']
    )
    
    if rules['enforce_city_filter']:
        if not geo_score.get('passed_hard_filter', False):
            # Geography is a hard filter - reject immediately
            return None
    
    # Score budget
    budget_score = score_budget(
        event.get('package_min') or 0,
        event.get('package_max') or 0,
        brand.get('spend_per_event_min') or 0,
        brand.get('spend_per_event_max') or 0,
        weights['budget'],
        rules['budget_near_boundary_ratio']
    )
    
    # Score categories
    category_score = score_categories(
        event.get('categories', []),
        brand.get('preferred_categories', []),
        brand.get('avoided_categories', []),
        weights['category']
    )
    
    # If event has avoided category, reject (hard filter)
    if category_score['match_factor'] == 0.0 and 'avoided' in category_score['explanation']:
        return None
    
    # Build breakdown
    breakdown = {
        "geography": geo_score,
        "budget": budget_score,
        "categories": category_score
    }
    
    # Calculate final score (dynamically from breakdown - no hardcoding)
    total_score = sum(score['contribution'] for score in breakdown.values())
    max_score = sum(score['weight'] for score in breakdown.values())
    match_percentage = (total_score / max_score * 100) if max_score > 0 else 0.0
    
    # Build explanation (only include positive matches)
    explanation_parts = []
    for name, score in breakdown.items():
        if score['match_factor'] > 0:
            explanation_parts.append(f"{name.title()}: {score['explanation']}")
    
    explanation = "\n".join(explanation_parts) if explanation_parts else "Minimal match"
    
    return {
        "event_org_id": event['event_org_id'],
        "event_name": event['event_name'],
        "total_score": round(total_score, 2),
        "max_score": round(max_score, 2),
        "match_percentage": round(match_percentage, 2),
        "breakdown": breakdown,
        "explanation": explanation
    }

# ============================================================================
# MAIN MATCHING LOGIC FOR BRANDS
# ============================================================================

def evaluate_brand_for_events(brand_org_id: int, event_org_id: int) -> Optional[Dict]:
    """
    Evaluate a single brand against events preferences.
    
    Returns:
        {
            'event_org_id': int,
            'event_name': str,
            'total_score': float,
            'max_score': float,
            'match_percentage': float,
            'breakdown': {...},
            'explanation': str
        }
        OR None if event doesn't pass hard filters
    
    WHY: Single function to score one brand-event pair. Used by batch matching.
    """
    # Get brand and event profiles
    brand = get_brand_profile(brand_org_id)
    event = get_event_profile(event_org_id)
    
    if not brand or not event:
        return None
    
    # Get match configuration (weights and rules)
    weight_set_id = brand.get('default_match_weight_set_id')
    rule_set_id = brand.get('default_match_rule_set_id')
    
    DEFAULT_WEIGHTS = {
        'category': 0.25,
        'geo': 0.20,
        'budget': 0.20,
        'audience': 0.20,
        'deliverables': 0.15
    }
    DEFAULT_RULES = {
        'enforce_must_have_deliverables': False,
        'enforce_city_filter': False,
        'enforce_date_window': False,
        'enforce_budget_overlap': False,
        'min_budget_overlap_ratio': 1.0,
        'allowed_date_slack_days': 0,
        'min_audience_overlap_score': 1.0,
        'budget_near_boundary_ratio': 0.1,
        'footfall_partial_match_ratio': 0.8
    }
    # Load weights (or use defaults when missing/inactive in configdb)
    weights = get_match_weight_set(weight_set_id) if weight_set_id else None
    if not weights:
        weights = DEFAULT_WEIGHTS
    # Load rules (or use defaults when missing/inactive in configdb)
    rules = get_match_rule_set(rule_set_id) if rule_set_id else None
    if not rules:
        rules = DEFAULT_RULES
    
    # Score geography (HARD FILTER)
    geo_score = score_geography(
        event.get('city_id'),
        brand,
        weights['geo']
    )

    if rules['enforce_city_filter']:
        if not geo_score.get('passed_hard_filter', False):
            # Geography is a hard filter - reject immediately
            return None

    
    # Score budget
    budget_score = score_budget(
        event.get('package_min') or 0,
        event.get('package_max') or 0,
        brand.get('spend_per_event_min') or 0,
        brand.get('spend_per_event_max') or 0,
        weights['budget'],
        rules['budget_near_boundary_ratio']
    )
    
    # Score categories
    category_score = score_categories(
        event.get('categories', []),
        brand.get('preferred_categories', []),
        brand.get('avoided_categories', []),
        weights['category']
    )
    
    # If event has avoided category, reject (hard filter)
    if category_score['match_factor'] == 0.0 and 'avoided' in category_score['explanation']:
        return None
    
    # Score audience overlap
    audience_score = score_audience_overlap(
        event.get('age_distribution', []),
        event.get('audience_types', []),
        event.get('interest_tags', []),
        brand.get('target_age_buckets', []),
        brand.get('target_audience_types', []),
        brand.get('target_interest_tags', []),
        weights['audience']
    )
    
    # Score deliverables (HARD FILTER for must_have)
    deliverables_score = score_deliverables(
        event.get('deliverables_offered', []),
        brand.get('wanted_deliverables', []),
        brand.get('must_have_deliverables', []),
        weights['deliverables']
    )
    
    if rules['enforce_must_have_deliverables']:
        if not deliverables_score.get('passed_hard_filter', True):
            # Must-have deliverables not met
            return None
    
    # Build breakdown
    breakdown = {
        "geography": geo_score,
        "budget": budget_score,
        "categories": category_score,
        "audience": audience_score,
        "deliverables": deliverables_score
    }
    
    # Calculate final score (dynamically from breakdown - no hardcoding)
    total_score = sum(score['contribution'] for score in breakdown.values())
    max_score = sum(score['weight'] for score in breakdown.values())
    match_percentage = (total_score / max_score * 100) if max_score > 0 else 0.0
    
    # Build explanation (only include positive matches)
    explanation_parts = []
    for name, score in breakdown.items():
        if score['match_factor'] > 0:
            explanation_parts.append(f"{name.title()}: {score['explanation']}")
    
    explanation = "\n".join(explanation_parts) if explanation_parts else "Minimal match"
    
    return {
        "event_org_id": event['event_org_id'],
        "event_name": event['event_name'],
        "total_score": round(total_score, 2),
        "max_score": round(max_score, 2),
        "match_percentage": round(match_percentage, 2),
        "breakdown": breakdown,
        "explanation": explanation
    }


def get_matches_for_brand(brand_org_id: int) -> Dict:
    """
    Get all matched events for a brand.
    
    Returns:
        {
            'brand_org_id': int,
            'brand_name': str,
            'matches': List[...]  # Sorted by match_percentage descending
        }
    
    WHY: Main API for brand-side matching (show brand their matches).
    """
    brand = get_brand_profile(brand_org_id)
    if not brand:
        return {
            "brand_org_id": brand_org_id,
            "brand_name": "Unknown",
            "matches": []
        }
    
    event_org_ids = get_all_event_orgs()
    matches = []
    
    for event_org_id in event_org_ids:
        match_result = evaluate_brand_for_events(brand_org_id, event_org_id)
        if match_result:
            matches.append(match_result)
    
    # Sort by match_percentage descending
    matches.sort(key=lambda x: x['match_percentage'], reverse=True)
    
    return {
        "brand_org_id": brand['brand_org_id'],
        "brand_name": brand['brand_name'],
        "matches": matches
    }


def get_matches_for_event(event_org_id: int) -> Dict:
    """
    Get all matched brands for an event.
    
    Returns:
        {
            'event_org_id': int,
            'event_name': str,
            'matches': List[...]  # Sorted by match_percentage descending
        }
    
    WHY: Reverse matching for event-side view (show event their matches).
    """
    event = get_event_profile(event_org_id)
    if not event:
        return {
            "event_org_id": event_org_id,
            "event_name": "Unknown",
            "matches": []
        }
    
    from .database import get_all_brand_orgs
    brand_org_ids = get_all_brand_orgs()
    matches = []
    
    for brand_org_id in brand_org_ids:
        match_result = evaluate_event_for_brands(brand_org_id, event_org_id)
        if match_result:
            # Add brand info to result (guard in case brand was removed between evaluate and fetch)
            brand = get_brand_profile(brand_org_id)
            if not brand:
                continue
            # Build match item for event-side view: brand + scores only (no repeated event_*)
            matches.append({
                'brand_org_id': brand_org_id,
                'brand_name': brand['brand_name'],
                'total_score': match_result['total_score'],
                'max_score': match_result['max_score'],
                'match_percentage': match_result['match_percentage'],
                'breakdown': match_result['breakdown'],
                'explanation': match_result['explanation'],
            })
    
    # Sort by match_percentage descending
    matches.sort(key=lambda x: x['match_percentage'], reverse=True)
    
    return {
        "event_org_id": event['event_org_id'],
        "event_name": event['event_name'],
        "matches": matches
    }


