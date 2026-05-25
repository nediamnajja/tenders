"""
back/routers/sgd.py
====================
SGD weight update — called internally after every partner decision.

The SGD reads from partner_decisions:
  - decision    → y (1=GO, 0=NO GO)
  - j_values    → per-feature relevance dict
  - score_breakdown on enriched_tender → active features + original p_go

Formula: alpha = alpha + eta * (y - p) * j * x
"""

import json
import logging
from copy import deepcopy
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from back.database import get_db
from back.models.db_models import EnrichedTender, PlatformUser, TenderScore, WeightsHistory
from back.routers.auth import get_current_user

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/sgd", tags=["sgd"])

# =============================================================================
# CONSTANTS — must match scoring_engine.py exactly
# =============================================================================

LEARNING_RATE  = 0.1
J_NOT_RELEVANT = 0.1   # default j when feature not in j_values dict

FEATURE_TO_COLUMN = {
    "tier_1": "tier_1", "tier_2": "tier_2", "tier_3": "tier_3",
    "tier_4": "tier_4", "tier_5": "tier_5", "tier_6": "tier_6",
    "sector_Environment & Climate":                     "sector_environment_climate",
    "sector_Social Protection & Poverty Reduction":     "sector_social_protection_poverty_reduction",
    "sector_Water, Sanitation & Waste":                 "sector_water_sanitation_waste",
    "sector_Digital Transformation":                    "sector_digital_transformation",
    "sector_Others":                                    "sector_others",
    "sector_Agriculture & Food Security":               "sector_agriculture_food_security",
    "sector_Risk & Compliance":                         "sector_risk_compliance",
    "sector_Health & Life Sciences":                    "sector_health_life_sciences",
    "sector_Construction & Infrastructure":             "sector_construction_infrastructure",
    "sector_Energy & Utilities":                        "sector_energy_utilities",
    "sector_Education & Training":                      "sector_education_training",
    "sector_Transport & Logistics":                     "sector_transport_logistics",
    "sector_Enterprise IT & Systems Implementation":    "sector_enterprise_it_systems_implementation",
    "sector_Business Strategy & Performance":           "sector_business_strategy_performance",
    "sector_Government Reform & Public Administration": "sector_government_reform_public_admin",
    "sector_Financial Services":                        "sector_financial_services",
    "sector_Marketing & Customer Experience":           "sector_marketing_customer_experience",
    "sector_Mining & Natural Resources":                "sector_mining_natural_resources",
    "sector_Data, AI & Analytics":                      "sector_data_ai_analytics",
    "sector_Employment & Skills Development":           "sector_employment_skills_development",
    "sector_Telecommunications":                        "sector_telecommunications",
    "sector_Organizational Reform & HR Management":     "sector_organizational_reform_hr_management",
    "sector_Cybersecurity & Data Security":             "sector_cybersecurity_data_security",
    "sector_Justice & Rule of Law":                     "sector_justice_rule_of_law",
    "proc_CONSULTING":     "proc_consulting",
    "proc_NON-CONSULTING": "proc_non_consulting",
    "proc_WORKS":          "proc_works",
    "proc_GOODS":          "proc_goods",
    "proc_Others":         "proc_others",
    "agency_World Bank":                                     "agency_world_bank",
    "agency_African Development Bank (AfDB)":                "agency_afdb",
    "agency_United Nations Development Programme (UNDP)":    "agency_undp",
    "agency_FAO":             "agency_fao",
    "agency_ILO":             "agency_ilo",
    "agency_UNICEF":          "agency_unicef",
    "agency_IOM":             "agency_iom",
    "agency_UNOPS":           "agency_unops",
    "agency_UNIDO":           "agency_unido",
    "agency_Other_UN_Agency": "agency_other_un",
    "budget_large":  "budget_large",
    "budget_medium": "budget_medium",
    "budget_small":  "budget_small",
    "deadline_2_20":    "deadline_2_20",
    "deadline_20_40":   "deadline_20_40",
    "deadline_over_40": "deadline_over_40",
}

# =============================================================================
# LOAD LATEST WEIGHTS
# =============================================================================

def load_latest_weights(db: Session) -> tuple[dict, float, int]:
    row = (
        db.query(WeightsHistory)
        .order_by(WeightsHistory.version.desc())
        .first()
    )
    if not row:
        raise RuntimeError("No weights found in weights_history. Run migration first.")

    col_to_feature = {v: k for k, v in FEATURE_TO_COLUMN.items()}
    weights = {}
    for col_name, feature_name in col_to_feature.items():
        val = getattr(row, col_name, None)
        if val is not None:
            weights[feature_name] = float(val)

    return weights, float(row.baseline), int(row.version)


# =============================================================================
# SAVE UPDATED WEIGHTS
# =============================================================================

def save_updated_weights(
    db:              Session,
    weights:         dict,
    baseline:        float,
    old_version:     int,
    tender_score_id: int,
    partner_name:    str,
    notes:           str = "",
) -> int:
    new_version = old_version + 1
    row = WeightsHistory()
    row.version                      = new_version
    row.baseline                     = round(baseline, 6)
    row.trained_at                   = datetime.now(timezone.utc)
    row.triggered_by_tender_score_id = tender_score_id
    row.triggered_by_partner         = partner_name
    row.notes                        = notes or f"SGD update v{old_version}→v{new_version} by {partner_name}"

    for feature_name, alpha in weights.items():
        col_name = FEATURE_TO_COLUMN.get(feature_name)
        if col_name:
            setattr(row, col_name, round(alpha, 6))

    db.add(row)
    logger.info(f"New weights version {new_version} queued (tender_score={tender_score_id})")
    return new_version


# =============================================================================
# SGD CORE
# Formula: alpha = alpha + eta * (y - p) * j * x
# =============================================================================

def run_sgd_update(
    db:           Session,
    tender_score: TenderScore,
    partner_name: str,
    j_values:     dict,    # {feature_key: 1.0 or 0.1} — one per active feature
    y:            int,     # 1 = GO, 0 = NO GO
) -> int:
    """
    Run one SGD step from a partner decision.
    Reads active features from score_breakdown on enriched_tender.
    Saves new weights version to weights_history.
    Does NOT commit — caller commits.
    Returns new version number.
    """

    # 1. Load current weights
    weights, baseline, old_version = load_latest_weights(db)

    # 2. Get active features from score_breakdown
    et = (
        db.query(EnrichedTender)
        .filter(EnrichedTender.id == tender_score.enriched_tender_id)
        .first()
    )
    if not et or not et.score_breakdown:
        raise ValueError(
            f"No score_breakdown on enriched_tender {tender_score.enriched_tender_id}. "
            f"Cannot run SGD."
        )

    breakdown     = json.loads(et.score_breakdown)
    contributions = breakdown.get("contributions", {})
    # contributions keys are the active features (x=1) for this tender
    # e.g. {"tier_1": 0.40, "proc_CONSULTING": 0.50, "budget_large": 0.35, ...}

    p     = float(tender_score.p_go)   # what the model predicted at scoring time
    error = y - p

    updated_weights  = deepcopy(weights)
    updated_baseline = baseline

    # 3. Update each active feature
    for feature_key in contributions:
        if feature_key not in weights:
            continue
        # Use j from partner's answers, default to J_NOT_RELEVANT if not answered
        j     = float(j_values.get(feature_key, J_NOT_RELEVANT))
        delta = LEARNING_RATE * error * j * 1   # x=1 for all active features
        updated_weights[feature_key] = weights[feature_key] + delta

    # 4. Baseline always updates with full j=1.0
    updated_baseline = baseline + (LEARNING_RATE * error * 1.0)

    logger.info(
        f"SGD: y={y} p={p:.4f} error={error:+.4f} | "
        f"{len(contributions)} features updated | "
        f"baseline {baseline:.4f}→{updated_baseline:.4f}"
    )

    # 5. Save new weights row (caller commits)
    new_version = save_updated_weights(
        db              = db,
        weights         = updated_weights,
        baseline        = updated_baseline,
        old_version     = old_version,
        tender_score_id = tender_score.id,
        partner_name    = partner_name,
        notes           = f"{'GO' if y==1 else 'NO GO'} on tender {et.id} (p={p:.2f})",
    )

    return new_version


# =============================================================================
# ADMIN ENDPOINT — read-only status
# =============================================================================

@router.get("/status")
def get_sgd_status(
    db:           Session     = Depends(get_db),
    current_user: PlatformUser = Depends(get_current_user),
):
    """Returns current model version info. Read-only."""
    row = (
        db.query(WeightsHistory)
        .order_by(WeightsHistory.version.desc())
        .first()
    )
    if not row:
        raise HTTPException(status_code=404, detail="No weights found")

    return {
        "version":              row.version,
        "baseline":             row.baseline,
        "trained_at":           row.trained_at,
        "triggered_by_partner": row.triggered_by_partner,
        "notes":                row.notes,
    }