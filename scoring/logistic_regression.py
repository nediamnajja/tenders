"""
scoring_engine.py
==================
KPMG Tender Scoring Engine — Step 2

What this script does:
1. Loads the latest weights from weights_history table
2. Reads all unscored enriched_tenders (p_go IS NULL, deadline >= 2 days)
3. For each tender:
   - Encodes features (country tier, sector, procurement, agency, budget, deadline)
   - Computes Z and P(GO) using logistic regression formula
   - Writes p_go + score_breakdown + model_version to enriched_tenders
   - If p_go >= 0.70 → generates justification and writes to tender_scores
4. Prints a summary of results

Run:
    python scoring_engine.py
"""

import ast
import json
import logging
import math
from datetime import datetime, timezone
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from db import get_session, get_unscored_enriched_tenders, save_score_to_enriched, save_tender_score
from models import WeightsHistory

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
logger = logging.getLogger(__name__)

# =============================================================================
# 1. CONSTANTS — Feature name → DB column name mapping
# Maps the feature key used in scoring to the column name in weights_history
# =============================================================================

FEATURE_TO_COLUMN = {
    # Country tiers
    "tier_1": "tier_1",
    "tier_2": "tier_2",
    "tier_3": "tier_3",
    "tier_4": "tier_4",
    "tier_5": "tier_5",
    "tier_6": "tier_6",
    # Sectors
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
    # Procurement
    "proc_CONSULTING":     "proc_consulting",
    "proc_NON-CONSULTING": "proc_non_consulting",
    "proc_WORKS":          "proc_works",
    "proc_GOODS":          "proc_goods",
    "proc_Others":         "proc_others",
    # Funding agency
    "agency_World Bank":                                     "agency_world_bank",
    "agency_African Development Bank (AfDB)":                "agency_afdb",
    "agency_United Nations Development Programme (UNDP)":    "agency_undp",
    "agency_FAO":          "agency_fao",
    "agency_ILO":          "agency_ilo",
    "agency_UNICEF":       "agency_unicef",
    "agency_IOM":          "agency_iom",
    "agency_UNOPS":        "agency_unops",
    "agency_UNIDO":        "agency_unido",
    "agency_Other_UN_Agency": "agency_other_un",
    # Budget
    "budget_large":  "budget_large",
    "budget_medium": "budget_medium",
    "budget_small":  "budget_small",
    # Deadline
    "deadline_2_20":    "deadline_2_20",
    "deadline_20_40":   "deadline_20_40",
    "deadline_over_40": "deadline_over_40",
}

# =============================================================================
# 2. COUNTRY TIER MAPPING
# =============================================================================

COUNTRY_TIERS = {
    # TIER 1 — Maghreb / North Africa
    "Tunisia": 1, "Tunisie": 1,
    "Morocco": 1, "Maroc": 1,
    "Algeria": 1, "Algérie": 1,
    "Libya": 1, "Egypt": 1,
    # TIER 2 — Sub-Saharan / West & Central Africa
    "Senegal": 2, "Sénégal": 2,
    "Côte D'Ivoire": 2, "Côte d'Ivoire": 2,
    "Cameroon": 2, "Cameroun": 2,
    "Ghana": 2, "Nigeria": 2, "Kenya": 2,
    "South Africa": 2, "Angola": 2, "Mozambique": 2,
    "Ethiopia": 2, "Tanzania": 2,
    "United Republic of Tanzania": 2,
    "Uganda": 2, "Rwanda": 2, "Zambia": 2,
    "Zimbabwe": 2, "Gabon": 2,
    "Benin": 2, "Bénin": 2, "Togo": 2,
    "Burkina Faso": 2, "Mali": 2,
    "Guinea": 2, "Guinée": 2,
    "Congo": 2, "Democratic Republic Of The Congo": 2,
    "Mauritius": 2, "Namibia": 2, "Madagascar": 2,
    "Liberia": 2, "Sierra Leone": 2,
    "Western And Central Africa": 2,
    "Eastern And Southern Africa": 2,
    "Central Africa": 2,
    # TIER 3 — Other Africa / Horn / Fragile
    "Niger": 3, "Chad": 3, "Tchad": 3,
    "Sudan": 3, "South Sudan": 3,
    "Somalia": 3, "Somaliland": 3,
    "Horn Of Africa": 3, "Djibouti": 3,
    "Burundi": 3, "Central African Republic": 3,
    "Equatorial Guinea": 3, "Comoros": 3, "Comores": 3,
    "Cabo Verde": 3,
    "Sao Tome And Principe": 3, "São Tome And Príncipe": 3,
    "Mauritania": 3, "Mauritanie": 3,
    "Gambia": 3, "Guinea Bissau": 3, "Guinée Bissau": 3,
    "Lesotho": 3, "Swaziland": 3, "Seychelles": 3,
    "Malawi": 3, "Malawai": 3,
    # TIER 4 — Europe
    "France": 4, "Germany": 4, "Italy": 4,
    "Denmark": 4, "Cyprus": 4, "Albania": 4,
    "Kosovo": 4, "Serbia": 4, "Montenegro": 4,
    "Bosnia And Herzegovina": 4,
    "Republic Of North Macedonia": 4,
    "Romania": 4, "Republic of Moldova": 4,
    "Ukraine": 4, "Georgia": 4,
    "Turkey": 4, "Turkiye": 4, "Armenia": 4,
    # TIER 5 — The Americas
    "United States Of America": 5,
    "Brazil": 5, "Mexico": 5, "Argentina": 5,
    "Colombia": 5, "Chile": 5, "Peru": 5,
    "Ecuador": 5, "Guatemala": 5, "Honduras": 5,
    "El Salvador": 5, "Costa Rica": 5,
    "Panama": 5, "Haiti": 5, "Belize": 5,
    "Guyana": 5, "Suriname": 5, "Caribbean": 5,
    "Dominica": 5, "St. Lucia": 5, "St Maarten": 5,
    "Plurinational State of Bolivia": 5,
    # TIER 6 — Asia & Pacific
    "India": 6, "Pakistan": 6, "Bangladesh": 6,
    "China": 6, "Indonesia": 6, "Philippines": 6,
    "Viet Nam": 6, "Myanmar": 6, "Cambodia": 6,
    "Laos": 6, "Lao People'S Democratic Republic": 6,
    "Sri Lanka": 6, "Nepal": 6, "Maldives": 6,
    "Mongolia": 6, "Timor Leste": 6, "Afghanistan": 6,
    "Kazakhstan": 6, "Kyrgyz Republic": 6, "Kyrgyzstan": 6,
    "Tajikistan": 6, "Uzbekistan": 6, "Central Asia": 6,
    "Iraq": 6, "Jordan": 6, "Lebanon": 6,
    "Syrian Arab Republic": 6, "Yemen": 6,
    "Palestinian Territory": 6,
    "Papua New Guinea": 6, "Fiji": 6,
    "Tuvalu": 6, "Kiribati": 6,
    "Marshall Islands": 6, "Pacific 1": 6,
}

MAJOR_AGENCIES = [
    "World Bank",
    "African Development Bank (AfDB)",
    "United Nations Development Programme (UNDP)",
    "FAO", "ILO", "UNICEF", "IOM", "UNOPS", "UNIDO",
]

KNOWN_SECTORS = [
    "Environment & Climate",
    "Social Protection & Poverty Reduction",
    "Water, Sanitation & Waste",
    "Digital Transformation",
    "Others",
    "Agriculture & Food Security",
    "Risk & Compliance",
    "Health & Life Sciences",
    "Construction & Infrastructure",
    "Energy & Utilities",
    "Education & Training",
    "Transport & Logistics",
    "Enterprise IT & Systems Implementation",
    "Business Strategy & Performance",
    "Government Reform & Public Administration",
    "Financial Services",
    "Marketing & Customer Experience",
    "Mining & Natural Resources",
    "Data, AI & Analytics",
    "Employment & Skills Development",
    "Telecommunications",
    "Organizational Reform & HR Management",
    "Cybersecurity & Data Security",
    "Justice & Rule of Law",
]

KNOWN_PROCUREMENT = ["CONSULTING", "NON-CONSULTING", "WORKS", "GOODS", "Others"]

# =============================================================================
# 3. LOAD WEIGHTS FROM DB
# =============================================================================

def load_latest_weights(session) -> tuple[dict, float, int]:
    """
    Load the most recent weights from weights_history.
    Returns: (weights_dict, baseline, version)
    """
    row = (
        session.query(WeightsHistory)
        .order_by(WeightsHistory.version.desc())
        .first()
    )
    if not row:
        raise RuntimeError(
            "No weights found in weights_history. "
            "Run migration_weights_history.sql first."
        )

    # Build weights dict — feature_key → alpha value
    # Use FEATURE_TO_COLUMN mapping in reverse to get feature keys
    col_to_feature = {v: k for k, v in FEATURE_TO_COLUMN.items()}

    weights = {}
    for col_name, feature_name in col_to_feature.items():
        val = getattr(row, col_name, None)
        if val is not None:
            weights[feature_name] = val
   
    logger.info(
        f"Loaded weights version {row.version} "
  
    )
    return weights, row.baseline, row.version

# =============================================================================
# 4. FEATURE ENCODING
# =============================================================================

def parse_sectors(s: str) -> list:
    try:
        parsed = ast.literal_eval(s)
        if isinstance(parsed, list):
            return [sec.strip() for sec in parsed]
    except:
        pass
    return []

def normalize_agency(agency: str) -> str:
    if not agency:
        return "Other_UN_Agency"
    agency = str(agency).strip()
    return agency if agency in MAJOR_AGENCIES else "Other_UN_Agency"

def encode_budget(val) -> str | None:
    if val is None:
        return None
    v = float(val)
    if v > 500_000:    return "budget_large"
    elif v >= 100_000: return "budget_medium"
    else:              return "budget_small"

def encode_deadline(days) -> str | None:
    if days is None or days < 2:
        return None
    d = int(days)
    if d <= 20:   return "deadline_2_20"
    elif d <= 40: return "deadline_20_40"
    else:         return "deadline_over_40"

def build_feature_vector(tender) -> dict:
    """
    Build {feature_name: 0 or 1} for a single EnrichedTender row.
    """
    features = {}

    # Country tier
    country = str(tender.country_name_normalized).strip() if tender.country_name_normalized else ""
    tier    = COUNTRY_TIERS.get(country, 0)
    for t in range(1, 7):
        features[f"tier_{t}"] = 1 if tier == t else 0

    # Sectors (multi-label)
    sectors = parse_sectors(tender.sector) if tender.sector else []
    for s in KNOWN_SECTORS:
        features[f"sector_{s}"] = 1 if s in sectors else 0

    # Procurement
    proc = str(tender.procurement_group).strip().upper() if tender.procurement_group else ""
    for p in KNOWN_PROCUREMENT:
        features[f"proc_{p}"] = 1 if proc == p.upper() else 0

    # Funding agency
    agency = normalize_agency(tender.funding_agency)
    features[f"agency_{agency}"] = 1
    for a in MAJOR_AGENCIES:
        if f"agency_{a}" not in features:
            features[f"agency_{a}"] = 0
    features["agency_Other_UN_Agency"] = 1 if agency == "Other_UN_Agency" else 0

    # Budget (unknown = all 0)
    bucket = encode_budget(tender.budget)
    features["budget_large"]  = 1 if bucket == "budget_large"  else 0
    features["budget_medium"] = 1 if bucket == "budget_medium" else 0
    features["budget_small"]  = 1 if bucket == "budget_small"  else 0

    # Deadline
    d_bucket = encode_deadline(tender.days_to_deadline)
    features["deadline_2_20"]    = 1 if d_bucket == "deadline_2_20"    else 0
    features["deadline_20_40"]   = 1 if d_bucket == "deadline_20_40"   else 0
    features["deadline_over_40"] = 1 if d_bucket == "deadline_over_40" else 0

    return features

# =============================================================================
# 5. SCORING FORMULA
# =============================================================================

def compute_score(features: dict, weights: dict, baseline: float) -> tuple[float, dict]:
    """
    Compute Z and P(GO) using logistic regression formula.
    Returns: (p_go, contributions_dict)
    contributions_dict = {feature: contribution} for active features only
    """
    Z = baseline
    contributions = {}

    for feature, x in features.items():
        if x == 1 and feature in weights:
            alpha        = weights[feature]
            contribution = alpha * x
            Z           += contribution
            contributions[feature] = round(contribution, 4)

    # Sigmoid
    p_go = 1 / (1 + math.exp(-Z))

    return round(p_go, 4), contributions

# =============================================================================
# 6. JUSTIFICATION GENERATOR (Option C)
# Only called for GO tenders (p_go >= 0.70)
# =============================================================================

# Human-readable labels for feature names
FEATURE_LABELS = {
    "tier_1": "Maghreb country",
    "tier_2": "Sub-Saharan Africa country",
    "tier_3": "Other Africa country",
    "tier_4": "European country",
    "tier_5": "Americas country",
    "tier_6": "Asian / Pacific country",
    "sector_Risk & Compliance":                         "Risk & Compliance sector",
    "sector_Digital Transformation":                    "Digital Transformation sector",
    "sector_Enterprise IT & Systems Implementation":    "Enterprise IT sector",
    "sector_Business Strategy & Performance":           "Business Strategy sector",
    "sector_Government Reform & Public Administration": "Government Reform sector",
    "sector_Financial Services":                        "Financial Services sector",
    "sector_Employment & Skills Development":           "Employment & Skills sector",
    "sector_Marketing & Customer Experience":           "Marketing & CX sector",
    "sector_Data, AI & Analytics":                      "Data & AI sector",
    "sector_Organizational Reform & HR Management":     "Org Reform & HR sector",
    "sector_Health & Life Sciences":                    "Health & Life Sciences sector",
    "sector_Environment & Climate":                     "Environment & Climate sector",
    "sector_Social Protection & Poverty Reduction":     "Social Protection sector",
    "sector_Water, Sanitation & Waste":                 "Water & Sanitation sector",
    "sector_Agriculture & Food Security":               "Agriculture sector",
    "sector_Construction & Infrastructure":             "Construction sector",
    "sector_Energy & Utilities":                        "Energy & Utilities sector",
    "sector_Education & Training":                      "Education sector",
    "sector_Transport & Logistics":                     "Transport & Logistics sector",
    "sector_Mining & Natural Resources":                "Mining & Resources sector",
    "sector_Telecommunications":                        "Telecommunications sector",
    "sector_Cybersecurity & Data Security":             "Cybersecurity sector",
    "sector_Justice & Rule of Law":                     "Justice & Rule of Law sector",
    "sector_Others":                                    "Other sector",
    "proc_CONSULTING":     "Consulting (core KPMG)",
    "proc_NON-CONSULTING": "Non-Consulting",
    "proc_WORKS":          "Works contract",
    "proc_GOODS":          "Goods contract",
    "proc_Others":         "Other procurement",
    "agency_World Bank":                                     "World Bank",
    "agency_African Development Bank (AfDB)":                "African Development Bank",
    "agency_United Nations Development Programme (UNDP)":    "UNDP",
    "agency_FAO":    "FAO",
    "agency_ILO":    "ILO",
    "agency_UNICEF": "UNICEF",
    "agency_IOM":    "IOM",
    "agency_UNOPS":  "UNOPS",
    "agency_UNIDO":  "UNIDO",
    "agency_Other_UN_Agency": "Other UN Agency",
    "budget_large":  "Large budget (>500k€)",
    "budget_medium": "Medium budget (100k-500k€)",
    "budget_small":  "Small budget (<100k€)",
    "deadline_2_20":    "Optimal deadline (2-20 days)",
    "deadline_20_40":   "Acceptable deadline (20-40 days)",
    "deadline_over_40": "Low urgency deadline (>40 days)",
}

def generate_justification(
    contributions: dict,
    p_go: float,
    recommendation: str,
) -> str:
    """
    Generate Option C justification text.
    Strong signals (> +0.5), Weak signals (-0.5 to +0.5 nonzero), Negatives (< -0.5)
    """
    strong   = {f: v for f, v in contributions.items() if v >= 0.5}
    moderate = {f: v for f, v in contributions.items() if 0.0 < v < 0.5}
    negative = {f: v for f, v in contributions.items() if v < -0.5}

    lines = []
    lines.append(f"{'='*52}")
    lines.append(f"  {recommendation}  —  P(GO) = {p_go:.1%}")
    lines.append(f"{'='*52}")

    if strong:
        lines.append("\nSTRONG SIGNALS:")
        for feature, val in sorted(strong.items(), key=lambda x: -x[1]):
            label = FEATURE_LABELS.get(feature, feature)
            lines.append(f"  ✅  {label:<42} {val:+.2f}")

    if moderate:
        lines.append("\nMODERATE SIGNALS:")
        for feature, val in sorted(moderate.items(), key=lambda x: -x[1]):
            label = FEATURE_LABELS.get(feature, feature)
            lines.append(f"  ➕  {label:<42} {val:+.2f}")

    if negative:
        lines.append("\nWEAK SIGNALS:")
        for feature, val in sorted(negative.items(), key=lambda x: x[1]):
            label = FEATURE_LABELS.get(feature, feature)
            lines.append(f"  ⚠️   {label:<42} {val:+.2f}")

    # Auto WHY sentence
    top_features = [
        FEATURE_LABELS.get(f, f)
        for f, v in sorted(contributions.items(), key=lambda x: -x[1])[:3]
        if v > 0
    ]
    if top_features:
        lines.append(f"\nWHY: This tender scores well primarily due to: {', '.join(top_features)}.")

    lines.append(f"{'='*52}")
    return "\n".join(lines)

# =============================================================================
# 7. RECOMMENDATION LABEL
# =============================================================================

def get_recommendation(p_go: float) -> str | None:
    if p_go >= 0.80:  return "STRONG GO"
    if p_go >= 0.70:  return "GO"
    return None  # Below threshold — not stored in tender_scores

# =============================================================================
# 8. MAIN SCORING ENGINE
# =============================================================================

def run_scoring_engine():
    logger.info("=" * 55)
    logger.info("  KPMG SCORING ENGINE — Starting")
    logger.info("=" * 55)

    with get_session() as session:

        # ── Load latest weights from DB ──────────────────────
        weights, baseline, model_version = load_latest_weights(session)
        logger.info(f"Using model version: {model_version}")

        # ── Fetch unscored tenders ────────────────────────────
        tenders = get_unscored_enriched_tenders(session)
        logger.info(f"Unscored tenders to process: {len(tenders)}")

        if not tenders:
            logger.info("Nothing to score — all tenders already scored.")
            return

        # ── Score each tender ─────────────────────────────────
        scored       = 0
        go_count     = 0
        strong_go    = 0
        errors       = 0

        for tender in tenders:
            try:
                # Build feature vector
                features = build_feature_vector(tender)

                # Compute Z and P(GO)
                p_go, contributions = compute_score(features, weights, baseline)

                # Score breakdown as JSON string
                breakdown = json.dumps({
                    "contributions": contributions,
                    "Z": round(
                        baseline + sum(contributions.values()), 4
                    ),
                    "p_go": p_go,
                })

                # Write p_go + breakdown + version to enriched_tenders
                save_score_to_enriched(
                    session        = session,
                    enriched_tender_id = tender.id,
                    p_go           = p_go,
                    score_breakdown= breakdown,
                    model_version  = model_version,
                )
                scored += 1

                # If GO → generate justification and write to tender_scores
                recommendation = get_recommendation(p_go)
                if recommendation:
                    justification = generate_justification(
                        contributions, p_go, recommendation
                    )
                    save_tender_score(
                        session            = session,
                        enriched_tender_id = tender.id,
                        p_go               = p_go,
                        recommendation     = recommendation,
                        justification      = justification,
                    )
                    go_count += 1
                    if recommendation == "STRONG GO":
                        strong_go += 1

            except Exception as e:
                logger.error(f"Error scoring tender {tender.id}: {e}")
                errors += 1

        # ── Summary ───────────────────────────────────────────
        logger.info("=" * 55)
        logger.info(f"  SCORING COMPLETE")
        logger.info(f"  Total scored       : {scored}")
        logger.info(f"  STRONG GO (≥80%)   : {strong_go}")
        logger.info(f"  GO (70-79%)        : {go_count - strong_go}")
        logger.info(f"  Below threshold    : {scored - go_count}")
        logger.info(f"  Errors             : {errors}")
        logger.info(f"  Model version used : {model_version}")
        logger.info("=" * 55)


if __name__ == "__main__":
    run_scoring_engine()