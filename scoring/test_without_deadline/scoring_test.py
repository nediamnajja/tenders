"""
scoring_test.py
================
Standalone scoring test — ignores deadline constraints.
Scores ALL enriched_tenders regardless of deadline.
Does NOT write anything to the database.
Results are only printed to terminal and exported to CSV.

Use this to test the scoring algorithm without affecting production data.

Run:
    python scoring/scoring_test.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ast
import csv
import json
import math
from datetime import datetime

from db import get_session
from models import EnrichedTender, WeightsHistory


# =============================================================================
# CONSTANTS — same as scoring_engine.py
# =============================================================================

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
    "agency_FAO":          "agency_fao",
    "agency_ILO":          "agency_ilo",
    "agency_UNICEF":       "agency_unicef",
    "agency_IOM":          "agency_iom",
    "agency_UNOPS":        "agency_unops",
    "agency_UNIDO":        "agency_unido",
    "agency_Other_UN_Agency": "agency_other_un",
    "budget_large":  "budget_large",
    "budget_medium": "budget_medium",
    "budget_small":  "budget_small",
    "deadline_2_20":    "deadline_2_20",
    "deadline_20_40":   "deadline_20_40",
    "deadline_over_40": "deadline_over_40",
}

COUNTRY_TIERS = {
    "Tunisia": 1, "Tunisie": 1, "Morocco": 1, "Maroc": 1,
    "Algeria": 1, "Algérie": 1, "Libya": 1, "Egypt": 1,
    "Senegal": 2, "Sénégal": 2, "Côte D'Ivoire": 2, "Côte d'Ivoire": 2,
    "Cameroon": 2, "Cameroun": 2, "Ghana": 2, "Nigeria": 2,
    "Kenya": 2, "South Africa": 2, "Angola": 2, "Mozambique": 2,
    "Ethiopia": 2, "Tanzania": 2, "United Republic of Tanzania": 2,
    "Uganda": 2, "Rwanda": 2, "Zambia": 2, "Zimbabwe": 2,
    "Gabon": 2, "Benin": 2, "Bénin": 2, "Togo": 2,
    "Burkina Faso": 2, "Mali": 2, "Guinea": 2, "Guinée": 2,
    "Congo": 2, "Democratic Republic Of The Congo": 2,
    "Mauritius": 2, "Namibia": 2, "Madagascar": 2,
    "Liberia": 2, "Sierra Leone": 2,
    "Western And Central Africa": 2, "Eastern And Southern Africa": 2,
    "Central Africa": 2,
    "Niger": 3, "Chad": 3, "Tchad": 3, "Sudan": 3, "South Sudan": 3,
    "Somalia": 3, "Somaliland": 3, "Horn Of Africa": 3, "Djibouti": 3,
    "Burundi": 3, "Central African Republic": 3, "Equatorial Guinea": 3,
    "Comoros": 3, "Comores": 3, "Cabo Verde": 3,
    "Sao Tome And Principe": 3, "São Tome And Príncipe": 3,
    "Mauritania": 3, "Mauritanie": 3, "Gambia": 3,
    "Guinea Bissau": 3, "Guinée Bissau": 3,
    "Lesotho": 3, "Swaziland": 3, "Seychelles": 3,
    "Malawi": 3, "Malawai": 3,
    "France": 4, "Germany": 4, "Italy": 4, "Denmark": 4,
    "Cyprus": 4, "Albania": 4, "Kosovo": 4, "Serbia": 4,
    "Montenegro": 4, "Bosnia And Herzegovina": 4,
    "Republic Of North Macedonia": 4, "Romania": 4,
    "Republic of Moldova": 4, "Ukraine": 4, "Georgia": 4,
    "Turkey": 4, "Turkiye": 4, "Armenia": 4,
    "United States Of America": 5, "Brazil": 5, "Mexico": 5,
    "Argentina": 5, "Colombia": 5, "Chile": 5, "Peru": 5,
    "Ecuador": 5, "Guatemala": 5, "Honduras": 5, "El Salvador": 5,
    "Costa Rica": 5, "Panama": 5, "Haiti": 5, "Belize": 5,
    "Guyana": 5, "Suriname": 5, "Caribbean": 5, "Dominica": 5,
    "St. Lucia": 5, "St Maarten": 5,
    "Plurinational State of Bolivia": 5,
    "India": 6, "Pakistan": 6, "Bangladesh": 6, "China": 6,
    "Indonesia": 6, "Philippines": 6, "Viet Nam": 6, "Myanmar": 6,
    "Cambodia": 6, "Laos": 6, "Lao People'S Democratic Republic": 6,
    "Sri Lanka": 6, "Nepal": 6, "Maldives": 6, "Mongolia": 6,
    "Timor Leste": 6, "Afghanistan": 6, "Kazakhstan": 6,
    "Kyrgyz Republic": 6, "Kyrgyzstan": 6, "Tajikistan": 6,
    "Uzbekistan": 6, "Central Asia": 6, "Iraq": 6, "Jordan": 6,
    "Lebanon": 6, "Syrian Arab Republic": 6, "Yemen": 6,
    "Palestinian Territory": 6, "Papua New Guinea": 6,
    "Fiji": 6, "Tuvalu": 6, "Kiribati": 6,
    "Marshall Islands": 6, "Pacific 1": 6,
}

MAJOR_AGENCIES = [
    "World Bank", "African Development Bank (AfDB)",
    "United Nations Development Programme (UNDP)",
    "FAO", "ILO", "UNICEF", "IOM", "UNOPS", "UNIDO",
]

KNOWN_SECTORS = [
    "Environment & Climate", "Social Protection & Poverty Reduction",
    "Water, Sanitation & Waste", "Digital Transformation", "Others",
    "Agriculture & Food Security", "Risk & Compliance",
    "Health & Life Sciences", "Construction & Infrastructure",
    "Energy & Utilities", "Education & Training", "Transport & Logistics",
    "Enterprise IT & Systems Implementation",
    "Business Strategy & Performance",
    "Government Reform & Public Administration", "Financial Services",
    "Marketing & Customer Experience", "Mining & Natural Resources",
    "Data, AI & Analytics", "Employment & Skills Development",
    "Telecommunications", "Organizational Reform & HR Management",
    "Cybersecurity & Data Security", "Justice & Rule of Law",
]

KNOWN_PROCUREMENT = ["CONSULTING", "NON-CONSULTING", "WORKS", "GOODS", "Others"]


# =============================================================================
# HELPERS
# =============================================================================

def parse_sectors(s):
    try:
        parsed = ast.literal_eval(s)
        if isinstance(parsed, list):
            return [sec.strip() for sec in parsed]
    except:
        pass
    return []

def normalize_agency(agency):
    if not agency:
        return "Other_UN_Agency"
    agency = str(agency).strip()
    return agency if agency in MAJOR_AGENCIES else "Other_UN_Agency"

def encode_budget(val):
    if val is None:
        return None
    v = float(val)
    if v > 500_000:    return "budget_large"
    elif v >= 100_000: return "budget_medium"
    else:              return "budget_small"

def encode_deadline(days):
    """
    TEST MODE: encode deadline even if < 2 or negative.
    Expired deadlines get their own bucket for visibility.
    """
    if days is None:
        return None
    d = int(days)
    if d <= 0:    return "deadline_expired"
    elif d <= 1:  return "deadline_1day"
    elif d <= 20: return "deadline_2_20"
    elif d <= 40: return "deadline_20_40"
    else:         return "deadline_over_40"

def get_recommendation(p_go):
    if p_go is None:    return "NOT SCORED"
    if p_go >= 0.80:    return "STRONG GO 🟢"
    if p_go >= 0.70:    return "GO 🟡"
    return "NO GO 🔴"

def format_budget(budget, currency):
    if budget:
        return f"{budget:,.0f} {currency or ''}".strip()
    return "Unknown"

def deadline_label(days):
    if days is None:    return "Unknown"
    if days <= 0:       return f"EXPIRED ({days}d)"
    if days == 1:       return "1 day left ⚠️"
    return f"{days} days"


# =============================================================================
# LOAD WEIGHTS
# =============================================================================

def load_latest_weights(session):
    row = (
        session.query(WeightsHistory)
        .order_by(WeightsHistory.version.desc())
        .first()
    )
    if not row:
        raise RuntimeError("No weights found. Run migration_weights_history.sql first.")

    col_to_feature = {v: k for k, v in FEATURE_TO_COLUMN.items()}
    weights = {}
    for col_name, feature_name in col_to_feature.items():
        val = getattr(row, col_name, None)
        if val is not None:
            weights[feature_name] = val

    print(f"\n  Loaded weights version {row.version} (trained {row.trained_at})")
    return weights, row.baseline, row.version


# =============================================================================
# FEATURE VECTOR
# =============================================================================

def build_feature_vector(tender):
    features = {}

    country = str(tender.country_name_normalized).strip() if tender.country_name_normalized else ""
    tier    = COUNTRY_TIERS.get(country, 0)
    for t in range(1, 7):
        features[f"tier_{t}"] = 1 if tier == t else 0

    sectors = parse_sectors(tender.sector) if tender.sector else []
    for s in KNOWN_SECTORS:
        features[f"sector_{s}"] = 1 if s in sectors else 0

    proc = str(tender.procurement_group).strip().upper() if tender.procurement_group else ""
    for p in KNOWN_PROCUREMENT:
        features[f"proc_{p}"] = 1 if proc == p.upper() else 0

    agency = normalize_agency(tender.funding_agency)
    for a in MAJOR_AGENCIES:
        features[f"agency_{a}"] = 1 if agency == a else 0
    features["agency_Other_UN_Agency"] = 1 if agency == "Other_UN_Agency" else 0

    bucket = encode_budget(tender.budget)
    features["budget_large"]  = 1 if bucket == "budget_large"  else 0
    features["budget_medium"] = 1 if bucket == "budget_medium" else 0
    features["budget_small"]  = 1 if bucket == "budget_small"  else 0

    # TEST MODE: include expired deadlines in deadline bucket
    d_bucket = encode_deadline(tender.days_to_deadline)
    features["deadline_2_20"]    = 1 if d_bucket == "deadline_2_20"    else 0
    features["deadline_20_40"]   = 1 if d_bucket == "deadline_20_40"   else 0
    features["deadline_over_40"] = 1 if d_bucket == "deadline_over_40" else 0
    # expired/1day get no deadline bucket — deadline doesn't contribute to Z

    return features


# =============================================================================
# SCORE
# =============================================================================

def compute_score(features, weights, baseline):
    Z = baseline
    contributions = {}
    for feature, x in features.items():
        if x == 1 and feature in weights:
            alpha        = weights[feature]
            contribution = round(alpha * x, 4)
            Z           += contribution
            contributions[feature] = contribution
    p_go = 1 / (1 + math.exp(-Z))
    return round(p_go, 4), contributions


# =============================================================================
# MAIN TEST RUNNER
# =============================================================================

def run_scoring_test():
    print("=" * 65)
    print("  KPMG SCORING TEST — All tenders, no deadline filter")
    print("  ⚠️  READ ONLY — Nothing written to database")
    print("=" * 65)

    timestamp   = datetime.now().strftime("%Y%m%d_%H%M")
    output_file = f"scoring_test2_{timestamp}.csv"

    with get_session() as session:

        # Load weights
        weights, baseline, model_version = load_latest_weights(session)

        # Fetch ALL enriched tenders — no deadline filter
        tenders = (
            session.query(EnrichedTender)
            .order_by(EnrichedTender.id)
            .all()
        )

        print(f"  Total tenders to score : {len(tenders)}")
        print(f"  Model version          : {model_version}")

        if not tenders:
            print("  No tenders found in enriched_tenders.")
            return

        # Score in memory
        results = []
        for tender in tenders:
            features = build_feature_vector(tender)
            p_go, contributions = compute_score(features, weights, baseline)

            sectors  = parse_sectors(tender.sector) if tender.sector else []
            deadline = tender.days_to_deadline

            results.append({
                "id":             tender.id,
                "p_go":           p_go,
                "recommendation": get_recommendation(p_go),
                "title":          tender.title_clean or "N/A",
                "country":        tender.country_name_normalized or "N/A",
                "tier":           COUNTRY_TIERS.get(
                                    str(tender.country_name_normalized or "").strip(), 0
                                  ),
                "sector":         ", ".join(sectors[:2]) or "N/A",
                "procurement":    tender.procurement_group or "N/A",
                "agency":         tender.funding_agency or "N/A",
                "budget":         format_budget(tender.budget, tender.currency),
                "deadline":       deadline_label(deadline),
                "days":           deadline,
                "deadline_status": (
                    "EXPIRED" if deadline is not None and deadline <= 0
                    else "URGENT" if deadline is not None and deadline == 1
                    else "VALID"  if deadline is not None and deadline >= 2
                    else "UNKNOWN"
                ),
                "contributions":  contributions,
                "model_version":  model_version,
                "source_portal":  tender.source_portal or "N/A",
                "source_url":     tender.source_url or "N/A",
            })

        # Sort by p_go descending
        results.sort(key=lambda x: x["p_go"], reverse=True)

        # ── Summary ───────────────────────────────────────────
        strong_go = sum(1 for r in results if r["p_go"] >= 0.80)
        go        = sum(1 for r in results if 0.70 <= r["p_go"] < 0.80)
        no_go     = sum(1 for r in results if r["p_go"] < 0.70)
        expired   = sum(1 for r in results if r["deadline_status"] == "EXPIRED")
        valid     = sum(1 for r in results if r["deadline_status"] == "VALID")

        print(f"\n  {'─' * 63}")
        print(f"  RESULTS SUMMARY")
        print(f"  {'─' * 63}")
        print(f"  STRONG GO  (≥80%)  : {strong_go}")
        print(f"  GO         (70-79%): {go}")
        print(f"  NO GO      (<70%)  : {no_go}")
        print(f"  {'─' * 63}")
        print(f"  Valid deadlines    : {valid}")
        print(f"  Expired deadlines  : {expired}")
        print(f"  {'─' * 63}")

        # ── Top 15 preview ────────────────────────────────────
        print(f"\n  TOP 15 PREVIEW:")
        print(f"  {'─' * 63}")
        print(f"  {'#':<4} {'REC':<11} {'P(GO)':<7} {'T':<2} {'STATUS':<8} {'COUNTRY':<18} {'TITLE':<22}")
        print(f"  {'─' * 63}")

        for i, r in enumerate(results[:15], 1):
            rec    = r["recommendation"].replace("🟢","").replace("🟡","").replace("🔴","").strip()
            country= r["country"][:16]
            title  = r["title"][:20]
            status = r["deadline_status"]
            tier   = r["tier"] if r["tier"] > 0 else "?"
            print(f"  {i:<4} {rec:<11} {r['p_go']:.1%}  {tier:<2} {status:<8} {country:<18} {title}")

        print(f"  {'─' * 63}")

        # ── Country distribution of GOs ───────────────────────
        go_results = [r for r in results if r["p_go"] >= 0.70]
        if go_results:
            from collections import Counter
            country_counts = Counter(r["country"] for r in go_results)
            print(f"\n  GO TENDERS BY COUNTRY (top 10):")
            print(f"  {'─' * 40}")
            for country, count in country_counts.most_common(10):
                tier = COUNTRY_TIERS.get(country.strip(), 0)
                print(f"  Tier {tier}  {country:<30} {count} tender(s)")
            print(f"  {'─' * 40}")

        # ── Export to CSV ─────────────────────────────────────
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "Rank", "Recommendation", "P(GO) %",
                "Deadline Status", "Title", "Country", "Tier",
                "Sector", "Procurement", "Funding Agency",
                "Budget", "Deadline", "Model Version",
                "Source Portal", "Source URL",
            ])
            for rank, r in enumerate(results, 1):
                writer.writerow([
                    rank,
                    r["recommendation"],
                    f"{r['p_go']:.1%}",
                    r["deadline_status"],
                    r["title"],
                    r["country"],
                    r["tier"],
                    r["sector"],
                    r["procurement"],
                    r["agency"],
                    r["budget"],
                    r["deadline"],
                    r["model_version"],
                    r["source_portal"],
                    r["source_url"],
                ])

        print(f"\n  ✅ Test results exported → {output_file}")
        print(f"  ⚠️  Nothing was written to the database.")
        print("=" * 65)


if __name__ == "__main__":
    run_scoring_test()