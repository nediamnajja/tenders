"""
sgd_updater.py
==============
KPMG Tender Scoring Engine — SGD Learning Module

SGD Update Formula:  alpha = alpha + eta x (y - p) x j x x

j values (from yes/no questions, not free text):
  - Feature explicitly marked relevant   -> j = 1.0  (full update)
  - Feature NOT marked relevant          -> j = 0.1  (small nudge — Option C)
  - "Other reason" answered YES          -> j = 0.1  (uniform small nudge on all)

Direction comes entirely from error (y - p):
  - GO decision   (y=1): error is positive -> weights go UP
  - NO GO decision(y=0): error is negative -> weights go DOWN

Run tests:
    python sgd_updater.py
"""

import sys
import os
import logging
from datetime import datetime, timezone
from copy import deepcopy

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

from db import get_session
from models import WeightsHistory

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
logger = logging.getLogger(__name__)

# =============================================================================
# CONSTANTS
# =============================================================================

LEARNING_RATE  = 0.1   # η — how fast the model learns
J_RELEVANT     = 1.0   # partner said YES — this factor drove the decision
J_NOT_RELEVANT = 0.1   # partner said NO  — small nudge anyway (Option C)

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

FEATURE_LABELS = {
    "tier_1": "Country: Maghreb / North Africa",
    "tier_2": "Country: Sub-Saharan Africa",
    "tier_3": "Country: Other Africa / Fragile",
    "tier_4": "Country: Europe",
    "tier_5": "Country: Americas",
    "tier_6": "Country: Asia & Pacific",
    "budget_large":   "Budget > 500k€",
    "budget_medium":  "Budget 100k–500k€",
    "budget_small":   "Budget < 100k€",
    "deadline_2_20":    "Deadline 2–20 days",
    "deadline_20_40":   "Deadline 20–40 days",
    "deadline_over_40": "Deadline > 40 days",
    "proc_CONSULTING":     "Procurement: Consulting",
    "proc_NON-CONSULTING": "Procurement: Non-Consulting",
    "proc_WORKS":          "Procurement: Works",
    "proc_GOODS":          "Procurement: Goods",
    "proc_Others":         "Procurement: Other",
    "sector_Energy & Utilities":                        "Sector: Energy & Utilities",
    "sector_Risk & Compliance":                         "Sector: Risk & Compliance",
    "sector_Digital Transformation":                    "Sector: Digital Transformation",
    "sector_Financial Services":                        "Sector: Financial Services",
    "sector_Data, AI & Analytics":                      "Sector: Data, AI & Analytics",
    "sector_Construction & Infrastructure":             "Sector: Construction & Infrastructure",
    "sector_Health & Life Sciences":                    "Sector: Health & Life Sciences",
    "sector_Education & Training":                      "Sector: Education & Training",
    "sector_Government Reform & Public Administration": "Sector: Government Reform",
    "sector_Agriculture & Food Security":               "Sector: Agriculture & Food Security",
    "sector_Environment & Climate":                     "Sector: Environment & Climate",
    "sector_Transport & Logistics":                     "Sector: Transport & Logistics",
    "sector_Water, Sanitation & Waste":                 "Sector: Water & Sanitation",
    "sector_Enterprise IT & Systems Implementation":    "Sector: Enterprise IT",
    "sector_Business Strategy & Performance":           "Sector: Business Strategy",
    "sector_Employment & Skills Development":           "Sector: Employment & Skills",
    "sector_Telecommunications":                        "Sector: Telecommunications",
    "sector_Organizational Reform & HR Management":     "Sector: Org Reform & HR",
    "sector_Cybersecurity & Data Security":             "Sector: Cybersecurity",
    "sector_Justice & Rule of Law":                     "Sector: Justice & Rule of Law",
    "sector_Mining & Natural Resources":                "Sector: Mining & Resources",
    "sector_Marketing & Customer Experience":           "Sector: Marketing & CX",
    "sector_Social Protection & Poverty Reduction":     "Sector: Social Protection",
    "sector_Others":                                    "Sector: Other",
    "agency_World Bank":                                     "Agency: World Bank",
    "agency_African Development Bank (AfDB)":                "Agency: AfDB",
    "agency_United Nations Development Programme (UNDP)":    "Agency: UNDP",
    "agency_FAO":             "Agency: FAO",
    "agency_ILO":             "Agency: ILO",
    "agency_UNICEF":          "Agency: UNICEF",
    "agency_IOM":             "Agency: IOM",
    "agency_UNOPS":           "Agency: UNOPS",
    "agency_UNIDO":           "Agency: UNIDO",
    "agency_Other_UN_Agency": "Agency: Other UN",
}

def label(f):
    return FEATURE_LABELS.get(f, f)

# =============================================================================
# FEATURE QUESTION LABELS
# Human-readable question shown in terminal per feature group
# =============================================================================

FEATURE_QUESTIONS = {
    # Country tiers — grouped, one question for whichever tier is active
    "tier_1": "the COUNTRY (Maghreb / North Africa)",
    "tier_2": "the COUNTRY (Sub-Saharan Africa)",
    "tier_3": "the COUNTRY (Other Africa / Fragile)",
    "tier_4": "the COUNTRY (Europe)",
    "tier_5": "the COUNTRY (Americas)",
    "tier_6": "the COUNTRY (Asia & Pacific)",
    # Budget
    "budget_large":  "the BUDGET (> 500k€  — large)",
    "budget_medium": "the BUDGET (100k–500k€ — medium)",
    "budget_small":  "the BUDGET (< 100k€  — small)",
    # Deadline
    "deadline_2_20":    "the DEADLINE (2–20 days — urgent)",
    "deadline_20_40":   "the DEADLINE (20–40 days — normal)",
    "deadline_over_40": "the DEADLINE (> 40 days — low urgency)",
    # Procurement
    "proc_CONSULTING":     "the PROCUREMENT TYPE (Consulting)",
    "proc_NON-CONSULTING": "the PROCUREMENT TYPE (Non-Consulting)",
    "proc_WORKS":          "the PROCUREMENT TYPE (Works)",
    "proc_GOODS":          "the PROCUREMENT TYPE (Goods)",
    "proc_Others":         "the PROCUREMENT TYPE (Other)",
    # Sectors
    "sector_Energy & Utilities":                        "the SECTOR (Energy & Utilities)",
    "sector_Risk & Compliance":                         "the SECTOR (Risk & Compliance)",
    "sector_Digital Transformation":                    "the SECTOR (Digital Transformation)",
    "sector_Financial Services":                        "the SECTOR (Financial Services)",
    "sector_Data, AI & Analytics":                      "the SECTOR (Data, AI & Analytics)",
    "sector_Construction & Infrastructure":             "the SECTOR (Construction & Infrastructure)",
    "sector_Health & Life Sciences":                    "the SECTOR (Health & Life Sciences)",
    "sector_Education & Training":                      "the SECTOR (Education & Training)",
    "sector_Government Reform & Public Administration": "the SECTOR (Government Reform)",
    "sector_Agriculture & Food Security":               "the SECTOR (Agriculture & Food Security)",
    "sector_Environment & Climate":                     "the SECTOR (Environment & Climate)",
    "sector_Transport & Logistics":                     "the SECTOR (Transport & Logistics)",
    "sector_Water, Sanitation & Waste":                 "the SECTOR (Water & Sanitation)",
    "sector_Enterprise IT & Systems Implementation":    "the SECTOR (Enterprise IT)",
    "sector_Business Strategy & Performance":           "the SECTOR (Business Strategy)",
    "sector_Employment & Skills Development":           "the SECTOR (Employment & Skills)",
    "sector_Telecommunications":                        "the SECTOR (Telecommunications)",
    "sector_Organizational Reform & HR Management":     "the SECTOR (Org Reform & HR)",
    "sector_Cybersecurity & Data Security":             "the SECTOR (Cybersecurity)",
    "sector_Justice & Rule of Law":                     "the SECTOR (Justice & Rule of Law)",
    "sector_Mining & Natural Resources":                "the SECTOR (Mining & Resources)",
    "sector_Marketing & Customer Experience":           "the SECTOR (Marketing & CX)",
    "sector_Social Protection & Poverty Reduction":     "the SECTOR (Social Protection)",
    "sector_Others":                                    "the SECTOR (Other)",
    # Agencies
    "agency_World Bank":                                     "the AGENCY (World Bank)",
    "agency_African Development Bank (AfDB)":                "the AGENCY (AfDB)",
    "agency_United Nations Development Programme (UNDP)":    "the AGENCY (UNDP)",
    "agency_FAO":             "the AGENCY (FAO)",
    "agency_ILO":             "the AGENCY (ILO)",
    "agency_UNICEF":          "the AGENCY (UNICEF)",
    "agency_IOM":             "the AGENCY (IOM)",
    "agency_UNOPS":           "the AGENCY (UNOPS)",
    "agency_UNIDO":           "the AGENCY (UNIDO)",
    "agency_Other_UN_Agency": "the AGENCY (Other UN)",
}

# =============================================================================
# YES/NO QUESTIONNAIRE — replaces free text parsing entirely
# =============================================================================

def ask_relevance_questions(active_features: dict) -> tuple[dict, str]:
    """
    Ask the partner YES/NO for each active feature.
    Returns:
      j_values dict  — {feature_key: j_value}
      other_reason   — free text if partner said YES to "other reason", else ""

    j rules:
      YES → j = 1.0  (this factor drove the decision — full update)
      NO  → j = 0.1  (not the reason — small nudge anyway, Option C)
    """
    print("\n  Was each factor relevant to your decision?")
    print("  (Y = yes, this drove my decision  |  N = not this factor)\n")

    j_values = {}

    for feature_key in active_features:
        question = FEATURE_QUESTIONS.get(feature_key, f"feature: {feature_key}")

        while True:
            answer = input(f"  Was {question} relevant? [Y/N] → ").strip().upper()
            if answer in ("Y", "YES"):
                j_values[feature_key] = J_RELEVANT      # 1.0
                break
            elif answer in ("N", "NO"):
                j_values[feature_key] = J_NOT_RELEVANT  # 0.1
                break
            else:
                print("    ⚠️  Please type Y or N")

    # Other reason question
    other_reason = ""
    print()
    while True:
        other = input("  Was there another reason NOT listed above? [Y/N] → ").strip().upper()
        if other in ("Y", "YES"):
            other_reason = input("  Briefly describe (stored for records): → ").strip()
            # When other reason exists, all features that were N get a small nudge
            # (already handled by J_NOT_RELEVANT = 0.1, so no change needed)
            print(f"  ✏️  Noted: '{other_reason}'")
            break
        elif other in ("N", "NO"):
            break
        else:
            print("    ⚠️  Please type Y or N")

    return j_values, other_reason

# =============================================================================
# LOAD / SAVE WEIGHTS
# =============================================================================

def load_latest_weights(session) -> tuple[dict, float, int]:
    row = (
        session.query(WeightsHistory)
        .order_by(WeightsHistory.version.desc())
        .first()
    )
    if not row:
        raise RuntimeError("No weights in weights_history. Run migration first.")

    col_to_feature = {v: k for k, v in FEATURE_TO_COLUMN.items()}
    weights = {}
    for col_name, feature_name in col_to_feature.items():
        val = getattr(row, col_name, None)
        if val is not None:
            weights[feature_name] = float(val)

    logger.info(f"Loaded weights version {row.version}")
    return weights, float(row.baseline), int(row.version)


def save_updated_weights(session, weights: dict, baseline: float, old_version: int, notes: str = "") -> int:
    new_version = old_version + 1
    new_row = WeightsHistory()
    new_row.version    = new_version
    new_row.baseline   = baseline
    new_row.trained_at = datetime.now(timezone.utc)
    new_row.notes      = notes or f"SGD update from version {old_version}"

    for feature_name, alpha in weights.items():
        col_name = FEATURE_TO_COLUMN.get(feature_name)
        if col_name:
            setattr(new_row, col_name, round(alpha, 6))

    session.add(new_row)
    session.commit()
    logger.info(f"Saved new weights as version {new_version}")
    return new_version

# =============================================================================
# SGD UPDATE CORE
# alpha = alpha + eta x (y - p) x j x x
# =============================================================================

def sgd_update(
    weights:  dict,
    baseline: float,
    features: dict,    # {feature_key: x (0 or 1)}
    y:        int,     # 1 = GO, 0 = NO GO
    p:        float,   # model predicted p_go
    j_values: dict,    # {feature_key: j}
    eta:      float = LEARNING_RATE,
) -> tuple[dict, float, dict]:
    """
    Apply SGD to all active features (x=1).
    Returns (updated_weights, updated_baseline, update_log)
    """
    error   = y - p
    updated = deepcopy(weights)
    update_log = {
        "error": round(error, 4),
        "y": y,
        "p": round(p, 4),
        "eta": eta,
        "updates": {}
    }

    for feature_key, x in features.items():
        if x != 1:
            continue
        if feature_key not in weights:
            continue

        j     = j_values.get(feature_key, J_NOT_RELEVANT)
        old   = weights[feature_key]
        delta = eta * error * j * x
        new   = old + delta

        updated[feature_key] = new
        update_log["updates"][feature_key] = {
            "old":   round(old,   6),
            "delta": round(delta, 6),
            "new":   round(new,   6),
            "j":     j,
        }

    # Baseline always updates with full j=1.0
    b_delta          = eta * error * 1.0
    updated_baseline = baseline + b_delta
    update_log["baseline"] = {
        "old":   round(baseline, 6),
        "delta": round(b_delta,  6),
        "new":   round(updated_baseline, 6),
    }

    return updated, updated_baseline, update_log

# =============================================================================
# PRINT REPORT
# =============================================================================

def print_update_report(scenario_name: str, update_log: dict, features: dict):
    error = update_log["error"]
    direction = "▲ UP (GO — model pushed positive)" if error > 0 else "▼ DOWN (NO GO — model corrected)"

    print(f"\n{'='*64}")
    print(f"  {scenario_name}")
    print(f"{'='*64}")
    print(f"  Decision  : {'✅ GO' if update_log['y'] == 1 else '❌ NO GO'}")
    print(f"  P(model)  : {update_log['p']:.1%}")
    print(f"  Error     : {error:+.4f}  →  {direction}")
    print(f"  η (rate)  : {update_log['eta']}")
    print(f"\n  {'Feature':<46} {'j':>5}  {'Δα':>9}  {'Old α':>9}  {'New α':>9}")
    print(f"  {'-'*46} {'-'*5}  {'-'*9}  {'-'*9}  {'-'*9}")

    # Sort by abs(delta) descending so biggest movers are at top
    for feat, info in sorted(update_log["updates"].items(), key=lambda x: -abs(x[1]["delta"])):
        j_str  = f"{info['j']:.1f}"
        marker = " ◀ KEY" if info["j"] == J_RELEVANT else ""
        print(
            f"  {label(feat):<46} "
            f"{j_str:>5}  "
            f"{info['delta']:>+9.5f}  "
            f"{info['old']:>9.5f}  "
            f"{info['new']:>9.5f}"
            f"{marker}"
        )

    b = update_log["baseline"]
    print(f"\n  {'[BASELINE]':<46} {'1.0':>5}  {b['delta']:>+9.5f}  {b['old']:>9.5f}  {b['new']:>9.5f}")
    print(f"{'='*64}")

# =============================================================================
# TEST SCENARIOS — now use j_values dict directly (no text parsing)
# =============================================================================

MOCK_WEIGHTS = {
    "tier_1": 0.40, "tier_2": 0.25, "tier_3": 0.10,
    "tier_4": 0.05, "tier_5": 0.00, "tier_6": -0.10,
    "sector_Energy & Utilities":                        0.20,
    "sector_Risk & Compliance":                         0.45,
    "sector_Digital Transformation":                    0.35,
    "sector_Construction & Infrastructure":            -0.30,
    "sector_Health & Life Sciences":                    0.15,
    "sector_Financial Services":                        0.30,
    "sector_Education & Training":                      0.10,
    "sector_Data, AI & Analytics":                      0.40,
    "sector_Government Reform & Public Administration": 0.20,
    "proc_CONSULTING":     0.50,
    "proc_NON-CONSULTING": 0.00,
    "proc_WORKS":         -0.40,
    "proc_GOODS":         -0.20,
    "proc_Others":         0.00,
    "agency_World Bank":                                     0.30,
    "agency_African Development Bank (AfDB)":                0.25,
    "agency_United Nations Development Programme (UNDP)":    0.20,
    "agency_Other_UN_Agency":                                0.05,
    "agency_FAO": 0.10, "agency_ILO": 0.10, "agency_UNICEF": 0.15,
    "agency_IOM": 0.05, "agency_UNOPS": 0.05, "agency_UNIDO": 0.05,
    "budget_large":   0.35,
    "budget_medium":  0.15,
    "budget_small":  -0.25,
    "deadline_2_20":     0.20,
    "deadline_20_40":    0.10,
    "deadline_over_40": -0.10,
}
MOCK_BASELINE = -1.20

TEST_SCENARIOS = [
    {
        "name": "Scenario 1 — GO (Algeria / Energy / Budget cited)",
        "description": "Model 55%, partner GO. Budget and country were the reason.",
        "features":   {"tier_1": 1, "sector_Energy & Utilities": 1, "budget_medium": 1,
                       "deadline_over_40": 1, "proc_CONSULTING": 1, "agency_Other_UN_Agency": 1},
        "p_go": 0.55, "y": 1,
        # Budget and country cited (j=1.0), everything else small nudge (j=0.1)
        "j_values": {"tier_1": 1.0, "budget_medium": 1.0,
                     "sector_Energy & Utilities": 0.1, "deadline_over_40": 0.1,
                     "proc_CONSULTING": 0.1, "agency_Other_UN_Agency": 0.1},
    },
    {
        "name": "Scenario 2 — NO GO (Wrong sector — Construction)",
        "description": "Model 80%, partner NO GO. Sector was the only reason.",
        "features":   {"tier_2": 1, "sector_Construction & Infrastructure": 1, "budget_large": 1,
                       "deadline_20_40": 1, "proc_WORKS": 1, "agency_World Bank": 1},
        "p_go": 0.80, "y": 0,
        # Only sector cited (j=1.0), others get small nudge (j=0.1)
        "j_values": {"sector_Construction & Infrastructure": 1.0,
                     "tier_2": 0.1, "budget_large": 0.1,
                     "deadline_20_40": 0.1, "proc_WORKS": 0.1, "agency_World Bank": 0.1},
    },
    {
        "name": "Scenario 3 — NO GO (Liberia / Digital — country was the problem)",
        "description": "Model 99%, partner NO GO. Country (Liberia) was the reason.",
        "features":   {"tier_2": 1, "sector_Digital Transformation": 1, "budget_large": 1,
                       "deadline_over_40": 1, "proc_CONSULTING": 1,
                       "agency_African Development Bank (AfDB)": 1},
        "p_go": 0.99, "y": 0,
        # Country cited (j=1.0), everything else small nudge (j=0.1)
        "j_values": {"tier_2": 1.0,
                     "sector_Digital Transformation": 0.1, "budget_large": 0.1,
                     "deadline_over_40": 0.1, "proc_CONSULTING": 0.1,
                     "agency_African Development Bank (AfDB)": 0.1},
    },
    {
        "name": "Scenario 4 — STRONG GO (Model was right — tiny updates)",
        "description": "Model 90%, partner GO. Everything was relevant.",
        "features":   {"tier_1": 1, "sector_Risk & Compliance": 1, "budget_large": 1,
                       "deadline_2_20": 1, "proc_CONSULTING": 1, "agency_World Bank": 1},
        "p_go": 0.90, "y": 1,
        # All cited — all j=1.0, but error is tiny (+0.10) so updates are small
        "j_values": {"tier_1": 1.0, "sector_Risk & Compliance": 1.0, "budget_large": 1.0,
                     "deadline_2_20": 1.0, "proc_CONSULTING": 1.0, "agency_World Bank": 1.0},
    },
    {
        "name": "Scenario 5 — GO (UNDP / Digital / Senegal)",
        "description": "Model 60%, partner GO. Sector and agency drove it.",
        "features":   {"tier_2": 1, "sector_Digital Transformation": 1, "budget_medium": 1,
                       "deadline_20_40": 1, "proc_CONSULTING": 1,
                       "agency_United Nations Development Programme (UNDP)": 1},
        "p_go": 0.60, "y": 1,
        "j_values": {"sector_Digital Transformation": 1.0,
                     "agency_United Nations Development Programme (UNDP)": 1.0,
                     "tier_2": 0.1, "budget_medium": 0.1,
                     "deadline_20_40": 0.1, "proc_CONSULTING": 0.1},
    },
]


def run_test_scenarios():
    print("\n" + "█"*64)
    print("  KPMG SGD UPDATER — TEST MODE")
    print("  j values set directly (no text parsing)")
    print("█"*64)
    print(f"\n  η (learning rate) = {LEARNING_RATE}")
    print(f"  j relevant        = {J_RELEVANT}   (cited as reason)")
    print(f"  j not relevant    = {J_NOT_RELEVANT}   (not cited — small nudge)")
    print(f"  Baseline          = {MOCK_BASELINE}")
    print(f"  Running {len(TEST_SCENARIOS)} scenarios...\n")

    weights  = deepcopy(MOCK_WEIGHTS)
    baseline = MOCK_BASELINE

    for scenario in TEST_SCENARIOS:
        print(f"\n  ► {scenario['description']}")
        weights, baseline, update_log = sgd_update(
            weights  = weights,
            baseline = baseline,
            features = scenario["features"],
            y        = scenario["y"],
            p        = scenario["p_go"],
            j_values = scenario["j_values"],
        )
        print_update_report(scenario["name"], update_log, scenario["features"])

    # Final summary
    print(f"\n\n{'█'*64}")
    print("  FINAL WEIGHTS AFTER ALL SCENARIOS")
    print(f"{'█'*64}")
    print(f"\n  {'Feature':<46} {'Initial':>10}  {'Final':>10}  {'Change':>10}")
    print(f"  {'-'*46} {'-'*10}  {'-'*10}  {'-'*10}")
    for feat in sorted(MOCK_WEIGHTS.keys()):
        initial = MOCK_WEIGHTS[feat]
        final   = weights.get(feat, initial)
        change  = final - initial
        marker  = " ▲" if change > 0.005 else (" ▼" if change < -0.005 else "  ")
        print(f"  {label(feat):<46} {initial:>10.5f}  {final:>10.5f}  {change:>+10.5f}{marker}")
    print(f"\n  {'Baseline':<46} {MOCK_BASELINE:>10.5f}  {baseline:>10.5f}  {baseline-MOCK_BASELINE:>+10.5f}")
    print(f"\n{'█'*64}\n")


if __name__ == "__main__":
    run_test_scenarios()