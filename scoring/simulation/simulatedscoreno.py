"""
scoring/label_simulation.py
============================
Reads simulation_input.csv, applies KPMG GO/NO GO assumptions,
and outputs simulation_labeled.csv with two new columns:

    go_no_go        — 1 = GO, 0 = NO GO
    score_breakdown — human-readable explanation of the decision

Scoring dimensions (max 18 points):
    sector            0   – 3.0
    budget            0.5 – 3.0
    country           1.0 – 3.0
    funding_agency    1.0 – 3.0
    deadline          0.5 – 3.0  (urgency: closer deadline = higher score)
    procurement_group 0   – 3.0

Hard disqualifiers (always NO GO regardless of score):
    - procurement_group = WORKS or GOODS
    - days_to_deadline < 7
    - lifecycle_stage = early_intelligence

Run:
    python scoring/label_simulation.py
    python scoring/label_simulation.py --input scoring/simulation_input.csv
    python scoring/label_simulation.py --output scoring/simulation_labeled.csv
    python scoring/label_simulation.py --threshold 13.0
"""

import argparse
import csv
import json
import logging
import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

DEFAULT_INPUT     = os.path.join(ROOT_DIR, "scoring", "simulation_input.csv")
DEFAULT_OUTPUT    = os.path.join(ROOT_DIR, "scoring", "simulation_labeled.csv")
DEFAULT_THRESHOLD = 13.0  # out of 18 max — ~72% of max score


# ─────────────────────────────────────────────────────────────────────────────
#  SECTOR SCORING
# ─────────────────────────────────────────────────────────────────────────────

SECTOR_SCORES = {
    # Tier 1 — Core KPMG expertise (3 points)
    "Financial Services":                        3.0,
    "Risk & Compliance":                         3.0,
    "Business Strategy & Performance":           3.0,
    "Government Reform & Public Administration": 3.0,
    "Organizational Reform & HR Management":     3.0,
    "Enterprise IT & Systems Implementation":    3.0,
    "Digital Transformation":                    3.0,

    # Tier 2 — Supporting expertise (2 points)
    "Data, AI & Analytics":                      2.0,
    "Cybersecurity & Data Security":             2.0,
    "Employment & Skills Development":           2.0,
    "Marketing & Customer Experience":           2.0,

    # Tier 3 — Client industries (1 point)
    "Energy & Utilities":                        1.0,
    "Construction & Infrastructure":             1.0,
    "Transport & Logistics":                     1.0,
    "Water, Sanitation & Waste":                 1.0,
    "Agriculture & Food Security":               1.0,
    "Environment & Climate":                     1.0,
    "Health & Life Sciences":                    1.0,
    "Education & Training":                      1.0,
    "Mining & Natural Resources":                1.0,
    "Telecommunications":                        1.0,

    # Special cases (1.5 points)
    "Justice & Rule of Law":                     1.5,
    "Social Protection & Poverty Reduction":     1.5,

    # Unknown
    "Others":                                    0.0,
}


def score_sector(sector_json: str) -> tuple[float, str]:
    """Parse sector JSON list, return best sector score + label."""
    if not sector_json:
        return 0.0, "sector=unknown(0.0)"

    try:
        sectors = json.loads(sector_json)
    except (json.JSONDecodeError, TypeError):
        return 0.0, "sector=parse_error(0.0)"

    if not sectors:
        return 0.0, "sector=empty(0.0)"

    best_score = 0.0
    best_label = sectors[0]
    for s in sectors:
        sc = SECTOR_SCORES.get(s, 0.0)
        if sc > best_score:
            best_score = sc
            best_label = s

    return best_score, f"sector={best_label}({best_score})"


# ─────────────────────────────────────────────────────────────────────────────
#  BUDGET SCORING
# ─────────────────────────────────────────────────────────────────────────────

def score_budget(budget_str: str, currency: str) -> tuple[float, str]:
    """
    Large  (> 500k EUR)  → 3.0
    Medium (100k–500k)   → 2.0
    Small  (< 100k)      → 0.5
    Unknown              → 1.0 (neutral)
    """
    if not budget_str or budget_str.strip() == "":
        return 1.0, "budget=unknown(1.0)"

    try:
        amount = float(budget_str)
    except (ValueError, TypeError):
        return 1.0, "budget=unknown(1.0)"

    currency = (currency or "").strip().upper()
    fx = {
        "USD": 0.92,
        "EUR": 1.00,
        "GBP": 1.17,
        "DZD": 0.0069,
        "XOF": 0.0015,
        "MAD": 0.093,
        "TND": 0.30,
    }
    rate       = fx.get(currency, 0.92)
    amount_eur = amount * rate

    if amount_eur >= 500_000:
        return 3.0, f"budget=large({amount_eur:,.0f}€)(3.0)"
    elif amount_eur >= 100_000:
        return 2.0, f"budget=medium({amount_eur:,.0f}€)(2.0)"
    else:
        return 0.5, f"budget=small({amount_eur:,.0f}€)(0.5)"


# ─────────────────────────────────────────────────────────────────────────────
#  COUNTRY SCORING
# ─────────────────────────────────────────────────────────────────────────────

TIER1_COUNTRIES = {
    "Tunisia", "Morocco", "Algeria", "Egypt", "Libya",
    "Senegal", "Côte D'Ivoire", "Cote D'Ivoire", "Ivory Coast",
    "Ghana", "Kenya", "Nigeria", "Ethiopia", "Tanzania",
    "Uganda", "Rwanda", "Cameroon", "Mali", "Burkina Faso",
    "Niger", "Guinea", "Guinée", "Sénégal", "Maroc",
    "Mauritania", "Mauritanie", "Madagascar", "Mozambique",
    "Zimbabwe", "Zambia", "Angola", "Burundi", "Congo",
    "Democratic Republic Of The Congo", "Togo", "Benin", "Bénin",
    "South Sudan", "Sudan", "Somalia", "Djibouti", "Eritrea",
    "Sierra Leone", "Liberia", "Gambia", "Guinea-Bissau",
    "Equatorial Guinea", "Gabon", "Chad", "Central African Republic",
    "Malawi", "Lesotho", "Eswatini", "Namibia", "Botswana",
    "South Africa", "Comoros", "Cape Verde", "Sao Tome And Principe",
    "Western And Central Africa", "Eastern And Southern Africa",
    "Multinational",
}

TIER2_COUNTRIES = {
    "Jordan", "Lebanon", "Palestine", "Palestinian Territory",
    "Iraq", "Yemen", "Syria", "Syrian Arab Republic", "Turkey", "Turkiye",
    "Georgia", "Armenia", "Azerbaijan", "Ukraine", "Moldova",
    "Republic of Moldova", "Bosnia And Herzegovina", "Kosovo",
    "Albania", "North Macedonia", "Serbia", "Montenegro",
}


def score_country(country: str) -> tuple[float, str]:
    """
    Tier 1 (Africa)             → 3.0
    Tier 2 (Middle East/nearby) → 2.0
    Tier 3 (other)              → 1.0
    """
    if not country or country.strip() == "":
        return 1.0, "country=unknown(1.0)"

    c = country.strip()

    if c in TIER1_COUNTRIES:
        return 3.0, f"country={c}(tier1=3.0)"
    elif c in TIER2_COUNTRIES:
        return 2.0, f"country={c}(tier2=2.0)"
    else:
        return 1.0, f"country={c}(tier3=1.0)"


# ─────────────────────────────────────────────────────────────────────────────
#  FUNDING AGENCY SCORING
#  Scores the tender based on the funding_agency field directly.
#  This field is populated for all portals:
#    - worldbank → "World Bank"
#    - undp      → "United Nations Development Programme (UNDP)"
#    - afdb      → "African Development Bank (AfDB)"
#    - ungm      → the procuring organisation's normalised name
# ─────────────────────────────────────────────────────────────────────────────

# Matched case-insensitively via substring — order matters (more specific first)
FUNDING_AGENCY_SCORES: list[tuple[str, float]] = [
    # Tier 1 — 3.0: Major MDBs and flagship UN development bodies
    ("world bank",                                   3.0),
    ("african development bank",                     3.0),
    ("afdb",                                         3.0),
    ("united nations development programme",         3.0),
    ("undp",                                         3.0),
    ("united nations office for project services",   3.0),
    ("unops",                                        3.0),
    ("united nations industrial development",        3.0),
    ("unido",                                        3.0),
    ("international labour organization",            3.0),
    ("ilo",                                          3.0),
    ("united nations capital development fund",      3.0),
    ("uncdf",                                        3.0),

    # Tier 2 — 2.5: Strong UN bodies with consulting spend
    ("united nations conference on trade",           2.5),
    ("unctad",                                       2.5),
    ("united nations office on drugs and crime",     2.5),
    ("unodc",                                        2.5),

    # Tier 2 — 2.0: Relevant UN agencies and funds
    ("un women",                                     2.0),
    ("unwomen",                                      2.0),
    ("united nations population fund",               2.0),
    ("unfpa",                                        2.0),
    ("united nations educational",                   2.0),
    ("unesco",                                       2.0),
    ("united nations environment",                   2.0),
    ("unep",                                         2.0),

    # Tier 3 — 1.5: Specialised agencies, moderate fit
    ("international maritime organization",          1.5),
    ("imo",                                          1.5),
    ("international telecommunication union",        1.5),
    ("itu",                                          1.5),
    ("international civil aviation",                 1.5),
    ("icao",                                         1.5),
    ("world intellectual property",                  1.5),
    ("wipo",                                         1.5),
    ("international atomic energy",                  1.5),
    ("iaea",                                         1.5),
    ("unicef",                                       1.5),
    ("united nations children",                      1.5),
    ("united nations high commissioner for refugees",1.5),
    ("unhcr",                                        1.5),

    # Tier 4 — 1.0: Humanitarian / lower consulting spend
    ("world food programme",                         1.0),
    ("wfp",                                          1.0),
    ("food and agriculture",                         1.0),
    ("fao",                                          1.0),
    ("world health organization",                    1.0),
    ("who",                                          1.0),
    ("international organization for migration",     1.0),
    ("iom",                                          1.0),
]


def score_funding_agency(funding_agency: str) -> tuple[float, str]:
    """
    Score a tender by its funding_agency field.

    Iterates FUNDING_AGENCY_SCORES (most specific first) and returns
    the score of the first substring match (case-insensitive).
    Falls back to 1.0 (neutral) for unknown agencies.
    """
    if not funding_agency or funding_agency.strip() == "":
        return 1.0, "funding_agency=unknown(1.0)"

    fa_lower = funding_agency.strip().lower()

    for keyword, score in FUNDING_AGENCY_SCORES:
        if keyword in fa_lower:
            return score, f"funding_agency={funding_agency}({score})"

    # Known value but not in our list — neutral
    return 1.0, f"funding_agency={funding_agency}(unknown=1.0)"


# ─────────────────────────────────────────────────────────────────────────────
#  DEADLINE SCORING — URGENCY BASED
#  Closer deadline = more urgent = higher score
# ─────────────────────────────────────────────────────────────────────────────

def score_deadline(days_str: str) -> tuple[float, str, bool]:
    """
    Returns (score, label, is_disqualified).

    Urgency logic — closer = more urgent = higher score:
        < 7 days   → disqualify (impossible to respond)
        7–14 days  → 3.0 (very urgent — decide immediately)
        14–30 days → 2.0 (urgent)
        30–60 days → 1.0 (normal)
        > 60 days  → 0.5 (low urgency)
        unknown    → 1.0 (neutral)
    """
    if not days_str or days_str.strip() == "":
        return 1.0, "deadline=unknown(1.0)", False

    try:
        days = abs(int(float(days_str)))
    except (ValueError, TypeError):
        return 1.0, "deadline=unknown(1.0)", False

    if days < 7:
        return 0.0, f"deadline={days}d(too_urgent=DISQUALIFIED)", True
    elif days <= 14:
        return 3.0, f"deadline={days}d(very_urgent=3.0)", False
    elif days <= 30:
        return 2.0, f"deadline={days}d(urgent=2.0)", False
    elif days <= 60:
        return 1.0, f"deadline={days}d(normal=1.0)", False
    else:
        return 0.5, f"deadline={days}d(low_urgency=0.5)", False


# ─────────────────────────────────────────────────────────────────────────────
#  PROCUREMENT GROUP SCORING
#  KPMG is a consulting firm — consulting is the most valuable.
#  WORKS and GOODS are hard disqualifiers.
# ─────────────────────────────────────────────────────────────────────────────

PROCUREMENT_GROUP_SCORES = {
    "CONSULTING":     3.0,   # core KPMG business
    "NON-CONSULTING": 1.0,   # operational services — lower value
    "Others":         1.5,   # unrecognized — could be consulting, benefit of doubt
}

DISQUALIFIED_GROUPS = {"WORKS", "GOODS"}


def score_procurement_group(pg: str) -> tuple[float, str, bool]:
    """
    Returns (score, label, is_disqualified).

    CONSULTING      → 3.0
    NON-CONSULTING  → 1.0
    Others          → 1.5 (could be consulting, benefit of doubt)
    WORKS           → disqualify
    GOODS           → disqualify
    unknown/empty   → 1.5 (same as Others — benefit of doubt)
    """
    if not pg or pg.strip() == "":
        return 1.5, "proc_group=unknown(1.5)", False

    pg_upper = pg.strip().upper()

    if pg_upper in DISQUALIFIED_GROUPS:
        return 0.0, f"proc_group={pg_upper}(DISQUALIFIED)", True

    score = PROCUREMENT_GROUP_SCORES.get(pg_upper, 1.5)
    return score, f"proc_group={pg_upper}({score})", False


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN LABELING FUNCTION
# ─────────────────────────────────────────────────────────────────────────────

def label_row(row: dict, threshold: float) -> tuple[int, str]:
    """Apply all scoring rules to one row. Returns (go_no_go, score_breakdown)."""
    reasons      = []
    disqualified = False

    # ── Hard disqualifier: lifecycle ─────────────────────────────────────────
    lifecycle = (row.get("lifecycle_stage") or "").strip().lower()
    if lifecycle == "early_intelligence":
        reasons.append("lifecycle=early_intelligence(DISQUALIFIED)")
        disqualified = True

    # ── Procurement group: score + possible disqualifier ─────────────────────
    pg_score, pg_label, pg_disq = score_procurement_group(
        row.get("procurement_group", "")
    )
    reasons.append(pg_label)
    if pg_disq:
        disqualified = True

    # ── Scored dimensions ─────────────────────────────────────────────────────
    sector_score,         sector_label         = score_sector(
        row.get("sector", "")
    )
    budget_score,         budget_label         = score_budget(
        row.get("budget", ""), row.get("currency", "")
    )
    country_score,        country_label        = score_country(
        row.get("country_name_normalized", "")
    )
    funding_agency_score, funding_agency_label = score_funding_agency(
        row.get("funding_agency", "")
    )
    deadline_score,       deadline_label, deadline_disq = score_deadline(
        row.get("days_to_deadline", "")
    )

    reasons += [
        sector_label,
        budget_label,
        country_label,
        funding_agency_label,
        deadline_label,
    ]

    if deadline_disq:
        disqualified = True

    # ── Final decision ────────────────────────────────────────────────────────
    if disqualified:
        reasons.append("TOTAL=0.0 → NO GO (disqualified)")
        return 0, " | ".join(reasons)

    total    = sector_score + budget_score + country_score + funding_agency_score + deadline_score + pg_score
    go_no_go = 1 if total >= threshold else 0
    decision = "GO" if go_no_go else "NO GO"
    reasons.append(f"TOTAL={total:.1f}/{threshold} → {decision}")

    return go_no_go, " | ".join(reasons)


# ─────────────────────────────────────────────────────────────────────────────
#  RUNNER
# ─────────────────────────────────────────────────────────────────────────────

def run(
    input_path:  str   = DEFAULT_INPUT,
    output_path: str   = DEFAULT_OUTPUT,
    threshold:   float = DEFAULT_THRESHOLD,
) -> None:

    if not os.path.exists(input_path):
        log.error("Input file not found: %s", input_path)
        return

    with open(input_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows   = list(reader)
        fieldnames = reader.fieldnames or []

    log.info("Loaded %d rows from %s", len(rows), input_path)

    go_count   = 0
    nogo_count = 0

    output_fields = list(fieldnames) + ["go_no_go", "score_breakdown"]

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=output_fields)
        writer.writeheader()

        for row in rows:
            go_no_go, breakdown    = label_row(row, threshold)
            row["go_no_go"]        = go_no_go
            row["score_breakdown"] = breakdown
            writer.writerow(row)

            if go_no_go == 1:
                go_count += 1
            else:
                nogo_count += 1

    total = len(rows)
    log.info(
        "Labeling done — total=%d  GO=%d (%.1f%%)  NO GO=%d (%.1f%%)",
        total,
        go_count,   go_count   / total * 100,
        nogo_count, nogo_count / total * 100,
    )
    log.info("Output saved to %s", output_path)

    go_pct = go_count / total * 100
    if go_pct < 25:
        log.warning(
            "Only %.1f%% GO — consider lowering --threshold (currently %.1f)",
            go_pct, threshold,
        )
    elif go_pct > 50:
        log.warning(
            "%.1f%% GO — consider raising --threshold (currently %.1f)",
            go_pct, threshold,
        )
    else:
        log.info(
            "Good balance: %.1f%% GO / %.1f%% NO GO",
            go_pct, 100 - go_pct,
        )


# ─────────────────────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Label simulation data with GO/NO GO based on KPMG assumptions."
    )
    parser.add_argument(
        "--input",     type=str,   default=DEFAULT_INPUT,
        help=f"Input CSV path (default: {DEFAULT_INPUT}).",
    )
    parser.add_argument(
        "--output",    type=str,   default=DEFAULT_OUTPUT,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT}).",
    )
    parser.add_argument(
        "--threshold", type=float, default=DEFAULT_THRESHOLD,
        help=f"Minimum score to label as GO (default: {DEFAULT_THRESHOLD}).",
    )
    args = parser.parse_args()

    run(
        input_path  = args.input,
        output_path = args.output,
        threshold   = args.threshold,
    )