"""
KPMG TENDER SCORING SYSTEM — LOGISTIC REGRESSION TRAINING
Version 2 — 1729 rows, updated funding agency grouping
"""

import pandas as pd
import numpy as np
import ast
import json
from datetime import datetime
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import (accuracy_score, precision_score,
                             recall_score, f1_score, confusion_matrix)

# =============================================================================
# 1. COUNTRY TIER MAPPING
# Temporary solution — when real data grows, each country gets its own alpha
# initialized from its tier value then updated via SGD
# =============================================================================

COUNTRY_TIERS = {
    # TIER 1 — Maghreb / North Africa (alpha: 3.0)
    "Tunisia": 1, "Tunisie": 1,
    "Morocco": 1, "Maroc": 1,
    "Algeria": 1, "Algérie": 1,
    "Libya": 1,
    "Egypt": 1,

    # TIER 2 — Sub-Saharan / West & Central Africa (alpha: 2.5)
    "Senegal": 2, "Sénégal": 2,
    "Côte D'Ivoire": 2, "Côte d'Ivoire": 2,
    "Cameroon": 2, "Cameroun": 2,
    "Ghana": 2, "Nigeria": 2,
    "Kenya": 2, "South Africa": 2,
    "Angola": 2, "Mozambique": 2,
    "Ethiopia": 2, "Tanzania": 2,
    "United Republic of Tanzania": 2,
    "Uganda": 2, "Rwanda": 2,
    "Zambia": 2, "Zimbabwe": 2,
    "Gabon": 2, "Benin": 2, "Bénin": 2,
    "Togo": 2, "Burkina Faso": 2,
    "Mali": 2, "Guinea": 2, "Guinée": 2,
    "Congo": 2, "Democratic Republic Of The Congo": 2,
    "Mauritius": 2, "Namibia": 2,
    "Madagascar": 2, "Liberia": 2,
    "Sierra Leone": 2, "Ivory Coast": 2,
    "Western And Central Africa": 2,
    "Eastern And Southern Africa": 2,
    "Central Africa": 2,

    # TIER 3 — Other Africa / Horn / Fragile States (alpha: 2.0)
    "Niger": 3, "Chad": 3, "Tchad": 3,
    "Sudan": 3, "South Sudan": 3,
    "Somalia": 3, "Somaliland": 3,
    "Horn Of Africa": 3, "Djibouti": 3,
    "Burundi": 3, "Central African Republic": 3,
    "Equatorial Guinea": 3,
    "Comoros": 3, "Comores": 3,
    "Cabo Verde": 3,
    "Sao Tome And Principe": 3, "São Tome And Príncipe": 3,
    "Mauritania": 3, "Mauritanie": 3,
    "Gambia": 3, "Guinea Bissau": 3, "Guinée Bissau": 3,
    "Lesotho": 3, "Swaziland": 3,
    "Seychelles": 3, "Malawi": 3, "Malawai": 3,

    # TIER 4 — Europe (alpha: 1.5)
    "France": 4, "Germany": 4, "Italy": 4,
    "Denmark": 4, "Cyprus": 4, "Albania": 4,
    "Kosovo": 4, "Serbia": 4, "Montenegro": 4,
    "Bosnia And Herzegovina": 4,
    "Republic Of North Macedonia": 4,
    "Romania": 4, "Republic of Moldova": 4,
    "Ukraine": 4, "Georgia": 4,
    "Turkey": 4, "Turkiye": 4, "Armenia": 4,

    # TIER 5 — The Americas (alpha: 1.0)
    "United States Of America": 5,
    "Brazil": 5, "Mexico": 5, "Argentina": 5,
    "Colombia": 5, "Chile": 5, "Peru": 5,
    "Ecuador": 5, "Guatemala": 5, "Honduras": 5,
    "El Salvador": 5, "Costa Rica": 5,
    "Panama": 5, "Haiti": 5, "Belize": 5,
    "Guyana": 5, "Suriname": 5, "Caribbean": 5,
    "Dominica": 5, "St. Lucia": 5, "St Maarten": 5,
    "Plurinational State of Bolivia": 5,

    # TIER 6 — Asia & Pacific (alpha: 0.5)
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

TIER_ALPHAS = {
    1: 3.0,   # Maghreb
    2: 2.5,   # Sub-Saharan Africa
    3: 2.0,   # Other Africa / Fragile
    4: 1.5,   # Europe
    5: 1.0,   # Americas
    6: 0.5,   # Asia & Pacific
    0: 1.0,   # Unknown / Multinational → neutral
}

# =============================================================================
# 2. FUNDING AGENCY GROUPING
# Major agencies get their own alpha.
# Rare agencies grouped into "Other_UN_Agency" to avoid sparse weights.
# =============================================================================

MAJOR_AGENCIES = [
    "World Bank",
    "African Development Bank (AfDB)",
    "United Nations Development Programme (UNDP)",
    "FAO",
    "ILO",
    "UNICEF",
    "IOM",
    "UNOPS",
    "UNIDO",
]

def normalize_agency(agency):
    """Return agency name if major, else 'Other_UN_Agency'."""
    if pd.isna(agency):
        return "Other_UN_Agency"
    agency = str(agency).strip()
    if agency in MAJOR_AGENCIES:
        return agency
    return "Other_UN_Agency"

KNOWN_FUNDING_AGENCIES = MAJOR_AGENCIES + ["Other_UN_Agency"]

# =============================================================================
# 3. OTHER KNOWN CATEGORIES
# =============================================================================

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
BUDGET_BUCKETS    = ["budget_large", "budget_medium", "budget_small"]
DEADLINE_BUCKETS  = ["deadline_2_20", "deadline_20_40", "deadline_over_40"]
TIER_FEATURES     = [f"tier_{i}" for i in range(1, 7)]

# =============================================================================
# 4. FEATURE ENGINEERING
# =============================================================================

def parse_sectors(s):
    try:
        parsed = ast.literal_eval(s)
        if isinstance(parsed, list):
            return [sec.strip() for sec in parsed]
    except:
        pass
    return []

def encode_budget(val):
    if pd.isna(val):
        return None
    v = float(val)
    if v > 500_000:   return "budget_large"
    elif v >= 100_000: return "budget_medium"
    else:              return "budget_small"

def encode_deadline(days):
    if pd.isna(days) or days < 2:
        return None
    d = int(days)
    if d <= 20:   return "deadline_2_20"
    elif d <= 40: return "deadline_20_40"
    else:         return "deadline_over_40"

def build_feature_vector(row):
    features = {}

    # --- Country tier (one-hot, 6 tiers) ---
    tier = COUNTRY_TIERS.get(str(row["country_name_normalized"]).strip(), 0)
    for t in range(1, 7):
        features[f"tier_{t}"] = 1 if tier == t else 0

    # --- Sectors (multi-label one-hot) ---
    sectors = parse_sectors(row["sector"]) if pd.notna(row["sector"]) else []
    for s in KNOWN_SECTORS:
        features[f"sector_{s}"] = 1 if s in sectors else 0

    # --- Procurement (one-hot) ---
    proc = str(row["procurement_group"]).strip().upper() if pd.notna(row["procurement_group"]) else ""
    for p in KNOWN_PROCUREMENT:
        features[f"proc_{p}"] = 1 if proc == p.upper() else 0

    # --- Funding agency (one-hot, grouped) ---
    agency = normalize_agency(row["funding_agency"])
    for a in KNOWN_FUNDING_AGENCIES:
        features[f"agency_{a}"] = 1 if agency == a else 0

    # --- Budget (one-hot, unknown = all 0) ---
    bucket = encode_budget(row["budget"])
    for b in BUDGET_BUCKETS:
        features[b] = 1 if bucket == b else 0

    # --- Deadline (one-hot, invalid rows dropped before this point) ---
    d_bucket = encode_deadline(row["days_to_deadline"])
    for d in DEADLINE_BUCKETS:
        features[d] = 1 if d_bucket == d else 0

    return features

# =============================================================================
# 5. LOAD & FILTER DATA
# =============================================================================

print("=" * 65)
print("  KPMG TENDER SCORING — LOGISTIC REGRESSION TRAINING v2")
print("=" * 65)

df = pd.read_csv("/projects/tenders/scoring/simulation_labeled.csv")
print(f"\nRaw data          : {len(df)} rows")

df = df[df["days_to_deadline"].notna()]
df = df[df["days_to_deadline"] >= 2]
print(f"After deadline filter (≥2 days) : {len(df)} rows")

df = df[df["country_name_normalized"].notna()]
print(f"After country filter            : {len(df)} rows")

go_count   = df["go_no_go"].sum()
nogo_count = (df["go_no_go"] == 0).sum()
print(f"\nGO: {go_count} | NO GO: {nogo_count} | Ratio: {go_count/len(df):.1%} GO")

# =============================================================================
# 6. BUILD FEATURE MATRIX
# =============================================================================

print("\nBuilding feature vectors...")
feature_dicts = [build_feature_vector(row) for _, row in df.iterrows()]
X = pd.DataFrame(feature_dicts).fillna(0).astype(float)
y = df["go_no_go"].values

print(f"Feature matrix    : {X.shape[0]} rows × {X.shape[1]} features")

# =============================================================================
# 7. TRAIN / TEST SPLIT (80/20, stratified)
# =============================================================================

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
print(f"Train             : {len(X_train)} rows")
print(f"Test              : {len(X_test)} rows")

# =============================================================================
# 8. TRAIN LOGISTIC REGRESSION
# Algorithm : Batch Gradient Descent (lbfgs solver)
# All training rows used every iteration
# C=1.0 : regularization penalty to prevent weights from growing too large
# =============================================================================

print("\nTraining logistic regression (Batch Gradient Descent)...")
model = LogisticRegression(
    max_iter=1000,
    random_state=42,
    solver="lbfgs",
    C=1.0,
)
model.fit(X_train, y_train)
print("Training complete ✅")

# =============================================================================
# 9. EVALUATE
# =============================================================================

y_pred = model.predict(X_test)
y_prob = model.predict_proba(X_test)[:, 1]

acc  = accuracy_score(y_test, y_pred)
prec = precision_score(y_test, y_pred, zero_division=0)
rec  = recall_score(y_test, y_pred, zero_division=0)
f1   = f1_score(y_test, y_pred, zero_division=0)
cm   = confusion_matrix(y_test, y_pred)

print("\n" + "=" * 65)
print("  BASELINE RESULTS — HELD-OUT TEST SET (20%)")
print("=" * 65)
print(f"\n  Accuracy   : {acc:.1%}  (overall correct predictions)")
print(f"  Precision  : {prec:.1%}  (when it says GO, how often right?)")
print(f"  Recall     : {rec:.1%}  (of all real GOs, how many caught?)")
print(f"  F1 Score   : {f1:.1%}  (balance of precision & recall)")
print(f"\n  Confusion Matrix:")
print(f"                    Predicted NO GO   Predicted GO")
print(f"  Actual NO GO          {cm[0][0]:>5}            {cm[0][1]:>5}")
print(f"  Actual GO             {cm[1][0]:>5}            {cm[1][1]:>5}")

# =============================================================================
# 10. DISPLAY WEIGHTS — ORDERED BY FIELD
# =============================================================================

feature_names = list(X.columns)
alphas        = model.coef_[0]
baseline      = model.intercept_[0]

weights = pd.DataFrame({
    "feature": feature_names,
    "alpha"  : alphas
})

def display_group(label, prefix, weights_df):
    group = weights_df[weights_df["feature"].str.startswith(prefix)]
    group = group.sort_values("alpha", ascending=False)
    if group.empty:
        return
    print(f"\n  {'─'*55}")
    print(f"  {label}")
    print(f"  {'─'*55}")
    for _, r in group.iterrows():
        name = r["feature"].replace(prefix, "").replace("_", " ")
        bar  = "█" * max(1, int(abs(r["alpha"]) * 4))
        sign = "+" if r["alpha"] >= 0 else "-"
        direction = "GO ↑" if r["alpha"] > 0.1 else ("NO GO ↓" if r["alpha"] < -0.1 else "neutral →")
        print(f"  {name:<48} {sign}{abs(r['alpha']):.4f}  {direction}")
        print(f"  {'':48} {bar}")

print("\n" + "=" * 65)
print("  LEARNED WEIGHTS (ALPHAS) — ORDERED BY FIELD")
print("=" * 65)
print(f"\n  Baseline (default before any feature)   : {baseline:+.4f}")
print(f"  → P(GO) with zero features             : {1/(1+np.exp(-baseline)):.1%}")

categories = [
    ("COUNTRY TIERS",    "tier_"),
    ("SECTORS",          "sector_"),
    ("PROCUREMENT",      "proc_"),
    ("FUNDING AGENCY",   "agency_"),
    ("BUDGET",           "budget_"),
    ("DEADLINE",         "deadline_"),
]

for label, prefix in categories:
    display_group(label, prefix, weights)

# =============================================================================
# 11. SAVE WEIGHTS TO JSON
# =============================================================================

weights_json = {
    "version"    : 2,
    "trained_at" : datetime.now().strftime("%Y-%m-%d %H:%M"),
    "training_rows" : int(len(X_train)),
    "baseline"   : float(baseline),
    "weights"    : {
        row["feature"]: float(row["alpha"])
        for _, row in weights.iterrows()
    },
    "metadata": {
        "accuracy"  : round(float(acc), 4),
        "precision" : round(float(prec), 4),
        "recall"    : round(float(rec), 4),
        "f1"        : round(float(f1), 4),
        "go_count"  : int(go_count),
        "nogo_count": int(nogo_count),
        "features"  : int(X.shape[1]),
    }
}

with open ("model_weights_v2.json", "w")  as f:
    json.dump(weights_json, f, indent=2)

print("\n" + "=" * 65)
print("  Weights saved → model_weights_v2.json")
print("  Ready for Step 2: Scoring Engine + SGD Live Learning")
print("=" * 65)