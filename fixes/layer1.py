"""
layer1_final.py  —  v6 FINAL
=============================
Layer 1 keyword classifier — production ready.
Integrate this into stage3.py as the pre-filter before NLP.

Changes vs v5:
  1. Removed 'human capital' from ALL hint lists
  2. New conflict: job creation + social/recovery → demote Employment
  3. New conflict: energy efficiency + building/public buildings → demote Energy
  4. New conflict: food systems resilience + one health/pandemic → demote Agriculture
  5. New conflict: education system + health professionals/health → demote Education
"""

import re
from collections import defaultdict

# =============================================================================
# WEIGHTS
# =============================================================================

STOP_WEIGHT  = 45
HINT_WEIGHT  = 12
PHRASE_BONUS = 1.5

THRESHOLD_STOP = 70
THRESHOLD_HINT = 40

SECTOR_PRIORITY = {
    "Health & Life Sciences":                    10,
    "Energy & Utilities":                        10,
    "Water, Sanitation & Waste":                 10,
    "Agriculture & Food Security":               10,
    "Construction & Infrastructure":             9,
    "Transport & Logistics":                     9,
    "Enterprise IT & Systems Implementation":    9,
    "Cybersecurity & Data Security":             9,
    "Telecommunications":                        9,
    "Justice & Rule of Law":                     8,
    "Mining & Natural Resources":                8,
    "Education & Training":                      8,
    "Data, AI & Analytics":                      8,
    "Financial Services":                        8,
    "Employment & Skills Development":           7,
    "Environment & Climate":                     7,
    "Social Protection & Poverty Reduction":     7,
    "Digital Transformation":                    7,
    "Organizational Reform & HR Management":     6,
    "Business Strategy & Performance":           6,
    "Risk & Compliance":                         5,
    "Marketing & Customer Experience":           5,
    "Government Reform & Public Administration": 4,
    "Others":                                    1,
}

# =============================================================================
# KEYWORDS
# =============================================================================

SECTOR_KEYWORDS = {

    "Agriculture & Food Security": {
        "stop": [
            "agricultural value chain", "fisheries and aquaculture",
            "irrigation system", "agricultural equipment", "livestock sector",
            "smallholder farmer", "seed supply", "fertilizer supply",
            "food security", "agro-processing",
        ],
        "hint": [
            "agricultural value", "farming practices", "agricultural productivity",
            "agricultural market", "nutrition-sensitive agriculture",
            "food systems resilience",
            "irrigation and water", "crop production", "animal husbandry",
            "agricultural technology", "agricultural supply chain",
        ],
    },

    "Business Strategy & Performance": {
        "stop": [
            "msme competitiveness", "value chain competitiveness",
            "business environment reform",
        ],
        "hint": [
            "innovation and competitiveness", "business environment",
            "enterprise growth", "business advisory", "corporate performance",
            "economic diversification", "startup ecosystem", "business acceleration",
            "private sector development",
        ],
    },

    "Construction & Infrastructure": {
        "stop": [
            "facilities construction", "public infrastructure", "civil works",
            "road construction", "bridge construction", "construction works",
            "building construction",
        ],
        "hint": [
            "housing infrastructure", "industrial infrastructure",
            "infrastructure maintenance", "infrastructure asset",
            "public facilities", "infrastructure rehabilitation",
            "transport corridor",
        ],
    },

    "Cybersecurity & Data Security": {
        "stop": [
            "cybersecurity framework", "cyber threat detection",
            "penetration testing", "cybersecurity capacity",
            "cyber resilience", "cybersecurity governance",
            "information security management",
        ],
        "hint": [
            "secure digital", "cybersecurity policy",
            "security infrastructure", "security operations", "digital forensics",
            "data protection", "information security",
        ],
    },

    "Data, AI & Analytics": {
        "stop": [
            "data analytics", "data infrastructure", "machine learning",
            "big data analytics", "artificial intelligence", "business intelligence",
            "predictive analytics", "data governance framework", "statistical system",
        ],
        "hint": [
            "data governance", "big data", "ai innovation", "data quality",
            "analytics platform", "data collection system",
            "data visualization", "data interoperability", "data management",
        ],
    },

    "Digital Transformation": {
        "stop": [
            "digital innovation", "digital economy", "digital public services",
            "e-government transformation", "digital transformation",
            "digital government", "national digital strategy",
        ],
        "hint": [
            "digital services delivery", "digital innovation ecosystem",
            "digital strategy", "digital inclusion", "smart government",
            "innovation ecosystem", "digital enablement", "digital ecosystem",
            "digital services",
        ],
    },

    "Education & Training": {
        "stop": [
            "education quality", "teacher training", "school construction",
            "school rehabilitation", "learning materials", "educational materials",
            "vocational education", "literacy program", "curriculum development",
            "higher education",
        ],
        "hint": [
            "digital learning", "school infrastructure", "school readiness",
            "education innovation", "learning outcomes",
            # "education system" kept but conflict rule handles health context
            "education system", "school program",
        ],
    },

    "Employment & Skills Development": {
        "stop": [
            "labor market", "youth employment",
            "job creation", "workforce development",
            "vocational training", "labor market program", "skills for jobs",
        ],
        "hint": [
            "labor market integration", "training and employment",
            "job placement", "youth skills", "skills training",
            "workforce upskilling", "entrepreneurship training",
            "employment promotion",
        ],
    },

    "Energy & Utilities": {
        "stop": [
            "renewable energy", "clean energy", "energy transition",
            "energy infrastructure", "solar energy", "hydropower",
            "solar photovoltaic", "wind energy", "rural electrification",
            "electricity access", "power generation", "energy storage",
            "energy efficiency",
        ],
        "hint": [
            "clean energy transition", "electricity access expansion",
            "power distribution", "energy sector reform",
            "off-grid energy", "smart grid", "power supply",
        ],
    },

    "Enterprise IT & Systems Implementation": {
        "stop": [
            "enterprise it", "enterprise resource planning",
            "erp implementation", "software deployment", "it systems",
            "ict infrastructure", "information systems implementation",
        ],
        "hint": [
            "it infrastructure", "it infrastructure upgrade", "enterprise resource",
            "digital platform", "platform integration", "enterprise data",
            "it operations", "systems integration", "cloud migration",
        ],
    },

    "Environment & Climate": {
        "stop": [
            "climate resilience", "climate change", "environmental protection",
            "biodiversity conservation", "ecosystem restoration",
            "carbon market", "climate finance", "climate adaptation",
        ],
        "hint": [
            "disaster risk reduction", "green growth", "ecosystem management",
            "climate mitigation", "green economy", "environmental sustainability",
            "sustainable land management", "natural ecosystems", "climate action",
        ],
    },

    "Financial Services": {
        "stop": [
            "tax administration", "fiscal management", "microfinance",
            "capital markets", "financial sector development",
            "banking system", "digital payments", "financial inclusion",
            "revenue mobilization",
        ],
        "hint": [
            "financial market", "sme financing", "financial literacy",
            "credit access", "financial stability",
            "financial regulation", "payment systems",
        ],
    },

    "Government Reform & Public Administration": {
        "stop": [
            "public administration reform", "civil service reform",
            "governance reform", "anti-corruption",
        ],
        "hint": [
            "public service delivery", "public sector accountability",
            "administrative efficiency", "public administration",
            "government performance", "institutional governance",
            "decentralization", "regulatory reform",
        ],
    },

    "Health & Life Sciences": {
        "stop": [
            "public health", "health system", "medical equipment",
            "pharmaceutical", "vaccines", "disease surveillance",
            "health facility", "hospital infrastructure",
            "primary health care", "maternal and child health",
            "health financing",
        ],
        "hint": [
            "preparedness and response", "universal health coverage",
            "nutrition and health", "pandemic preparedness",
            "health workforce", "epidemiological monitoring",
            "community health", "disease prevention", "health security",
        ],
    },

    "Justice & Rule of Law": {
        "stop": [
            "judicial reform", "rule of law", "access to justice",
            "court system", "justice sector",
        ],
        "hint": [
            "legal framework", "legal aid", "human rights", "legal reform",
            "prison reform", "legal services", "legal empowerment",
            "alternative dispute", "justice delivery", "legal institution",
        ],
    },

    "Marketing & Customer Experience": {
        "stop": [
            "customer experience", "customer engagement",
            "awareness campaign", "communication campaign",
            "branding strategy", "media production",
        ],
        "hint": [
            "digital marketing", "marketing strategy", "market research",
            "customer satisfaction", "marketing analytics",
            "customer journey", "marketing innovation", "brand development",
        ],
    },

    "Mining & Natural Resources": {
        "stop": [
            "resource governance", "extractive industries",
            "geological survey", "oil and gas", "mineral resource",
            "mining sector",
        ],
        "hint": [
            "natural resource", "natural resource policy", "mining investment",
            "resource revenue", "mining infrastructure", "resource monitoring",
            "sustainable mining", "natural resources management", "extractive sector",
        ],
    },

    "Organizational Reform & HR Management": {
        "stop": [
            "human resource management", "workforce planning",
            "organizational reform", "payroll system", "job classification",
            "hr systems", "talent management",
            # human capital REMOVED ENTIRELY — caused 4 wrong decisions
        ],
        "hint": [
            # human capital REMOVED ENTIRELY
            "human resource", "organizational transformation",
            "hr policy", "institutional hr", "organizational effectiveness",
            "hr governance", "workforce performance",
        ],
    },

    "Risk & Compliance": {
        "stop": [
            "compliance audit", "due diligence",
            "safeguards assessment", "esmp", "risk assessment",
        ],
        "hint": [
            "compliance monitoring", "regulatory compliance",
            "governance risk", "internal audit", "risk analytics",
            "risk governance", "operational risk",
            "assessment and mitigation", "environmental audit",
        ],
    },

    "Social Protection & Poverty Reduction": {
        "stop": [
            "social protection", "poverty reduction", "social assistance",
            "social safety nets", "cash transfer", "safety nets",
            "poverty alleviation",
        ],
        "hint": [
            "economic inclusion", "social welfare", "community resilience",
            "livelihood support", "humanitarian assistance",
            "vulnerable populations", "social development",
        ],
    },

    "Telecommunications": {
        "stop": [
            "telecom infrastructure", "network deployment", "broadband expansion",
            "fiber optic network", "mobile network expansion",
            "telecommunications infrastructure", "ict connectivity", "broadband access",
        ],
        "hint": [
            "mobile network", "fiber optic", "digital connectivity",
            "communication infrastructure", "wireless network",
            "connectivity infrastructure", "last-mile connectivity", "telecom sector",
        ],
    },

    "Transport & Logistics": {
        "stop": [
            "transport connectivity", "transport and logistics",
            "freight transport", "customs clearance", "multimodal transport",
            "logistics efficiency", "trade corridor", "transport facilitation",
        ],
        "hint": [
            "transport corridor", "logistics network", "port management",
            "road safety", "supply chain", "logistics infrastructure",
            "urban mobility", "road transport",
        ],
    },

    "Water, Sanitation & Waste": {
        "stop": [
            "water and sanitation", "solid waste", "wastewater treatment",
            "sanitation services", "water supply system",
            "water supply and sanitation", "sanitation access",
            "water quality", "water infrastructure",
        ],
        "hint": [
            "water supply", "water infrastructure rehabilitation",
            "water resource", "waste recycling", "waste collection",
            "drainage system", "water distribution", "clean water",
        ],
    },

    "Others": {
        "stop": [
            "office supplies", "venue rental", "printing services",
            "cleaning services", "catering services", "hotel accommodation",
            "travel services", "event management services",
        ],
        "hint": [
            "hotel booking", "consumables",
            "supply of furniture", "supply of uniforms",
        ],
    },
}

# =============================================================================
# CONFLICT RULES — v6 adds 5 new rules
# =============================================================================

def apply_conflict_rules(predicted: str, text: str) -> bool:
    def has(phrase):
        return phrase in text
    def has_any(phrases):
        return any(p in text for p in phrases)

    # --- EXISTING RULES ---

    # 1. preparedness + response → only Health if medical context present
    if predicted == "Health & Life Sciences":
        if has("preparedness and response") and \
           not has_any(["health", "medical", "disease", "clinic",
                        "hospital", "pharmaceutical", "vaccine"]):
            return True

    # 2. water supply + solar/energy → demote Water (solar water pumps)
    if predicted == "Water, Sanitation & Waste":
        if has("water supply") and \
           has_any(["solar", "photovoltaic", "solar-powered",
                    "power station", "hybrid solar"]):
            return True

    # 3. transport corridor → demote Construction back to Transport
    if predicted == "Construction & Infrastructure":
        if has("transport corridor") and \
           has_any(["transport", "logistics", "corridor development",
                    "trade corridor", "connectivity"]):
            return True

    # 4. disaster risk reduction + catering/venue → not Environment
    if predicted == "Environment & Climate":
        if has("disaster risk reduction") and \
           has_any(["catering", "venue", "accommodation",
                    "meals", "training for", "seminar"]):
            return True

    # 5. natural resource + agriculture/food context → not Mining
    if predicted == "Mining & Natural Resources":
        if has("natural resource") and \
           has_any(["greenhouse", "hydroponics", "agriculture",
                    "farming", "crop", "food"]):
            return True

    # 6. Others + training/workshop → suppress Others
    if predicted == "Others":
        if has_any(["training", "workshop", "conference"]) and \
           has_any(["catering", "accommodation", "venue"]):
            return True

    # 7. energy transition + skilling/employment context → not Energy
    if predicted == "Energy & Utilities":
        if has("energy transition") and \
           has_any(["skilling", "employment", "jobs", "workforce",
                    "workers", "just transition"]):
            return True

    # --- NEW RULES (v6) ---

    # 8. job creation + social/recovery context → not Employment
    #    "Social Recovery and Job Creation" should be Social Protection
    if predicted == "Employment & Skills Development":
        if has("job creation") and \
           has_any(["social recovery", "recovery", "social and job",
                    "safety net", "social protection", "resilience"]):
            return True

    # 9. energy efficiency + building/public buildings → not Energy
    #    "Energy Efficiency in Public Buildings" → Construction
    if predicted == "Energy & Utilities":
        if has("energy efficiency") and \
           has_any(["public buildings", "building", "buildings",
                    "seismic", "school", "hospital building"]):
            return True

    # 10. food systems resilience + one health/pandemic → not Agriculture
    #     "One Health for Pandemic Prevention, Food Systems Resilience"
    if predicted == "Agriculture & Food Security":
        if has("food systems resilience") and \
           has_any(["one health", "pandemic", "health prevention",
                    "ecosystem health", "disease"]):
            return True

    # 11. education system + health professionals/health context → not Education
    #     "Education System for Health Professionals" → Health
    if predicted == "Education & Training":
        if has("education system") and \
           has_any(["health professionals", "health workers",
                    "medical professionals", "nurses", "doctors"]):
            return True

    return False


# =============================================================================
# SCORING
# =============================================================================

def score_title(title: str) -> dict:
    if not title:
        return _empty_result()

    text = title.lower().strip()
    scores  = {sector: 0.0 for sector in SECTOR_KEYWORDS}
    matched = []

    for sector, kw_dict in SECTOR_KEYWORDS.items():
        stops = kw_dict.get("stop", [])
        hints = kw_dict.get("hint", [])

        for phrase in stops:
            if phrase in text:
                w = STOP_WEIGHT * PHRASE_BONUS
                scores[sector] += w
                matched.append((phrase, sector, w))

        for phrase in hints:
            if phrase in text:
                w = HINT_WEIGHT * PHRASE_BONUS
                scores[sector] += w
                matched.append((phrase, sector, w))

    # Others suppression
    active = {s: v for s, v in scores.items() if v > 0}
    if "Others" in active and len(active) > 1:
        other_max = max(v for s, v in active.items() if s != "Others")
        if other_max >= STOP_WEIGHT:
            active.pop("Others", None)

    if not active:
        return _empty_result()

    sorted_scores = sorted(
        active.items(),
        key=lambda x: (round(x[1], 1), SECTOR_PRIORITY.get(x[0], 0)),
        reverse=True,
    )

    winner, top_score = sorted_scores[0]
    second_sector     = sorted_scores[1][0] if len(sorted_scores) > 1 else ""
    second_score      = sorted_scores[1][1] if len(sorted_scores) > 1 else 0

    conflict = apply_conflict_rules(winner, text)

    total    = sum(active.values())
    raw_conf = (top_score / total) * 100 if total > 0 else 0

    gap = top_score - second_score
    if gap < 15:   raw_conf *= 0.65
    elif gap < 30: raw_conf *= 0.82

    if top_score <= HINT_WEIGHT * PHRASE_BONUS:
        raw_conf = min(raw_conf, 48.0)

    if conflict:
        raw_conf = min(raw_conf, THRESHOLD_HINT - 1)

    confidence = min(raw_conf, 99.0)

    if confidence >= THRESHOLD_STOP:
        decision = "STOP"
    elif confidence >= THRESHOLD_HINT:
        decision = "HINT"
    else:
        decision = "NLP"

    if conflict:
        decision += "_CONFLICT"

    top_matched = sorted(
        [m for m in matched if m[1] == winner],
        key=lambda x: x[2], reverse=True
    )[:3]

    return {
        "winner":           winner,
        "confidence":       round(confidence, 1),
        "decision":         decision,
        "matched_keywords": ", ".join(f"{m[0]}({m[2]:.0f})" for m in top_matched),
        "second_sector":    second_sector,
        "second_score":     round(second_score, 1),
    }


def _empty_result():
    return {
        "winner":           None,
        "confidence":       0.0,
        "decision":         "NLP",
        "matched_keywords": "",
        "second_sector":    "",
        "second_score":     0.0,
    }


# =============================================================================
# PUBLIC API — this is what stage3.py calls
# =============================================================================

def layer1_classify(title: str) -> dict:
    """
    Main entry point. Returns:
      {
        'sector':     str or None,   # sector name if decided
        'decision':   str,           # STOP / HINT / NLP / HINT_CONFLICT / NLP_CONFLICT
        'confidence': float,
        'keywords':   str,
      }

    Usage in stage3.py:
        l1 = layer1_classify(title)
        if l1['decision'] == 'STOP':
            primary = l1['sector']
            # skip NLP entirely
        else:
            hint = l1['sector'] if 'HINT' in l1['decision'] else None
            primary, secondary, _ = classify_sectors(text, classifier, hint=hint)
    """
    result = score_title(title)
    return {
        "sector":     result["winner"],
        "decision":   result["decision"],
        "confidence": result["confidence"],
        "keywords":   result["matched_keywords"],
    }