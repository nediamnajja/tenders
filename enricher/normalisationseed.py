"""
enricher/stage3_nlp.py
=======================
PIPELINE STEP 3 — NLP enrichment: sector classification + keyword extraction.

Reads raw text from the `tenders` table (title + description only),
runs zero-shot sector classification and keyword extraction,
then writes results to `enriched_tenders`.

INPUT  (always from raw tenders table):
    tenders.title          — all portals
    tenders.description    — WorldBank (structured description field)
    tenders.notice_text    — NOT used (too long, too noisy)

OUTPUT (written to enriched_tenders):
    sector                 — JSON list: up to 2 sectors e.g. ["Energy & Utilities"]
    keywords               — JSON list: 5-8 single/two-word keywords
    procurement_group      — UNGM + UNDP only, fills null gaps from title

MERGE RULE:
    procurement_group is only written if currently null in enriched_tenders.
    sector and keywords are always written (NLP is the authoritative source).

CLASSIFICATION RULES:
    Primary sector   → top score > 0.75  (else "Others")
    Secondary sector → second score > 0.50 AND primary was assigned
                       (never a secondary without a primary)

PORTALS:
    AfDB + WorldBank → title + description as input
    UNGM + UNDP      → title only as input
                       + procurement_group extraction from title

Run:
    python enricher/stage3_nlp.py                    # all portals
    python enricher/stage3_nlp.py --dry-run          # print results, no DB writes
    python enricher/stage3_nlp.py --limit 20         # first 20 tenders only
    python enricher/stage3_nlp.py --portals afdb     # one portal only
    python enricher/stage3_nlp.py --device cuda      # use GPU if available

Dependencies:
    pip install transformers torch keybert sentence-transformers
"""

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from typing import Optional

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
#  CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_PORTALS = ["afdb", "worldbank", "ungm", "undp"]

# Portals that have a description field worth using
DESCRIPTION_PORTALS = {"afdb", "worldbank"}

# Score thresholds
# With 22 candidate labels, zero-shot scores are diluted across all labels.
# A clear single-sector tender typically scores 0.25-0.45 for the winner.
# 0.75 is unreachable in practice — use realistic thresholds.
PRIMARY_THRESHOLD   = 0.20   # top score must beat this to avoid "Others"
SECONDARY_THRESHOLD = 0.12   # second sector must beat this AND be >= 60% of primary

# Keyword extraction settings
KEYWORD_MIN_NGRAM = 1   # single words allowed
KEYWORD_MAX_NGRAM = 2   # up to two-word phrases
KEYWORD_TOP_N     = 8   # extract up to 8 keywords

# ─────────────────────────────────────────────────────────────────────────────
#  SECTOR DEFINITIONS
#  Each sector has a name and a description used as the NLI hypothesis.
#  The description is what gets compared against the tender text —
#  richer descriptions improve classification accuracy.
# ─────────────────────────────────────────────────────────────────────────────

SECTORS: list[dict] = [
    {
        "name": "Digital Transformation",
        "description": (
            "Modernizing organizations or governments through technology. "
            "E-government, digitization, digital strategy, platform modernization, "
            "digital public services, digital innovation, paperless processes, "
            "transformation numérique, système d'information, digitalisation."
        ),
    },
    {
        "name": "Cybersecurity & Data Security",
        "description": (
            "Protecting information systems, networks, and data from attacks or breaches. "
            "Cybersecurity audit, information security, data protection, GDPR, "
            "intrusion detection, vulnerability assessment, firewall, "
            "sécurité informatique, audit sécurité, protection des données."
        ),
    },
    {
        "name": "Data, AI & Analytics",
        "description": (
            "Collecting, managing, and analyzing data to generate insights. "
            "Data analytics, business intelligence, machine learning, predictive model, "
            "data warehouse, dashboard, statistical analysis, Big Data, "
            "revenue forecasting model, people analytics, AI implementation."
        ),
    },
    {
        "name": "Telecommunications",
        "description": (
            "Communication networks — mobile, broadband, fiber, satellite. "
            "Network infrastructure, digital connectivity, broadband, fiber optic, "
            "mobile network, spectrum, last-mile connectivity, "
            "réseau télécom, infrastructure numérique, connectivité."
        ),
    },
    {
        "name": "Technology & IT",
        "description": (
            "Implementing or managing IT systems within organizations. "
            "ERP, SAP, CRM, IT infrastructure, software implementation, "
            "systems integration, enterprise architecture, IT audit, servers, "
            "supply of computers, IT equipment, helpdesk, application software."
        ),
    },
    {
        "name": "Energy & Utilities",
        "description": (
            "Power generation, transmission, distribution, and energy sector. "
            "Electricity, power grid, solar, wind, hydropower, electrification, "
            "tariff regulation, utility management, renewable energy, "
            "énergie renouvelable, réseau électrique, électrification."
        ),
    },
    {
        "name": "Construction & Infrastructure",
        "description": (
            "Physical building and civil engineering — roads, bridges, buildings, ports. "
            "Civil works, construction, rehabilitation, road, bridge, building, dam, "
            "infrastructure, génie civil, travaux, réhabilitation, aménagement."
        ),
    },
    {
        "name": "Transport & Logistics",
        "description": (
            "Moving people or goods — ports, airports, rail, fleet management, supply chain. "
            "Transport, logistics, fleet management, shipping, port, airport, railway, "
            "corridor, customs, fret, chaîne logistique, transport de marchandises."
        ),
    },
    {
        "name": "Water, Sanitation & Waste",
        "description": (
            "Access to clean water, sewage, drainage, solid waste, sanitation facilities. "
            "Water supply, sanitation, sewage, drainage, waste management, latrines, "
            "boreholes, WASH, assainissement, eau potable, déchets, collecte des ordures."
        ),
    },
    {
        "name": "Agriculture & Food Security",
        "description": (
            "Farming systems, irrigation, food production, agricultural value chains. "
            "Agriculture, irrigation, food security, farming, crop, livestock, "
            "rural development, value chain, agribusiness, filière agricole, "
            "sécurité alimentaire, développement rural."
        ),
    },
    {
        "name": "Environment & Climate",
        "description": (
            "Climate change adaptation and mitigation, environmental protection, ESG. "
            "Climate change, climate finance, carbon, biodiversity, "
            "environmental assessment, ESG, sustainability, green finance, "
            "CDN, accord de Paris, changement climatique, décarbonisation."
        ),
    },
    {
        "name": "Education & Training",
        "description": (
            "School systems, universities, vocational training, capacity building programs. "
            "Education, training, capacity building, school, university, vocational, "
            "curriculum, skills development, formation professionnelle, "
            "renforcement des capacités, apprentissage."
        ),
    },
    {
        "name": "Health & Life Sciences",
        "description": (
            "Healthcare systems, hospitals, medical equipment, pharmaceuticals, public health. "
            "Health, medical, hospital, pharmaceutical, public health, healthcare, "
            "laboratory, equipment médical, santé, vaccin, médicaments, "
            "système de santé, life sciences."
        ),
    },
    {
        "name": "Financial Services",
        "description": (
            "Banking, insurance, microfinance, capital markets, financial inclusion, fintech. "
            "Banking, insurance, microfinance, financial inclusion, fintech, "
            "capital markets, financial sector reform, banque, assurance, "
            "secteur financier, inclusion financière."
        ),
    },
    {
        "name": "Public Sector & Governance",
        "description": (
            "Government reform, public administration, policy, institutional strengthening. "
            "Governance, public administration, institutional reform, policy, "
            "decentralization, anti-corruption, public financial management, PFM, "
            "civil service, gestion publique, réforme institutionnelle, gouvernance."
        ),
    },
    {
        "name": "Risk & Compliance",
        "description": (
            "Risk frameworks, internal audit, regulatory compliance, financial crime prevention. "
            "Risk management, internal audit, compliance, AML, CFT, financial crime, "
            "business continuity, internal controls, crisis management, "
            "contrôle interne, gestion des risques, lutte anti-blanchiment."
        ),
    },
    {
        "name": "Human Capital & Organization",
        "description": (
            "HR strategy, talent management, organizational restructuring, change management. "
            "Human resources, talent management, organizational change, workforce, "
            "job evaluation, restructuring, HR reform, ressources humaines, "
            "gestion des talents, réforme organisationnelle."
        ),
    },
    {
        "name": "Operations & Supply Chain",
        "description": (
            "Process improvement, operational efficiency, procurement systems, cost optimization. "
            "Process optimization, procurement, supply chain, operational efficiency, "
            "cost reduction, inventory, purchasing, chaîne d'approvisionnement, "
            "optimisation des processus, gestion des achats."
        ),
    },
    {
        "name": "Business Strategy & Performance",
        "description": (
            "Strategic planning, performance measurement, business transformation. "
            "Strategic plan, performance management, benchmarking, business transformation, "
            "KPIs, balanced scorecard, growth strategy, stratégie d'entreprise, "
            "plan stratégique, performance organisationnelle."
        ),
    },
    {
        "name": "Marketing & Customer Experience",
        "description": (
            "Sales strategy, customer journey, CRM, brand strategy, market studies. "
            "Marketing, customer experience, CRM, sales, brand, distribution, "
            "market study, customer journey, stratégie commerciale, étude de marché."
        ),
    },
    {
        "name": "Mining & Natural Resources",
        "description": (
            "Mining sector governance, resource extraction, oil, gas, geological surveys. "
            "Mining, extractives, oil, gas, geology, natural resources, mining code, "
            "revenue management, secteur minier, ressources naturelles, "
            "exploitation minière, industries extractives."
        ),
    },
    {
        "name": "Social Protection & Poverty Reduction",
        "description": (
            "Safety nets, cash transfers, social assistance, poverty reduction, vulnerable populations. "
            "Social protection, cash transfer, poverty reduction, safety net, "
            "vulnerable, social assistance, inclusion, protection sociale, "
            "transferts monétaires, pauvreté, filets sociaux."
        ),
    },
]

# Just the label strings passed to the classifier
SECTOR_LABELS = [s["name"] for s in SECTORS]
SECTOR_DESCRIPTIONS = {s["name"]: s["description"] for s in SECTORS}


# ─────────────────────────────────────────────────────────────────────────────
#  PROCUREMENT GROUP EXTRACTION (UNGM + UNDP title only)
#  Rule-based, two-pass: prefix code first, keyword body second.
# ─────────────────────────────────────────────────────────────────────────────

# Pass 1 — title prefix codes → procurement group
_PREFIX_MAP: dict[str, str] = {
    # Consulting
    "RFP":  "CONSULTING", "REOI": "CONSULTING", "EOI":  "CONSULTING",
    "IC":   "CONSULTING", "RFI":  "CONSULTING", "AMI":  "CONSULTING",
    "LCC":  "CONSULTING",
    # Goods
    "RFQ":  "GOODS",      "LPO":  "GOODS",      "ITQ":  "GOODS",
    # Works  (ITB can also be goods — pass 2 disambiguates)
    "ITB":  "WORKS",      "IFB":  "WORKS",      "LTL":  "WORKS",
    "AON":  "WORKS",      "AOI":  "WORKS",
    # Non-consulting
    "SSS":  "NON-CONSULTING",
}

# Pass 2 — keyword signals in title body
_PROC_KEYWORDS: list[tuple[str, list[str]]] = [
    ("CONSULTING", [
        r"\bconsult(?:ing|ant|ancy|ance)s?\b", r"\bindividual\s+consultant\b",
        r"\btechnical\s+assistance\b", r"\bTA\b", r"\bexperts?\b",
        r"\baudits?\b", r"\bsupervi(?:sion|sor)\b", r"\bfeasibility\b",
        r"\bcapacity\s+building\b", r"\btraining\s+services?\b",
        r"\bmonitoring\s+(?:and\s+)?evaluation\b", r"\bM&E\b",
        r"\bassessment\b", r"\breview\b", r"\bstudy\b", r"\bsurvey\b",
        r"\bprestations?\s+intellectuelles?\b", r"\b[eé]tudes?\b",
        r"\bassistance\s+technique\b", r"\bformation\b", r"\baudit\b",
    ]),
    ("WORKS", [
        r"\bconstruct(?:ion|ing)\b", r"\brehabilitat(?:ion|ing)\b",
        r"\bcivil\s+works?\b", r"\brenovation\b", r"\binstallation\b",
        r"\bbuilding\b", r"\bbridge\b", r"\broad\b", r"\bdam\b",
        r"\binfrastructure\b", r"\btravaux\b", r"\bréhabilitation\b",
        r"\bgénie\s+civil\b", r"\baménagement\b", r"\bassainissement\b",
    ]),
    ("NON-CONSULTING", [
        r"\bnon[\s\-]consult\w+\b", r"\bsecurity\s+(?:guard|services?)\b",
        r"\bcleaning\s+services?\b", r"\bmaintenance\s+services?\b",
        r"\btransport(?:ation)?\s+services?\b", r"\bcatering\b",
        r"\bprinting\b", r"\bguard(?:ing)?\s+services?\b",
        r"\bwaste\s+collection\b", r"\bjanitorial\b",
        r"\bnettoyage\b", r"\bentretien\b",
    ]),
    ("GOODS", [
        r"\bsuppl(?:y|ies|ier)\b", r"\bdelivery\s+of\b",
        r"\bsupply\s+and\s+delivery\b", r"\bprocurement\s+of\b",
        r"\bpurchase\s+of\b", r"\bequipment\b", r"\bvehicles?\b",
        r"\bfurniture\b", r"\bcomputers?\b", r"\bmedical\s+supplies\b",
        r"\bfournitures?\b", r"\bmatériels?\b", r"\bacquisition\s+de\b",
        r"\blivraison\b",
    ]),
]


def extract_procurement_group_from_title(title: str) -> Optional[str]:
    """
    Two-pass extraction of procurement group from a UNGM/UNDP title.

    Pass 1: check if the title starts with a known prefix code.
    Pass 2: scan the full title for keyword signals.
    Priority: CONSULTING > WORKS > NON-CONSULTING > GOODS.

    Returns one of: GOODS | WORKS | CONSULTING | NON-CONSULTING | None
    """
    if not title:
        return None

    # Pass 1 — prefix code
    # Extract the first token (before any space, dash, slash, or underscore)
    prefix_match = re.match(r'^([A-Z]{2,6})', title.strip().upper())
    if prefix_match:
        prefix = prefix_match.group(1)
        if prefix in _PREFIX_MAP:
            candidate = _PREFIX_MAP[prefix]
            # ITB/IFB/AOI/AON are ambiguous — fall through to pass 2 to confirm WORKS vs GOODS
            if candidate == "WORKS":
                pass  # don't return yet, confirm below
            else:
                return candidate

    # Pass 2 — keyword body scan (priority order)
    lower = title.lower()
    for group, patterns in _PROC_KEYWORDS:
        for pat in patterns:
            if re.search(pat, lower, re.IGNORECASE):
                return group

    # If pass 1 gave WORKS but pass 2 found nothing to confirm, trust pass 1
    if prefix_match and _PREFIX_MAP.get(prefix_match.group(1).upper()) == "WORKS":
        return "WORKS"

    return None


# ─────────────────────────────────────────────────────────────────────────────
#  MODEL LOADER  (lazy — loaded once on first use)
# ─────────────────────────────────────────────────────────────────────────────

_classifier = None
_keybert    = None


def _load_classifier(device: str = "cpu"):
    """
    Load the zero-shot classification pipeline.
    Model: joeddav/xlm-roberta-large-xnli
    Multilingual NLI model — handles French and English natively.
    ~1.8 GB download on first use, cached locally after.
    """
    global _classifier
    if _classifier is not None:
        return _classifier

    log.info("Loading zero-shot classifier (xlm-roberta-large-xnli)...")
    from transformers import pipeline
    _classifier = pipeline(
        "zero-shot-classification",
        model="joeddav/xlm-roberta-large-xnli",
        device=0 if device == "cuda" else -1,
    )
    log.info("Classifier loaded.")
    return _classifier


def _load_keybert(device: str = "cpu"):
    """
    Load KeyBERT with a multilingual sentence-transformer backbone.
    Model: paraphrase-multilingual-MiniLM-L12-v2
    ~120 MB, fast, handles French and English.
    """
    global _keybert
    if _keybert is not None:
        return _keybert

    log.info("Loading KeyBERT (paraphrase-multilingual-MiniLM-L12-v2)...")
    from keybert import KeyBERT
    # Pass the model name directly to KeyBERT — it initialises
    # SentenceTransformer internally using the legacy path which
    # does not trigger the audio modality check in newer versions.
    _keybert = KeyBERT(model="paraphrase-multilingual-MiniLM-L12-v2")
    log.info("KeyBERT loaded.")
    return _keybert


# ─────────────────────────────────────────────────────────────────────────────
#  TEXT PREPARATION
# ─────────────────────────────────────────────────────────────────────────────

def _clean(text: Optional[str]) -> str:
    """Strip HTML, collapse whitespace."""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&[a-zA-Z]+;", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def build_input_text(tender_title: str, tender_description: Optional[str], portal: str) -> str:
    """
    Build the text to feed into NLP models.

    AfDB + WorldBank: title + description (if available)
    UNGM + UNDP:      title only

    Caps at ~512 tokens (~400 words) which is the practical limit
    for reliable zero-shot classification.
    """
    title = _clean(tender_title)

    if portal in DESCRIPTION_PORTALS and tender_description:
        desc = _clean(tender_description)
        # Truncate description to ~300 words to stay under token limit
        desc_words = desc.split()
        if len(desc_words) > 300:
            desc = " ".join(desc_words[:300]) + "..."
        combined = f"{title}. {desc}" if desc else title
    else:
        combined = title

    # Log a snippet so you can verify the model is receiving real text
    log.debug("  input_text (%d words): %s", len(combined.split()), combined[:120])
    return combined


# ─────────────────────────────────────────────────────────────────────────────
#  SECTOR CLASSIFICATION
# ─────────────────────────────────────────────────────────────────────────────

def classify_sectors(
    text: str,
    classifier,
) -> tuple[Optional[str], Optional[str]]:
    """
    Run zero-shot NLI classification against 22 sector descriptions.

    Returns (primary_sector, secondary_sector).
    Uses sector descriptions (not just names) as hypothesis labels
    for richer, more accurate matching.

    Rules:
        Primary   → top score > 0.75, else "Others"
        Secondary → second score > 0.50 AND primary was assigned
    """
    if not text or not text.strip():
        return "Others", None

    # Use sector NAMES as candidate labels.
    # Short labels work better for zero-shot NLI than long descriptions —
    # the model compares the tender text against "This text is about X"
    # where X is the label. Short and clear beats long and detailed.
    candidate_labels = SECTOR_LABELS  # just the names e.g. "Energy & Utilities"

    result = classifier(
        text,
        candidate_labels=candidate_labels,
        multi_label=True,    # each label scored independently (not sum-to-1)
                             # better for multi-sector tenders
    )

    # Already sorted by score descending
    scores: list[tuple[str, float]] = list(
        zip(result["labels"], result["scores"])
    )

    top_name,  top_score  = scores[0]
    sec_name,  sec_score  = scores[1] if len(scores) > 1 else (None, 0.0)

    log.debug(
        "  top=%-35s %.3f  | 2nd=%-35s %.3f",
        top_name, top_score,
        sec_name or "", sec_score,
    )

    # Primary
    primary = top_name if top_score >= PRIMARY_THRESHOLD else "Others"

    # Secondary — only if:
    #   (1) primary was assigned
    #   (2) second score clears threshold
    #   (3) second score is at least 60% of primary (genuine competitor, not noise)
    secondary = None
    if (
        primary != "Others"
        and sec_score >= SECONDARY_THRESHOLD
        and top_score > 0
        and (sec_score / top_score) >= 0.60
    ):
        secondary = sec_name

    return primary, secondary


# ─────────────────────────────────────────────────────────────────────────────
#  KEYWORD EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

# Stop words to exclude from keywords — common words that add no signal
_STOP_WORDS = {
    # English
    "the", "and", "for", "with", "this", "that", "from", "into", "under",
    "have", "been", "will", "shall", "may", "its", "their", "project",
    "services", "service", "support", "development", "national", "provide",
    "including", "related", "based", "through", "within", "ensure",
    # French
    "les", "des", "pour", "dans", "par", "sur", "avec", "une", "son",
    "ses", "qui", "que", "est", "sont", "aux", "projet", "services",
    "service", "appui", "développement", "national", "cadre",
}


def extract_keywords(text: str, keybert) -> list[str]:
    """
    Extract 5-8 single words or two-word phrases from text.

    Uses KeyBERT with MMR (Maximal Marginal Relevance) to ensure
    diversity — avoids extracting the same concept multiple times
    in slightly different forms.

    Returns a list of clean keyword strings.
    """
    if not text or len(text.split()) < 4:
        return []

    try:
        raw_keywords = keybert.extract_keywords(
            text,
            keyphrase_ngram_range=(KEYWORD_MIN_NGRAM, KEYWORD_MAX_NGRAM),
            stop_words="english",       # basic English stop word filtering
            use_mmr=True,               # diversity — avoid near-duplicate keywords
            diversity=0.5,              # balance relevance vs diversity
            top_n=KEYWORD_TOP_N + 3,    # extract a few extra, we'll filter below
        )
    except Exception as e:
        log.warning("  KeyBERT failed: %s", e)
        return []

    # Clean and filter
    keywords = []
    seen = set()
    for kw, score in raw_keywords:
        kw_clean = kw.strip().lower()

        # Skip stop words, single characters, pure numbers
        if kw_clean in _STOP_WORDS:
            continue
        if len(kw_clean) <= 2:
            continue
        if kw_clean.isdigit():
            continue
        # Skip near-duplicates (one is substring of another already in list)
        if any(kw_clean in existing or existing in kw_clean for existing in seen):
            continue

        seen.add(kw_clean)
        keywords.append(kw_clean)

        if len(keywords) >= KEYWORD_TOP_N:
            break

    return keywords


# ─────────────────────────────────────────────────────────────────────────────
#  SINGLE TENDER PROCESSOR
# ─────────────────────────────────────────────────────────────────────────────

def process_one_tender(
    tender_dict: dict,
    classifier,
    keybert,
) -> dict:
    """
    Run full NLP pipeline on one tender.

    Input dict keys:
        tender_id, enriched_tender_id, portal,
        title, description,
        existing_procurement_group

    Returns dict with:
        primary_sector, secondary_sector,
        keywords (JSON string),
        procurement_group (or None if not applicable / already filled)
    """
    portal      = tender_dict["portal"]
    title       = tender_dict.get("title") or ""
    description = tender_dict.get("description")

    # Build input text
    input_text = build_input_text(title, description, portal)

    # Sector classification
    primary, secondary = classify_sectors(input_text, classifier)

    # Keyword extraction
    keywords = extract_keywords(input_text, keybert)

    # Procurement group (UNGM + UNDP only, fills null gaps)
    procurement_group = None
    if portal in ("ungm", "undp"):
        existing_pg = tender_dict.get("existing_procurement_group")
        if not existing_pg:
            procurement_group = extract_procurement_group_from_title(title)

    return {
        "primary_sector":    primary,
        "secondary_sector":  secondary,
        "keywords":          json.dumps(keywords, ensure_ascii=False),
        "procurement_group": procurement_group,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  DB UPDATE
# ─────────────────────────────────────────────────────────────────────────────

def update_enriched_tender(session, enriched_id: int, result: dict) -> None:
    """
    Write NLP results to enriched_tenders using a raw UPDATE statement.
    Avoids loading the full ORM object which would trigger SELECT on all
    columns including ones deleted from the database.

    sector field stores a JSON list:
        ["Energy & Utilities"]
        ["Energy & Utilities", "Construction & Infrastructure"]

    procurement_group: only written if currently null (merge rule).
    """
    from sqlalchemy import text

    # Build sector list
    sectors = []
    if result["primary_sector"]:
        sectors.append(result["primary_sector"])
    if result["secondary_sector"]:
        sectors.append(result["secondary_sector"])

    sectors_json  = json.dumps(sectors, ensure_ascii=False)
    keywords_json = result["keywords"]
    now           = datetime.now(timezone.utc)

    # Base fields always written
    params = {
        "sector":             sectors_json,
        "keywords":           keywords_json,
        "enrichment_status":  "nlp_complete",
        "enriched_at":        now,
        "enriched_id":        enriched_id,
    }

    # Procurement group — merge rule: only fill if currently null
    pg = result.get("procurement_group")
    if pg:
        stmt = text("""
            UPDATE enriched_tenders
            SET sector            = :sector,
                keywords          = :keywords,
                enrichment_status = :enrichment_status,
                enriched_at       = :enriched_at,
                procurement_group = COALESCE(procurement_group, :pg)
            WHERE id = :enriched_id
        """)
        params["pg"] = pg
    else:
        stmt = text("""
            UPDATE enriched_tenders
            SET sector            = :sector,
                keywords          = :keywords,
                enrichment_status = :enrichment_status,
                enriched_at       = :enriched_at
            WHERE id = :enriched_id
        """)

    session.execute(stmt, params)
    session.commit()

    kw_count = len(json.loads(keywords_json))
    log.info(
        "  ✓ id=%-6s  sectors=%-45s  kw_count=%d%s",
        enriched_id,
        str(sectors),
        kw_count,
        f"  pg={pg}" if pg else "",
    )


# ─────────────────────────────────────────────────────────────────────────────
#  BATCH RUN
# ─────────────────────────────────────────────────────────────────────────────

def run_nlp_enrichment(
    dry_run: bool = False,
    limit:   int | None = None,
    portals: list[str] | None = None,
    device:  str = "cpu",
) -> None:
    """
    Process all EnrichedTender rows with status = 'rules_complete'.
    Reads title + description from the raw tenders table.
    Writes sector, keywords, procurement_group to enriched_tenders.
    """
    try:
        from db import get_session
        from models import EnrichedTender, Tender
        from sqlalchemy import select
    except ImportError as e:
        log.error("Import error — run from project root: %s", e)
        return

    portals  = [p.lower() for p in (portals or DEFAULT_PORTALS)]
    counters = dict(total=0, success=0, failed=0, skipped=0)

    # Load models once before processing
    classifier = _load_classifier(device)
    keybert    = _load_keybert(device)

    # Fetch tender dicts — select only the columns we actually need.
    # This avoids errors from model columns that exist in models.py
    # but have not yet been added to the database via migration.
    with get_session() as session:
        stmt = (
            select(
                # EnrichedTender — only columns we need
                EnrichedTender.id,
                EnrichedTender.tender_id,
                EnrichedTender.source_portal,
                EnrichedTender.procurement_group,
                # Tender — raw text fields (always from raw tenders table)
                Tender.title,
                Tender.description,
            )
            .join(Tender, EnrichedTender.tender_id == Tender.id)
            .where(
                EnrichedTender.enrichment_status == "rules_complete",
                EnrichedTender.source_portal.in_(portals),
            )
            .order_by(EnrichedTender.tender_id)
        )
        if limit:
            stmt = stmt.limit(limit)

        rows = session.execute(stmt).all()
        tender_dicts = [
            {
                "enriched_tender_id":         row.id,
                "tender_id":                  row.tender_id,
                "portal":                     row.source_portal,
                # Both from raw tenders table — never from enriched
                "title":                      row.title,
                "description":                row.description,
                # Existing value for merge rule
                "existing_procurement_group": row.procurement_group,
            }
            for row in rows
        ]

    counters["total"] = len(tender_dicts)
    log.info(
        "Found %d tenders pending NLP enrichment (portals: %s)",
        counters["total"], ", ".join(portals),
    )

    if counters["total"] == 0:
        log.info("Nothing to do — run stage2b first.")
        return

    for i, td in enumerate(tender_dicts, 1):
        log.info(
            "[%d/%d] Processing enriched_id=%s portal=%s",
            i, counters["total"], td["enriched_tender_id"], td["portal"],
        )
        try:
            result = process_one_tender(td, classifier, keybert)

            if dry_run:
                sectors = [s for s in [result["primary_sector"], result["secondary_sector"]] if s]
                kws     = json.loads(result["keywords"])
                log.info(
                    "  [DRY-RUN] sectors=%s  keywords=%s%s",
                    sectors, kws,
                    f"  pg={result['procurement_group']}" if result["procurement_group"] else "",
                )
                counters["success"] += 1
                continue

            from db import get_session as _gs
            with _gs() as session:
                update_enriched_tender(session, td["enriched_tender_id"], result)
            counters["success"] += 1

        except Exception as e:
            log.error(
                "  Failed enriched_id=%s: %s",
                td.get("enriched_tender_id"), e, exc_info=True,
            )
            counters["failed"] += 1

    log.info(
        "NLP enrichment done — total=%d  success=%d  failed=%d  skipped=%d%s",
        counters["total"], counters["success"],
        counters["failed"], counters["skipped"],
        "  [DRY-RUN]" if dry_run else "",
    )


# ─────────────────────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Stage 3 — NLP sector classification and keyword extraction."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Run NLP and print results without writing to DB.",
    )
    parser.add_argument(
        "--limit", type=int, default=None, metavar="N",
        help="Process at most N tenders.",
    )
    parser.add_argument(
        "--portals", nargs="+", default=DEFAULT_PORTALS, metavar="PORTAL",
        help="Source portals to process (default: all four).",
    )
    parser.add_argument(
        "--device", default="cpu", choices=["cpu", "cuda"],
        help="Device for model inference. Use 'cuda' if GPU is available (default: cpu).",
    )
    args = parser.parse_args()

    run_nlp_enrichment(
        dry_run = args.dry_run,
        limit   = args.limit,
        portals = args.portals,
        device  = args.device,
    )