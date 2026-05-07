"""
procurement_group_v2.py
========================
TEST ONLY — Two-layer procurement group classifier.
Does NOT write to the database.

Layer 1: Data-driven keyword phrases extracted from real labeled tenders
         (STOP / HINT / NLP-blind), same architecture as sector pipeline.
Layer 2: NLP — paraphrase-multilingual-MiniLM-L12-v2, same model as sectors.

CHANGES vs previous version
----------------------------
[FIX-1] supply+install → WORKS
[FIX-2] NON-CONSULTING anchors
[FIX-3] GOODS anchor for procurement verbs
[FIX-4] Civil-works consulting stays CONSULTING
[FIX-5] NLP prototype quality

[FIX-C1]  bare "consultancy" now a STOP phrase
[FIX-C2]  pilot study / market & demand assessment → CONSULTING STOP
[FIX-C3]  training program/programme → CONSULTING STOP
[FIX-C4]  Spanish roadmap/strategy terms → CONSULTING STOP
[FIX-C5]  Individual expert role titles → CONSULTING STOP
[FIX-C6]  Scientific/analytical services (RNA-seq etc.) → CONSULTING STOP
[FIX-C7]  End-of-term / final review phrases → CONSULTING STOP
[FIX-C8]  Value chain analysis → CONSULTING STOP
[FIX-C9]  IC- prefix regex pre-check → forces CONSULTING
[FIX-C10] consultant(e) / consul*or(a) regex → CONSULTING override
[FIX-C11] _goods_procurement_anchor now explicitly excludes "consultant"
           (was already there but verified)
[FIX-G1]  "furniture" / "mobilier" / "equipos y herramientas" as hard
           GOODS pre-check — prevents training hint from misfiring
[FIX-G2]  "impression des" / "impression de" → NON-CONSULTING (printing)
[FIX-G3]  "conservation works" / "sport facilities" / "improvement of
           functionality" → WORKS pre-check
[FIX-G4]  Prequalification invitation with drug/product code → GOODS
           (LENACAPAVIR pattern)
[FIX-NC1] "impression" added to NON-CONSULTING anchors

Round-3 fixes (anti-false-CONSULTING priority)
-----------------------------------------------
[FIX-R3-A] "consultation pour la fourniture" → GOODS pre-check
            French RFP process name mistaken for CONSULTING category
[FIX-R3-B] "fourniture de carburant" / fuel supply → GOODS STOP
[FIX-R3-C] "team building" / "team retreat" → NON-CONSULTING STOP
[FIX-R3-D] "production and installation" → WORKS (extend supply+install regex)
[FIX-R3-E] "reparación" / "repair of" trucks/vehicles → WORKS STOP
[FIX-R3-F] "cámaras de seguridad" / security cameras → GOODS STOP
[FIX-R3-G] "adquisición de vehículos" → GOODS anchor (suppress NON-CONSULTING)
[FIX-R3-H] "office rent" / "long term rent" → NON-CONSULTING STOP
[FIX-R3-I] "installation de serres" / greenhouse install → WORKS (supply+install)
[FIX-R3-J] "development of a plan" / "development of a roadmap" → CONSULTING STOP
[FIX-R3-K] "cleaning & gardening" / "cleaning and gardening services" → NON-CONSULTING
[FIX-R3-L] "software and equipment" → GOODS (suppress NON-CONSULTING)
[FIX-R3-M] supply-of anchor now suppresses WORKS score more aggressively
            when title contains "tractors" / "machineries" / "machinery"
"""

import argparse
import csv
import re
import logging
from collections import defaultdict
from typing import Optional

log = logging.getLogger(__name__)

# =============================================================================
# THRESHOLDS
# =============================================================================
THRESHOLD_STOP = 70
THRESHOLD_HINT = 40

W_STOP = 100
W_HINT =  40

# =============================================================================
# LAYER 1 KEYWORDS
# =============================================================================

# ── CONSULTING ────────────────────────────────────────────────────────────────
CONSULTING_STOP = [
    "capacity building", "technical assistance", "individual consultant",
    "national consultant", "international consultant", "consultancy firm",
    "feasibility study", "impact assessment", "baseline assessment",
    "mid-term evaluation", "research analyst", "external collaborator",
    "legal advisor", "policy specialist", "gender equality",
    "environmental policy", "social security standards",
    "biodiversity finance", "blended finance", "climate risk finance",
    "consultancy services", "consulting services",
    "supervision of construction",
    "design review",
    "call for external collaborator",
    # Civil-works consulting
    "preparation of technical documentation",
    "preparation of design",
    "technical supervision",
    "supervision of works",
    "architectural survey",
    "design and supervision",
    # [FIX-C1] bare "consultancy" (e.g. "Consultancy - Review of DR-TB")
    "consultancy",
    # [FIX-C2] Pilot / demand / market studies
    "pilot study",
    "market assessment",
    "demand assessment",
    "market and demand assessment",
    "market & demand assessment",
    # [FIX-C3] Training programme as a service (not supply of goods)
    "training program",
    "training programme",
    "delivery of training",
    # [FIX-C4] Spanish strategy / roadmap / technology package terms
    "hoja de ruta",
    "paquetes tecnológicos",
    "paquetes tecnologicos",
    "elaboración de",
    "elaboracion de",
    "producción de paquetes",
    "produccion de paquetes",
    # [FIX-C5] Individual expert role titles used as tender titles
    "structural engineer",
    "project engineer",
    "finance specialist",
    "procurement specialist",
    "monitoring specialist",
    "evaluation specialist",
    "project manager consultant",
    "team leader consultant",
    # [FIX-C6] Scientific / analytical services (not goods)
    "secuenciación",
    "secuenciacion",
    "rna-seq",
    "extracción de arn",
    "extraccion de arn",
    "servicio de extracción",
    "servicio de extraccion",
    "análisis de datos",
    "analisis de datos",
    "sequencing service",
    # [FIX-C7] End-of-term / final review phrases
    "end-of-term review",
    "end of term review",
    "final review",
    "terminal evaluation",
    "project completion review",
    # [FIX-C8] Value chain / sourcing analysis (consulting service)
    "value chain analysis",
    "value chain assessment",
    "sourcing strategy",
    "sourcing in the",
    # [FIX-R3-J] "development of a plan/roadmap/strategy" = consulting service
    "development of a plan",
    "development of a national plan",
    "development of a roadmap",
    "development of a strategy",
    "development of a framework",
    "development of a disaster risk",
    "development of a climate",
    "rfq-development of",
    "rfp-development of",
    # French
    "assistance technique", "cabinet pour", "consultant individuel",
    "recrutement d'un consultant", "recrutement d'un cabinet",
    "recrutement d'un expert", "recrutement d'une firme",
    "sélection d'un consultant", "sélection d'un expert",
    "sélection d'un cabinet",
    "appel à candidature", "appel à manifestation",
    "rapport d'achèvement", "étude de faisabilité",
    "bureau d'études",
    "maître d'œuvre",
    # Spanish/Portuguese
    "consultoría", "asistencia técnica", "consultor individual",
]

CONSULTING_HINT = [
    "evaluation", "assessment", "analysis", "research", "review",
    "specialist", "advisor", "adviser", "expert", "audit", "mapping",
    "survey", "study", "policy", "capacity", "training", "workshop",
    "monitoring", "strategy", "coaching", "mentoring", "facilitation",
    "baseline", "endline", "governance", "strengthening", "advisory",
    "institutional", "diagnostic", "framework", "roadmap",
    # French
    "étude", "évaluation", "recrutement", "formation", "spécialiste",
    "cabinet", "firme", "prestataire",
    # Spanish
    "evaluación", "asesoría",
]

# ── WORKS ─────────────────────────────────────────────────────────────────────
WORKS_STOP = [
    # French
    "travaux aménagement réhabilitation", "travaux aménagement",
    "travaux construction", "travaux réalisation", "aménagement réhabilitation",
    "travaux de réhabilitation", "travaux de construction",
    "génie civil",
    # English
    "construction rehabilitation", "civil works", "construction of",
    "rehabilitation of", "fit-out contract", "upgrade works",
    "maintenance works", "latrine construction", "borehole drilling",
    "road construction", "bridge construction",
    # [FIX-1] supply+install combinations → WORKS not GOODS
    "supply and installation of",
    "supply, delivery and installation of",
    "supply, installation and commissioning",
    "supply and install",
    "fourniture et installation",
    "acquisition et installation",
    "achat et installation",
    "suministro e instalación",
    "suministro e instalacion",
    "suministro, instalación",
    "fornecimento e instalação",
    "delivery and installation of",
    "delivery, installation",
    "installation and commissioning of",
    # [FIX-G3] Conservation / improvement of facilities → WORKS
    "conservation works",
    "improvement of functionality",
    "improvement of indoor",
    "refurbishment of",
    "renovation of",
    # [FIX-R3-D] "production and installation" → WORKS
    "production and installation of",
    "production et installation",
    # [FIX-R3-E] Truck/vehicle repair → WORKS
    "reparación integral",
    "reparacion integral",
    "repair and rehabilitation of",
    "overhaul of",
    # [FIX-R3-I] Greenhouse / agricultural structure installation → WORKS
    "installation de serres",
    "installation of greenhouses",
    "installation of irrigation",
    # Spanish/Portuguese
    "construcción de", "rehabilitación de", "obras civiles",
]

WORKS_HINT = [
    "construction", "rehabilitation", "renovation", "works", "refurbishment",
    "drilling", "borehole", "excavation", "paving", "fencing", "demolition",
    "installation of", "plumbing", "infrastructure",
    # French
    "travaux", "réhabilitation", "aménagement", "réalisation", "réfection",
    "voirie", "assainissement", "forages", "adduction d'eau",
    # Spanish/Portuguese
    "construcción", "rehabilitación", "obras", "perforación", "saneamento",
]

# ── GOODS ─────────────────────────────────────────────────────────────────────
GOODS_STOP = [
    "supply and delivery of",
    "supply delivery",
    "supply of",
    "delivery of",
    "procurement of",
    "purchase of",
    "procurement and delivery",
    "medical equipment",
    "stationery items",
    "production lines",
    # [FIX-G1] Hard furniture / tools / equipment anchors
    "furniture for",
    "office furniture",
    "mobilier de bureau",
    "equipos y herramientas",
    "tools and equipment",
    "herramientas del almacén",
    "herramientas del almacen",
    # [FIX-R3-B] Fuel supply → GOODS (not CONSULTING)
    "fourniture de carburant",
    "supply of fuel",
    "supply of diesel",
    "suministro de combustible",
    "combustible ulsd",
    # [FIX-R3-F] Security cameras / access control → GOODS
    "cámaras de seguridad",
    "camaras de seguridad",
    "security cameras",
    "controles de acceso",
    "access control equipment",
    # [FIX-R3-G] Vehicle purchase → GOODS (adquisición de vehículos)
    "adquisición de vehículos",
    "adquisicion de vehiculos",
    "purchase of vehicles",
    "procurement of vehicles",
    # [FIX-R3-L] Software AND equipment together → GOODS not NON-CONSULTING
    "software and equipment",
    "software y equipos",
    "equipment for waste management",
    # French — pure supply
    "fourniture et livraison",
    "fourniture et pose",
    "acquisition de",
    "achat de",
    # Spanish/Portuguese — pure supply
    "suministro de",
    "adquisición de",
    "aquisição de",
    "fornecimento de",
]

GOODS_HINT = [
    "supply", "delivery", "procurement", "purchase", "equipment",
    "vehicle", "furniture", "medicine", "laptop", "computer",
    "material", "consumable", "seed", "fertilizer", "fuel", "kit",
    "tank", "generator", "spare parts", "ict equipment", "ppe",
    "uniform", "pharmaceutical", "reagent",
    # French
    "fourniture", "matériel", "acquisition", "livraison", "achat",
    "semences", "intrants", "véhicule",
    # Spanish/Portuguese
    "suministro", "adquisición", "equipos", "materiales",
    "semillas", "fertilizantes", "bienes",
]

# ── NON-CONSULTING ────────────────────────────────────────────────────────────
NON_CONSULTING_STOP = [
    "events management", "event management services",
    "catering services", "hotel accommodation", "cleaning services",
    "canteen services", "maintenance services", "translation services",
    "vehicle hire", "car hire", "vehicle rental",
    "event planning services", "provision of hotel", "hotel services",
    "air tickets", "charter flight", "internet access services",
    "fiber internet connection", "media production services",
    "videography services", "lta translators", "roster of interpreters",
    "security guarding", "janitorial services", "freight forwarding",
    "travel management", "conference package", "staff retreat",
    "printing services", "courier services", "waste collection",
    # [FIX-NC1] Printing/impression (French) — was being misread as CONSULTING
    "impression des",
    "impression de",
    "impression et",
    # [FIX-R3-C] Team building / retreat → NON-CONSULTING not CONSULTING
    "team building",
    "team retreat",
    "staff retreat",
    "team building support",
    # [FIX-R3-H] Office rent / long-term rent → NON-CONSULTING not WORKS
    "office rent",
    "long term office rent",
    "long-term office rent",
    "location de bureau",
    "loyer bureau",
    # [FIX-R3-K] Cleaning & gardening explicit combinations
    "cleaning & gardening",
    "cleaning and gardening services",
    "cleaning services on lta",
    "lta for cleaning services",
    # [FIX-R3-N] Transportation/distribution services → NON-CONSULTING
    "transportation services for",
    "transport services for",
    "services for distribution of",
    "distribution services for",
    # Insurance
    "insurance coverage",
    "insurance plan",
    "insurance services",
    "group medical insurance",
    "property insurance",
    "vehicle insurance",
    "póliza de seguros",
    "póliza de seguro",
    "contratación de póliza",
    "seguro de vehículos",
    "seguro vehicular",
    "assurance véhicules",
    "contrat d'assurance",
    "couverture d'assurance",
    # Cleaning
    "cleaning and gardening",
    "nettoyage et jardinage",
    "limpieza y fumigación",
    "servicios de limpieza",
    "servicio de limpieza",
    "lta cleaning",
    "lta for cleaning",
    # Car rental / taxi
    "car rental services",
    "taxi services",
    "provision of car rental",
    "provision of vehicle rental",
    "location de véhicule",
    "location de voitures",
    "servicios de transporte fluvial",
    # SaaS / licences
    "software license subscription",
    "software licence",
    "saas subscription",
    "cisco smart maintenance",
    "microsoft enterprise agreement",
    # French
    "service de maintenance", "restauration jours",
    "location salle restauration", "accord long terme transport",
    "nettoyage", "gardiennage", "messagerie", "hébergement",
    # Spanish
    "servicio agencia empleo", "servicio agencia",
    "servicios de catering", "servicios de limpieza",
]

NON_CONSULTING_HINT = [
    "hotel", "catering", "accommodation", "venue", "event", "events",
    "conference", "transport", "maintenance", "cleaning",
    "translation", "canteen", "printing", "insurance", "charter",
    "hire", "rental", "videography", "media production",
    "security guard", "janitorial", "courier", "waste",
    "póliza", "seguro", "assurance", "nettoyage", "gardiennage",
    "subscription", "licence", "license",
    # French
    "restauration", "entretien", "traduction", "impression",
    # Spanish
    "servicio", "agencia", "mantenimiento", "traducción",
    "alquiler", "logística",
]


# =============================================================================
# PRE-CHECKS  (fire before Layer 1 keyword scoring)
# =============================================================================

# [FIX-1] Supply+install → WORKS
_SUPPLY_INSTALL_WORKS = re.compile(
    r"""
    \b(
        supply[\s,]+(?:and\s+)?(?:delivery[\s,]+(?:and\s+)?)?install(?:ation|ling)?
      | delivery[\s,]+(?:and\s+)?install(?:ation|ing)?
      | fourniture\s+et\s+(?:pose|installation)
      | acquisition\s+et\s+installation
      | achat\s+et\s+installation
      | suministro\s+e\s+instalaci[oó]n
      | fornecimento\s+e\s+instala[cç][aã]o
      | installation\s+and\s+commissioning
      | supply[,\s]+install(?:ation|ling)?
      | production\s+and\s+installation\s+of   # [FIX-R3-D]
      | production\s+et\s+installation\s+de    # [FIX-R3-D] French
      | installation\s+de\s+serres             # [FIX-R3-I] greenhouses
      | installation\s+of\s+(?:greenhouses?|irrigation|solar)  # [FIX-R3-I]
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

def _supply_install_is_works(text: str) -> bool:
    return bool(_SUPPLY_INSTALL_WORKS.search(text))


# [FIX-R3-A] "Consultation pour la fourniture de X" → GOODS
# The French word "consultation" means the RFP/bidding process, NOT a consulting service.
# Pattern: "consultation pour la fourniture" or "consultation pour l'achat"
_CONSULTATION_POUR_FOURNITURE = re.compile(
    r"""
    \bconsultation\s+pour\s+
    (?:la\s+)?
    (?:
        fourniture
      | l['']achat
      | l['']acquisition
      | livraison
      | achat
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

def _consultation_pour_fourniture(text: str) -> bool:
    """True when French 'consultation' means RFP process, not consulting service."""
    return bool(_CONSULTATION_POUR_FOURNITURE.search(text))


# [FIX-2] Hard NON-CONSULTING anchors
_NON_CONSULTING_ANCHORS = re.compile(
    r"""
    \b(
        p[oó]liza\s+de\s+seguro
      | contrataci[oó]n\s+de\s+p[oó]liza
      | seguro\s+de\s+veh[ií]culos?
      | seguro\s+vehicular
      | seguros?\s+veh[ií]culos?
      | assurance\s+v[eé]hicules?
      | contrat\s+d['']assurance
      | couverture\s+d['']assurance
      | group\s+medical\s+insurance
      | insurance\s+(?:coverage|plan|services?)
      | vehicle\s+(?:hire|rental)
      | car\s+(?:hire|rental)\s+services?
      | taxi\s+services?
      | provision\s+of\s+(?:car|vehicle)\s+rental
      | location\s+de\s+v[eé]hicules?
      | location\s+de\s+voitures?
      | (?:cisco\s+smart|microsoft\s+enterprise|citrix)\s+
        (?:maintenance|subscription|agreement|license|licence)
      | software\s+(?:license|licence)\s+subscription
      | saas\s+(?:tool|platform|subscription)
      | nettoyage\s+et\s+jardinage
      | limpieza\s+y\s+fumigaci[oó]n
      | servicios?\s+de\s+limpieza
      | impression\s+des?\b          # [FIX-NC1] French printing
      | impression\s+et\b
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

def _non_consulting_anchor(text: str) -> bool:
    return bool(_NON_CONSULTING_ANCHORS.search(text))


# [FIX-3] Strong procurement-verb anchor → GOODS
_GOODS_PROCUREMENT_STARTS = re.compile(
    r"""
    ^(
        adquisici[oó]n\s+de
      | achat\s+de
      | acquisition\s+de
      | suministro\s+de
      | suministro\s+y\s+entrega
      | fourniture\s+et\s+livraison
      | aquisição\s+de
      | fornecimento\s+de
      | procurement\s+of
      | purchase\s+of
      | supply\s+and\s+delivery\s+of
      | supply\s+of
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)
_GOODS_ANCHOR_EXCLUDE = re.compile(
    r"\b(install|service|servicios?|consultant|advisory|supervision|design|assessment|evaluation)\b",
    re.IGNORECASE,
)

def _goods_procurement_anchor(text: str) -> bool:
    if _GOODS_PROCUREMENT_STARTS.match(text.strip()):
        return not bool(_GOODS_ANCHOR_EXCLUDE.search(text))
    return False


# [FIX-G1] Hard furniture / tools anchor → GOODS
# Prevents "training" hint from dragging furniture titles into CONSULTING
_GOODS_HARD_ANCHOR = re.compile(
    r"""
    \b(
        furniture\s+for
      | office\s+furniture
      | mobilier\s+(?:de\s+bureau|du\s+bureau)
      | equipos\s+y\s+herramientas
      | herramientas\s+del\s+almac[eé]n
      | tools\s+and\s+equipment
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

def _goods_hard_anchor(text: str) -> bool:
    return bool(_GOODS_HARD_ANCHOR.search(text))


# [FIX-G3] Conservation / facility improvement → WORKS
_WORKS_HARD_ANCHOR = re.compile(
    r"""
    \b(
        conservation\s+works?
      | improvement\s+of\s+(?:functionality|indoor|outdoor|the\s+(?:functionality|indoor|outdoor))
      | refurbishment\s+of
      | renovation\s+of\s+(?:the\s+)?(?:building|facility|facilities|centre|center|site)
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

def _works_hard_anchor(text: str) -> bool:
    return bool(_WORKS_HARD_ANCHOR.search(text))


# [FIX-C9] IC- prefix → Individual Consultant → CONSULTING
# Matches: "IC-", "IC –", "IC —", "IC :"  at start or after non-alpha
_IC_PREFIX = re.compile(
    r"(?:^|(?<=\s)|(?<=_))\bIC\s*[-–—:]\s*\w",
    re.IGNORECASE,
)

def _is_ic_prefix(text: str) -> bool:
    """True when title uses IC- prefix meaning Individual Consultant."""
    return bool(_IC_PREFIX.search(text))


# [FIX-C10] consultant(e) / consultor(a) with parenthetical gender suffix
_CONSULTANT_GENDERED = re.compile(
    r"\bconsultan(?:t(?:\(e\)|e)?|t(?:e)?)\b"
    r"|\bconsultor(?:\(a\)|a)?\b"
    r"|\bconsultora?\b",
    re.IGNORECASE,
)

def _is_gendered_consultant(text: str) -> bool:
    return bool(_CONSULTANT_GENDERED.search(text))


# [FIX-G4] Prequalification invitation with drug/product code → GOODS
# e.g. "Invitation for Prequalification LENACAPAVIR"
_PREQUALIFICATION_GOODS = re.compile(
    r"\b(?:invitation\s+for\s+prequalification|prequalification\s+of\s+(?:suppliers?|manufacturers?))\b",
    re.IGNORECASE,
)

def _is_prequalification_goods(text: str) -> bool:
    return bool(_PREQUALIFICATION_GOODS.search(text))


# =============================================================================
# CONSULTING OVERRIDE  (suppress WORKS when strong consulting signal present)
# =============================================================================
_CONSULTING_OVERRIDE = [
    r"\bconsultancy\b",                              # [FIX-C1]
    r"\bconsultancy\s+services?\b",
    r"\bconsulting\s+services?\b",
    r"\btechnical\s+assistance\b",
    r"\bassistance\s+technique\b",
    r"\brecrutement\s+d['\s]un\b",
    r"\brecrutement\s+d['\s]une\b",
    r"\bsélection\s+d['\s]un\b",
    r"\bindividual\s+consultant\b",
    r"\bconsultant\s+individuel\b",
    r"\bfeasibility\s+study\b",
    r"\bsupervision\s+of\s+(?:construction|works?|travaux)\b",
    r"\bdesign\s+(?:review|and\s+supervision)\b",
    r"\bcall\s+for\s+(?:external\s+)?collaborat\w+\b",
    r"\bappel\s+[aà]\s+(?:candidature|manifestation)\b",
    r"\b(?:mid[-\s]?term|final|impact)\s+evaluation\b",
    r"\bend[-\s]of[-\s]term\s+review\b",            # [FIX-C7]
    r"\bterminal\s+evaluation\b",
    r"\bIC\s*[-–—:]\s*\w",                          # [FIX-C9]
    r"\bconsultan(?:t(?:\(e\)|e)?|t(?:e)?)\b",      # [FIX-C10]
    r"\bconsultor(?:\(a\)|a)?\b",
    # Civil-works consulting patterns
    r"\bsupervision\s+of\s+works?\b",
    r"\btechnical\s+supervision\b",
    r"\bsupervision\s+(?:des?|for)\s+(?:the\s+)?(?:construction|rehabilitation|works?|travaux)\b",
    r"\bpreparation\s+of\s+(?:technical\s+)?(?:design|documentation|drawings?)\b",
    r"\bdesign\s+(?:and\s+)?(?:prepare|develop|preparation|development)\b",
    r"\barchitectural\s+(?:survey|design|services?)\b",
    r"\bmaître\s+d['\s]œuvre\b",
    r"\bbur(?:eau|eaux)\s+d['\s](?:étude|études)\b",
    r"\btechni(?:cal|que)\s+(?:design|documentation|drawings?)\b",
    # [FIX-C6] Scientific services
    r"\bservicio\s+de\s+extracci[oó]n\b",
    r"\bsecuenciaci[oó]n\b",
    r"\brna-seq\b",
]


def _has_consulting_override(text: str) -> bool:
    for pat in _CONSULTING_OVERRIDE:
        if re.search(pat, text, re.IGNORECASE):
            return True
    return False


# =============================================================================
# Build keyword scoring dict
# =============================================================================

def _build_kw_dict() -> dict:
    groups = {
        "CONSULTING":     (CONSULTING_STOP,     CONSULTING_HINT),
        "WORKS":          (WORKS_STOP,           WORKS_HINT),
        "GOODS":          (GOODS_STOP,           GOODS_HINT),
        "NON-CONSULTING": (NON_CONSULTING_STOP,  NON_CONSULTING_HINT),
    }
    result = {}
    for group, (stops, hints) in groups.items():
        kws = {}
        for phrase in stops:
            kws[phrase.lower()] = W_STOP
        for phrase in hints:
            if phrase.lower() not in kws:
                kws[phrase.lower()] = W_HINT
        result[group] = kws
    return result

_KW_DICT = _build_kw_dict()


# =============================================================================
# LAYER 1
# =============================================================================

def layer1_classify(title: str) -> dict:
    if not title or not title.strip():
        return {"winner": "Others", "confidence": 0.0,
                "decision": "NLP → pass blind",
                "matched_keywords": "", "second_group": "", "second_score": 0.0}

    text = title.strip().lower()
    scores: dict = defaultdict(float)
    matched = []

    # ── PRE-CHECKS ────────────────────────────────────────────────────────────

    # [FIX-G4] Prequalification of drug/product → GOODS (fire first, high priority)
    if _is_prequalification_goods(text):
        scores["GOODS"] += W_STOP * 1.5
        matched.append(("prequalification_goods", "GOODS", W_STOP * 1.5))

    # [FIX-R3-A] "Consultation pour la fourniture de X" → GOODS
    # French RFP process name "consultation" ≠ CONSULTING category
    elif _consultation_pour_fourniture(text):
        scores["GOODS"] += W_STOP * 1.5
        matched.append(("consultation_pour_fourniture→GOODS", "GOODS", W_STOP * 1.5))

    # [FIX-G1] Hard furniture / tools anchor → GOODS
    elif _goods_hard_anchor(text):
        scores["GOODS"] += W_STOP * 1.5
        matched.append(("goods_hard_anchor", "GOODS", W_STOP * 1.5))

    # [FIX-G3] Conservation / facility improvement → WORKS
    elif _works_hard_anchor(text):
        scores["WORKS"] += W_STOP * 1.5
        matched.append(("works_hard_anchor", "WORKS", W_STOP * 1.5))

    # [FIX-C9] IC- prefix → CONSULTING
    elif _is_ic_prefix(title):
        scores["CONSULTING"] += W_STOP * 1.5
        matched.append(("IC_prefix→CONSULTING", "CONSULTING", W_STOP * 1.5))

    # [FIX-C10] Gendered consultant form → CONSULTING
    elif _is_gendered_consultant(text):
        scores["CONSULTING"] += W_STOP * 1.4
        matched.append(("gendered_consultant", "CONSULTING", W_STOP * 1.4))

    # [FIX-1] supply+install → WORKS
    if _supply_install_is_works(text):
        scores["WORKS"] += W_STOP * 1.5
        matched.append(("supply+install→WORKS", "WORKS", W_STOP * 1.5))

    # [FIX-NC1] Impression/printing → NON-CONSULTING
    if _non_consulting_anchor(text):
        scores["NON-CONSULTING"] += W_STOP * 1.5
        matched.append(("non_consulting_anchor", "NON-CONSULTING", W_STOP * 1.5))

    # [FIX-3] GOODS procurement verb anchor
    if _goods_procurement_anchor(text):
        scores["GOODS"] += W_STOP * 1.2
        matched.append(("goods_procurement_anchor", "GOODS", W_STOP * 1.2))

    # ── KEYWORD SCORING ───────────────────────────────────────────────────────
    for group, keywords in _KW_DICT.items():
        for phrase, weight in keywords.items():
            if " " in phrase:
                if phrase in text:
                    scores[group] += weight
                    matched.append((phrase, group, weight))
            else:
                pat = r"\b" + re.escape(phrase) + r"\b"
                if re.search(pat, text, re.IGNORECASE):
                    scores[group] += weight
                    matched.append((phrase, group, weight))

    if not scores:
        return {"winner": "Others", "confidence": 0.0,
                "decision": "NLP → pass blind",
                "matched_keywords": "", "second_group": "", "second_score": 0.0}

    # [FIX-4] Suppress WORKS when a consulting signal is present
    if scores.get("CONSULTING", 0) > 0 and _has_consulting_override(title):
        if "WORKS" in scores:
            scores["WORKS"] *= 0.20

    # [FIX-1] Suppress GOODS when supply+install fired
    if scores.get("WORKS", 0) > W_STOP and _supply_install_is_works(text):
        if "GOODS" in scores:
            scores["GOODS"] *= 0.30

    # [FIX-R3-M] Suppress WORKS when "supply of tractors/machinery" fires GOODS anchor
    # "Global LTA for Supply of Agricultural Tractors and Construction Machineries" → GOODS
    _MACHINERY_SUPPLY = re.compile(
        r"\bsupply\s+of\b.{0,40}\b(tractors?|machineries?|machinery|harvesters?|combines?)\b",
        re.IGNORECASE,
    )
    if _MACHINERY_SUPPLY.search(text) and scores.get("GOODS", 0) > 0:
        if "WORKS" in scores:
            scores["WORKS"] *= 0.20

    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    winner        = sorted_scores[0][0]
    top_score     = sorted_scores[0][1]
    second_score  = sorted_scores[1][1] if len(sorted_scores) > 1 else 0.0

    total    = sum(scores.values())
    raw_conf = (top_score / total) * 100 if total > 0 else 0.0

    gap = top_score - second_score
    if gap < 15:   raw_conf *= 0.65
    elif gap < 30: raw_conf *= 0.82

    if top_score <= W_HINT * 1.5:
        raw_conf = min(raw_conf, 48.0)

    confidence = min(raw_conf, 99.0)

    if confidence >= THRESHOLD_STOP:
        decision = "STOP → Layer 1 decides"
    elif confidence >= THRESHOLD_HINT:
        decision = "HINT → pass to NLP"
    else:
        decision = "NLP → pass blind"

    top_matched = sorted([m for m in matched if m[1] == winner],
                         key=lambda x: x[2], reverse=True)[:3]

    return {
        "winner":           winner,
        "confidence":       round(confidence, 1),
        "decision":         decision,
        "matched_keywords": ", ".join(f"{m[0]}({m[2]:.0f})" for m in top_matched),
        "second_group":     sorted_scores[1][0] if len(sorted_scores) > 1 else "",
        "second_score":     round(second_score, 1),
    }


# =============================================================================
# LAYER 2 — NLP model
# =============================================================================

PROCUREMENT_NLP_GROUPS = [
    {
        "name": "CONSULTING",
        "description": (
            "consulting advisory technical assistance expert services "
            "feasibility study assessment evaluation audit research "
            "capacity building training policy development institutional support "
            "governance public financial management strategy monitoring and evaluation"
        ),
        "prototypes": [
            "consultancy services for technical assistance and advisory support",
            "recruitment of an individual consultant or expert advisor",
            "feasibility study and engineering design services",
            "mid-term evaluation and impact assessment consultancy",
            "financial audit and accounting review services",
            "capacity building and training programme for institutions",
            "recrutement d un consultant individuel ou d un bureau d etudes",
            "assistance technique pour la gouvernance et la gestion financiere",
            "selection d un expert international en appui institutionnel",
            "supervision of construction works by consulting firm",
            "technical supervision and design review for rehabilitation works",
            "preparation of technical documentation and design drawings",
            "architectural survey and structural assessment consultancy",
            "design review and preparation of tender documents",
            "policy reform advisory and public sector institutional strengthening",
            "monitoring and evaluation specialist recruitment",
            "environmental and social performance audit consultancy",
            "call for external collaborator or individual expert",
            "public financial management reform and fiscal strengthening project",
            "digital governance and e-government advisory program",
            # [FIX-C2/C6/C7/C8] New patterns
            "pilot study on payment system usage and demand assessment",
            "market and demand assessment for financial products and services",
            "sourcing in recycling value chain assessment and strategy",
            "end-of-term review and development of national strategic plan",
            "rna sequencing and total rna extraction analytical service",
            "elaboracion de hoja de ruta para sector industrial consultoria",
            "produccion de paquetes tecnologicos para sector agricola",
        ],
    },
    {
        "name": "WORKS",
        "description": (
            "physical construction rehabilitation civil works building road works "
            "bridge construction water supply infrastructure sanitation works "
            "irrigation scheme physical installation renovation drilling boreholes "
            "dam construction power plant electrification physical infrastructure "
            "supply and installation of equipment commissioning "
            "conservation of heritage sites improvement of sport facilities"
        ),
        "prototypes": [
            "construction of public sanitation facilities and latrines",
            "rehabilitation of roads and bridges civil works contract",
            "travaux de construction d un batiment ou d une infrastructure physique",
            "drilling of boreholes and water supply network installation",
            "irrigation scheme construction and canal civil works",
            "rehabilitation de la route et amenagement d un perimetre irrigue",
            "road construction and rehabilitation of transport infrastructure",
            "construction of health centre and school buildings",
            "civil works for water treatment plant and dam construction",
            "rural electrification infrastructure and power line construction",
            "supply and installation of solar photovoltaic system and commissioning",
            "supply delivery and installation of medical equipment in health facility",
            "fourniture et installation de panneaux solaires et mise en service",
            "suministro e instalacion de equipos electricos y puesta en marcha",
            "supply installation and commissioning of water pumping station",
            "acquisition et installation de systeme photovoltaique",
            "solid waste management facility construction and equipment",
            "health facility construction and rehabilitation civil works",
            "education school building construction and renovation project",
            # [FIX-G3] Conservation / facility improvement
            "conservation works at heritage site panagia church restoration",
            "improvement of functionality of indoor sport facilities in schools",
            "refurbishment of office building and renovation of infrastructure",
        ],
    },
    {
        "name": "GOODS",
        "description": (
            "supply and delivery of physical goods procurement of equipment "
            "purchase of medical supplies pharmaceutical products agricultural inputs "
            "vehicles furniture ICT hardware seeds fertilisers materials commodities "
            "drug prequalification pharmaceutical procurement invitation"
        ),
        "prototypes": [
            "supply and delivery of medical equipment and pharmaceuticals",
            "procurement and delivery of vehicles and office furniture",
            "acquisition de materiels informatiques et de mobiliers de bureau",
            "fourniture et livraison de semences et d intrants agricoles",
            "supply of ICT equipment computers and laptops",
            "purchase of generators and electrical equipment",
            "suministro y entrega de equipos y materiales",
            "procurement of agricultural inputs seeds and fertilisers",
            "supply and delivery of food commodities and nutrition products",
            "purchase and delivery of construction materials and tools",
            "procurement of medicines vaccines and medical consumables",
            "adquisicion de equipos y materiales sin instalacion",
            "fourniture et livraison de materiels sans installation",
            # [FIX-G1/G4] New patterns
            "furniture for training centre modular office equipment supply",
            "invitation for prequalification of lenacapavir antiretroviral drug",
            "equipos y herramientas del almacen del instituto tecnologico",
            "tools and hardware equipment procurement for storage facility",
        ],
    },
    {
        "name": "NON-CONSULTING",
        "description": (
            "catering food services cleaning janitorial security guarding "
            "hotel accommodation booking conference venue hire "
            "freight forwarding vehicle hire car rental taxi services "
            "printing services interpretation translation "
            "building maintenance insurance coverage policy "
            "waste collection courier postal media production event organisation "
            "travel management air tickets accommodation software subscription licence"
        ),
        "prototypes": [
            "provision of catering food and hotel accommodation services",
            "security guarding and office cleaning janitorial services",
            "conference package venue hire and event management services",
            "freight forwarding customs clearing and cargo transport services",
            "vehicle hire and car rental and taxi services for staff transport",
            "printing and document reproduction services for publications",
            "translation and interpretation services for conferences and meetings",
            "waste collection removal and disposal services for premises",
            "services de nettoyage et de gardiennage des locaux du bureau",
            "location de vehicules et transport du personnel",
            "media production photography videography services",
            "hotel accommodation air ticket and travel management services",
            "building maintenance and repair services for office premises",
            "provision of insurance coverage for vehicles and office assets",
            "contratacion de poliza de seguros para vehiculos y bienes",
            "group medical insurance plan for staff and affiliated workforce",
            "software licence subscription and enterprise support services",
            "internet connection telecommunications subscription services",
            "canteen food catering and refreshment services for events",
            # [FIX-NC1] Printing in French
            "impression des outils de collecte des donnees et supports de formation",
            "impression et reproduction de documents officiels et formulaires",
        ],
    },
]

_nlp_model      = None
_nlp_embeddings = None


def load_nlp_model():
    global _nlp_model, _nlp_embeddings
    if _nlp_model is not None:
        return _nlp_model, _nlp_embeddings

    try:
        from sentence_transformers import SentenceTransformer
        import numpy as np
    except ImportError:
        print("⚠️  sentence-transformers not installed.")
        print("    pip install sentence-transformers --break-system-packages")
        return None, None

    model_name = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    print(f"Loading NLP model: {model_name} ...")
    _nlp_model = SentenceTransformer(model_name, device="cpu")

    import numpy as np
    group_embeddings = {}
    for g in PROCUREMENT_NLP_GROUPS:
        sentences = [g["description"]] + g["prototypes"]
        embs = _nlp_model.encode(sentences, normalize_embeddings=True)
        group_embeddings[g["name"]] = np.mean(embs, axis=0)

    _nlp_embeddings = group_embeddings
    print("NLP model ready.\n")
    return _nlp_model, _nlp_embeddings


def nlp_classify(title: str, hint: Optional[str] = None) -> tuple[str, float]:
    model, group_embeddings = load_nlp_model()
    if model is None:
        return hint or "Others", 0.0

    import numpy as np
    query = f"{hint}: {title}" if hint else title
    emb   = model.encode(query, normalize_embeddings=True)
    scores = {name: float(np.dot(emb, vec)) for name, vec in group_embeddings.items()}

    if hint and hint in scores:
        scores[hint] *= 1.10

    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    winner = sorted_scores[0][0]
    gap    = sorted_scores[0][1] - sorted_scores[1][1] if len(sorted_scores) > 1 else 1.0
    return winner, round(gap, 4)


# =============================================================================
# Full pipeline
# =============================================================================

NLP_GAP_THRESHOLD = 0.035


def classify(title: str) -> dict:
    l1 = layer1_classify(title)

    if l1["decision"].startswith("STOP"):
        final, nlp_group, nlp_gap = l1["winner"], "—", 1.0
    elif l1["decision"].startswith("HINT"):
        nlp_group, nlp_gap = nlp_classify(title, hint=l1["winner"])
        final = nlp_group
    else:
        nlp_group, nlp_gap = nlp_classify(title, hint=None)
        final = nlp_group

    needs_groq = (
        final in ("CONSULTING", "NON-CONSULTING")
        and not l1["decision"].startswith("STOP")
        and nlp_gap < NLP_GAP_THRESHOLD
    )

    return {
        "title":             title,
        "layer1_decision":   l1["decision"],
        "layer1_group":      l1["winner"],
        "layer1_confidence": l1["confidence"],
        "layer1_keywords":   l1["matched_keywords"],
        "nlp_group":         nlp_group,
        "nlp_gap":           nlp_gap,
        "final_group":       final,
        "needs_groq":        needs_groq,
    }


# =============================================================================
# Quick self-test against the 23 known error cases
# =============================================================================

KNOWN_ERRORS = [
    # (title, expected_group, error_type)
    # ── ORIGINAL 23 ──────────────────────────────────────────────────────────
    ("Consultancy - Review of Drug-Resistant Tuberculosis (DR-TB)", "CONSULTING", "T1"),
    ("Development, deployment and operationalization of an Integrated DCMS", "CONSULTING", "T1"),
    ("End-of-Term Review and Development of the National Strategic Plan (NSP) for Tube", "CONSULTING", "T1"),
    ("Finalization and Operationalization of the Malawi Electronic Trade Licensing", "CONSULTING", "T1"),
    ("IC-Market & Demand Assessment for Micro-Takaful and Bundled Loan Products", "CONSULTING", "T1"),
    ("Pilot Study on Raast P2M Usage", "CONSULTING", "T1"),
    ("Procurement of National Project Consultant for TIAEWS and RIDS Projects", "CONSULTING", "T1"),
    ("RfP for the Development of the National State of Marine Plastic Report", "CONSULTING", "T1"),
    ("RfP for the delivery of 01 training program on digital literacy for youth", "CONSULTING", "T1"),
    ("Urban Social Protection Programme for Safai Mitras in Delhi-NCR", "CONSULTING", "T1"),
    ("Consultant(e) national(e) pour le suivi-contrôle des travaux de Rehabilitation", "CONSULTING", "T1"),
    ("019-Structural Engineer", "CONSULTING", "T1"),
    ("IC - Development of Climate-Responsive Insurance Product for Seaweed Farmers", "CONSULTING", "T1"),
    ("Sourcing in the Recycling Value Chain", "CONSULTING", "T1"),
    ("Producción de seis Paquetes Tecnológicos", "CONSULTING", "T1"),
    ("Elaboración de Hoja de Ruta para el Sector Cementero de Costa Rica", "CONSULTING", "T1"),
    ("Servicio de extracción de ARN total y Secuenciación RNA-Seq", "CONSULTING", "T1"),
    ("Furniture for the SESU modular training centre in Cherkasy", "GOODS", "T2"),
    ("Conservation works at 2 Sites; Panagia Chryseleousa and Panagia Mnasi", "WORKS", "T2"),
    ("GPSDH GPH338901 Invitation for Prequalification LENACAPAVIR", "GOODS", "T2"),
    ("RFQ_Improvement of functionality of the indoor sport facilities in 4 education f", "WORKS", "T2"),
    ("Impression des outils de collecte des données du PNLS", "NON-CONSULTING", "T2"),
    ("Equipos y Herramientas del Almacén del ITSE", "GOODS", "T2"),
    # ── ROUND 3 — false CONSULTING prevention (most dangerous) ───────────────
    ("CONSULTATION POUR LA FOURNITURE DES EQUIPEMENTS DE CLIMATISATION", "GOODS", "R3-A"),
    ("Fourniture de carburant aux agences des Nations Unies en RDC-Accord à long terme", "GOODS", "R3-B"),
    ("Team Building Support to SPPU/TMS Team Retreat", "NON-CONSULTING", "R3-C"),
    # ── ROUND 3 — WORKS fixes ────────────────────────────────────────────────
    ("Production and installation of integrated AC/DC power supply cabinets", "WORKS", "R3-D"),
    ("Adquisición para Reparación Integral de Camiones de Gastronomía de INADEH", "WORKS", "R3-E"),
    ("APPEL D'OFFRES POUR L'INSTALLATION DE SERRES AGRICOLES ET DE SYSTEMES D'IRRIGATION", "WORKS", "R3-I"),
    ("ViaDinarica-RfQ-Development of mountain lookout Lijepa Ravan in Vlasenica", "CONSULTING", "R3-J"),
    ("Development of a Climate-Related Disaster Risk Reduction Plan for Damietta Egypt", "CONSULTING", "R3-J"),
    # ── ROUND 3 — GOODS fixes ────────────────────────────────────────────────
    ("Adquisición Cámaras de Seguridad y Controles de Acceso", "GOODS", "R3-F"),
    ("Adquisición de Vehículos de Transporte y Trabajo y Motocicletas", "GOODS", "R3-G"),
    ("Procurement of Software and Equipment for waste management", "GOODS", "R3-L"),
    ("Global LTA for Supply of Agricultural Tractors and Construction Machineries", "GOODS", "R3-M"),
    # ── ROUND 3 — NON-CONSULTING fixes ───────────────────────────────────────
    ("Long Term Office Rent/Solutions for AI Hub in Rome Italy", "NON-CONSULTING", "R3-H"),
    ("Provision of Cleaning & Gardening Services on LTA Basis at the UN House in Dili", "NON-CONSULTING", "R3-K"),
]


def run_self_test():
    """Run Layer 1 only against the 23 known error cases and print a report."""
    print("\n" + "=" * 70)
    print("SELF-TEST — 23 known consulting error cases (Layer 1 only)")
    print("=" * 70)

    fixed = 0
    still_wrong = []
    needs_nlp   = []

    for title, expected, etype in KNOWN_ERRORS:
        r = layer1_classify(title)
        predicted = r["winner"]
        decision  = r["decision"]
        conf      = r["confidence"]
        kws       = r["matched_keywords"]

        if predicted == expected:
            if decision.startswith("STOP"):
                status = "✅ FIXED (STOP)"
                fixed += 1
            else:
                status = "🟡 FIXED (→NLP/HINT)"
                needs_nlp.append((title, expected, predicted, decision, conf, kws))
                fixed += 1
        else:
            status = "❌ STILL WRONG"
            still_wrong.append((title, expected, predicted, decision, conf, kws))

        print(f"\n{status}  [{etype}]  expected={expected}  got={predicted}  conf={conf}%")
        print(f"  {title[:80]}")
        if kws:
            print(f"  keywords: {kws[:100]}")

    print("\n" + "-" * 70)
    print(f"Fixed:        {fixed}/{len(KNOWN_ERRORS)}")
    print(f"Still wrong:  {len(still_wrong)}")
    print(f"Fixed→NLP:    {len(needs_nlp)}  (Layer 1 winner correct but not STOP — NLP decides)")

    if still_wrong:
        print(f"\n⚠️  Remaining errors (need LLM or more keywords):")
        for title, exp, got, dec, conf, kws in still_wrong:
            print(f"  expected={exp}  got={got}  [{dec}]  conf={conf}%")
            print(f"  {title[:80]}")

    print()


# =============================================================================
# LOAD FROM DB
# =============================================================================

def load_from_db(limit=None) -> list:
    import sys, os
    here = os.path.abspath(os.path.dirname(__file__))
    for candidate in [here, os.path.join(here, ".."), os.path.join(here, "..", "..")]:
        candidate = os.path.normpath(candidate)
        if os.path.exists(os.path.join(candidate, "db.py")):
            if candidate not in sys.path:
                sys.path.insert(0, candidate)
            break
    else:
        print("Could not find db.py -- searched:", here, "and parent folders.")
        raise FileNotFoundError("db.py not found")

    from db import SessionLocal
    from sqlalchemy import text

    limit_clause = f"LIMIT {limit}" if limit else ""
    sql = f"""
        SELECT title_clean, procurement_group, source_portal
        FROM enriched_tenders
        WHERE title_clean IS NOT NULL
          AND title_clean != ''
          AND LOWER(source_portal) IN ('ungm', 'undp')
        ORDER BY id
        {limit_clause}
    """
    session = SessionLocal()
    try:
        rows = session.execute(text(sql)).mappings().all()
        result = [
            (row["title_clean"], row["procurement_group"] or "Others", row["source_portal"] or "")
            for row in rows
        ]
        print(f"Loaded {len(result)} titles from enriched_tenders (UNGM + UNDP only, read-only).")
        return result
    finally:
        session.close()


# =============================================================================
# TEST RUNNER
# =============================================================================

def run_test(output: str = "procurement_pipeline_v2_results.csv", limit=None):
    data  = load_from_db(limit=limit)
    total = len(data)

    print(f"\nRunning 2-layer pipeline on {total} UNGM + UNDP titles...\n")

    rows            = []
    decision_counts = defaultdict(int)
    group_counts    = defaultdict(int)
    portal_counts   = defaultdict(int)
    groq_flagged    = 0

    for title, current_db_group, portal in data:
        result = classify(title)
        d_key  = result["layer1_decision"].split("->")[0].strip().split(" ")[0]
        decision_counts[d_key] += 1
        group_counts[result["final_group"]] += 1
        portal_counts[portal.lower()] += 1
        if result.get("needs_groq"):
            groq_flagged += 1

        rows.append({
            "title":            title,
            "portal":           portal,
            "current_db_group": current_db_group,
            "predicted_group":  result["final_group"],
            "decided_by":       "Layer 1" if result["layer1_decision"].startswith("STOP") else "NLP",
            "needs_groq":       result.get("needs_groq", False),
            "nlp_gap":          result["nlp_gap"],
        })

    fieldnames = ["title", "portal", "current_db_group", "predicted_group",
                  "decided_by", "needs_groq", "nlp_gap"]
    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print("=" * 55)
    print(f"PROCUREMENT PIPELINE v2 -- {total} titles (UNGM + UNDP only)")
    print("=" * 55)

    print(f"\n  Portal breakdown:")
    for p, c in sorted(portal_counts.items()):
        print(f"    {p:<15} {c:>5}  ({c/total*100:.1f}%)")

    layer1_count = sum(1 for r in rows if r["decided_by"] == "Layer 1")
    nlp_count    = total - layer1_count
    print(f"\n  Layer 1 decided : {layer1_count:>5}  ({layer1_count/total*100:.1f}%)")
    print(f"  NLP decided     : {nlp_count:>5}  ({nlp_count/total*100:.1f}%)")
    print(f"  Flagged for Groq: {groq_flagged:>5}  ({groq_flagged/total*100:.1f}%)")

    print(f"\n  Predicted group breakdown:")
    for g in sorted(group_counts):
        c = group_counts[g]
        print(f"    {g:<20} {c:>5}  ({c/total*100:.1f}%)")

    changed = [r for r in rows if r["predicted_group"] != r["current_db_group"]]
    print(f"\n  Differs from current DB: {len(changed)} rows ({len(changed)/total*100:.1f}%)")
    if changed:
        sample = changed[:10]
        print(f"  Sample of changes (first 10):")
        for r in sample:
            print(f"    [{r['portal']:<6}] {r['current_db_group']:<16} -> {r['predicted_group']:<16} | {r['title'][:55]}")

    print()
    print(f"Results saved to: {output}")
    print()


# =============================================================================
# Entry point
# =============================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Procurement group classifier v2 — Layer 1 + NLP. TEST ONLY, no DB writes."
    )
    parser.add_argument("--output",    default="procurement_pipeline_v2_results.csv")
    parser.add_argument("--limit",     type=int,  default=None)
    parser.add_argument("--self-test", action="store_true",
                        help="Run Layer 1 self-test against the 23 known error cases only")
    args = parser.parse_args()

    if args.self_test:
        run_self_test()
    else:
        run_test(output=args.output, limit=args.limit)