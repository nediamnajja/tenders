"""
enricher/stage3.py
==================
PIPELINE STEP 3 — NLP enrichment using EMBEDDING similarity.

Reads raw text from the `tenders` table (title + description only),
runs embedding-based sector classification and keyword extraction,
then writes results to `enriched_tenders`.

INPUT:
    tenders.title          — all portals
    tenders.description    — AfDB + WorldBank when available

OUTPUT:
    sector                 — JSON list: up to 2 sectors
    keywords               — JSON list: 5-8 keywords
    procurement_group      — UNGM + UNDP only, fills null gaps from title

Run:
    python enricher/stage3.py
    python enricher/stage3.py --dry-run
    python enricher/stage3.py --limit 20
    python enricher/stage3.py --portals afdb

CHANGES vs previous version:
    - Environment & Climate: removed generic words (resilience, adaptation, recovery)
                             tightened to concrete environmental instruments
    - Social Protection:     removed (inclusive, recovery, empowerment, resilience)
                             tightened to cash transfers, safety nets, welfare
    - Transport & Logistics: strengthened with corridor, mobility, road safety,
                             urban transit, waterway, trade facilitation
    - Health & Life Sciences: added pandemic preparedness prototype,
                              added health-specific keywords
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

# Layer 1 keyword classifier — runs before NLP, skips NLP on confident decisions
from layer1 import layer1_classify

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------

DEFAULT_PORTALS = ["afdb", "worldbank", "ungm", "undp"]
DESCRIPTION_PORTALS = {"afdb", "worldbank"}

EMBEDDING_MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"

# Thresholds
PRIMARY_THRESHOLD = 0.33
SECONDARY_THRESHOLD = 0.35
MAX_SECOND_GAP = 0.025
MIN_GAP_FOR_OTHERS = 0.01
MIN_MEANINGFUL_WORDS = 3

# Hybrid weighting
DESCRIPTION_WEIGHT = 0.6
PROTOTYPE_WEIGHT = 0.4

# Keyword extraction settings
KEYWORD_MIN_NGRAM = 1
KEYWORD_MAX_NGRAM = 2
KEYWORD_TOP_N = 8

# Per-keyword score bonus
KEYWORD_BOOST_VALUE = 0.04
KEYWORD_BOOST_CAP = 0.12

# ---------------------------------------------------------------------
# SECTORS — DESCRIPTION + PROTOTYPES
# ---------------------------------------------------------------------

SECTORS = [
    {
        "name": "Digital Transformation",
        "description": "digital transformation, digitalization, e-government, digital public services, digital strategy, digital acceleration, digital integration, transformation numerique, digitalisation",
        "prototypes": [
            "digital transformation of government services",
            "e-government platforms and digital public services",
            "digital acceleration and regional digital integration programs",
            "digitalization of organizations and administrative services",
        ],
    },
    {
        "name": "Cybersecurity & Data Security",
        "description": "cybersecurity, data security, information security, data protection, privacy, access control, intrusion detection, vulnerability assessment, firewall, security systems, protection des donnees",
        "prototypes": [
            "cybersecurity audit and vulnerability assessment",
            "data protection privacy and access control systems",
            "information security intrusion detection and firewalls",
            "security and surveillance systems protection",
        ],
    },
    {
        "name": "Data, AI & Analytics",
        "description": "data analytics, statistics, dashboards, business intelligence, data collection, data systems, artificial intelligence, machine learning, analytics modernization, statistical capacity, monitoring data",
        "prototypes": [
            "data collection systems monitoring and reporting",
            "statistics analytics dashboards and business intelligence",
            "artificial intelligence and machine learning solutions",
            "data warehouse analytics modernization and reporting tools",
        ],
    },
    {
        "name": "Telecommunications",
        "description": "telecommunications, telecom, connectivity, internet connection, fiber optic, fibre optique, broadband, mobile network, sms services, viber messaging, communication network, telecom infrastructure",
        "prototypes": [
            "internet connectivity fiber optic and broadband services",
            "mobile network sms and messaging services",
            "telecom infrastructure and communication networks",
            "connectivity equipment and telecom services",
        ],
    },
    {
        "name": "Enterprise IT & Systems Implementation",
        "description": "enterprise IT, information systems, software, platform development, system implementation, ERP, CRM, SaaS, ICT equipment, IT infrastructure, application development, drupal development, digital platform",
        "prototypes": [
            "software implementation and enterprise information systems",
            "platform development application development and SaaS",
            "ERP CRM and IT infrastructure deployment",
            "digital platform and system integration projects",
        ],
    },
    {
        "name": "Energy & Utilities",
        "description": "energy, electricity, power systems, power generation, solar power, electrification, substation, transmission line, transformer, hydropower, generator, renewable energy, electric grid, utility services",
        "prototypes": [
            "solar power electrification and renewable energy systems",
            "power grid transmission line substation and transformer",
            "hydropower plant generator and power generation systems",
            "electricity distribution and utility infrastructure",
        ],
    },
    {
        "name": "Construction & Infrastructure",
        "description": "construction, civil works, infrastructure, buildings, rehabilitation, engineering works, roads, bridges, renovation, site preparation, travaux, genie civil, architectural design",
        "prototypes": [
            "construction and rehabilitation of buildings and infrastructure",
            "civil works engineering roads bridges and buildings",
            "engineering design and supervision of construction works",
            "renovation rehabilitation and site preparation works",
        ],
    },

    # =========================================================================
    # FIXED: Transport & Logistics
    # Was too weak — losing corridor/mobility/connectivity/road safety titles
    # to Environment & Climate and Social Protection
    # =========================================================================
    {
        "name": "Transport & Logistics",
        "description": (
            "road transport, road construction, road rehabilitation, road safety, "
            "transport corridor, transport connectivity, urban mobility, "
            "railway development, port infrastructure, airport connectivity, "
            "freight transport, logistics systems, customs clearance, warehousing, "
            "multimodal transport, public transit, bus rapid transit, "
            "trade facilitation, supply chain, transport infrastructure, "
            "waterway transport, transport sector reform, fleet management, "
            "corridor development, traffic management"
        ),
        "prototypes": [
            "road construction rehabilitation and transport corridor development",
            "urban mobility bus rapid transit and public transport improvement",
            "railway port and airport infrastructure connectivity project",
            "freight transport logistics customs clearance and trade facilitation",
            "multimodal transport and waterway logistics development program",
            "road safety improvement and transport sector reform",
            "transport infrastructure resilience and connectivity enhancement",
            "supply chain optimization and fleet management program",
        ],
    },

    {
        "name": "Water, Sanitation & Waste",
        "description": "water supply, sewerage, sanitation, WASH, wastewater, drainage, boreholes, pipelines, pipes, latrines, solid waste, waste management, rainwater harvesting, assainissement, eau potable, water services",
        "prototypes": [
            "water supply systems pipelines boreholes and drinking water",
            "sewerage wastewater drainage sanitation infrastructure",
            "solid waste management landfill recycling and waste systems",
            "urban water services and institutional water support",
        ],
    },
    {
        "name": "Agriculture & Food Security",
        "description": "agriculture, food security, seeds, fertilizers, irrigation, livestock, fisheries, aquaculture, cocoa, coffee, cassava, agro-processing, value chain, farming, crop production, horticulture, securite alimentaire",
        "prototypes": [
            "seeds fertilizers irrigation and crop production",
            "livestock fisheries aquaculture and animal production",
            "agro-processing cocoa coffee cassava and food value chains",
            "food security rural livelihoods and agricultural support",
        ],
    },

    # =========================================================================
    # FIXED: Environment & Climate
    # Was too magnetic — grabbing everything with resilience/recovery/sustainable
    # which appear in almost every development project title.
    # Now focused on concrete environmental instruments and outputs only.
    # =========================================================================
    {
        "name": "Environment & Climate",
        "description": (
            "climate change mitigation, climate change adaptation, "
            "biodiversity conservation, ecosystem restoration, "
            "carbon markets, net zero emissions, climate finance, "
            "disaster risk reduction, environmental protection, "
            "nature conservation, forest conservation, coastal protection, "
            "air pollution control, green economy, carbon abatement, "
            "greenhouse gas emissions, climate data monitoring, "
            "blue economy, sustainable land management, "
            "environmental compliance, deforestation, reforestation, "
            "wetlands, watershed management"
        ),
        "prototypes": [
            "biodiversity conservation and ecosystem restoration project",
            "climate change mitigation and carbon market development",
            "disaster risk reduction and climate adaptation financing",
            "air pollution control and environmental protection program",
            "forest conservation and sustainable land management initiative",
            "coastal protection and blue economy development project",
            "greenhouse gas emissions reduction and net zero program",
            "climate finance and environmental compliance monitoring",
        ],
    },

    {
        "name": "Education & Training",
        "description": "education, schools, classrooms, curriculum, teaching, students, school rehabilitation, educational materials, literacy, vocational training, teacher training, learning systems, formation, enseignement",
        "prototypes": [
            "schools classrooms and education infrastructure",
            "curriculum teaching students and learning materials",
            "vocational training teacher training and skills development",
            "literacy school education and educational reform",
        ],
    },

    # =========================================================================
    # FIXED: Health & Life Sciences
    # Was losing titles to Environment & Climate on preparedness/response/resilience
    # Added specific health emergency prototype and richer health vocabulary
    # =========================================================================
    {
        "name": "Health & Life Sciences",
        "description": (
            "healthcare systems, hospitals, clinics, medical equipment, "
            "public health programs, disease surveillance, disease prevention, "
            "pandemic preparedness, health emergency response, "
            "primary health care, maternal and child health, "
            "universal health coverage, health financing, health workforce, "
            "pharmaceuticals, vaccines, laboratory systems, "
            "nutrition programs, mental health services, "
            "epidemiological monitoring, health infrastructure, "
            "infectious disease control, health security, medical supplies, "
            "one health, immunization, epidemic response"
        ),
        "prototypes": [
            "primary health care strengthening and disease surveillance program",
            "pandemic preparedness health emergency response and resilience",
            "hospital infrastructure and medical equipment modernization",
            "universal health coverage and health financing reform",
            "maternal child health nutrition and vaccination program",
            "public health emergency preparedness and infectious disease control",
            "health workforce development and primary care accessibility",
            "pharmaceutical supply laboratory systems and health security",
        ],
    },

    {
        "name": "Financial Services",
        "description": "financial services, banking, insurance, tax administration, tax law, fiscal management, financial inclusion, microfinance, actuarial services, capital markets, revenue systems, public finance, finance sector",
        "prototypes": [
            "banking insurance and financial sector development",
            "tax administration fiscal management and revenue systems",
            "financial inclusion microfinance and credit products",
            "actuarial services insurance and capital markets",
        ],
    },
    {
        "name": "Government Reform & Public Administration",
        "description": "government reform, governance, public administration, public sector management, institutional reform, decentralization, public service delivery, accountability, anti-corruption, civil service reform",
        "prototypes": [
            "government reform governance and accountability projects",
            "public administration institutional reform and decentralization",
            "public sector management and service delivery improvement",
            "civil service reform anti-corruption and state institutions",
        ],
    },
    {
        "name": "Justice & Rule of Law",
        "description": "justice, legal reform, judiciary, courts, constitution, legislation, legal drafting, labour law, legal database, rule of law, judicial reform, access to justice",
        "prototypes": [
            "justice judiciary courts and legal reform",
            "constitution legislation legal drafting and legal framework",
            "rule of law judicial reform and access to justice",
            "labour law legal database and legal advisory services",
        ],
    },
    {
        "name": "Risk & Compliance",
        "description": "risk management, compliance, audit, safeguards, due diligence, internal controls, accreditation, environmental and social safeguards, risk assessment, regulatory compliance, monitoring, ESMP",
        "prototypes": [
            "risk management compliance and internal controls",
            "audit due diligence and regulatory compliance",
            "environmental and social safeguards and ESMP",
            "accreditation monitoring and compliance assessment",
        ],
    },
    {
        "name": "Organizational Reform & HR Management",
        "description": "organizational reform, organizational development, HR management, human resources, workforce planning, staff retreat, recruitment systems, payroll systems, job classification, organizational restructuring",
        "prototypes": [
            "organizational development and change management",
            "human resources workforce planning and recruitment systems",
            "staff management payroll systems and job classification",
            "organizational restructuring and HR reform",
        ],
    },
    {
        "name": "Employment & Skills Development",
        "description": "employment, job creation, employability, skills development, vocational skills, labor market, wage negotiation, job centres, youth employment, entrepreneurship training, workforce skills",
        "prototypes": [
            "employment action plans and labor market programs",
            "job creation employability and youth employment",
            "skills development vocational training and workforce skills",
            "job centres entrepreneurship training and wage negotiation",
        ],
    },
    {
        "name": "Business Strategy & Performance",
        "description": "business strategy, strategic planning, performance improvement, benchmarking, MSME competitiveness, enterprise development, productivity, growth strategy, business transformation, competitiveness",
        "prototypes": [
            "strategic planning and business transformation",
            "benchmarking performance improvement and productivity",
            "MSME competitiveness enterprise development and growth",
            "business strategy and operational performance",
        ],
    },
    {
        "name": "Marketing & Customer Experience",
        "description": "marketing, communications, public relations, photography, videography, digital advertising, media campaign, influencer partnerships, branding, customer experience, communication services",
        "prototypes": [
            "marketing communications and public relations services",
            "photography videography and media production",
            "digital advertising campaigns branding and promotion",
            "customer experience communication and outreach",
        ],
    },
    {
        "name": "Mining & Natural Resources",
        "description": "mining, extractive industries, natural resources, minerals, geological survey, mining governance, oil and gas, resource extraction, mining sector, mineral resources",
        "prototypes": [
            "mining extractive industries and mineral resources",
            "geological survey and natural resource assessment",
            "oil and gas and extractive sector governance",
            "natural resources and mining sector development",
        ],
    },

    # =========================================================================
    # FIXED: Social Protection & Poverty Reduction
    # Was too magnetic — grabbing titles with inclusive/recovery/empowerment/
    # resilience which appear everywhere in development project titles.
    # Now tightened to concrete social protection instruments only.
    # =========================================================================
    {
        "name": "Social Protection & Poverty Reduction",
        "description": (
            "social safety nets, cash transfer programs, social assistance delivery, "
            "poverty reduction strategies, social protection systems, "
            "livelihood support programs, humanitarian assistance, "
            "vulnerable populations support, disability support, refugee assistance, "
            "social welfare systems, economic inclusion programs, "
            "social pension schemes, food assistance, unconditional cash transfers, "
            "community-based social protection, social registry systems, "
            "targeted social assistance, poverty alleviation"
        ),
        "prototypes": [
            "social safety nets and cash transfer program for vulnerable populations",
            "poverty reduction and social assistance delivery system",
            "social protection systems and economic inclusion project",
            "humanitarian assistance and livelihood support program",
            "social welfare systems and community-based protection",
            "social registry and targeted social assistance program",
            "unconditional cash transfers and food assistance initiative",
            "refugee support and displaced population assistance program",
        ],
    },
]

# ---------------------------------------------------------------------
# OPTIONAL KEYWORD BOOSTING
# UPDATED for 4 fixed sectors
# ---------------------------------------------------------------------

SECTOR_KEYWORDS = {
    "Digital Transformation": [
        "digital", "digitalization", "digitalisation", "e-government",
        "egovernment", "platform", "transformation", "integration", "id"
    ],
    "Cybersecurity & Data Security": [
        "cyber", "cybersecurity", "security", "data protection",
        "privacy", "intrusion", "firewall", "vulnerability"
    ],
    "Data, AI & Analytics": [
        "data", "analytics", "statistics", "dashboard", "reporting",
        "monitoring", "ai", "machine learning", "survey"
    ],
    "Telecommunications": [
        "telecom", "telecommunications", "connectivity", "internet",
        "fiber", "fibre", "broadband", "sms", "messaging", "network"
    ],
    "Enterprise IT & Systems Implementation": [
        "software", "system", "systems", "platform", "erp", "crm",
        "ict", "application", "drupal", "implementation", "digital health information"
    ],
    "Energy & Utilities": [
        "energy", "electricity", "electric", "power", "solar", "hydro",
        "hydropower", "substation", "transmission", "transformer", "grid", "utility"
    ],
    "Construction & Infrastructure": [
        "construction", "rehabilitation", "renovation", "civil works",
        "building", "buildings", "infrastructure", "engineering", "design",
        "supervision", "travaux"
    ],

    # FIXED: added corridor, mobility, road safety, waterway, transit
    "Transport & Logistics": [
        "transport", "logistics", "railway", "port", "terminal", "freight",
        "shipping", "airport", "customs", "warehousing", "train",
        "corridor", "mobility", "transit", "highway", "motorway",
        "bus rapid", "waterway", "ferry", "trucking", "road safety",
        "traffic", "trade facilitation", "fleet"
    ],

    "Water, Sanitation & Waste": [
        "water", "sanitation", "sewerage", "sewer", "waste", "wastewater",
        "drainage", "borehole", "wash", "latrine", "solid waste", "assainissement"
    ],
    "Agriculture & Food Security": [
        "agriculture", "agricultural", "food", "seed", "seeds", "fertilizer",
        "irrigation", "livestock", "fisheries", "aquaculture",
        "cocoa", "coffee", "cassava", "agro", "horticulture"
    ],

    # FIXED: removed resilience, adaptation, recovery, environment (too generic)
    # kept only specific environmental terms
    "Environment & Climate": [
        "climate", "biodiversity", "ecosystem", "carbon", "emissions",
        "conservation", "deforestation", "pollution", "watershed",
        "wetland", "reforestation", "greenhouse", "blue economy",
        "net zero", "climate finance", "habitat"
    ],

    "Education & Training": [
        "education", "school", "schools", "classroom", "curriculum",
        "teacher", "teachers", "training", "learning", "literacy", "kindergarten"
    ],

    # FIXED: added preparedness, epidemic, outbreak, immunization, one health
    "Health & Life Sciences": [
        "health", "healthcare", "hospital", "hospitals", "clinic", "medical",
        "medicine", "pharmaceutical", "pharmaceuticals", "vaccination",
        "laboratory", "diagnostic", "preparedness", "epidemic", "outbreak",
        "surveillance", "immunization", "quarantine", "one health",
        "antimicrobial", "telemedicine"
    ],

    "Financial Services": [
        "finance", "financial", "bank", "banking", "insurance", "tax",
        "fiscal", "microfinance", "actuarial", "revenue"
    ],
    "Government Reform & Public Administration": [
        "government", "governance", "public sector", "public administration",
        "institutional", "decentralization", "accountability", "civil service", "policy"
    ],
    "Justice & Rule of Law": [
        "justice", "legal", "law", "judiciary", "court", "constitution",
        "legislation", "rule of law"
    ],
    "Risk & Compliance": [
        "risk", "compliance", "audit", "safeguards", "due diligence",
        "controls", "accreditation", "esmp", "monitoring", "evaluation"
    ],
    "Organizational Reform & HR Management": [
        "organizational", "organization", "human resources", "hr", "workforce",
        "recruitment", "payroll", "staff", "job classification"
    ],
    "Employment & Skills Development": [
        "employment", "jobs", "job", "employability", "skills", "vocational",
        "youth employment", "entrepreneurship", "wage"
    ],
    "Business Strategy & Performance": [
        "strategy", "strategic", "performance", "benchmarking",
        "competitiveness", "enterprise development", "msme", "productivity"
    ],
    "Marketing & Customer Experience": [
        "marketing", "communications", "public relations", "photography",
        "videography", "advertising", "media", "branding", "campaign", "influencer"
    ],
    "Mining & Natural Resources": [
        "mining", "extractive", "minerals", "natural resources", "oil",
        "gas", "geological"
    ],

    # FIXED: removed recovery, resilience, inclusion (too generic)
    # kept only specific social protection terms
    "Social Protection & Poverty Reduction": [
        "social protection", "safety net", "poverty", "cash transfer",
        "livelihood", "vulnerable", "refugee", "disability",
        "humanitarian", "social assistance", "welfare",
        "social pension", "food assistance", "social registry"
    ],
}

# ---------------------------------------------------------------------
# PROCUREMENT GROUP EXTRACTION
# ---------------------------------------------------------------------

_PREFIX_MAP: dict[str, str] = {
    "RFP": "CONSULTING", "REOI": "CONSULTING", "EOI": "CONSULTING",
    "IC": "CONSULTING", "RFI": "CONSULTING", "AMI": "CONSULTING",
    "LCC": "CONSULTING",
    "RFQ": "GOODS", "LPO": "GOODS", "ITQ": "GOODS",
    "ITB": "WORKS", "IFB": "WORKS", "LTL": "WORKS",
    "AON": "WORKS", "AOI": "WORKS",
    "SSS": "NON-CONSULTING",
}

_PROC_KEYWORDS: list[tuple[str, list[str]]] = [
    ("CONSULTING", [
        r"\bconsult(?:ing|ant|ancy|ance)s?\b", r"\bindividual\s+consultant\b",
        r"\btechnical\s+assistance\b", r"\bexperts?\b", r"\baudits?\b",
        r"\bfeasibility\b", r"\bassessment\b", r"\breview\b", r"\bstudy\b",
        r"\bsurvey\b", r"\b[eé]tudes?\b", r"\bassistance\s+technique\b",
        r"\bformation\b", r"\baudit\b",
    ]),
    ("WORKS", [
        r"\bconstruct(?:ion|ing)\b", r"\brehabilitat(?:ion|ing)\b",
        r"\bcivil\s+works?\b", r"\brenovation\b", r"\binstallation\b",
        r"\bbuilding\b", r"\bbridge\b", r"\broad\b", r"\bdam\b",
        r"\binfrastructure\b", r"\btravaux\b", r"\bréhabilitation\b",
    ]),
    ("NON-CONSULTING", [
        r"\bnon[\s\-]consult\w+\b", r"\bsecurity\s+(?:guard|services?)\b",
        r"\bcleaning\s+services?\b", r"\bmaintenance\s+services?\b",
        r"\btransport(?:ation)?\s+services?\b", r"\bcatering\b",
        r"\bprinting\b",
    ]),
    ("GOODS", [
        r"\bsuppl(?:y|ies|ier)\b", r"\bdelivery\s+of\b",
        r"\bsupply\s+and\s+delivery\b", r"\bprocurement\s+of\b",
        r"\bpurchase\s+of\b", r"\bequipment\b", r"\bvehicles?\b",
        r"\bfurniture\b", r"\bcomputers?\b", r"\bmedical\s+supplies\b",
        r"\bfournitures?\b", r"\bmatériels?\b", r"\bacquisition\s+de\b",
    ]),
]

_classifier = None
_keybert = None
_sector_embeddings = None

# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------

def extract_procurement_group_from_title(title: str) -> Optional[str]:
    if not title:
        return None

    prefix_match = re.match(r'^([A-Z]{2,6})', title.strip().upper())
    if prefix_match:
        prefix = prefix_match.group(1)
        if prefix in _PREFIX_MAP:
            candidate = _PREFIX_MAP[prefix]
            if candidate != "WORKS":
                return candidate

    lower = title.lower()
    for group, patterns in _PROC_KEYWORDS:
        for pat in patterns:
            if re.search(pat, lower, re.IGNORECASE):
                return group

    if prefix_match and _PREFIX_MAP.get(prefix_match.group(1).upper()) == "WORKS":
        return "WORKS"

    return None


def _load_classifier(device: str = "cpu"):
    global _classifier, _sector_embeddings
    if _classifier is not None:
        return _classifier

    from sentence_transformers import SentenceTransformer

    log.info("Loading embedding model: %s", EMBEDDING_MODEL_NAME)
    _classifier = SentenceTransformer(
        EMBEDDING_MODEL_NAME,
        device=device,
    )

    log.info("Encoding sector descriptions and prototypes...")
    _sector_embeddings = []

    for sector in SECTORS:
        desc_emb = _classifier.encode(
            sector["description"],
            normalize_embeddings=True,
            convert_to_tensor=True,
            show_progress_bar=False,
        )

        proto_embs = _classifier.encode(
            sector["prototypes"],
            normalize_embeddings=True,
            convert_to_tensor=True,
            show_progress_bar=False,
        )

        _sector_embeddings.append({
            "name": sector["name"],
            "desc": desc_emb,
            "protos": proto_embs,
        })

    log.info("Embedding classifier loaded.")
    return _classifier


def _load_keybert(device: str = "cpu"):
    global _keybert
    if _keybert is not None:
        return _keybert

    log.info("Loading KeyBERT (paraphrase-multilingual-MiniLM-L12-v2)...")
    from keybert import KeyBERT
    _keybert = KeyBERT(model="paraphrase-multilingual-MiniLM-L12-v2")
    log.info("KeyBERT loaded.")
    return _keybert


def _clean(text: Optional[str]) -> str:
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&[a-zA-Z]+;", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _strip_noise_prefixes(text: str) -> str:
    text = re.sub(
        r"^(rfq|itb|eoi|reoi|rfi|ic|ami|aoi|aon|lrps|lrfq|rfx|rfp|ppm|gpn|spn)[\s:/\-_.0-9]*",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"^[A-Z]{2,10}[-_/][A-Z0-9-_/]+", "", text)
    return re.sub(r"\s+", " ", text).strip()


def build_classification_text(tender_title: str) -> str:
    title = _clean(tender_title)
    title = _strip_noise_prefixes(title)
    return title


def build_keyword_text(tender_title: str, tender_description: Optional[str], portal: str) -> str:
    title = _clean(tender_title)

    if portal in DESCRIPTION_PORTALS and tender_description:
        desc = _clean(tender_description)
        desc_words = desc.split()
        if len(desc_words) > 300:
            desc = " ".join(desc_words[:300]) + "..."
        return f"{title}. {desc}" if desc else title

    return title


def _keyword_boost(text: str, sector_name: str) -> float:
    text_lower = text.lower()
    boost = 0.0

    for kw in SECTOR_KEYWORDS.get(sector_name, []):
        if kw in text_lower:
            boost += KEYWORD_BOOST_VALUE

    return min(boost, KEYWORD_BOOST_CAP)


def classify_sectors(text: str, classifier, return_debug: bool = False):
    if not text or not text.strip():
        return "Others", None, []

    meaningful_words = [w for w in re.findall(r"\w+", text) if len(w) > 2]
    if len(meaningful_words) < MIN_MEANINGFUL_WORDS:
        return "Others", None, []

    from sentence_transformers import util

    query = classifier.encode(
        text,
        normalize_embeddings=True,
        convert_to_tensor=True,
        show_progress_bar=False,
    )

    results = []
    for sector in _sector_embeddings:
        desc_score = float(util.cos_sim(query, sector["desc"]).item())
        proto_scores = util.cos_sim(query, sector["protos"])[0]
        best_proto = float(proto_scores.max().item())

        semantic_score = (
            DESCRIPTION_WEIGHT * desc_score +
            PROTOTYPE_WEIGHT * best_proto
        )
        boost = _keyword_boost(text, sector["name"])
        final_score = semantic_score + boost

        results.append((sector["name"], final_score))

    results.sort(key=lambda x: x[1], reverse=True)

    if not results:
        return "Others", None, []

    top_name, top_score = results[0]
    sec_name, sec_score = results[1] if len(results) > 1 else (None, 0.0)

    if top_score < PRIMARY_THRESHOLD:
        return "Others", None, results if return_debug else []

    # If top two are nearly tied and still weak, better return Others.
    if (top_score - sec_score) < MIN_GAP_FOR_OTHERS and top_score < (PRIMARY_THRESHOLD + 0.02):
        return "Others", None, results if return_debug else []

    secondary = None
    if (
        sec_name
        and sec_score >= SECONDARY_THRESHOLD
        and (top_score - sec_score) <= MAX_SECOND_GAP
    ):
        secondary = sec_name

    return top_name, secondary, results if return_debug else []


_STOP_WORDS = {
    "the", "and", "for", "with", "this", "that", "from", "into", "under",
    "have", "been", "will", "shall", "may", "its", "their", "project",
    "services", "service", "support", "development", "national", "provide",
    "including", "related", "based", "through", "within", "ensure",
    "les", "des", "pour", "dans", "par", "sur", "avec", "une", "son",
    "ses", "qui", "que", "est", "sont", "aux", "projet", "appui",
    "développement", "cadre",
}


def extract_keywords(text: str, keybert) -> list[str]:
    if not text or len(text.split()) < 4:
        return []

    try:
        raw_keywords = keybert.extract_keywords(
            text,
            keyphrase_ngram_range=(KEYWORD_MIN_NGRAM, KEYWORD_MAX_NGRAM),
            stop_words="english",
            use_mmr=True,
            diversity=0.5,
            top_n=KEYWORD_TOP_N + 3,
        )
    except Exception as e:
        log.warning("  KeyBERT failed: %s", e)
        return []

    keywords = []
    seen = set()

    for kw, _score in raw_keywords:
        kw_clean = kw.strip().lower()

        if kw_clean in _STOP_WORDS:
            continue
        if len(kw_clean) <= 2:
            continue
        if kw_clean.isdigit():
            continue
        if any(kw_clean in existing or existing in kw_clean for existing in seen):
            continue

        seen.add(kw_clean)
        keywords.append(kw_clean)

        if len(keywords) >= KEYWORD_TOP_N:
            break

    return keywords


def process_one_tender(tender_dict: dict, classifier, keybert) -> dict:
    portal = tender_dict["portal"]
    title = tender_dict.get("title") or ""
    description = tender_dict.get("description")

    classification_text = build_classification_text(title)
    keyword_text = build_keyword_text(title, description, portal)

    # -------------------------------------------------------------------------
    # LAYER 1 — keyword classifier (runs first, fast)
    # If confident (STOP), skip NLP entirely.
    # If HINT, NLP still runs but we pass the hint sector.
    # If NLP, run NLP blind as before.
    # -------------------------------------------------------------------------
    l1 = layer1_classify(title)
    l1_decision = l1["decision"]

    if l1_decision == "STOP":
        # Layer 1 is confident — use its sector, skip NLP
        primary   = l1["sector"]
        secondary = None
        log.info("  layer1 : STOP → %s  [%s]", primary, l1["keywords"])
    else:
        # Run NLP (blind or with hint)
        primary, secondary, _debug_scores = classify_sectors(
            classification_text, classifier
        )
        if l1_decision.startswith("HINT"):
            log.info("  layer1 : HINT → %s (NLP decided: %s)", l1["sector"], primary)

    keywords = extract_keywords(keyword_text, keybert)

    procurement_group = None
    if portal in ("ungm", "undp"):
        existing_pg = tender_dict.get("existing_procurement_group")
        if not existing_pg:
            procurement_group = extract_procurement_group_from_title(title)

    return {
        "primary_sector": primary,
        "secondary_sector": secondary,
        "keywords": json.dumps(keywords, ensure_ascii=False),
        "procurement_group": procurement_group,
    }


def update_enriched_tender(session, enriched_id: int, result: dict) -> None:
    from sqlalchemy import text

    sectors = []
    if result["primary_sector"]:
        sectors.append(result["primary_sector"])
    if result["secondary_sector"]:
        sectors.append(result["secondary_sector"])

    sectors_json = json.dumps(sectors, ensure_ascii=False)
    keywords_json = result["keywords"]
    now = datetime.now(timezone.utc)

    params = {
        "sector": sectors_json,
        "keywords": keywords_json,
        "enrichment_status": "nlp_complete",
        "enriched_at": now,
        "enriched_id": enriched_id,
    }

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

    kw_list = json.loads(keywords_json)
    log.info(
        "  ✓ id=%-6s  sectors=%-45s  kw_count=%d%s",
        enriched_id,
        str(sectors),
        len(kw_list),
        f"  pg={pg}" if pg else "",
    )


def run_nlp_enrichment(
    dry_run: bool = False,
    limit: int | None = None,
    portals: list[str] | None = None,
    device: str = "cpu",
) -> None:
    try:
        from db import get_session
        from models import EnrichedTender, Tender
        from sqlalchemy import select
    except ImportError as e:
        log.error("Import error — run from project root: %s", e)
        return

    portals = [p.lower() for p in (portals or DEFAULT_PORTALS)]
    counters = dict(total=0, success=0, failed=0, skipped=0)

    classifier = _load_classifier(device)
    keybert = _load_keybert(device)

    with get_session() as session:
        stmt = (
            select(
                EnrichedTender.id,
                EnrichedTender.tender_id,
                EnrichedTender.source_portal,
                EnrichedTender.procurement_group,
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
                "enriched_tender_id": row.id,
                "tender_id": row.tender_id,
                "portal": row.source_portal,
                "title": row.title,
                "description": row.description,
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
        title_full = (td.get("title") or "").strip()
        title_preview = title_full[:100] + ("..." if len(title_full) > 100 else "")
        log.info(
            "[%d/%d] id=%-6s  portal=%s",
            i, counters["total"], td["enriched_tender_id"], td["portal"],
        )
        log.info("  title   : %s", title_preview)

        try:
            result = process_one_tender(td, classifier, keybert)

            sectors = [s for s in [result["primary_sector"], result["secondary_sector"]] if s]
            kws = json.loads(result["keywords"])
            log.info("  sectors : %s", sectors)
            if kws:
                log.info("  keywords: %s", ", ".join(kws))
            if result.get("procurement_group"):
                log.info("  proc_grp: %s", result["procurement_group"])

            if dry_run:
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Stage 3 — Embedding-based sector classification and keyword extraction."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run NLP and print results without writing to DB.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Process at most N tenders.",
    )
    parser.add_argument(
        "--portals",
        nargs="+",
        default=DEFAULT_PORTALS,
        metavar="PORTAL",
        help="Source portals to process (default: all four).",
    )
    parser.add_argument(
        "--device",
        default="cpu",
        choices=["cpu", "cuda"],
        help="Device for model inference (default: cpu).",
    )
    args = parser.parse_args()

    run_nlp_enrichment(
        dry_run=args.dry_run,
        limit=args.limit,
        portals=args.portals,
        device=args.device,
    )