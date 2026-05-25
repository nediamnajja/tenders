"""
normalizer/common.py
====================
Reads raw tenders from the `tenders` table (all portals),
normalizes the common fields, and upserts into `normalized_tenders`.

Portals handled: worldbank | afdb | ungm | undp

Run:
    python normalizer/common.py                  # all portals
    python normalizer/common.py --portal ungm    # one portal only
    python normalizer/common.py --dry-run        # no DB writes
"""

import argparse
import json
import logging
import os
import re
import sys
import unicodedata
from datetime import datetime, timezone
from typing import Optional

# ── Project root on sys.path ─────────────────────────────────────────────────
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from db import get_session
from models import (
    Contact,
    NormalizationLog,
    NormalizedTender,
    Organisation,
    Tender,
)

# ─────────────────────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
#  LOOKUP MAPS
# ─────────────────────────────────────────────────────────────────────────────

# ── Notice type: raw portal value → internal code ────────────────────────────
NOTICE_TYPE_MAP: dict[str, str] = {
    # Planning
    "PPM":                                "PPM",
    "AGPM":                               "GPN",
    # EOI
    "AMI":                                "EOI",
    "Request for EOI":                    "EOI",
    "Request for Expression of Interest": "EOI",
    "Call for individual consultants":    "EOI",
    "Call for implementing partners":     "EOI",
    # RFI
    "RFI - Request for Information":      "RFI",
    # Prequalification
    "Invitation for Prequalification":    "PREQUAL",
    # RFP
    "Request for Proposals":              "RFP",
    "Request for proposal":               "RFP",
    "RFP - Request for proposal":         "RFP",
    "Grant support-call for proposal":    "RFP",
    # IFB
    "Invitation for Bids":                "IFB",
    "Invitation to bid":                  "IFB",
    "ITB - Invitation to bid":            "IFB",
    "Request for Bids":                   "IFB",
    "SPN":                                "IFB",
    "AOI":                                "IFB",
    "AAO":                                "IFB",
    # RFQ
    "Request for quotation":              "RFQ",
    "RFQ - Request for quotation":        "RFQ",
}

# ── Internal code → full display name ────────────────────────────────────────
NOTICE_TYPE_LABELS: dict[str, str] = {
    "PPM":     "Prior Procurement Plan",
    "GPN":     "General Procurement Notice",
    "EOI":     "Expression of Interest",
    "RFI":     "Request for Information",
    "PREQUAL": "Invitation for Prequalification",
    "RFP":     "Request for Proposals",
    "IFB":     "Invitation for Bids",
    "RFQ":     "Request for Quotation",
    "AWARD":   "Contract Award",
}

# ── Notice code → (lifecycle_stage, status_normalized) ───────────────────────
LIFECYCLE_MAP: dict[str, tuple[str, str]] = {
    "PPM":     ("early_intelligence", "programming"),
    "GPN":     ("early_intelligence", "forecast"),
    "EOI":     ("procurement",        "open"),
    "RFI":     ("procurement",        "open"),
    "PREQUAL": ("procurement",        "open"),
    "RFP":     ("procurement",        "open"),
    "IFB":     ("procurement",        "open"),
    "RFQ":     ("procurement",        "open"),
    "AWARD":   ("implementation",     "awarded"),
}

# ── Procurement group: code or full name → normalized pipeline value ─────────
PROCUREMENT_GROUP_MAP: dict[str, str] = {
    # WorldBank codes
    "CS":                      "CONSULTING",
    "CW":                      "WORKS",
    "GO":                      "GOODS",
    "NC":                      "NON-CONSULTING",
    # Full name variants
    "Civil Works":             "WORKS",
    "Consulting Services":     "CONSULTING",
    "Goods":                   "GOODS",
    "Non-Consulting Services": "NON-CONSULTING",
}

# ── Procurement method: code or full name → full name ────────────────────────
PROCUREMENT_METHOD_MAP: dict[str, str] = {
    "CQS":  "Consultant Qualification Selection",
    "ICS":  "Individual Consultant Selection",
    "QBS":  "Quality Based Selection",
    "QCBS": "Quality and Cost-Based Selection",
    "LCS":  "Least Cost Selection",
    "IC":   "Individual Contractor",
    "Consultant Qualification Selection": "Consultant Qualification Selection",
    "Individual Consultant Selection":    "Individual Consultant Selection",
    "Quality Based Selection":            "Quality Based Selection",
    "Quality and Cost-Based Selection":   "Quality and Cost-Based Selection",
    "Least Cost Selection":               "Least Cost Selection",
    "IC - Individual contractor":         "Individual Contractor",
}

# ── ISO-4217 currency ─────────────────────────────────────────────────────────
CURRENCY_MAP: dict[str, str] = {
    "USD": "USD", "US$": "USD", "US DOLLAR": "USD", "DOLLAR": "USD",
    "EUR": "EUR", "EURO": "EUR", "€": "EUR",
    "GBP": "GBP", "POUND": "GBP", "£": "GBP",
    "XOF": "XOF", "FCFA": "XOF", "CFA": "XOF",
    "XAF": "XAF",
    "KES": "KES", "SHILLING": "KES",
    "NGN": "NGN", "NAIRA": "NGN",
    "GHS": "GHS", "CEDI": "GHS",
    "ZAR": "ZAR", "RAND": "ZAR",
    "ETB": "ETB", "BIRR": "ETB",
    "TZS": "TZS", "UGX": "UGX", "RWF": "RWF", "MZN": "MZN",
    "CHF": "CHF", "FRANC": "CHF",
    "JPY": "JPY", "YEN": "JPY",
    "CNY": "CNY", "RMB": "CNY",
    "CAD": "CAD", "AUD": "AUD",
    "INR": "INR", "RUPEE": "INR",
}

# ── Language ──────────────────────────────────────────────────────────────────
LANGUAGE_MAP: dict[str, str] = {
    "english":    "English", "en": "English", "eng": "English",
    "french":     "French",  "fr": "French",  "fra": "French",  "français": "French",
    "spanish":    "Spanish", "es": "Spanish", "spa": "Spanish",
    "arabic":     "Arabic",  "ar": "Arabic",
    "portuguese": "Portuguese", "pt": "Portuguese",
    "russian":    "Russian", "ru": "Russian",
}

# ── Country aliases ───────────────────────────────────────────────────────────
# Keys: lowercased + stripped (all variants we've ever seen)
# Values: single canonical English name
#
# Rules:
#   - Always add BOTH the accented French form AND the unaccented form
#   - Always add ALL apostrophe variants (straight ' and curly \u2019)
#   - UN official long names map to our short canonical names
COUNTRY_ALIASES: dict[str, str] = {

    # ── Tunisia ───────────────────────────────────────────────────────────────
    "tunisie":                               "Tunisia",
    "tunisia":                               "Tunisia",

    # ── Morocco ───────────────────────────────────────────────────────────────
    "maroc":                                 "Morocco",
    "morocco":                               "Morocco",

    # ── Algeria ───────────────────────────────────────────────────────────────
    "algérie":                               "Algeria",
    "algerie":                               "Algeria",
    "algeria":                               "Algeria",

    # ── Côte d'Ivoire ─────────────────────────────────────────────────────────
    "côte d'ivoire":                         "Côte d'Ivoire",   # accented, straight apostrophe
    "cote d'ivoire":                         "Côte d'Ivoire",   # unaccented, straight apostrophe
    "côte d\u2019ivoire":                    "Côte d'Ivoire",   # accented, curly apostrophe
    "cote d\u2019ivoire":                    "Côte d'Ivoire",   # unaccented, curly apostrophe
    "cote divoire":                          "Côte d'Ivoire",   # no apostrophe
    "ivory coast":                           "Côte d'Ivoire",

    # ── Senegal ───────────────────────────────────────────────────────────────
    "sénégal":                               "Senegal",
    "senegal":                               "Senegal",

    # ── Guinea ────────────────────────────────────────────────────────────────
    "guinée":                                "Guinea",
    "guinee":                                "Guinea",
    "guinea":                                "Guinea",

    # ── Guinea-Bissau ─────────────────────────────────────────────────────────
    "guinée-bissau":                         "Guinea-Bissau",
    "guinee-bissau":                         "Guinea-Bissau",
    "guinée bissau":                         "Guinea-Bissau",
    "guinee bissau":                         "Guinea-Bissau",
    "guinea-bissau":                         "Guinea-Bissau",

    # ── Equatorial Guinea ─────────────────────────────────────────────────────
    "guinée équatoriale":                    "Equatorial Guinea",
    "guinee equatoriale":                    "Equatorial Guinea",
    "guinée equatoriale":                    "Equatorial Guinea",
    "equatorial guinea":                     "Equatorial Guinea",

    # ── Benin ─────────────────────────────────────────────────────────────────
    "bénin":                                 "Benin",
    "benin":                                 "Benin",

    # ── Chad ──────────────────────────────────────────────────────────────────
    "tchad":                                 "Chad",
    "chad":                                  "Chad",

    # ── Cameroon ─────────────────────────────────────────────────────────────
    "cameroun":                              "Cameroon",
    "cameroon":                              "Cameroon",

    # ── Comoros ───────────────────────────────────────────────────────────────
    "comores":                               "Comoros",
    "comoros":                               "Comoros",

    # ── Mauritania ────────────────────────────────────────────────────────────
    "mauritanie":                            "Mauritania",
    "mauritania":                            "Mauritania",

    # ── Syria ─────────────────────────────────────────────────────────────────
    "syrian arab republic":                  "Syria",
    "syria":                                 "Syria",

    # ── Turkey ───────────────────────────────────────────────────────────────
    "türkiye":                               "Turkey",
    "turkiye":                               "Turkey",
    "turkey":                                "Turkey",

    # ── Kyrgyzstan ────────────────────────────────────────────────────────────
    "kyrgyz republic":                       "Kyrgyzstan",
    "kyrgyzstan":                            "Kyrgyzstan",

    # ── Moldova ───────────────────────────────────────────────────────────────
    "republic of moldova":                   "Moldova",
    "moldova":                               "Moldova",

    # ── Tanzania ──────────────────────────────────────────────────────────────
    "united republic of tanzania":           "Tanzania",
    "tanzania":                              "Tanzania",

    # ── North Macedonia ───────────────────────────────────────────────────────
    "republic of north macedonia":           "North Macedonia",
    "north macedonia":                       "North Macedonia",

    # ── DR Congo ──────────────────────────────────────────────────────────────
    "democratic republic of the congo":      "DR Congo",
    "democratic republic of congo":          "DR Congo",
    "congo, democratic republic":            "DR Congo",
    "drc":                                   "DR Congo",
    "drc - angola":                          "DR Congo",
    "dr congo":                              "DR Congo",

    # ── Congo (Republic) ──────────────────────────────────────────────────────
    "congo":                                 "Congo",
    "republic of the congo":                 "Congo",
    "congo, rep.":                           "Congo",

    # ── Bolivia ───────────────────────────────────────────────────────────────
    "plurinational state of bolivia":        "Bolivia",
    "bolivia":                               "Bolivia",

    # ── Vietnam ───────────────────────────────────────────────────────────────
    "viet nam":                              "Vietnam",
    "vietnam":                               "Vietnam",

    # ── Laos ──────────────────────────────────────────────────────────────────
    "lao people's democratic republic":      "Laos",
    "lao people\u2019s democratic republic": "Laos",
    "lao pdr":                               "Laos",
    "lao":                                   "Laos",
    "laos":                                  "Laos",

    # ── Palestine ─────────────────────────────────────────────────────────────
    "palestinian territories":               "Palestine",
    "palestinian territory":                 "Palestine",
    "west bank and gaza":                    "Palestine",
    "west bank":                             "Palestine",
    "gaza":                                  "Palestine",
    "palestine":                             "Palestine",

    # ── Eswatini (formerly Swaziland) ─────────────────────────────────────────
    "swaziland":                             "Eswatini",
    "eswatini":                              "Eswatini",

    # ── Timor-Leste ───────────────────────────────────────────────────────────
    "timor leste":                           "Timor-Leste",
    "timor-leste":                           "Timor-Leste",

    # ── Cape Verde ────────────────────────────────────────────────────────────
    "cabo verde":                            "Cape Verde",
    "cape verde":                            "Cape Verde",

    # ── Sao Tome and Principe ─────────────────────────────────────────────────
    "sao tome and principe":                 "São Tomé and Príncipe",
    "são tome and príncipe":                 "São Tomé and Príncipe",
    "são tomé and príncipe":                 "São Tomé and Príncipe",

    # ── Malawi (typo fix) ─────────────────────────────────────────────────────
    "malawai":                               "Malawi",
    "malawi":                                "Malawi",

    # ── United States ─────────────────────────────────────────────────────────
    "united states of america":              "United States",
    "united states":                         "United States",
    "usa":                                   "United States",

    # ── Bosnia ────────────────────────────────────────────────────────────────
    "bosnia and herzegovina":                "Bosnia and Herzegovina",

    # ── Micronesia ────────────────────────────────────────────────────────────
    "federated states of micronesia":        "Micronesia",
    "micronesia":                            "Micronesia",

    # ── Korea ─────────────────────────────────────────────────────────────────
    "south korea":                           "Republic of Korea",
    "republic of korea":                     "Republic of Korea",
    "north korea":                           "North Korea",
    "democratic people's republic of korea": "North Korea",

    # ── Russia ────────────────────────────────────────────────────────────────
    "russia":                                "Russian Federation",
    "russian federation":                    "Russian Federation",

    # ── Iran ──────────────────────────────────────────────────────────────────
    "iran":                                  "Iran",
    "islamic republic of iran":              "Iran",

    # ── Saint Lucia / Sint Maarten ────────────────────────────────────────────
    "st. lucia":                             "Saint Lucia",
    "saint lucia":                           "Saint Lucia",
    "st maarten":                            "Sint Maarten",
    "sint maarten":                          "Sint Maarten",

    # ── Trinidad ──────────────────────────────────────────────────────────────
    "trinidad":                              "Trinidad and Tobago",
    "trinidad and tobago":                   "Trinidad and Tobago",

    # ── Saint Kitts ───────────────────────────────────────────────────────────
    "saint kitts":                           "Saint Kitts and Nevis",
    "saint kitts and nevis":                 "Saint Kitts and Nevis",

    # ── Already clean — ensure consistent casing ──────────────────────────────
    "pakistan":                              "Pakistan",
    "india":                                 "India",
    "nigeria":                               "Nigeria",
    "bangladesh":                            "Bangladesh",
    "ukraine":                               "Ukraine",
    "colombia":                              "Colombia",
    "madagascar":                            "Madagascar",
    "somalia":                               "Somalia",
    "kenya":                                 "Kenya",
    "lebanon":                               "Lebanon",
    "afghanistan":                           "Afghanistan",
    "burundi":                               "Burundi",
    "philippines":                           "Philippines",
    "mozambique":                            "Mozambique",
    "angola":                                "Angola",
    "ethiopia":                              "Ethiopia",
    "uzbekistan":                            "Uzbekistan",
    "south sudan":                           "South Sudan",
    "brazil":                                "Brazil",
    "indonesia":                             "Indonesia",
    "nepal":                                 "Nepal",
    "uganda":                                "Uganda",
    "liberia":                               "Liberia",
    "honduras":                              "Honduras",
    "niger":                                 "Niger",
    "sierra leone":                          "Sierra Leone",
    "montenegro":                            "Montenegro",
    "mongolia":                              "Mongolia",
    "zimbabwe":                              "Zimbabwe",
    "switzerland":                           "Switzerland",
    "sri lanka":                             "Sri Lanka",
    "jordan":                                "Jordan",
    "central african republic":              "Central African Republic",
    "albania":                               "Albania",
    "panama":                                "Panama",
    "haiti":                                 "Haiti",
    "kazakhstan":                            "Kazakhstan",
    "tajikistan":                            "Tajikistan",
    "guatemala":                             "Guatemala",
    "ecuador":                               "Ecuador",
    "egypt":                                 "Egypt",
    "gambia":                                "Gambia",
    "georgia":                               "Georgia",
    "lesotho":                               "Lesotho",
    "cambodia":                              "Cambodia",
    "peru":                                  "Peru",
    "djibouti":                              "Djibouti",
    "togo":                                  "Togo",
    "armenia":                               "Armenia",
    "china":                                 "China",
    "mali":                                  "Mali",
    "zambia":                                "Zambia",
    "argentina":                             "Argentina",
    "chile":                                 "Chile",
    "burkina faso":                          "Burkina Faso",
    "iraq":                                  "Iraq",
    "sudan":                                 "Sudan",
    "serbia":                                "Serbia",
    "ghana":                                 "Ghana",
    "yemen":                                 "Yemen",
    "mexico":                                "Mexico",
    "venezuela":                             "Venezuela",
    "cuba":                                  "Cuba",
    "papua new guinea":                      "Papua New Guinea",
    "italy":                                 "Italy",
    "thailand":                              "Thailand",
    "el salvador":                           "El Salvador",
    "maldives":                              "Maldives",
    "rwanda":                                "Rwanda",
    "gabon":                                 "Gabon",
    "myanmar":                               "Myanmar",
    "libya":                                 "Libya",
    "paraguay":                              "Paraguay",
    "south africa":                          "South Africa",
    "suriname":                              "Suriname",
    "denmark":                               "Denmark",
    "kiribati":                              "Kiribati",
    "costa rica":                            "Costa Rica",
    "dominican republic":                    "Dominican Republic",
    "namibia":                               "Namibia",
    "seychelles":                            "Seychelles",
    "samoa":                                 "Samoa",
    "fiji":                                  "Fiji",
    "belize":                                "Belize",
    "malaysia":                              "Malaysia",
    "turkmenistan":                          "Turkmenistan",
    "romania":                               "Romania",
    "bhutan":                                "Bhutan",
    "kosovo":                                "Kosovo",
    "jamaica":                               "Jamaica",
    "bahrain":                               "Bahrain",
    "solomon islands":                       "Solomon Islands",
    "guyana":                                "Guyana",
    "cyprus":                                "Cyprus",
    "vanuatu":                               "Vanuatu",
    "barbados":                              "Barbados",
    "marshall islands":                      "Marshall Islands",
    "united kingdom":                        "United Kingdom",
    "mauritius":                             "Mauritius",
    "netherlands":                           "Netherlands",
    "qatar":                                 "Qatar",
    "tonga":                                 "Tonga",
    "germany":                               "Germany",
    "eritrea":                               "Eritrea",
    "belarus":                               "Belarus",
    "dominica":                              "Dominica",
    "botswana":                              "Botswana",
    "uruguay":                               "Uruguay",
    "nicaragua":                             "Nicaragua",
    "portugal":                              "Portugal",
    "canada":                                "Canada",
    "bahamas":                               "Bahamas",
    "greece":                                "Greece",
    "tuvalu":                                "Tuvalu",
    "norway":                                "Norway",
    "azerbaijan":                            "Azerbaijan",
    "western sahara":                        "Western Sahara",
    "bulgaria":                              "Bulgaria",
    "france":                                "France",
    "antigua and barbuda":                   "Antigua and Barbuda",
    "somaliland":                            "Somaliland",
    "saudi arabia":                          "Saudi Arabia",

    # ── Regional / multi-country ──────────────────────────────────────────────
    "multiple destinations":                 "Multiple Countries",
    "multinational":                         "Multiple Countries",
    "multi-country":                         "Multiple Countries",
    "various":                               "Multiple Countries",
    "global":                                "Multiple Countries",
    "worldwide":                             "Multiple Countries",
    "eastern and southern africa":           "Eastern and Southern Africa",
    "western and central africa":            "Western and Central Africa",
    "central asia":                          "Central Asia",
    "east asia and pacific":                 "East Asia and Pacific",
    "caribbean":                             "Caribbean",
    "western balkans":                       "Western Balkans",
    "pacific 1":                             "Pacific",
    "pacific 2":                             "Pacific",
    "southwest indian ocean":                "Southwest Indian Ocean",
    "horn of africa":                        "Horn of Africa",
    "southern africa":                       "Southern Africa",
    "central africa":                        "Central Africa",
    "oecs countries":                        "OECS Countries",
    "regional":                              "Regional",
}

# Keywords that trigger is_multi_country = True
_GLOBAL_KEYWORDS = {
    "multiple countries", "multinational", "multi-country",
    "various", "global", "worldwide", "regional",
    "eastern and southern africa", "western and central africa",
    "central asia", "east asia and pacific", "caribbean",
    "western balkans", "pacific", "southwest indian ocean",
    "horn of africa", "southern africa", "central africa",
    "oecs countries",
}


# ─────────────────────────────────────────────────────────────────────────────
#  AfDB TITLE — notice type and country extraction
# ─────────────────────────────────────────────────────────────────────────────

_AFDB_NOTICE_PREFIX_MAP: dict[str, str] = {
    "AMI":    "EOI",
    "AOI":    "IFB",
    "AAO":    "IFB",
    "AON":    "IFB",
    "SPN":    "IFB",
    "GPN":    "GPN",
    "AGPM":   "GPN",
    "PPM":    "GPN",
    "EOI":    "EOI",
    "REOI":   "EOI",
    "RFP":    "RFP",
    "RFQ":    "RFQ",
    "attribution de contrats":            "AWARD",
    "avis d'attribution":                 "AWARD",
    "contract award":                     "AWARD",
    "invitation for bids":                "IFB",
    "request for proposals":              "RFP",
    "request for expression of interest": "EOI",
    "general procurement notice":         "GPN",
}

_AFDB_COUNTRY_ABBR: dict[str, str] = {
    "RDC":           "DR Congo",
    "DRC":           "DR Congo",
    "RCA":           "Central African Republic",
    "CAR":           "Central African Republic",
    "ROC":           "Congo",
    "MULTINATIONAL": "Multiple Countries",
}

_AFDB_BODY_SIGNALS = {
    "supply", "design", "construction", "provision", "consulting",
    "recruitment", "recrutement", "technical", "services", "upgrading",
    "study", "acquisition", "rehabilitation", "hiring", "support",
}


def _parse_afdb_title(raw_title: str) -> tuple[str | None, str | None]:
    if not raw_title:
        return None, None

    title    = raw_title.replace("\u2013", "-").replace("\u2014", "-")
    segments = [s.strip() for s in re.split(r"\s+-\s+", title) if s.strip()]

    if not segments:
        return None, None

    notice_code = None
    seg0_upper  = segments[0].upper()
    seg0_lower  = segments[0].lower()

    if seg0_upper in _AFDB_NOTICE_PREFIX_MAP:
        notice_code = _AFDB_NOTICE_PREFIX_MAP[seg0_upper]
    else:
        for phrase, code in _AFDB_NOTICE_PREFIX_MAP.items():
            if seg0_lower == phrase:
                notice_code = code
                break

    country_raw = None
    if notice_code is not None and len(segments) >= 2:
        seg1     = segments[1].strip()
        expanded = _AFDB_COUNTRY_ABBR.get(seg1.upper())
        if expanded:
            country_raw = expanded
        else:
            first_word = seg1.split()[0].lower() if seg1 else ""
            if (
                "," not in seg1
                and first_word not in _AFDB_BODY_SIGNALS
                and not re.search(r"\d", seg1)
                and len(seg1) <= 40
            ):
                country_raw = seg1

    return notice_code, country_raw


def _extract_afdb_notice_and_country(tender) -> tuple:
    notice_code = normalize_notice_type(tender.notice_type)
    title_code, title_country = _parse_afdb_title(tender.title or "")

    if not notice_code and title_code:
        notice_code = title_code

    notice_label      = notice_type_label(notice_code)
    raw_notice        = (tender.notice_type or "").strip() or None
    notice_type_final = notice_label or notice_code or raw_notice

    country_source                         = tender.country or title_country
    country_norm, is_multi, countries_json = normalize_country(country_source)

    return notice_code, notice_type_final, country_norm, is_multi, countries_json


# ─────────────────────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ── Date parsing ──────────────────────────────────────────────────────────────

_DATE_FORMATS = [
    "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d",
    "%d %b %Y", "%d %B %Y", "%B %d, %Y", "%b %d, %Y",
    "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S",
]


def parse_date(raw: Optional[str], time_part: Optional[str] = None) -> Optional[datetime]:
    if not raw:
        return None
    raw = str(raw).strip()

    if time_part:
        combined = f"{raw} {str(time_part).strip()}"
        for fmt in ["%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S",
                    "%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S"]:
            try:
                return datetime.strptime(combined, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                pass

    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    log.debug("Could not parse date: %r", raw)
    return None


def days_between(from_dt: Optional[datetime], to_dt: Optional[datetime]) -> Optional[int]:
    if from_dt is None or to_dt is None:
        return None
    return (to_dt - from_dt).days


# ── Budget parsing ────────────────────────────────────────────────────────────

_BUDGET_RE = re.compile(
    r"([A-Z]{2,4}|US\$|€|£|¥)?\s*([\d,\.]+)\s*([A-Z]{2,4}|US\$|€|£|¥)?",
    re.IGNORECASE,
)
_MULTIPLIERS = {
    "K": 1_000, "M": 1_000_000, "MN": 1_000_000, "MILLION": 1_000_000,
    "B": 1_000_000_000, "BILLION": 1_000_000_000,
}


def parse_budget(raw: Optional[str]) -> tuple[Optional[float], Optional[str]]:
    if not raw:
        return None, None
    text = str(raw).strip().upper()
    text = re.sub(r"(\d)\s(\d)", r"\1\2", text)

    m = _BUDGET_RE.search(text)
    if not m:
        return None, None

    curr_before, number_str, curr_after = m.group(1), m.group(2), m.group(3)
    raw_currency = curr_before or curr_after or ""
    number_str = number_str.replace(",", "").replace(" ", "")
    try:
        value = float(number_str)
    except ValueError:
        return None, None

    suffix_match = re.search(r"(\d)\s*(MILLION|BILLION|MN|[KMB])\b", text)
    if suffix_match:
        value *= _MULTIPLIERS.get(suffix_match.group(2), 1)

    currency_iso = CURRENCY_MAP.get(raw_currency.strip()) if raw_currency else None
    return value, currency_iso


# ── Notice type ───────────────────────────────────────────────────────────────

def normalize_notice_type(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    raw = str(raw).strip()
    code = NOTICE_TYPE_MAP.get(raw)
    if code:
        return code
    for key, val in NOTICE_TYPE_MAP.items():
        if key.lower() == raw.lower():
            return val
    return None


def notice_type_label(code: Optional[str]) -> Optional[str]:
    return NOTICE_TYPE_LABELS.get(code) if code else None


# ── Lifecycle + status ────────────────────────────────────────────────────────

def resolve_lifecycle(
    notice_code: Optional[str],
    deadline_dt: Optional[datetime],
    status_id:   Optional[int],
) -> tuple[Optional[str], Optional[str]]:
    now = _now_utc()
    AWARD_STATUS_IDS     = {5, 6, 50, 51}
    SHORTLIST_STATUS_IDS = {3, 30}

    if status_id in AWARD_STATUS_IDS:
        return "implementation", "awarded"
    if status_id in SHORTLIST_STATUS_IDS:
        return "procurement", "shortlisted"
    if deadline_dt and deadline_dt < now:
        return "procurement", "closed"
    if notice_code:
        return LIFECYCLE_MAP.get(notice_code, (None, None))
    return None, None


# ── Procurement group ─────────────────────────────────────────────────────────

def normalize_procurement_group(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    raw = str(raw).strip()
    full = PROCUREMENT_GROUP_MAP.get(raw.upper())
    if full:
        return full
    for _, name in PROCUREMENT_GROUP_MAP.items():
        if raw.lower() == name.lower():
            return name
    return raw


# ── Procurement method ────────────────────────────────────────────────────────

def normalize_procurement_method(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    raw = str(raw).strip()
    result = PROCUREMENT_METHOD_MAP.get(raw)
    if result:
        return result
    for key, val in PROCUREMENT_METHOD_MAP.items():
        if key.lower() == raw.lower():
            return val
    return raw


# ── Language ──────────────────────────────────────────────────────────────────

def normalize_language(raw: Optional[str]) -> Optional[str]:
    """Returns full English name e.g. 'French', 'English' — matches DB values."""
    if not raw:
        return None
    return LANGUAGE_MAP.get(str(raw).strip().lower(), str(raw).strip())


# ── Country ───────────────────────────────────────────────────────────────────

def _lookup_alias(key: str) -> Optional[str]:
    """
    4-step lookup against COUNTRY_ALIASES:
      1. Direct match
      2. Strip Unicode accents, try again
      3. Replace curly apostrophes with straight, try again
      4. Both accent-strip + apostrophe replace, try again
    Returns canonical name or None if no match found.
    """
    # 1. direct
    if key in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[key]

    # 2. strip accents
    key_na = "".join(
        c for c in unicodedata.normalize("NFD", key)
        if unicodedata.category(c) != "Mn"
    )
    if key_na in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[key_na]

    # 3. normalize apostrophes
    key_ap = key.replace("\u2019", "'").replace("\u2018", "'")
    if key_ap in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[key_ap]

    # 4. both
    key_ap_na = "".join(
        c for c in unicodedata.normalize("NFD", key_ap)
        if unicodedata.category(c) != "Mn"
    )
    if key_ap_na in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[key_ap_na]

    return None


def normalize_country(raw: Optional[str]) -> tuple[Optional[str], bool, Optional[str]]:
    """
    Returns (country_name_normalized, is_multi_country, countries_list_json).

    Handles:
    - Single country strings in English or French, any casing
    - Multi-country strings separated by ; , | /
    - UN official long names → short canonical names
    - Accented characters and curly apostrophes
    - Regional / global keywords
    """
    if not raw:
        return None, False, None

    raw = str(raw).strip()

    # split on common separators to handle multi-country strings
    parts = [p.strip() for p in re.split(r"[;,|/]", raw) if p.strip()]

    normalized_parts = []
    for part in parts:
        key      = part.lower()
        resolved = _lookup_alias(key)
        if resolved:
            normalized_parts.append(resolved)
        else:
            # unknown → title-case as fallback, same as old behaviour
            normalized_parts.append(part.title())

    is_multi = len(normalized_parts) > 1

    # check if any part is a global/regional keyword
    if any(p.lower() in _GLOBAL_KEYWORDS for p in normalized_parts):
        return "Multiple Countries", True, json.dumps(normalized_parts)

    primary        = normalized_parts[0] if normalized_parts else None
    countries_json = json.dumps(normalized_parts) if is_multi else None

    return primary, is_multi, countries_json


# ── Organisation name ─────────────────────────────────────────────────────────

def normalize_org_name(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    return " ".join(str(raw).strip().split())


# ── Funding agency ────────────────────────────────────────────────────────────

_PORTAL_FUNDING_AGENCY: dict[str, str] = {
    "worldbank": "World Bank",
    "undp":      "UNDP",
    "afdb":      "AFDB",
}


def resolve_funding_agency(
    portal: str,
    org_name_normalized: Optional[str],
) -> Optional[str]:
    static = _PORTAL_FUNDING_AGENCY.get(portal)
    if static:
        return static
    if portal == "ungm":
        return org_name_normalized
    return None


# ── Text cleaning ─────────────────────────────────────────────────────────────

def _clean_text(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    text = re.sub(r"<[^>]+>", " ", str(raw))
    text = re.sub(r"&[a-zA-Z]+;", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


# ─────────────────────────────────────────────────────────────────────────────
#  UNGM / AfDB IC title patterns
# ─────────────────────────────────────────────────────────────────────────────

_UNGM_IC_TITLE_RE = re.compile(
    r'\b('
    r'Individual\s+Consultant'
    r'|Individual\s+Contractor'
    r'|International\s*/\s*National\s+Consultant'
    r'|IC\s*\(Individual\s+Contractor\)'
    r'|External\s+Collaborator'
    r'|Trainer'
    r')\b',
    re.IGNORECASE,
)
_AFDB_IC_TITLE_RE = re.compile(
    r"\b("
    r"consultant(?:e)?\s+individuel(?:le)?"
    r"|individual\s+consultant"
    r"|individual\s+contractor"
    r"|recrutement\s+d['']un(?:e)?\s+consultant(?:e)?"
    r"|recruitment\s+of\s+a(?:n)?\s+(?:junior\s+)?consultant"
    r"|s[eé]lection\s+d['']un(?:e)?\s+consultant(?:e)?\s+individuel(?:le)?"
    r"|selection\s+of\s+(?:an?\s+)?individual\s+consultant"
    r"|expert(?:e)?\s+individuel(?:le)?"
    r")\b",
    re.IGNORECASE,
)

# ─────────────────────────────────────────────────────────────────────────────
#  TITLE NORMALIZER
# ─────────────────────────────────────────────────────────────────────────────

def normalize_title(raw: Optional[str], portal: str) -> Optional[str]:
    if not raw or not isinstance(raw, str):
        return None

    t = _clean_text(raw)
    if not t:
        return None

    if portal == "ungm":
        t = re.sub(r'^[A-Z0-9][A-Za-z0-9_]*(?:/[A-Za-z0-9_]+)+[/_]', '', t)
        t = re.sub(r'^L?R[A-Z]{1,3}[-\s]\d[\d\-]*\s+(?:for\s+(?:the\s+)?)?', '', t, flags=re.IGNORECASE)
        t = re.sub(r'^(?:Request\s+for\s+(?:Quotation|Proposal|Information)|Call\s+for\s+(?:Quotation|Proposals?))\s*(?:for\s+(?:the\s+)?|:\s*|-\s*)?', '', t, flags=re.IGNORECASE)
        t = re.sub(r'^(?:For\s+[^.]{0,50})\.\s+', '', t, flags=re.IGNORECASE)
        t = re.sub(r'^(WFP|WHO|ILO|IOM|ITU|FAO|IFC|IDLO)\s+', '', t)
        t = re.sub(r'\s*\[.*?\]', '', t)
        org_names = {
            r'\bILO\b':  'International Labour Organization',
            r'\bIOM\b':  'International Organization for Migration',
            r'\bITU\b':  'International Telecommunication Union',
            r'\bFAO\b':  'Food and Agriculture Organization',
            r'\bWHO\b':  'World Health Organization',
            r'\bIFC\b':  'International Finance Corporation',
            r'\bWFP\b':  'World Food Programme',
            r'\bIDLO\b': 'International Development Law Organization',
        }
        for abbr, full in org_names.items():
            t = re.sub(abbr, full, t)
        t = re.sub(r'^[\s\-_,]+', '', t)
        t = re.sub(r'[\s\-_,]+$', '', t)
        t = re.sub(r'\s{2,}', ' ', t)
        t = t.strip(' .')

    elif portal == "undp":
        t = re.sub(r'^\d+[_\s]+', '', t)
        if t and t[0].islower():
            t = t[0].upper() + t[1:]
        t = re.sub(r'^\*\*[^*]+\*\*\s*', '', t)
        t = re.sub(r'^INVITACI[OÓ]N\s+A\s+LICITAR\s+(?:IAL\s+)?', '', t, flags=re.IGNORECASE)
        t = re.sub(r'^IAL\s+(?:No\.\s+)?', '', t, flags=re.IGNORECASE)
        t = re.sub(r'^REQUEST\s+FOR\s+(?:QUOTATION|PROPOSAL|EXPRESSION\s+OF\s+INTEREST)\s*(?:\([^)]+\))?\s*(?:[-–]\s*)?', '', t, flags=re.IGNORECASE)
        t = re.sub(r'^UNDP[-/][A-Z0-9]+(?:[-/][A-Z0-9]+)*\s*[:\s;/-]\s*', '', t, flags=re.IGNORECASE)
        t = re.sub(r'^[A-Z]{2,4}-[A-Z0-9]+(?:-[A-Z0-9]+)*[-\s]+', lambda m: '' if re.search(r'\d', m.group()) else m.group(), t)
        t = re.sub(r'^(?:RFP|RFQ|ITB|EOI|IFB|REOI)\s*[-–]?\s*\d[\d\-/]*(?:\s+for\s+|\s*[-–:;/]\s*|\s+)', '', t, flags=re.IGNORECASE)
        t = re.sub(r'^[A-Za-z]+\d[A-Za-z0-9]*(?:[-/][A-Za-z0-9]+)*\s*(?:[-/:;]\s*|(?<=\d)\s+)', lambda m: '' if re.search(r'\d', m.group()) else m.group(), t)
        t = re.sub(r'^(?:[A-Z]{2,5}[-/])+\d[\d\-A-Z]*[-/]', '', t, flags=re.IGNORECASE)
        t = re.sub(r'^(?:INCL?|Framework)\s*[-–\s]+', '', t, flags=re.IGNORECASE)
        t = re.sub(r'^IC\s+(?=[A-Z])', '', t)
        t = re.sub(r'^(?:RFP|RFQ|ITB|EOI|IFB)\s*[-–]\s*(?:Rebid|Re\w+)\s+', '', t, flags=re.IGNORECASE)
        t = re.sub(r'^UNDP\s+[A-Z][a-zA-Z\s]{2,35}\s*[-–]\s*', '', t)
        t = re.sub(r'^(?:to|for|of|the|du|d\'|en|et)\s+(?=[A-Z])', '', t)
        t = re.sub(r'\s+[A-Z]{2,}(?:[/\-][A-Z0-9]+)+(?:[\-\s]+[A-Z]+)?\s*$', '', t)
        t = re.sub(r'\s*=\s*', ' ', t)
        t = re.sub(r'^[\s\-_,;:]+', '', t)
        if t and re.match(r'^[A-Z0-9\-/]+$', t):
            return None

    elif portal == "afdb":
        AFDB_COUNTRY_EXPANSIONS = {
            r'\bRDC\b': 'DR Congo',
            r'\bRCA\b': 'Central African Republic',
        }
        for pattern, replacement in AFDB_COUNTRY_EXPANSIONS.items():
            t = re.sub(pattern, replacement, t)
        t = re.sub(r'^(?:AOI|SPN|PPM|EOI|RFP|RFQ|AMI|AAO|AGPM|REOI|AON|GPN)\s*-\s*', '', t, flags=re.IGNORECASE)
        t = re.sub(r'^([^-]+?)\s*-\s*', r'\1 ', t)
        t = re.sub(r'\s+-\s+[A-Z][A-Z0-9\-]+$', '', t)

    # strip trailing project code for ALL portals
    t = re.sub(r'\s+-\s+[A-Z][A-Z0-9\-]+$', '', t)

    # final cleanup
    t = re.sub(r'\s{2,}', ' ', t).strip()
    return t or None


# ── Validation ────────────────────────────────────────────────────────────────

REQUIRED_FIELDS = [
    "source_portal", "notice_id", "title_clean",
    "publication_datetime", "country_name_normalized", "notice_type_normalized",
]


def validate(record: dict) -> tuple[bool, list[str], list[str]]:
    missing = [f for f in REQUIRED_FIELDS if not record.get(f)]
    flags: list[str] = []

    if record.get("deadline_datetime") and record.get("publication_datetime"):
        if record["deadline_datetime"] < record["publication_datetime"]:
            flags.append("deadline_before_publication")

    if record.get("budget_numeric") is not None and record["budget_numeric"] < 0:
        flags.append("negative_budget")

    if record.get("days_to_deadline") is not None and record["days_to_deadline"] < -365:
        flags.append("deadline_very_old")

    return len(missing) == 0 and len(flags) == 0, missing, flags


# ─────────────────────────────────────────────────────────────────────────────
#  CORE NORMALIZER
# ─────────────────────────────────────────────────────────────────────────────

def normalize_tender(tender: Tender, session) -> Optional[dict]:
    try:
        now = _now_utc()

        pub_dt  = parse_date(tender.publication_date)
        dead_dt = parse_date(tender.deadline_date, tender.deadline_time)
        days    = days_between(now, dead_dt)

        if tender.source_portal == "afdb":
            notice_code, notice_type_final, country_norm, is_multi, countries_json = (
                _extract_afdb_notice_and_country(tender)
            )
        else:
            notice_code       = normalize_notice_type(tender.notice_type)
            notice_label      = notice_type_label(notice_code)
            raw_notice        = tender.notice_type if tender.notice_type and tender.notice_type.strip() else None
            notice_type_final = notice_label or notice_code or raw_notice
            country_norm, is_multi, countries_json = normalize_country(tender.country)

        stage, status = resolve_lifecycle(notice_code, dead_dt, tender.status_id)

        budget_num, currency_iso = parse_budget(tender.budget)
        if not currency_iso and tender.currency:
            currency_iso = CURRENCY_MAP.get(str(tender.currency).strip().upper())
        budget_missing = budget_num is None

        proc_group  = normalize_procurement_group(tender.procurement_group)
        proc_method = normalize_procurement_method(
            tender.procurement_method_name or tender.procurement_method_code
        )

        if tender.source_portal == "ungm" and not proc_method:
            if _UNGM_IC_TITLE_RE.search(tender.title or ""):
                proc_method = "Individual Contractor"
        if tender.source_portal == "afdb" and not proc_method:
            if _AFDB_IC_TITLE_RE.search(tender.title or ""):
                proc_method = "Individual Consultant"
                if not proc_group:
                    proc_group = "CONSULTING"

        org_name_norm = None
        contact_name  = None
        contact_email = None
        contact_phone = None

        if tender.organisation_id:
            org = session.get(Organisation, tender.organisation_id)
            if org:
                org_name_norm = normalize_org_name(org.name)
                contact = session.execute(
                    select(Contact)
                    .where(Contact.organisation_id == org.id)
                    .limit(1)
                ).scalar_one_or_none()
                if contact:
                    contact_name  = contact.name
                    contact_email = contact.email
                    contact_phone = contact.phone

        has_pdf        = bool(tender.pdf_path)
        funding_agency = resolve_funding_agency(tender.source_portal, org_name_norm)

        record = {
            "tender_id":                    tender.id,
            "source_portal":                tender.source_portal,
            "notice_id":                    tender.tender_id,
            "source_url":                   tender.source_url,
            "organisation_id":              tender.organisation_id,
            "organisation_name_normalized": org_name_norm,
            "funding_agency":               funding_agency,
            "title_clean":                  normalize_title(tender.title, tender.source_portal),
            "description_clean":            None,
            "publication_datetime":         pub_dt,
            "deadline_datetime":            dead_dt,
            "days_to_deadline":             days,
            "created_at":                   now,
            "updated_at":                   now,
            "country_name_normalized":      country_norm,
            "is_multi_country":             is_multi,
            "countries_list":               countries_json,
            "notice_type_normalized":       notice_type_final,
            "lifecycle_stage":              stage,
            "status_normalized":            status,
            "project_id":                   tender.project_id,
            "procurement_group_normalized": proc_group,
            "procurement_method_name":      proc_method,
            "budget_numeric":               budget_num,
            "currency_iso":                 currency_iso,
            "budget_missing":               budget_missing,
            "pdf_path":                     tender.pdf_path,
            "has_pdf":                      has_pdf,
            "document_count":               1 if has_pdf else 0,
            "contact_name":                 contact_name,
            "contact_email":                contact_email,
            "contact_phone":                contact_phone,
            "status_id":                    tender.status_id,
            "source_status_raw":            str(tender.status_id) if tender.status_id is not None else None,
            "cpv_code":                     None,
            "cpv_label":                    None,
            "unspsc_code":                  None,
            "unspsc_label":                 None,
            "sector_source":                None,
            "normalization_status":         "success",
            "normalized_at":                now,
        }

        is_valid, missing, flags = validate(record)
        record["is_valid"]         = is_valid
        record["missing_fields"]   = "|".join(missing) if missing else None
        record["validation_flags"] = "|".join(flags)   if flags   else None
        if missing or flags:
            record["normalization_status"] = "flagged"

        return record

    except Exception as exc:
        log.error("Failed to normalize tender id=%s: %s", tender.id, exc, exc_info=True)
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  UPSERT
# ─────────────────────────────────────────────────────────────────────────────

def upsert_normalized(session, record: dict) -> None:
    stmt = pg_insert(NormalizedTender.__table__).values(**record)
    update_cols = {
        c.name: stmt.excluded[c.name]
        for c in NormalizedTender.__table__.columns
        if c.name not in ("id", "tender_id", "created_at")
    }
    stmt = stmt.on_conflict_do_update(
        index_elements=["tender_id"],
        set_=update_cols,
    )
    session.execute(stmt)
    session.flush()


# ─────────────────────────────────────────────────────────────────────────────
#  RUN LOGIC
# ─────────────────────────────────────────────────────────────────────────────

SUPPORTED_PORTALS = {"worldbank", "afdb", "ungm", "undp"}


def run_normalization(portal: Optional[str] = None, dry_run: bool = False) -> None:
    portals = [portal] if portal else list(SUPPORTED_PORTALS)
    for p in portals:
        if p not in SUPPORTED_PORTALS:
            log.warning("Unknown portal '%s' — skipping.", p)
            continue
        log.info("▶ Starting normalization for portal: %s", p)
        _run_portal(p, dry_run)


def _run_portal(portal: str, dry_run: bool) -> None:
    counters = dict(total=0, success=0, flagged=0, failed=0, skipped=0)

    with get_session() as session:
        tender_ids = session.execute(
            select(Tender.id).where(Tender.source_portal == portal)
        ).scalars().all()

    counters["total"] = len(tender_ids)
    log.info("  Found %d raw tenders for %s", counters["total"], portal)

    for tender_id in tender_ids:
        try:
            with get_session() as session:
                tender_obj = session.get(Tender, tender_id)
                if not tender_obj:
                    counters["skipped"] += 1
                    continue

                record = normalize_tender(tender_obj, session)
                if record is None:
                    counters["failed"] += 1
                    continue

                if not dry_run:
                    upsert_normalized(session, record)

                if record.get("normalization_status") == "flagged":
                    counters["flagged"] += 1
                else:
                    counters["success"] += 1

        except Exception as exc:
            log.error("  Failed tender id=%s: %s", tender_id, exc, exc_info=True)
            counters["failed"] += 1

    if not dry_run:
        try:
            with get_session() as session:
                session.add(NormalizationLog(
                    portal      = portal,
                    run_at      = _now_utc(),
                    total_input = counters["total"],
                    success     = counters["success"],
                    flagged     = counters["flagged"],
                    failed      = counters["failed"],
                    skipped     = counters["skipped"],
                    notes       = None,
                ))
        except Exception as exc:
            log.error("  Failed to write normalization log for %s: %s", portal, exc, exc_info=True)

    log.info(
        "  ✓ %s done — success=%d  flagged=%d  failed=%d  skipped=%d",
        portal, counters["success"], counters["flagged"],
        counters["failed"], counters["skipped"],
    )


# ─────────────────────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Normalize tenders into normalized_tenders table."
    )
    parser.add_argument(
        "--portal",
        choices=list(SUPPORTED_PORTALS),
        default=None,
        help="Process a single portal only (default: all portals).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run normalization logic without writing to the DB.",
    )
    args = parser.parse_args()
    run_normalization(portal=args.portal, dry_run=args.dry_run)