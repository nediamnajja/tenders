# """
# normalizer/common.py
# ====================
# Reads raw tenders from the `tenders` table (all portals),
# normalizes the common fields, and upserts into `normalized_tenders`.

# Portals handled: worldbank | afdb | ungm | undp

# Run:
#     python normalizer/common.py                  # all portals
#     python normalizer/common.py --portal ungm    # one portal only
#     python normalizer/common.py --dry-run        # no DB writes
# """

# import argparse
# import json
# import logging
# import os
# import re
# import sys
# from datetime import datetime, timezone
# from typing import Optional

# # ── Project root on sys.path ─────────────────────────────────────────────────
# # This file lives in <root>/normalizer/common.py
# # db.py and models.py live in <root>/
# _ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# if _ROOT not in sys.path:
#     sys.path.insert(0, _ROOT)

# from sqlalchemy import select
# from sqlalchemy.dialects.postgresql import insert as pg_insert

# from db import get_session
# from models import (
#     Contact,
#     NormalizationLog,
#     NormalizedTender,
#     Organisation,
#     Tender,
# )

# # ─────────────────────────────────────────────────────────────────────────────
# #  LOGGING
# # ─────────────────────────────────────────────────────────────────────────────

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
# )
# log = logging.getLogger(__name__)

# # ─────────────────────────────────────────────────────────────────────────────
# #  LOOKUP MAPS
# # ─────────────────────────────────────────────────────────────────────────────

# # ── Notice type: raw portal value → internal code ────────────────────────────
# NOTICE_TYPE_MAP: dict[str, str] = {
#     # Planning
#     "PPM":                                "PPM",
#     "AGPM":                               "GPN",
#     # EOI
#     "AMI":                                "EOI",
#     "Request for EOI":                    "EOI",
#     "Request for Expression of Interest": "EOI",
#     "Call for individual consultants":    "EOI",
#     "Call for implementing partners":     "EOI",
#     # RFI
#     "RFI - Request for Information":      "RFI",
#     # Prequalification
#     "Invitation for Prequalification":    "PREQUAL",
#     # RFP
#     "Request for Proposals":              "RFP",
#     "Request for proposal":               "RFP",
#     "RFP - Request for proposal":         "RFP",
#     "Grant support-call for proposal":    "RFP",
#     # IFB
#     "Invitation for Bids":                "IFB",
#     "Invitation to bid":                  "IFB",
#     "ITB - Invitation to bid":            "IFB",
#     "Request for Bids":                   "IFB",
#     "SPN":                                "IFB",
#     "AOI":                                "IFB",
#     "AAO":                                "IFB",
#     # RFQ
#     "Request for quotation":              "RFQ",
#     "RFQ - Request for quotation":        "RFQ",
# }

# # ── Internal code → full display name ────────────────────────────────────────
# NOTICE_TYPE_LABELS: dict[str, str] = {
#     "PPM":     "Prior Procurement Plan",
#     "GPN":     "General Procurement Notice",
#     "EOI":     "Expression of Interest",
#     "RFI":     "Request for Information",
#     "PREQUAL": "Invitation for Prequalification",
#     "RFP":     "Request for Proposals",
#     "IFB":     "Invitation for Bids",
#     "RFQ":     "Request for Quotation",
# }

# # ── Notice code → (lifecycle_stage, status_normalized) ───────────────────────
# # Deadline-based and status_id-based overrides are applied in resolve_lifecycle()
# LIFECYCLE_MAP: dict[str, tuple[str, str]] = {
#     "PPM":     ("early_intelligence", "programming"),
#     "GPN":     ("early_intelligence", "forecast"),
#     "EOI":     ("procurement",        "open"),
#     "RFI":     ("procurement",        "open"),
#     "PREQUAL": ("procurement",        "open"),
#     "RFP":     ("procurement",        "open"),
#     "IFB":     ("procurement",        "open"),
#     "RFQ":     ("procurement",        "open"),
# }

# # ── Procurement group: code or full name → full name ─────────────────────────
# PROCUREMENT_GROUP_MAP: dict[str, str] = {
#     "CS": "Consulting Services",
#     "CW": "Civil Works",
#     "GO": "Goods",
#     "NC": "Non-Consulting Services",
# }

# # ── Procurement method: code or full name → full name ────────────────────────
# PROCUREMENT_METHOD_MAP: dict[str, str] = {
#     # Codes
#     "CQS":  "Consultant Qualification Selection",
#     "ICS":  "Individual Consultant Selection",
#     "QBS":  "Quality Based Selection",
#     "QCBS": "Quality and Cost-Based Selection",
#     "LCS":  "Least Cost Selection",
#     "IC":   "Individual Contractor",
#     # Full names (pass-through)
#     "Consultant Qualification Selection": "Consultant Qualification Selection",
#     "Individual Consultant Selection":    "Individual Consultant Selection",
#     "Quality Based Selection":            "Quality Based Selection",
#     "Quality and Cost-Based Selection":   "Quality and Cost-Based Selection",
#     "Least Cost Selection":               "Least Cost Selection",
#     "IC - Individual contractor":         "Individual Contractor",
# }

# # ── ISO-4217 currency ─────────────────────────────────────────────────────────
# CURRENCY_MAP: dict[str, str] = {
#     "USD": "USD", "US$": "USD", "US DOLLAR": "USD", "DOLLAR": "USD",
#     "EUR": "EUR", "EURO": "EUR", "€": "EUR",
#     "GBP": "GBP", "POUND": "GBP", "£": "GBP",
#     "XOF": "XOF", "FCFA": "XOF", "CFA": "XOF",
#     "XAF": "XAF",
#     "KES": "KES", "SHILLING": "KES",
#     "NGN": "NGN", "NAIRA": "NGN",
#     "GHS": "GHS", "CEDI": "GHS",
#     "ZAR": "ZAR", "RAND": "ZAR",
#     "ETB": "ETB", "BIRR": "ETB",
#     "TZS": "TZS", "UGX": "UGX", "RWF": "RWF", "MZN": "MZN",
#     "CHF": "CHF", "FRANC": "CHF",
#     "JPY": "JPY", "YEN": "JPY",
#     "CNY": "CNY", "RMB": "CNY",
#     "CAD": "CAD", "AUD": "AUD",
#     "INR": "INR", "RUPEE": "INR",
# }

# # ── Language ──────────────────────────────────────────────────────────────────
# LANGUAGE_MAP: dict[str, str] = {
#     "english":    "en", "en": "en", "eng": "en",
#     "french":     "fr", "fr": "fr", "fra": "fr", "français": "fr",
#     "spanish":    "es", "es": "es", "spa": "es",
#     "arabic":     "ar", "ar": "ar",
#     "portuguese": "pt", "pt": "pt",
#     "russian":    "ru", "ru": "ru",
# }

# # ── Country aliases → normalized name ────────────────────────────────────────
# COUNTRY_ALIASES: dict[str, str] = {
#     "democratic republic of congo":  "Democratic Republic of the Congo",
#     "drc":                           "Democratic Republic of the Congo",
#     "congo, democratic republic":    "Democratic Republic of the Congo",
#     "congo, rep.":                   "Republic of the Congo",
#     "tanzania":                      "United Republic of Tanzania",
#     "ivory coast":                   "Côte d'Ivoire",
#     "cote d'ivoire":                 "Côte d'Ivoire",
#     "cote divoire":                  "Côte d'Ivoire",
#     "west bank and gaza":            "Palestinian Territory",
#     "west bank":                     "Palestinian Territory",
#     "gaza":                          "Palestinian Territory",
#     "lao pdr":                       "Laos",
#     "lao":                           "Laos",
#     "vietnam":                       "Viet Nam",
#     "viet nam":                      "Viet Nam",
#     "south korea":                   "Republic of Korea",
#     "north korea":                   "Democratic People's Republic of Korea",
#     "russia":                        "Russian Federation",
#     "iran":                          "Islamic Republic of Iran",
#     "syria":                         "Syrian Arab Republic",
#     "bolivia":                       "Plurinational State of Bolivia",
#     "moldova":                       "Republic of Moldova",
#     "micronesia":                    "Federated States of Micronesia",
#     "trinidad":                      "Trinidad and Tobago",
#     "saint kitts":                   "Saint Kitts and Nevis",
#     "global":                        "Global / Multi-Country",
#     "multi-country":                 "Global / Multi-Country",
#     "regional":                      "Regional",
#     "various":                       "Global / Multi-Country",
# }

# # ─────────────────────────────────────────────────────────────────────────────
# #  HELPERS
# # ─────────────────────────────────────────────────────────────────────────────

# def _now_utc() -> datetime:
#     return datetime.now(timezone.utc)


# # ── Date parsing ──────────────────────────────────────────────────────────────

# _DATE_FORMATS = [
#     "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d",
#     "%d %b %Y", "%d %B %Y", "%B %d, %Y", "%b %d, %Y",
#     "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S",
# ]


# def parse_date(raw: Optional[str], time_part: Optional[str] = None) -> Optional[datetime]:
#     """Parse a date string (+optional time) into a UTC-aware datetime."""
#     if not raw:
#         return None
#     raw = str(raw).strip()

#     if time_part:
#         combined = f"{raw} {str(time_part).strip()}"
#         for fmt in ["%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S",
#                     "%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S"]:
#             try:
#                 return datetime.strptime(combined, fmt).replace(tzinfo=timezone.utc)
#             except ValueError:
#                 pass

#     for fmt in _DATE_FORMATS:
#         try:
#             return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
#         except ValueError:
#             pass

#     log.debug("Could not parse date: %r", raw)
#     return None


# def days_between(from_dt: Optional[datetime], to_dt: Optional[datetime]) -> Optional[int]:
#     if from_dt is None or to_dt is None:
#         return None
#     return (to_dt - from_dt).days


# # ── Budget parsing ─────────────────────────────────────────────────────────────

# _BUDGET_RE = re.compile(
#     r"([A-Z]{2,4}|US\$|€|£|¥)?\s*([\d,\.]+)\s*([A-Z]{2,4}|US\$|€|£|¥)?",
#     re.IGNORECASE,
# )
# _MULTIPLIERS = {
#     "K": 1_000, "M": 1_000_000, "MN": 1_000_000, "MILLION": 1_000_000,
#     "B": 1_000_000_000, "BILLION": 1_000_000_000,
# }


# def parse_budget(raw: Optional[str]) -> tuple[Optional[float], Optional[str]]:
#     """Returns (numeric_value, currency_iso) or (None, None)."""
#     if not raw:
#         return None, None
#     text = str(raw).strip().upper()
#     text = re.sub(r"(\d)\s(\d)", r"\1\2", text)

#     m = _BUDGET_RE.search(text)
#     if not m:
#         return None, None

#     curr_before, number_str, curr_after = m.group(1), m.group(2), m.group(3)
#     raw_currency = curr_before or curr_after or ""
#     number_str = number_str.replace(",", "").replace(" ", "")
#     try:
#         value = float(number_str)
#     except ValueError:
#         return None, None

#     suffix_match = re.search(r"(\d)\s*(MILLION|BILLION|MN|[KMB])\b", text)
#     if suffix_match:
#         value *= _MULTIPLIERS.get(suffix_match.group(2), 1)

#     currency_iso = CURRENCY_MAP.get(raw_currency.strip()) if raw_currency else None
#     return value, currency_iso


# # ── Notice type ───────────────────────────────────────────────────────────────

# def normalize_notice_type(raw: Optional[str]) -> Optional[str]:
#     """Returns internal code (e.g. 'IFB') or None."""
#     if not raw:
#         return None
#     raw = str(raw).strip()
#     code = NOTICE_TYPE_MAP.get(raw)
#     if code:
#         return code
#     for key, val in NOTICE_TYPE_MAP.items():
#         if key.lower() == raw.lower():
#             return val
#     return None


# def notice_type_label(code: Optional[str]) -> Optional[str]:
#     """Full display name, e.g. 'IFB' → 'Invitation for Bids'."""
#     return NOTICE_TYPE_LABELS.get(code) if code else None


# # ── Lifecycle + status ────────────────────────────────────────────────────────

# def resolve_lifecycle(
#     notice_code: Optional[str],
#     deadline_dt: Optional[datetime],
#     status_id:   Optional[int],
# ) -> tuple[Optional[str], Optional[str]]:
#     """
#     Returns (lifecycle_stage, status_normalized).
#     Priority: award status_id > shortlist status_id > deadline passed > notice type map.
#     """
#     now = _now_utc()
#     # Adjust these to match your actual portal status_id values
#     AWARD_STATUS_IDS     = {5, 6, 50, 51}
#     SHORTLIST_STATUS_IDS = {3, 30}

#     if status_id in AWARD_STATUS_IDS:
#         return "implementation", "awarded"
#     if status_id in SHORTLIST_STATUS_IDS:
#         return "procurement", "shortlisted"
#     if deadline_dt and deadline_dt < now:
#         return "procurement", "closed"
#     if notice_code:
#         return LIFECYCLE_MAP.get(notice_code, (None, None))
#     return None, None


# # ── Procurement group ─────────────────────────────────────────────────────────

# def normalize_procurement_group(raw: Optional[str]) -> Optional[str]:
#     if not raw:
#         return None
#     raw = str(raw).strip()
#     full = PROCUREMENT_GROUP_MAP.get(raw.upper())
#     if full:
#         return full
#     for _, name in PROCUREMENT_GROUP_MAP.items():
#         if raw.lower() == name.lower():
#             return name
#     return raw


# # ── Procurement method ────────────────────────────────────────────────────────

# def normalize_procurement_method(raw: Optional[str]) -> Optional[str]:
#     if not raw:
#         return None
#     raw = str(raw).strip()
#     result = PROCUREMENT_METHOD_MAP.get(raw)
#     if result:
#         return result
#     for key, val in PROCUREMENT_METHOD_MAP.items():
#         if key.lower() == raw.lower():
#             return val
#     return raw


# # ── Language ──────────────────────────────────────────────────────────────────

# def normalize_language(raw: Optional[str]) -> Optional[str]:
#     if not raw:
#         return None
#     return LANGUAGE_MAP.get(str(raw).strip().lower(), str(raw).strip().lower()[:5])


# # ── Country ───────────────────────────────────────────────────────────────────

# def normalize_country(raw: Optional[str]) -> tuple[Optional[str], bool, Optional[str]]:
#     """Returns (country_name_normalized, is_multi_country, countries_list_json)."""
#     if not raw:
#         return None, False, None
#     raw = str(raw).strip()
#     parts = [p.strip() for p in re.split(r"[;,|/]", raw) if p.strip()]

#     normalized_parts = []
#     for part in parts:
#         alias = COUNTRY_ALIASES.get(part.lower())
#         normalized_parts.append(alias if alias else part.title())

#     is_multi = len(normalized_parts) > 1

#     global_keywords = {"global", "regional", "various", "multi-country", "worldwide"}
#     if any(p.lower() in global_keywords for p in parts):
#         return "Global / Multi-Country", True, json.dumps(normalized_parts)

#     primary = normalized_parts[0] if normalized_parts else None
#     countries_json = json.dumps(normalized_parts) if is_multi else None
#     return primary, is_multi, countries_json


# # ── Organisation name ─────────────────────────────────────────────────────────

# def normalize_org_name(raw: Optional[str]) -> Optional[str]:
#     if not raw:
#         return None
#     return " ".join(str(raw).strip().split())


# # ── Text cleaning ─────────────────────────────────────────────────────────────

# def _clean_text(raw: Optional[str]) -> Optional[str]:
#     if not raw:
#         return None
#     text = re.sub(r"<[^>]+>", " ", str(raw))
#     text = re.sub(r"&[a-zA-Z]+;", " ", text)
#     text = re.sub(r"\s+", " ", text).strip()
#     return text or None


# # ── Validation ────────────────────────────────────────────────────────────────

# REQUIRED_FIELDS = [
#     "source_portal", "notice_id", "title_clean",
#     "publication_datetime", "country_name_normalized", "notice_type_normalized",
# ]


# def validate(record: dict) -> tuple[bool, list[str], list[str]]:
#     """Returns (is_valid, missing_fields, validation_flags)."""
#     missing = [f for f in REQUIRED_FIELDS if not record.get(f)]
#     flags: list[str] = []

#     if record.get("deadline_datetime") and record.get("publication_datetime"):
#         if record["deadline_datetime"] < record["publication_datetime"]:
#             flags.append("deadline_before_publication")

#     if record.get("budget_numeric") is not None and record["budget_numeric"] < 0:
#         flags.append("negative_budget")

#     if record.get("days_to_deadline") is not None and record["days_to_deadline"] < -365:
#         flags.append("deadline_very_old")

#     return len(missing) == 0 and len(flags) == 0, missing, flags


# # ─────────────────────────────────────────────────────────────────────────────
# #  CORE NORMALIZER
# # ─────────────────────────────────────────────────────────────────────────────

# def normalize_tender(tender: Tender, session) -> Optional[dict]:
#     """
#     Takes a raw Tender ORM object.
#     Returns a dict ready to upsert into normalized_tenders, or None on hard failure.
#     """
#     try:
#         now = _now_utc()

#         # Dates
#         pub_dt  = parse_date(tender.publication_date)
#         dead_dt = parse_date(tender.deadline_date, tender.deadline_time)
#         days    = days_between(now, dead_dt)  # positive = future, negative = past

#         # Notice type
#         notice_code  = normalize_notice_type(tender.notice_type)
#         notice_label = notice_type_label(notice_code)
#         raw_notice   = tender.notice_type if tender.notice_type and tender.notice_type.strip() else None
#         notice_type_final = notice_label or notice_code or raw_notice

#         # Lifecycle + status
#         stage, status = resolve_lifecycle(notice_code, dead_dt, tender.status_id)

#         # Country
#         country_norm, is_multi, countries_json = normalize_country(tender.country)

#         # Budget
#         budget_num, currency_iso = parse_budget(tender.budget)
#         if not currency_iso and tender.currency:
#             currency_iso = CURRENCY_MAP.get(str(tender.currency).strip().upper())
#         budget_missing = budget_num is None

        

#         # Procurement
#         proc_group  = normalize_procurement_group(tender.procurement_group)
#         proc_method = normalize_procurement_method(
#             tender.procurement_method_name or tender.procurement_method_code
#         )

#         # Organisation + contact
#         org_name_norm = None
#         contact_name  = None
#         contact_email = None
#         contact_phone = None

#         if tender.organisation_id:
#             org = session.get(Organisation, tender.organisation_id)
#             if org:
#                 org_name_norm = normalize_org_name(org.name)
#                 contact = session.execute(
#                     select(Contact)
#                     .where(Contact.organisation_id == org.id)
#                     .limit(1)
#                 ).scalar_one_or_none()
#                 if contact:
#                     contact_name  = contact.name
#                     contact_email = contact.email
#                     contact_phone = contact.phone

#         has_pdf = bool(tender.pdf_path)

#         record = {
#             # Core identification
#             "tender_id":                    tender.id,
#             "source_portal":                tender.source_portal,
#             "notice_id":                    tender.tender_id,
#             "source_url":                   tender.source_url,
#             "organisation_id":              tender.organisation_id,
#             "organisation_name_normalized": org_name_norm,
#             # Title & text
#             "title_clean":         _clean_text(tender.title),
#             "description_clean":   None,   # reserved for NLP pipeline
        
#             # Dates
#             "publication_datetime": pub_dt,
#             "deadline_datetime":    dead_dt,
#             "days_to_deadline":     days,
#             "created_at":           now,
#             "updated_at":           now,
#             # Country
#             "country_name_normalized": country_norm,
#             "is_multi_country":        is_multi,
#             "countries_list":          countries_json,
#             # Notice type
#             "notice_type_normalized": notice_type_final,
#             # Lifecycle
#             "lifecycle_stage":   stage,
#             "status_normalized": status,
#             # Procurement
#             "project_id":                   tender.project_id,
#             "procurement_group_normalized": proc_group,
#             "procurement_method_name":      proc_method,
#             # Budget
#             "budget_numeric": budget_num,
#             "currency_iso":   currency_iso,
#             "budget_missing": budget_missing,
#             # Documents
#             "pdf_path":       tender.pdf_path,
#             "has_pdf":        has_pdf,
#             "document_count": 1 if has_pdf else 0,
#             # Contact
#             "contact_name":  contact_name,
#             "contact_email": contact_email,
#             "contact_phone": contact_phone,
#             # Status
#             "status_id":         tender.status_id,
#             "source_status_raw": str(tender.status_id) if tender.status_id is not None else None,
#             # Sector — reserved for NLP pipeline
#             "cpv_code":      None,
#             "cpv_label":     None,
#             "unspsc_code":   None,
#             "unspsc_label":  None,
#             "sector_source": None,
#             # Data integrity (filled below)
#             "normalization_status": "success",
#             "normalized_at":        now,
#         }

#         is_valid, missing, flags = validate(record)
#         record["is_valid"]         = is_valid
#         record["missing_fields"]   = "|".join(missing) if missing else None
#         record["validation_flags"] = "|".join(flags)   if flags   else None
#         if missing or flags:
#             record["normalization_status"] = "flagged"

#         return record

#     except Exception as exc:
#         log.error("Failed to normalize tender id=%s: %s", tender.id, exc, exc_info=True)
#         return None


# # ─────────────────────────────────────────────────────────────────────────────
# #  UPSERT
# # ─────────────────────────────────────────────────────────────────────────────

# def upsert_normalized(session, record: dict) -> None:
#     """
#     PostgreSQL upsert on (tender_id).
#     On conflict: update all fields except id, tender_id, and created_at.
#     """
#     stmt = pg_insert(NormalizedTender.__table__).values(**record)
#     update_cols = {
#         c.name: stmt.excluded[c.name]
#         for c in NormalizedTender.__table__.columns
#         if c.name not in ("id", "tender_id", "created_at")
#     }
#     stmt = stmt.on_conflict_do_update(
#         index_elements=["tender_id"],
#         set_=update_cols,
#     )
#     session.execute(stmt)
#     session.flush()


# # ─────────────────────────────────────────────────────────────────────────────
# #  RUN LOGIC
# # ─────────────────────────────────────────────────────────────────────────────

# SUPPORTED_PORTALS = {"worldbank", "afdb", "ungm", "undp"}


# def run_normalization(portal: Optional[str] = None, dry_run: bool = False) -> None:
#     portals = [portal] if portal else list(SUPPORTED_PORTALS)
#     for p in portals:
#         if p not in SUPPORTED_PORTALS:
#             log.warning("Unknown portal '%s' — skipping.", p)
#             continue
#         log.info("▶ Starting normalization for portal: %s", p)
#         _run_portal(p, dry_run)


# def _run_portal(portal: str, dry_run: bool) -> None:
#     counters = dict(total=0, success=0, flagged=0, failed=0, skipped=0)

#     # Step 1: collect IDs in a short session (avoids keeping a long-lived transaction open)
#     with get_session() as session:
#         tender_ids = session.execute(
#             select(Tender.id).where(Tender.source_portal == portal)
#         ).scalars().all()

#     counters["total"] = len(tender_ids)
#     log.info("  Found %d raw tenders for %s", counters["total"], portal)

#     # Step 2: process each tender in its own session
#     for tender_id in tender_ids:
#         try:
#             with get_session() as session:
#                 tender_obj = session.get(Tender, tender_id)
#                 if not tender_obj:
#                     counters["skipped"] += 1
#                     continue

#                 record = normalize_tender(tender_obj, session)
#                 if record is None:
#                     counters["failed"] += 1
#                     continue

#                 if not dry_run:
#                     upsert_normalized(session, record)

#                 if record.get("normalization_status") == "flagged":
#                     counters["flagged"] += 1
#                 else:
#                     counters["success"] += 1

#         except Exception as exc:
#             log.error("  Failed tender id=%s: %s", tender_id, exc, exc_info=True)
#             counters["failed"] += 1

#     # Step 3: write normalization log
#     if not dry_run:
#         try:
#             with get_session() as session:
#                 session.add(NormalizationLog(
#                     portal      = portal,
#                     run_at      = _now_utc(),
#                     total_input = counters["total"],
#                     success     = counters["success"],
#                     flagged     = counters["flagged"],
#                     failed      = counters["failed"],
#                     skipped     = counters["skipped"],
#                     notes       = None,
#                 ))
#         except Exception as exc:
#             log.error("  Failed to write normalization log for %s: %s", portal, exc, exc_info=True)

#     log.info(
#         "  ✓ %s done — success=%d  flagged=%d  failed=%d  skipped=%d",
#         portal, counters["success"], counters["flagged"],
#         counters["failed"], counters["skipped"],
#     )


# # ─────────────────────────────────────────────────────────────────────────────
# #  CLI
# # ─────────────────────────────────────────────────────────────────────────────

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(
#         description="Normalize tenders into normalized_tenders table."
#     )
#     parser.add_argument(
#         "--portal",
#         choices=list(SUPPORTED_PORTALS),
#         default=None,
#         help="Process a single portal only (default: all portals).",
#     )
#     parser.add_argument(
#         "--dry-run",
#         action="store_true",
#         help="Run normalization logic without writing to the DB.",
#     )
#     args = parser.parse_args()
#     run_normalization(portal=args.portal, dry_run=args.dry_run)





















# """
# normalizer/common.py
# ====================
# Reads raw tenders from the `tenders` table (all portals),
# normalizes the common fields, and upserts into `normalized_tenders`.

# Portals handled: worldbank | afdb | ungm | undp

# Run:
#     python normalizer/common.py                  # all portals
#     python normalizer/common.py --portal ungm    # one portal only
#     python normalizer/common.py --dry-run        # no DB writes
# """

# import argparse
# import json
# import logging
# import os
# import re
# import sys
# from datetime import datetime, timezone
# from typing import Optional

# # ── Project root on sys.path ─────────────────────────────────────────────────
# # This file lives in <root>/normalizer/common.py
# # db.py and models.py live in <root>/
# _ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# if _ROOT not in sys.path:
#     sys.path.insert(0, _ROOT)

# from sqlalchemy import select
# from sqlalchemy.dialects.postgresql import insert as pg_insert

# from db import get_session
# from models import (
#     Contact,
#     NormalizationLog,
#     NormalizedTender,
#     Organisation,
#     Tender,
# )

# # ─────────────────────────────────────────────────────────────────────────────
# #  LOGGING
# # ─────────────────────────────────────────────────────────────────────────────

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
# )
# log = logging.getLogger(__name__)

# # ─────────────────────────────────────────────────────────────────────────────
# #  LOOKUP MAPS
# # ─────────────────────────────────────────────────────────────────────────────

# # ── Notice type: raw portal value → internal code ────────────────────────────
# NOTICE_TYPE_MAP: dict[str, str] = {
#     # Planning
#     "PPM":                                "PPM",
#     "AGPM":                               "GPN",
#     # EOI
#     "AMI":                                "EOI",
#     "Request for EOI":                    "EOI",
#     "Request for Expression of Interest": "EOI",
#     "Call for individual consultants":    "EOI",
#     "Call for implementing partners":     "EOI",
#     # RFI
#     "RFI - Request for Information":      "RFI",
#     # Prequalification
#     "Invitation for Prequalification":    "PREQUAL",
#     # RFP
#     "Request for Proposals":              "RFP",
#     "Request for proposal":               "RFP",
#     "RFP - Request for proposal":         "RFP",
#     "Grant support-call for proposal":    "RFP",
#     # IFB
#     "Invitation for Bids":                "IFB",
#     "Invitation to bid":                  "IFB",
#     "ITB - Invitation to bid":            "IFB",
#     "Request for Bids":                   "IFB",
#     "SPN":                                "IFB",
#     "AOI":                                "IFB",
#     "AAO":                                "IFB",
#     # RFQ
#     "Request for quotation":              "RFQ",
#     "RFQ - Request for quotation":        "RFQ",
# }

# # ── Internal code → full display name ────────────────────────────────────────
# NOTICE_TYPE_LABELS: dict[str, str] = {
#     "PPM":     "Prior Procurement Plan",
#     "GPN":     "General Procurement Notice",
#     "EOI":     "Expression of Interest",
#     "RFI":     "Request for Information",
#     "PREQUAL": "Invitation for Prequalification",
#     "RFP":     "Request for Proposals",
#     "IFB":     "Invitation for Bids",
#     "RFQ":     "Request for Quotation",
# }

# # ── Notice code → (lifecycle_stage, status_normalized) ───────────────────────
# # Deadline-based and status_id-based overrides are applied in resolve_lifecycle()
# LIFECYCLE_MAP: dict[str, tuple[str, str]] = {
#     "PPM":     ("early_intelligence", "programming"),
#     "GPN":     ("early_intelligence", "forecast"),
#     "EOI":     ("procurement",        "open"),
#     "RFI":     ("procurement",        "open"),
#     "PREQUAL": ("procurement",        "open"),
#     "RFP":     ("procurement",        "open"),
#     "IFB":     ("procurement",        "open"),
#     "RFQ":     ("procurement",        "open"),
# }

# # ── Procurement group: code or full name → full name ─────────────────────────
# PROCUREMENT_GROUP_MAP: dict[str, str] = {
#     "CS": "Consulting Services",
#     "CW": "Civil Works",
#     "GO": "Goods",
#     "NC": "Non-Consulting Services",
# }

# # ── Procurement method: code or full name → full name ────────────────────────
# PROCUREMENT_METHOD_MAP: dict[str, str] = {
#     # Codes
#     "CQS":  "Consultant Qualification Selection",
#     "ICS":  "Individual Consultant Selection",
#     "QBS":  "Quality Based Selection",
#     "QCBS": "Quality and Cost-Based Selection",
#     "LCS":  "Least Cost Selection",
#     "IC":   "Individual Contractor",
#     # Full names (pass-through)
#     "Consultant Qualification Selection": "Consultant Qualification Selection",
#     "Individual Consultant Selection":    "Individual Consultant Selection",
#     "Quality Based Selection":            "Quality Based Selection",
#     "Quality and Cost-Based Selection":   "Quality and Cost-Based Selection",
#     "Least Cost Selection":               "Least Cost Selection",
#     "IC - Individual contractor":         "Individual Contractor",
# }

# # ── ISO-4217 currency ─────────────────────────────────────────────────────────
# CURRENCY_MAP: dict[str, str] = {
#     "USD": "USD", "US$": "USD", "US DOLLAR": "USD", "DOLLAR": "USD",
#     "EUR": "EUR", "EURO": "EUR", "€": "EUR",
#     "GBP": "GBP", "POUND": "GBP", "£": "GBP",
#     "XOF": "XOF", "FCFA": "XOF", "CFA": "XOF",
#     "XAF": "XAF",
#     "KES": "KES", "SHILLING": "KES",
#     "NGN": "NGN", "NAIRA": "NGN",
#     "GHS": "GHS", "CEDI": "GHS",
#     "ZAR": "ZAR", "RAND": "ZAR",
#     "ETB": "ETB", "BIRR": "ETB",
#     "TZS": "TZS", "UGX": "UGX", "RWF": "RWF", "MZN": "MZN",
#     "CHF": "CHF", "FRANC": "CHF",
#     "JPY": "JPY", "YEN": "JPY",
#     "CNY": "CNY", "RMB": "CNY",
#     "CAD": "CAD", "AUD": "AUD",
#     "INR": "INR", "RUPEE": "INR",
# }

# # ── Language ──────────────────────────────────────────────────────────────────
# LANGUAGE_MAP: dict[str, str] = {
#     "english":    "en", "en": "en", "eng": "en",
#     "french":     "fr", "fr": "fr", "fra": "fr", "français": "fr",
#     "spanish":    "es", "es": "es", "spa": "es",
#     "arabic":     "ar", "ar": "ar",
#     "portuguese": "pt", "pt": "pt",
#     "russian":    "ru", "ru": "ru",
# }

# # ── Country aliases → normalized name ────────────────────────────────────────
# COUNTRY_ALIASES: dict[str, str] = {
#     "democratic republic of congo":  "Democratic Republic of the Congo",
#     "drc":                           "Democratic Republic of the Congo",
#     "congo, democratic republic":    "Democratic Republic of the Congo",
#     "congo, rep.":                   "Republic of the Congo",
#     "tanzania":                      "United Republic of Tanzania",
#     "ivory coast":                   "Côte d'Ivoire",
#     "cote d'ivoire":                 "Côte d'Ivoire",
#     "cote divoire":                  "Côte d'Ivoire",
#     "west bank and gaza":            "Palestinian Territory",
#     "west bank":                     "Palestinian Territory",
#     "gaza":                          "Palestinian Territory",
#     "lao pdr":                       "Laos",
#     "lao":                           "Laos",
#     "vietnam":                       "Viet Nam",
#     "viet nam":                      "Viet Nam",
#     "south korea":                   "Republic of Korea",
#     "north korea":                   "Democratic People's Republic of Korea",
#     "russia":                        "Russian Federation",
#     "iran":                          "Islamic Republic of Iran",
#     "syria":                         "Syrian Arab Republic",
#     "bolivia":                       "Plurinational State of Bolivia",
#     "moldova":                       "Republic of Moldova",
#     "micronesia":                    "Federated States of Micronesia",
#     "trinidad":                      "Trinidad and Tobago",
#     "saint kitts":                   "Saint Kitts and Nevis",
#     "global":                        "Global / Multi-Country",
#     "multi-country":                 "Global / Multi-Country",
#     "regional":                      "Regional",
#     "various":                       "Global / Multi-Country",
# }

# # ─────────────────────────────────────────────────────────────────────────────
# #  HELPERS
# # ─────────────────────────────────────────────────────────────────────────────

# def _now_utc() -> datetime:
#     return datetime.now(timezone.utc)


# # ── Date parsing ──────────────────────────────────────────────────────────────

# _DATE_FORMATS = [
#     "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d",
#     "%d %b %Y", "%d %B %Y", "%B %d, %Y", "%b %d, %Y",
#     "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S",
# ]


# def parse_date(raw: Optional[str], time_part: Optional[str] = None) -> Optional[datetime]:
#     """Parse a date string (+optional time) into a UTC-aware datetime."""
#     if not raw:
#         return None
#     raw = str(raw).strip()

#     if time_part:
#         combined = f"{raw} {str(time_part).strip()}"
#         for fmt in ["%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S",
#                     "%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S"]:
#             try:
#                 return datetime.strptime(combined, fmt).replace(tzinfo=timezone.utc)
#             except ValueError:
#                 pass

#     for fmt in _DATE_FORMATS:
#         try:
#             return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
#         except ValueError:
#             pass

#     log.debug("Could not parse date: %r", raw)
#     return None


# def days_between(from_dt: Optional[datetime], to_dt: Optional[datetime]) -> Optional[int]:
#     if from_dt is None or to_dt is None:
#         return None
#     return (to_dt - from_dt).days


# # ── Budget parsing ─────────────────────────────────────────────────────────────

# _BUDGET_RE = re.compile(
#     r"([A-Z]{2,4}|US\$|€|£|¥)?\s*([\d,\.]+)\s*([A-Z]{2,4}|US\$|€|£|¥)?",
#     re.IGNORECASE,
# )
# _MULTIPLIERS = {
#     "K": 1_000, "M": 1_000_000, "MN": 1_000_000, "MILLION": 1_000_000,
#     "B": 1_000_000_000, "BILLION": 1_000_000_000,
# }


# def parse_budget(raw: Optional[str]) -> tuple[Optional[float], Optional[str]]:
#     """Returns (numeric_value, currency_iso) or (None, None)."""
#     if not raw:
#         return None, None
#     text = str(raw).strip().upper()
#     text = re.sub(r"(\d)\s(\d)", r"\1\2", text)

#     m = _BUDGET_RE.search(text)
#     if not m:
#         return None, None

#     curr_before, number_str, curr_after = m.group(1), m.group(2), m.group(3)
#     raw_currency = curr_before or curr_after or ""
#     number_str = number_str.replace(",", "").replace(" ", "")
#     try:
#         value = float(number_str)
#     except ValueError:
#         return None, None

#     suffix_match = re.search(r"(\d)\s*(MILLION|BILLION|MN|[KMB])\b", text)
#     if suffix_match:
#         value *= _MULTIPLIERS.get(suffix_match.group(2), 1)

#     currency_iso = CURRENCY_MAP.get(raw_currency.strip()) if raw_currency else None
#     return value, currency_iso


# # ── Notice type ───────────────────────────────────────────────────────────────

# def normalize_notice_type(raw: Optional[str]) -> Optional[str]:
#     """Returns internal code (e.g. 'IFB') or None."""
#     if not raw:
#         return None
#     raw = str(raw).strip()
#     code = NOTICE_TYPE_MAP.get(raw)
#     if code:
#         return code
#     for key, val in NOTICE_TYPE_MAP.items():
#         if key.lower() == raw.lower():
#             return val
#     return None


# def notice_type_label(code: Optional[str]) -> Optional[str]:
#     """Full display name, e.g. 'IFB' → 'Invitation for Bids'."""
#     return NOTICE_TYPE_LABELS.get(code) if code else None


# # ── Lifecycle + status ────────────────────────────────────────────────────────

# def resolve_lifecycle(
#     notice_code: Optional[str],
#     deadline_dt: Optional[datetime],
#     status_id:   Optional[int],
# ) -> tuple[Optional[str], Optional[str]]:
#     """
#     Returns (lifecycle_stage, status_normalized).
#     Priority: award status_id > shortlist status_id > deadline passed > notice type map.
#     """
#     now = _now_utc()
#     AWARD_STATUS_IDS     = {5, 6, 50, 51}
#     SHORTLIST_STATUS_IDS = {3, 30}

#     if status_id in AWARD_STATUS_IDS:
#         return "implementation", "awarded"
#     if status_id in SHORTLIST_STATUS_IDS:
#         return "procurement", "shortlisted"
#     if deadline_dt and deadline_dt < now:
#         return "procurement", "closed"
#     if notice_code:
#         return LIFECYCLE_MAP.get(notice_code, (None, None))
#     return None, None


# # ── Procurement group ─────────────────────────────────────────────────────────

# def normalize_procurement_group(raw: Optional[str]) -> Optional[str]:
#     if not raw:
#         return None
#     raw = str(raw).strip()
#     full = PROCUREMENT_GROUP_MAP.get(raw.upper())
#     if full:
#         return full
#     for _, name in PROCUREMENT_GROUP_MAP.items():
#         if raw.lower() == name.lower():
#             return name
#     return raw


# # ── Procurement method ────────────────────────────────────────────────────────

# def normalize_procurement_method(raw: Optional[str]) -> Optional[str]:
#     if not raw:
#         return None
#     raw = str(raw).strip()
#     result = PROCUREMENT_METHOD_MAP.get(raw)
#     if result:
#         return result
#     for key, val in PROCUREMENT_METHOD_MAP.items():
#         if key.lower() == raw.lower():
#             return val
#     return raw


# # ── Language ──────────────────────────────────────────────────────────────────

# def normalize_language(raw: Optional[str]) -> Optional[str]:
#     if not raw:
#         return None
#     return LANGUAGE_MAP.get(str(raw).strip().lower(), str(raw).strip().lower()[:5])


# # ── Country ───────────────────────────────────────────────────────────────────

# def normalize_country(raw: Optional[str]) -> tuple[Optional[str], bool, Optional[str]]:
#     """Returns (country_name_normalized, is_multi_country, countries_list_json)."""
#     if not raw:
#         return None, False, None
#     raw = str(raw).strip()
#     parts = [p.strip() for p in re.split(r"[;,|/]", raw) if p.strip()]

#     normalized_parts = []
#     for part in parts:
#         alias = COUNTRY_ALIASES.get(part.lower())
#         normalized_parts.append(alias if alias else part.title())

#     is_multi = len(normalized_parts) > 1

#     global_keywords = {"global", "regional", "various", "multi-country", "worldwide"}
#     if any(p.lower() in global_keywords for p in parts):
#         return "Global / Multi-Country", True, json.dumps(normalized_parts)

#     primary = normalized_parts[0] if normalized_parts else None
#     countries_json = json.dumps(normalized_parts) if is_multi else None
#     return primary, is_multi, countries_json


# # ── Organisation name ─────────────────────────────────────────────────────────

# def normalize_org_name(raw: Optional[str]) -> Optional[str]:
#     if not raw:
#         return None
#     return " ".join(str(raw).strip().split())


# # ── Text cleaning ─────────────────────────────────────────────────────────────

# def _clean_text(raw: Optional[str]) -> Optional[str]:
#     """Strip HTML tags, decode common entities, collapse whitespace."""
#     if not raw:
#         return None
#     text = re.sub(r"<[^>]+>", " ", str(raw))
#     text = re.sub(r"&[a-zA-Z]+;", " ", text)
#     text = re.sub(r"\s+", " ", text).strip()
#     return text or None


# # ─────────────────────────────────────────────────────────────────────────────
# def normalize_title(raw: Optional[str], portal: str) -> Optional[str]:
#     """
#     Clean and normalize a tender title based on its source portal.

#     UNGM  — strip leading codes like "RFQ/67/2026/MEQ/ROLAC_" or "Qu26/03077_Redact/"
#     UNDP  — strip leading codes like "IC-", "LBN-CO-ITB-68-26-", "RFP07/2026/",
#              "SDC-03-2026 ", "Qu26/03077_Redact/"
#              strip trailing reference codes like "ILO/MDG/RFQ/2026/48- ERR"
#              remove stray "= " artefacts
#     AfDB  — strip leading notice-type + country prefix:
#              "AOI/SPN/PPM/EOI/RFP/RFQ/AMI/GPN - Country - "
#     All   — HTML stripping, entity decoding, whitespace collapse via _clean_text()
#     """
#     if not raw or not isinstance(raw, str):
#         return None

#     t = _clean_text(raw)
#     if not t:
#         return None
#     if portal == "ungm":
#      # 1. Slash-based reference codes
#      #    "RFQ/67/2026/MEQ/ROLAC_"  /  "Qu26/03077_Redact/"
#          t = re.sub(r'^[A-Z0-9][A-Za-z0-9_]*(?:/[A-Za-z0-9_]+)+[/_]', '', t)

#      # 2. LRFQ/LRFP/LRPS codes + optional filler "for [the]" 
#      #    "LRFQ-2026-9203124"  /  "LRFP 9203186 for the"
#          t = re.sub(r'^L?R[A-Z]{1,3}[-\s]\d[\d\-]*\s+(?:for\s+(?:the\s+)?)?', '', t, flags=re.IGNORECASE)

#      # 3. "Request for Quotation/Proposal[for/:/- ]" and "Call for quotation[-]"
#          t = re.sub(
#               r'^(?:Request\s+for\s+(?:Quotation|Proposal|Information)|Call\s+for\s+(?:Quotation|Proposals?))'
#               r'\s*(?:for\s+(?:the\s+)?|:\s*|-\s*)?',
#                '', t, flags=re.IGNORECASE
#          )

#       # 4. Leading qualifiers ending with period before real title
#       #    "For Egyptian companies only . "
#          t = re.sub(r'^(?:For\s+[^.]{0,50})\.\s+', '', t, flags=re.IGNORECASE)

#        # 5. <Strip org-name prefix at the very start
#          t = re.sub(r'^(WFP|WHO|ILO|IOM|ITU|FAO|IFC|IDLO)\s+', '', t)

#      # 6. Remove [Re-Tender] / [Extension] bracketed annotations
#          t = re.sub(r'\s*\[.*?\]', '', t)

#       # 7. Expand org abbreviations to full names anywhere in title
#          org_names = {
#               r'\bILO\b':  'International Labour Organization',
#              r'\bIOM\b':  'International Organization for Migration',
#              r'\bITU\b':  'International Telecommunication Union',
#              r'\bFAO\b':  'Food and Agriculture Organization',
#              r'\bWHO\b':  'World Health Organization',
#              r'\bIFC\b':  'International Finance Corporation',
#              r'\bWFP\b':  'World Food Programme',
#              r'\bIDLO\b': 'International Development Law Organization',
#          }
#          for abbr, full in org_names.items():
#               t = re.sub(abbr, full, t)

#         # 8. Remove leading/trailing - , _ and clean up double spaces
#          t = re.sub(r'^[\s\-_,]+', '', t)
#          t = re.sub(r'[\s\-_,]+$', '', t)
#          t = re.sub(r'\s{2,}', ' ', t)
#          t = t.strip(' .')
#     elif portal == "undp":
#         # ── Leading prefix ────────────────────────────────────────────────
#         # Covers all these patterns (must be followed by a real word):
#         #   "IC- "               "IC-"               (2-letter type code)
#         #   "IC/CFN/2603/05 - "  "LBN/CO/RFP/75/26 " (slash-separated)
#         #   "LBN-CO-ITB-68-26-"                       (dash-separated)
#         #   "RFP07/2026/"        "SDC-03-2026 "       (code with digits)
#         #   "Qu26/03077_Redact/"                      (mixed alphanumeric)
#         #
#         # Guard: lookahead (?=[A-Z][a-z]|[a-z]) protects all-caps French
#         # titles like "IMPRESSION POUR LE GUIDE..." from being stripped.
#         t = re.sub(
#             r'^[A-Z][A-Za-z0-9]*(?:[/\-][A-Za-z0-9]+)*(?:\s*[\-/]\s*|\s+)(?=[A-Z][a-z]|[a-z])',
#             '',
#             t,
#         )

#         # ── Trailing reference suffix ─────────────────────────────────────
#         # Catches codes appended at the end after a space or dash:
#         #   "... en langue malagasy ILO/MDG/RFQ/2026/48- ERR"
#         #   "... ILO/MDG/RFQ/2026/48-ERR"
#         # Pattern: whitespace or dash + all-caps code with slashes/digits
#         # ending the string (optionally with a trailing word like "ERR")
#         t = re.sub(
#             r'\s+[A-Z]{2,}(?:[/\-][A-Z0-9]+)+(?:[\-\s]+[A-Z]+)?\s*$',
#             '',
#             t,
#         )

#         # ── Stray "= " artefacts ──────────────────────────────────────────
#         t = re.sub(r'\s*=\s*', ' ', t)

#     elif portal == "afdb":
#          AFDB_COUNTRY_EXPANSIONS = {
#              r'\bRDC\b': 'Democratic Republic of the Congo',
#               r'\bRCA\b': 'Central African Republic',
#          }
#          for pattern, replacement in AFDB_COUNTRY_EXPANSIONS.items():
#               t = re.sub(pattern, replacement, t)
#          # Strip leading notice-type prefix ONLY — keep the country token.
#          # "AOI - "  "SPN - "  "AMI - "  "GPN - " etc.
#          t = re.sub(
#              r'^(?:AOI|SPN|PPM|EOI|RFP|RFQ|AMI|AAO|AGPM|PPM|REOI|AON|GPN)\s*-\s*',
#              '',
#               t,
#              flags=re.IGNORECASE,
#          )

#          t = re.sub(
#              r'^([^-]+?)\s*-\s*',
#              r'\1 ',
#              t,
#           )
#          t = re.sub(
#              r'\s+-\s+[A-Z][A-Z0-9\-]+$',
#               '',
#              t,
#           )
        

#     # Strip trailing project code: " - PASEA-RD"  " - RWSSIP"  " - TACFiC"
#     # Pattern: space-dash-space + 1 or more uppercase/digit/dash words at end of string.
#     # The project code is typically all-uppercase or mixed with digits and dashes.
#     # We protect lowercase words so real title endings are not affected.
#     t = re.sub(
#         r'\s+-\s+[A-Z][A-Z0-9\-]+$',
#         '',
#         t,
#     )
#     # Final cleanup for all portals
#     t = re.sub(r'\s{2,}', ' ', t).strip()
#     return t or None


# # ── Validation ────────────────────────────────────────────────────────────────

# REQUIRED_FIELDS = [
#     "source_portal", "notice_id", "title_clean",
#     "publication_datetime", "country_name_normalized", "notice_type_normalized",
# ]


# def validate(record: dict) -> tuple[bool, list[str], list[str]]:
#     """Returns (is_valid, missing_fields, validation_flags)."""
#     missing = [f for f in REQUIRED_FIELDS if not record.get(f)]
#     flags: list[str] = []

#     if record.get("deadline_datetime") and record.get("publication_datetime"):
#         if record["deadline_datetime"] < record["publication_datetime"]:
#             flags.append("deadline_before_publication")

#     if record.get("budget_numeric") is not None and record["budget_numeric"] < 0:
#         flags.append("negative_budget")

#     if record.get("days_to_deadline") is not None and record["days_to_deadline"] < -365:
#         flags.append("deadline_very_old")

#     return len(missing) == 0 and len(flags) == 0, missing, flags


# # ─────────────────────────────────────────────────────────────────────────────
# #  CORE NORMALIZER
# # ─────────────────────────────────────────────────────────────────────────────

# def normalize_tender(tender: Tender, session) -> Optional[dict]:
#     """
#     Takes a raw Tender ORM object.
#     Returns a dict ready to upsert into normalized_tenders, or None on hard failure.
#     """
#     try:
#         now = _now_utc()

#         # Dates
#         pub_dt  = parse_date(tender.publication_date)
#         dead_dt = parse_date(tender.deadline_date, tender.deadline_time)
#         days    = days_between(now, dead_dt)  # positive = future, negative = past

#         # Notice type
#         notice_code  = normalize_notice_type(tender.notice_type)
#         notice_label = notice_type_label(notice_code)
#         raw_notice   = tender.notice_type if tender.notice_type and tender.notice_type.strip() else None
#         notice_type_final = notice_label or notice_code or raw_notice

#         # Lifecycle + status
#         stage, status = resolve_lifecycle(notice_code, dead_dt, tender.status_id)

#         # Country
#         country_norm, is_multi, countries_json = normalize_country(tender.country)

#         # Budget
#         budget_num, currency_iso = parse_budget(tender.budget)
#         if not currency_iso and tender.currency:
#             currency_iso = CURRENCY_MAP.get(str(tender.currency).strip().upper())
#         budget_missing = budget_num is None

#         # Procurement
#         proc_group  = normalize_procurement_group(tender.procurement_group)
#         proc_method = normalize_procurement_method(
#             tender.procurement_method_name or tender.procurement_method_code
#         )

#         # Organisation + contact
#         org_name_norm = None
#         contact_name  = None
#         contact_email = None
#         contact_phone = None

#         if tender.organisation_id:
#             org = session.get(Organisation, tender.organisation_id)
#             if org:
#                 org_name_norm = normalize_org_name(org.name)
#                 contact = session.execute(
#                     select(Contact)
#                     .where(Contact.organisation_id == org.id)
#                     .limit(1)
#                 ).scalar_one_or_none()
#                 if contact:
#                     contact_name  = contact.name
#                     contact_email = contact.email
#                     contact_phone = contact.phone

#         has_pdf = bool(tender.pdf_path)

#         record = {
#             # Core identification
#             "tender_id":                    tender.id,
#             "source_portal":                tender.source_portal,
#             "notice_id":                    tender.tender_id,
#             "source_url":                   tender.source_url,
#             "organisation_id":              tender.organisation_id,
#             "organisation_name_normalized": org_name_norm,
#             # Title & text — normalize_title handles portal-specific prefix stripping
#             "title_clean":       normalize_title(tender.title, tender.source_portal),
#             "description_clean": None,   # reserved for future NLP pipeline
#             # Dates
#             "publication_datetime": pub_dt,
#             "deadline_datetime":    dead_dt,
#             "days_to_deadline":     days,
#             "created_at":           now,
#             "updated_at":           now,
#             # Country
#             "country_name_normalized": country_norm,
#             "is_multi_country":        is_multi,
#             "countries_list":          countries_json,
#             # Notice type
#             "notice_type_normalized": notice_type_final,
#             # Lifecycle
#             "lifecycle_stage":   stage,
#             "status_normalized": status,
#             # Procurement
#             "project_id":                   tender.project_id,
#             "procurement_group_normalized": proc_group,
#             "procurement_method_name":      proc_method,
#             # Budget
#             "budget_numeric": budget_num,
#             "currency_iso":   currency_iso,
#             "budget_missing": budget_missing,
#             # Documents
#             "pdf_path":       tender.pdf_path,
#             "has_pdf":        has_pdf,
#             "document_count": 1 if has_pdf else 0,
#             # Contact
#             "contact_name":  contact_name,
#             "contact_email": contact_email,
#             "contact_phone": contact_phone,
#             # Status
#             "status_id":         tender.status_id,
#             "source_status_raw": str(tender.status_id) if tender.status_id is not None else None,
#             # Sector — reserved for NLP pipeline
#             "cpv_code":      None,
#             "cpv_label":     None,
#             "unspsc_code":   None,
#             "unspsc_label":  None,
#             "sector_source": None,
#             # Data integrity (filled below)
#             "normalization_status": "success",
#             "normalized_at":        now,
#         }

#         is_valid, missing, flags = validate(record)
#         record["is_valid"]         = is_valid
#         record["missing_fields"]   = "|".join(missing) if missing else None
#         record["validation_flags"] = "|".join(flags)   if flags   else None
#         if missing or flags:
#             record["normalization_status"] = "flagged"

#         return record

#     except Exception as exc:
#         log.error("Failed to normalize tender id=%s: %s", tender.id, exc, exc_info=True)
#         return None


# # ─────────────────────────────────────────────────────────────────────────────
# #  UPSERT
# # ─────────────────────────────────────────────────────────────────────────────

# def upsert_normalized(session, record: dict) -> None:
#     """
#     PostgreSQL upsert on (tender_id).
#     On conflict: update all fields except id, tender_id, and created_at.
#     """
#     stmt = pg_insert(NormalizedTender.__table__).values(**record)
#     update_cols = {
#         c.name: stmt.excluded[c.name]
#         for c in NormalizedTender.__table__.columns
#         if c.name not in ("id", "tender_id", "created_at")
#     }
#     stmt = stmt.on_conflict_do_update(
#         index_elements=["tender_id"],
#         set_=update_cols,
#     )
#     session.execute(stmt)
#     session.flush()


# # ─────────────────────────────────────────────────────────────────────────────
# #  RUN LOGIC
# # ─────────────────────────────────────────────────────────────────────────────

# SUPPORTED_PORTALS = {"worldbank", "afdb", "ungm", "undp"}


# def run_normalization(portal: Optional[str] = None, dry_run: bool = False) -> None:
#     portals = [portal] if portal else list(SUPPORTED_PORTALS)
#     for p in portals:
#         if p not in SUPPORTED_PORTALS:
#             log.warning("Unknown portal '%s' — skipping.", p)
#             continue
#         log.info("▶ Starting normalization for portal: %s", p)
#         _run_portal(p, dry_run)


# def _run_portal(portal: str, dry_run: bool) -> None:
#     counters = dict(total=0, success=0, flagged=0, failed=0, skipped=0)

#     # Step 1: collect IDs in a short session (avoids long-lived transaction)
#     with get_session() as session:
#         tender_ids = session.execute(
#             select(Tender.id).where(Tender.source_portal == portal)
#         ).scalars().all()

#     counters["total"] = len(tender_ids)
#     log.info("  Found %d raw tenders for %s", counters["total"], portal)

#     # Step 2: process each tender in its own session
#     for tender_id in tender_ids:
#         try:
#             with get_session() as session:
#                 tender_obj = session.get(Tender, tender_id)
#                 if not tender_obj:
#                     counters["skipped"] += 1
#                     continue

#                 record = normalize_tender(tender_obj, session)
#                 if record is None:
#                     counters["failed"] += 1
#                     continue

#                 if not dry_run:
#                     upsert_normalized(session, record)

#                 if record.get("normalization_status") == "flagged":
#                     counters["flagged"] += 1
#                 else:
#                     counters["success"] += 1

#         except Exception as exc:
#             log.error("  Failed tender id=%s: %s", tender_id, exc, exc_info=True)
#             counters["failed"] += 1

#     # Step 3: write normalization log
#     if not dry_run:
#         try:
#             with get_session() as session:
#                 session.add(NormalizationLog(
#                     portal      = portal,
#                     run_at      = _now_utc(),
#                     total_input = counters["total"],
#                     success     = counters["success"],
#                     flagged     = counters["flagged"],
#                     failed      = counters["failed"],
#                     skipped     = counters["skipped"],
#                     notes       = None,
#                 ))
#         except Exception as exc:
#             log.error("  Failed to write normalization log for %s: %s", portal, exc, exc_info=True)

#     log.info(
#         "  ✓ %s done — success=%d  flagged=%d  failed=%d  skipped=%d",
#         portal, counters["success"], counters["flagged"],
#         counters["failed"], counters["skipped"],
#     )


# # ─────────────────────────────────────────────────────────────────────────────
# #  CLI
# # ─────────────────────────────────────────────────────────────────────────────

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(
#         description="Normalize tenders into normalized_tenders table."
#     )
#     parser.add_argument(
#         "--portal",
#         choices=list(SUPPORTED_PORTALS),
#         default=None,
#         help="Process a single portal only (default: all portals).",
#     )
#     parser.add_argument(
#         "--dry-run",
#         action="store_true",
#         help="Run normalization logic without writing to the DB.",
#     )
#     args = parser.parse_args()
#     run_normalization(portal=args.portal, dry_run=args.dry_run)





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
}

# ── Procurement group ─────────────────────────────────────────────────────────
PROCUREMENT_GROUP_MAP: dict[str, str] = {
    "CS": "Consulting Services",
    "CW": "Civil Works",
    "GO": "Goods",
    "NC": "Non-Consulting Services",
}

# ── Procurement method ────────────────────────────────────────────────────────
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
    "english":    "en", "en": "en", "eng": "en",
    "french":     "fr", "fr": "fr", "fra": "fr", "français": "fr",
    "spanish":    "es", "es": "es", "spa": "es",
    "arabic":     "ar", "ar": "ar",
    "portuguese": "pt", "pt": "pt",
    "russian":    "ru", "ru": "ru",
}

# ── Country aliases → normalized name ────────────────────────────────────────
COUNTRY_ALIASES: dict[str, str] = {
    "democratic republic of congo":  "Democratic Republic of the Congo",
    "drc":                           "Democratic Republic of the Congo",
    "congo, democratic republic":    "Democratic Republic of the Congo",
    "congo, rep.":                   "Republic of the Congo",
    "tanzania":                      "United Republic of Tanzania",
    "ivory coast":                   "Côte d'Ivoire",
    "cote d'ivoire":                 "Côte d'Ivoire",
    "cote divoire":                  "Côte d'Ivoire",
    "west bank and gaza":            "Palestinian Territory",
    "west bank":                     "Palestinian Territory",
    "gaza":                          "Palestinian Territory",
    "lao pdr":                       "Laos",
    "lao":                           "Laos",
    "vietnam":                       "Viet Nam",
    "viet nam":                      "Viet Nam",
    "south korea":                   "Republic of Korea",
    "north korea":                   "Democratic People's Republic of Korea",
    "russia":                        "Russian Federation",
    "iran":                          "Islamic Republic of Iran",
    "syria":                         "Syrian Arab Republic",
    "bolivia":                       "Plurinational State of Bolivia",
    "moldova":                       "Republic of Moldova",
    "micronesia":                    "Federated States of Micronesia",
    "trinidad":                      "Trinidad and Tobago",
    "saint kitts":                   "Saint Kitts and Nevis",
    "global":                        "Global / Multi-Country",
    "multi-country":                 "Global / Multi-Country",
    "regional":                      "Regional",
    "various":                       "Global / Multi-Country",
}

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
    """Parse a date string (+optional time) into a UTC-aware datetime."""
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
    """Returns (numeric_value, currency_iso) or (None, None)."""
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
    """Returns internal code (e.g. 'IFB') or None."""
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
    """Full display name, e.g. 'IFB' → 'Invitation for Bids'."""
    return NOTICE_TYPE_LABELS.get(code) if code else None


# ── Lifecycle + status ────────────────────────────────────────────────────────

def resolve_lifecycle(
    notice_code: Optional[str],
    deadline_dt: Optional[datetime],
    status_id:   Optional[int],
) -> tuple[Optional[str], Optional[str]]:
    """
    Returns (lifecycle_stage, status_normalized).
    Priority: award status_id > shortlist status_id > deadline passed > notice type map.
    """
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
    if not raw:
        return None
    return LANGUAGE_MAP.get(str(raw).strip().lower(), str(raw).strip().lower()[:5])


# ── Country ───────────────────────────────────────────────────────────────────

def normalize_country(raw: Optional[str]) -> tuple[Optional[str], bool, Optional[str]]:
    """Returns (country_name_normalized, is_multi_country, countries_list_json)."""
    if not raw:
        return None, False, None
    raw = str(raw).strip()
    parts = [p.strip() for p in re.split(r"[;,|/]", raw) if p.strip()]

    normalized_parts = []
    for part in parts:
        alias = COUNTRY_ALIASES.get(part.lower())
        normalized_parts.append(alias if alias else part.title())

    is_multi = len(normalized_parts) > 1

    global_keywords = {"global", "regional", "various", "multi-country", "worldwide"}
    if any(p.lower() in global_keywords for p in parts):
        return "Global / Multi-Country", True, json.dumps(normalized_parts)

    primary = normalized_parts[0] if normalized_parts else None
    countries_json = json.dumps(normalized_parts) if is_multi else None
    return primary, is_multi, countries_json


# ── Organisation name ─────────────────────────────────────────────────────────

def normalize_org_name(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    return " ".join(str(raw).strip().split())


# ── Text cleaning ─────────────────────────────────────────────────────────────

def _clean_text(raw: Optional[str]) -> Optional[str]:
    """Strip HTML tags, decode common entities, collapse whitespace."""
    if not raw:
        return None
    text = re.sub(r"<[^>]+>", " ", str(raw))
    text = re.sub(r"&[a-zA-Z]+;", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


# ─────────────────────────────────────────────────────────────────────────────
#  TITLE NORMALIZER
# ─────────────────────────────────────────────────────────────────────────────

def normalize_title(raw: Optional[str], portal: str) -> Optional[str]:
    """
    Clean and normalize a tender title based on its source portal.
    """
    if not raw or not isinstance(raw, str):
        return None

    t = _clean_text(raw)
    if not t:
        return None

    if portal == "ungm":

        # 1. Slash-based reference codes
        #    "RFQ/67/2026/MEQ/ROLAC_"  /  "Qu26/03077_Redact/"
        t = re.sub(r'^[A-Z0-9][A-Za-z0-9_]*(?:/[A-Za-z0-9_]+)+[/_]', '', t)

        # 2. LRFQ/LRFP/LRPS codes + optional filler "for [the]"
        #    "LRFQ-2026-9203124"  /  "LRFP 9203186 for the"
        t = re.sub(r'^L?R[A-Z]{1,3}[-\s]\d[\d\-]*\s+(?:for\s+(?:the\s+)?)?', '', t, flags=re.IGNORECASE)

        # 3. "Request for Quotation/Proposal[for/:/- ]" and "Call for quotation[-]"
        t = re.sub(
            r'^(?:Request\s+for\s+(?:Quotation|Proposal|Information)|Call\s+for\s+(?:Quotation|Proposals?))'
            r'\s*(?:for\s+(?:the\s+)?|:\s*|-\s*)?',
            '', t, flags=re.IGNORECASE
        )

        # 4. Leading qualifiers ending with period before real title
        #    "For Egyptian companies only . "
        t = re.sub(r'^(?:For\s+[^.]{0,50})\.\s+', '', t, flags=re.IGNORECASE)

        # 5. Strip org-name prefix at the very start
        t = re.sub(r'^(WFP|WHO|ILO|IOM|ITU|FAO|IFC|IDLO)\s+', '', t)

        # 6. Remove [Re-Tender] / [Extension] bracketed annotations
        t = re.sub(r'\s*\[.*?\]', '', t)

        # 7. Expand org abbreviations to full names anywhere in title
        org_names = {
            r'\bILO\b':  'International Labour Organization',
            r'\bIOM\b':  'International Organization for Migration',
            r'\bITU\b':  'International Telecommunication Union',
            r'\bFAO\b':  'Food and Agriculture Organization',
            r'\bWHO\b':  'World Health Organization',
            r'\bIFC\b':  'International Finance Corporation',
            r'\bWFP\b':  'World Food Programme',
            r'\bIDLO\b': 'Inter national Development Law Organization',
        }
        for abbr, full in org_names.items():
            t = re.sub(abbr, full, t)

        # 8. Remove leading/trailing - , _ and clean up double spaces
        t = re.sub(r'^[\s\-_,]+', '', t)
        t = re.sub(r'[\s\-_,]+$', '', t)
        t = re.sub(r'\s{2,}', ' ', t)
        t = t.strip(' .')

    elif portal == "undp":

        # Leading prefix codes
        t = re.sub(
            r'^[A-Z][A-Za-z0-9]*(?:[/\-][A-Za-z0-9]+)*(?:\s*[\-/]\s*|\s+)(?=[A-Z][a-z]|[a-z])',
            '',
            t,
        )

        # Trailing reference suffix
        t = re.sub(
            r'\s+[A-Z]{2,}(?:[/\-][A-Z0-9]+)+(?:[\-\s]+[A-Z]+)?\s*$',
            '',
            t,
        )

        # Stray "= " artefacts
        t = re.sub(r'\s*=\s*', ' ', t)

    elif portal == "afdb":

        # Country code expansions
        AFDB_COUNTRY_EXPANSIONS = {
            r'\bRDC\b': 'Democratic Republic of the Congo',
            r'\bRCA\b': 'Central African Republic',
        }
        for pattern, replacement in AFDB_COUNTRY_EXPANSIONS.items():
            t = re.sub(pattern, replacement, t)

        # Strip leading notice-type prefix
        t = re.sub(
            r'^(?:AOI|SPN|PPM|EOI|RFP|RFQ|AMI|AAO|AGPM|REOI|AON|GPN)\s*-\s*',
            '',
            t,
            flags=re.IGNORECASE,
        )

        t = re.sub(r'^([^-]+?)\s*-\s*', r'\1 ', t)
        t = re.sub(r'\s+-\s+[A-Z][A-Z0-9\-]+$', '', t)

    # Strip trailing project code for all portals: " - PASEA-RD" / " - RWSSIP"
    t = re.sub(r'\s+-\s+[A-Z][A-Z0-9\-]+$', '', t)

    # Final cleanup for all portals
    t = re.sub(r'\s{2,}', ' ', t).strip()
    return t or None


# ── Validation ────────────────────────────────────────────────────────────────

REQUIRED_FIELDS = [
    "source_portal", "notice_id", "title_clean",
    "publication_datetime", "country_name_normalized", "notice_type_normalized",
]


def validate(record: dict) -> tuple[bool, list[str], list[str]]:
    """Returns (is_valid, missing_fields, validation_flags)."""
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
    """
    Takes a raw Tender ORM object.
    Returns a dict ready to upsert into normalized_tenders, or None on hard failure.
    """
    try:
        now = _now_utc()

        # Dates
        pub_dt  = parse_date(tender.publication_date)
        dead_dt = parse_date(tender.deadline_date, tender.deadline_time)
        days    = days_between(now, dead_dt)

        # Notice type
        notice_code       = normalize_notice_type(tender.notice_type)
        notice_label      = notice_type_label(notice_code)
        raw_notice        = tender.notice_type if tender.notice_type and tender.notice_type.strip() else None
        notice_type_final = notice_label or notice_code or raw_notice

        # Lifecycle + status
        stage, status = resolve_lifecycle(notice_code, dead_dt, tender.status_id)

        # Country
        country_norm, is_multi, countries_json = normalize_country(tender.country)

        # Budget
        budget_num, currency_iso = parse_budget(tender.budget)
        if not currency_iso and tender.currency:
            currency_iso = CURRENCY_MAP.get(str(tender.currency).strip().upper())
        budget_missing = budget_num is None

        # Procurement
        proc_group  = normalize_procurement_group(tender.procurement_group)
        proc_method = normalize_procurement_method(
            tender.procurement_method_name or tender.procurement_method_code
        )

        # Organisation + contact
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

        has_pdf = bool(tender.pdf_path)

        record = {
            # Core identification
            "tender_id":                    tender.id,
            "source_portal":                tender.source_portal,
            "notice_id":                    tender.tender_id,
            "source_url":                   tender.source_url,
            "organisation_id":              tender.organisation_id,
            "organisation_name_normalized": normalize_org_name(org_name_norm),
            # Title
            "title_clean":  normalize_title(tender.title, tender.source_portal),
            "description_clean": None,
            # Dates
            "publication_datetime": pub_dt,
            "deadline_datetime":    dead_dt,
            "days_to_deadline":     days,
            "created_at":           now,
            "updated_at":           now,
            # Country
            "country_name_normalized": country_norm,
            "is_multi_country":        is_multi,
            "countries_list":          countries_json,
            # Notice type
            "notice_type_normalized": notice_type_final,
            # Lifecycle
            "lifecycle_stage":   stage,
            "status_normalized": status,
            # Procurement
            "project_id":                   tender.project_id,
            "procurement_group_normalized": proc_group,
            "procurement_method_name":      proc_method,
            # Budget
            "budget_numeric": budget_num,
            "currency_iso":   currency_iso,
            "budget_missing": budget_missing,
            # Documents
            "pdf_path":       tender.pdf_path,
            "has_pdf":        has_pdf,
            "document_count": 1 if has_pdf else 0,
            # Contact
            "contact_name":  contact_name,
            "contact_email": contact_email,
            "contact_phone": contact_phone,
            # Status
            "status_id":         tender.status_id,
            "source_status_raw": str(tender.status_id) if tender.status_id is not None else None,
            # Sector — reserved for NLP pipeline
            "cpv_code":      None,
            "cpv_label":     None,
            "unspsc_code":   None,
            "unspsc_label":  None,
            "sector_source": None,
            # Data integrity
            "normalization_status": "success",
            "normalized_at":        now,
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
    """
    PostgreSQL upsert on (tender_id).
    On conflict: update all fields except id, tender_id, and created_at.
    """
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
    "PPM":                                "PPM",
    "AGPM":                               "GPN",
    "AMI":                                "EOI",
    "Request for EOI":                    "EOI",
    "Request for Expression of Interest": "EOI",
    "Call for individual consultants":    "EOI",
    "Call for implementing partners":     "EOI",
    "RFI - Request for Information":      "RFI",
    "Invitation for Prequalification":    "PREQUAL",
    "Request for Proposals":              "RFP",
    "Request for proposal":               "RFP",
    "RFP - Request for proposal":         "RFP",
    "Grant support-call for proposal":    "RFP",
    "Invitation for Bids":                "IFB",
    "Invitation to bid":                  "IFB",
    "ITB - Invitation to bid":            "IFB",
    "Request for Bids":                   "IFB",
    "SPN":                                "IFB",
    "AOI":                                "IFB",
    "AAO":                                "IFB",
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

# ── Procurement group ─────────────────────────────────────────────────────────
PROCUREMENT_GROUP_MAP: dict[str, str] = {
    "CS": "Consulting Services",
    "CW": "Civil Works",
    "GO": "Goods",
    "NC": "Non-Consulting Services",
}

# ── Procurement method ────────────────────────────────────────────────────────
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
    "english":    "en", "en": "en", "eng": "en",
    "french":     "fr", "fr": "fr", "fra": "fr", "français": "fr",
    "spanish":    "es", "es": "es", "spa": "es",
    "arabic":     "ar", "ar": "ar",
    "portuguese": "pt", "pt": "pt",
    "russian":    "ru", "ru": "ru",
}

# ── Country aliases → normalized name ────────────────────────────────────────
COUNTRY_ALIASES: dict[str, str] = {
    "democratic republic of congo":  "Democratic Republic of the Congo",
    "drc":                           "Democratic Republic of the Congo",
    "congo, democratic republic":    "Democratic Republic of the Congo",
    "congo, rep.":                   "Republic of the Congo",
    "tanzania":                      "United Republic of Tanzania",
    "ivory coast":                   "Côte d'Ivoire",
    "cote d'ivoire":                 "Côte d'Ivoire",
    "cote divoire":                  "Côte d'Ivoire",
    "west bank and gaza":            "Palestinian Territory",
    "west bank":                     "Palestinian Territory",
    "gaza":                          "Palestinian Territory",
    "lao pdr":                       "Laos",
    "lao":                           "Laos",
    "vietnam":                       "Viet Nam",
    "viet nam":                      "Viet Nam",
    "south korea":                   "Republic of Korea",
    "north korea":                   "Democratic People's Republic of Korea",
    "russia":                        "Russian Federation",
    "iran":                          "Islamic Republic of Iran",
    "syria":                         "Syrian Arab Republic",
    "bolivia":                       "Plurinational State of Bolivia",
    "moldova":                       "Republic of Moldova",
    "micronesia":                    "Federated States of Micronesia",
    "trinidad":                      "Trinidad and Tobago",
    "saint kitts":                   "Saint Kitts and Nevis",
    "global":                        "Global / Multi-Country",
    "multi-country":                 "Global / Multi-Country",
    "regional":                      "Regional",
    "various":                       "Global / Multi-Country",
}

# ─────────────────────────────────────────────────────────────────────────────
#  AfDB TITLE — notice type and country extraction
#
#  AfDB raw title structure (always dash-separated):
#      {CODE} - {Country} - {Title body} - {Project code}
#
#  Examples:
#      "AMI - Sénégal - Consultant Individuel CI-01"  → EOI  / Sénégal
#      "PPM - RDC - Projet d'Aménagement..."          → GPN  / Democratic Republic of the Congo
#      "SPN - Supply, Delivery..."                    → IFB  / None  (no country)
#      "Attribution de contrats - Guinée - ..."       → AWARD / Guinée
#      "PPM – Cameroun - Programme..."                → GPN  / Cameroun  (en-dash handled)
# ─────────────────────────────────────────────────────────────────────────────

# Maps AfDB title prefix → internal notice type code.
# Covers abbreviations AND full French/English phrases seen in real data.
_AFDB_NOTICE_PREFIX_MAP: dict[str, str] = {
    "AMI":    "EOI",    # Avis à Manifestation d'Intérêt
    "AOI":    "IFB",    # Appel d'Offres International
    "AAO":    "IFB",    # Avis d'Appel d'Offres
    "AON":    "IFB",    # Appel d'Offres National
    "SPN":    "IFB",    # Specific Procurement Notice
    "GPN":    "GPN",    # General Procurement Notice
    "AGPM":   "GPN",    # Avis Général de Passation des Marchés
    "PPM":    "GPN",    # Plan de Passation des Marchés
    "EOI":    "EOI",
    "REOI":   "EOI",    # Request for Expression of Interest
    "RFP":    "RFP",
    "RFQ":    "RFQ",
    # Full phrases (matched lowercased)
    "attribution de contrats":            "AWARD",
    "avis d'attribution":                 "AWARD",
    "contract award":                     "AWARD",
    "invitation for bids":                "IFB",
    "request for proposals":              "RFP",
    "request for expression of interest": "EOI",
    "general procurement notice":         "GPN",
}

# Country abbreviations used in AfDB titles → expanded name.
# Expanded names pass through the existing normalize_country() pipeline.
_AFDB_COUNTRY_ABBR: dict[str, str] = {
    "RDC":           "Democratic Republic of the Congo",
    "DRC":           "Democratic Republic of the Congo",
    "RCA":           "Central African Republic",
    "CAR":           "Central African Republic",
    "ROC":           "Republic of the Congo",
    "MULTINATIONAL": "Multinational",
}

# First words that signal segment 1 is a title body, not a country name
_AFDB_BODY_SIGNALS = {
    "supply", "design", "construction", "provision", "consulting",
    "recruitment", "recrutement", "technical", "services", "upgrading",
    "study", "acquisition", "rehabilitation", "hiring", "support",
}


def _parse_afdb_title(raw_title: str) -> tuple[str | None, str | None]:
    """
    Extract (notice_type_code, country_raw) from a raw AfDB title string.

    Returns raw strings only — no normalization applied here.
    normalize_notice_type() and normalize_country() handle standardization
    downstream as part of the existing pipeline.

    Returns (None, None) if extraction fails.
    """
    if not raw_title:
        return None, None

    # Normalise en-dash / em-dash variants then split on " - "
    title    = raw_title.replace("\u2013", "-").replace("\u2014", "-")
    segments = [s.strip() for s in re.split(r"\s+-\s+", title) if s.strip()]

    if not segments:
        return None, None

    # ── Segment 0: notice type ────────────────────────────────────────────────
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

    # ── Segment 1: country ────────────────────────────────────────────────────
    country_raw = None

    if notice_code is not None and len(segments) >= 2:
        seg1     = segments[1].strip()
        expanded = _AFDB_COUNTRY_ABBR.get(seg1.upper())
        if expanded:
            country_raw = expanded
        else:
            # Reject if segment looks like the start of a title body:
            #   - contains a comma  ("Supply, Delivery and…")
            #   - first word is a body-signal verb/noun
            #   - contains digits   (reference numbers)
            #   - too long          (> 40 chars)
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
    """
    Resolve notice_type and country for an AfDB tender.

    Priority:
        1. Structured fields (tender.notice_type, tender.country) — always preferred.
        2. Title parsing — fills only what is missing.

    Returns:
        (notice_code, notice_type_final, country_norm, is_multi, countries_json)

    Drop-in replacement for the notice type + country block in normalize_tender().
    """
    # ── Notice type ───────────────────────────────────────────────────────────
    notice_code = normalize_notice_type(tender.notice_type)

    # Parse title once — used as fallback for both fields
    title_code, title_country = _parse_afdb_title(tender.title or "")

    if not notice_code and title_code:
        notice_code = title_code

    notice_label      = notice_type_label(notice_code)
    raw_notice        = (tender.notice_type or "").strip() or None
    notice_type_final = notice_label or notice_code or raw_notice

    # ── Country ───────────────────────────────────────────────────────────────
    # Structured tender.country wins; title parse fills the gap when missing
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
    """Parse a date string (+optional time) into a UTC-aware datetime."""
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
    """Returns (numeric_value, currency_iso) or (None, None)."""
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
    """Returns internal code (e.g. 'IFB') or None."""
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
    """Full display name, e.g. 'IFB' → 'Invitation for Bids'."""
    return NOTICE_TYPE_LABELS.get(code) if code else None


# ── Lifecycle + status ────────────────────────────────────────────────────────

def resolve_lifecycle(
    notice_code: Optional[str],
    deadline_dt: Optional[datetime],
    status_id:   Optional[int],
) -> tuple[Optional[str], Optional[str]]:
    """
    Returns (lifecycle_stage, status_normalized).
    Priority: award status_id > shortlist status_id > deadline passed > notice type map.
    """
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
    if not raw:
        return None
    return LANGUAGE_MAP.get(str(raw).strip().lower(), str(raw).strip().lower()[:5])


# ── Country ───────────────────────────────────────────────────────────────────

def normalize_country(raw: Optional[str]) -> tuple[Optional[str], bool, Optional[str]]:
    """Returns (country_name_normalized, is_multi_country, countries_list_json)."""
    if not raw:
        return None, False, None
    raw = str(raw).strip()
    parts = [p.strip() for p in re.split(r"[;,|/]", raw) if p.strip()]

    normalized_parts = []
    for part in parts:
        alias = COUNTRY_ALIASES.get(part.lower())
        normalized_parts.append(alias if alias else part.title())

    is_multi = len(normalized_parts) > 1

    global_keywords = {"global", "regional", "various", "multi-country", "worldwide"}
    if any(p.lower() in global_keywords for p in parts):
        return "Global / Multi-Country", True, json.dumps(normalized_parts)

    primary = normalized_parts[0] if normalized_parts else None
    countries_json = json.dumps(normalized_parts) if is_multi else None
    return primary, is_multi, countries_json


# ── Organisation name ─────────────────────────────────────────────────────────

def normalize_org_name(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    return " ".join(str(raw).strip().split())


# ── Text cleaning ─────────────────────────────────────────────────────────────

def _clean_text(raw: Optional[str]) -> Optional[str]:
    """Strip HTML tags, decode common entities, collapse whitespace."""
    if not raw:
        return None
    text = re.sub(r"<[^>]+>", " ", str(raw))
    text = re.sub(r"&[a-zA-Z]+;", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


# ─────────────────────────────────────────────────────────────────────────────
#  TITLE NORMALIZER
# ─────────────────────────────────────────────────────────────────────────────

def normalize_title(raw: Optional[str], portal: str) -> Optional[str]:
    """Clean and normalize a tender title based on its source portal."""
    if not raw or not isinstance(raw, str):
        return None

    t = _clean_text(raw)
    if not t:
        return None

    if portal == "ungm":
        t = re.sub(r'^[A-Z0-9][A-Za-z0-9_]*(?:/[A-Za-z0-9_]+)+[/_]', '', t)
        t = re.sub(r'^L?R[A-Z]{1,3}[-\s]\d[\d\-]*\s+(?:for\s+(?:the\s+)?)?', '', t, flags=re.IGNORECASE)
        t = re.sub(
            r'^(?:Request\s+for\s+(?:Quotation|Proposal|Information)|Call\s+for\s+(?:Quotation|Proposals?))'
            r'\s*(?:for\s+(?:the\s+)?|:\s*|-\s*)?',
            '', t, flags=re.IGNORECASE,
        )
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
        t = re.sub(
            r'^[A-Z][A-Za-z0-9]*(?:[/\-][A-Za-z0-9]+)*(?:\s*[\-/]\s*|\s+)(?=[A-Z][a-z]|[a-z])',
            '', t,
        )
        t = re.sub(
            r'\s+[A-Z]{2,}(?:[/\-][A-Z0-9]+)+(?:[\-\s]+[A-Z]+)?\s*$',
            '', t,
        )
        t = re.sub(r'\s*=\s*', ' ', t)

    elif portal == "afdb":
        # Expand country abbreviations in title
        for abbr, full in _AFDB_COUNTRY_ABBR.items():
            t = re.sub(rf'\b{abbr}\b', full, t)
        # Strip leading notice-type code prefix
        t = re.sub(
            r'^(?:AOI|SPN|PPM|EOI|RFP|RFQ|AMI|AAO|AGPM|REOI|AON|GPN)\s*-\s*',
            '', t, flags=re.IGNORECASE,
        )
        # Strip leading country segment (now that notice code is gone)
        t = re.sub(r'^([^-]+?)\s*-\s*', r'\1 ', t)
        t = re.sub(r'\s+-\s+[A-Z][A-Z0-9\-]+$', '', t)

    # Strip trailing project code for all portals: " - PASEA-RD" / " - RWSSIP"
    t = re.sub(r'\s+-\s+[A-Z][A-Z0-9\-]+$', '', t)

    # Final cleanup
    t = re.sub(r'\s{2,}', ' ', t).strip()
    return t or None


# ── Validation ────────────────────────────────────────────────────────────────

REQUIRED_FIELDS = [
    "source_portal", "notice_id", "title_clean",
    "publication_datetime", "country_name_normalized", "notice_type_normalized",
]


def validate(record: dict) -> tuple[bool, list[str], list[str]]:
    """Returns (is_valid, missing_fields, validation_flags)."""
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
    """
    Takes a raw Tender ORM object.
    Returns a dict ready to upsert into normalized_tenders, or None on hard failure.
    """
    try:
        now = _now_utc()

        # Dates
        pub_dt  = parse_date(tender.publication_date)
        dead_dt = parse_date(tender.deadline_date, tender.deadline_time)
        days    = days_between(now, dead_dt)

        # ── Notice type + Country ─────────────────────────────────────────────
        # AfDB: extract from title when structured fields are missing.
        # All other portals: use existing structured fields as before.
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

        # Lifecycle + status
        stage, status = resolve_lifecycle(notice_code, dead_dt, tender.status_id)

        # Budget
        budget_num, currency_iso = parse_budget(tender.budget)
        if not currency_iso and tender.currency:
            currency_iso = CURRENCY_MAP.get(str(tender.currency).strip().upper())
        budget_missing = budget_num is None

        # Procurement
        proc_group  = normalize_procurement_group(tender.procurement_group)
        proc_method = normalize_procurement_method(
            tender.procurement_method_name or tender.procurement_method_code
        )

        # Organisation + contact
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

        has_pdf = bool(tender.pdf_path)

        record = {
            # Core identification
            "tender_id":                    tender.id,
            "source_portal":                tender.source_portal,
            "notice_id":                    tender.tender_id,
            "source_url":                   tender.source_url,
            "organisation_id":              tender.organisation_id,
            "organisation_name_normalized": normalize_org_name(org_name_norm),
            # Title
            "title_clean":       normalize_title(tender.title, tender.source_portal),
            "description_clean": None,
            # Dates
            "publication_datetime": pub_dt,
            "deadline_datetime":    dead_dt,
            "days_to_deadline":     days,
            "created_at":           now,
            "updated_at":           now,
            # Country
            "country_name_normalized": country_norm,
            "is_multi_country":        is_multi,
            "countries_list":          countries_json,
            # Notice type
            "notice_type_normalized": notice_type_final,
            # Lifecycle
            "lifecycle_stage":   stage,
            "status_normalized": status,
            # Procurement
            "project_id":                   tender.project_id,
            "procurement_group_normalized": proc_group,
            "procurement_method_name":      proc_method,
            # Budget
            "budget_numeric": budget_num,
            "currency_iso":   currency_iso,
            "budget_missing": budget_missing,
            # Documents
            "pdf_path":       tender.pdf_path,
            "has_pdf":        has_pdf,
            "document_count": 1 if has_pdf else 0,
            # Contact
            "contact_name":  contact_name,
            "contact_email": contact_email,
            "contact_phone": contact_phone,
            # Status
            "status_id":         tender.status_id,
            "source_status_raw": str(tender.status_id) if tender.status_id is not None else None,
            # Sector — reserved for NLP pipeline
            "cpv_code":      None,
            "cpv_label":     None,
            "unspsc_code":   None,
            "unspsc_label":  None,
            "sector_source": None,
            # Data integrity
            "normalization_status": "success",
            "normalized_at":        now,
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
    """
    PostgreSQL upsert on (tender_id).
    On conflict: update all fields except id, tender_id, and created_at.
    """
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