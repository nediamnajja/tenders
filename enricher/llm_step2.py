"""
enricher/llm_step2_enrich.py
==============================
STEP 2 — Read cleaned notice texts, call OpenAI, write to enriched_tenders.

Reads normalized_tenders.notice_text_clean (written by Step 1).
Calls OpenAI GPT-4o-mini for each tender.
Writes 8 LLM fields directly into enriched_tenders llm_* columns.
Saves an audit CSV so you can review what was written.

Deadline source:
  - WorldBank / UNGM / UNDP → tenders.deadline_date
  - AfDB                    → enriched_tenders.deadline_datetime

Skips tenders where llm_scope_summary is already filled (resumable).
Use --all to re-enrich already enriched tenders.

Run:
    python enricher/llm_step2_enrich.py
    python enricher/llm_step2_enrich.py --limit 10
    python enricher/llm_step2_enrich.py --portal afdb
    python enricher/llm_step2_enrich.py --id 687
    python enricher/llm_step2_enrich.py --model gpt4o
    python enricher/llm_step2_enrich.py --dry-run
    python enricher/llm_step2_enrich.py --all      # re-enrich already enriched
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from datetime import date, datetime, timezone

from dotenv import load_dotenv
load_dotenv()

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

ALL_PORTALS = ["afdb", "worldbank", "ungm", "undp"]

# =============================================================================
# MODELS & FIELDS
# =============================================================================

MODELS = {
    "gpt4o":      "gpt-4o",
    "gpt4o_mini": "gpt-4o-mini",
}

FIELDS = [
    "scope_summary",
    "project_program",
    "financing_instrument",
    "bid_process_type",
    "contract_duration_months",
    "eligibility_summary",
    "specific_areas",
    "submission_process",
]

# Maps extracted field → enriched_tenders column
LLM_FIELD_MAP = {
    "scope_summary":            "llm_scope_summary",
    "project_program":          "llm_project_program",
    "financing_instrument":     "llm_financing_instrument",
    "bid_process_type":         "llm_bid_process_type",
    "contract_duration_months": "llm_contract_duration_months",
    "eligibility_summary":      "llm_eligibility_summary",
    "specific_areas":           "llm_specific_areas",
    "submission_process":       "llm_submission_process",
}

# =============================================================================
# SYSTEM PROMPT
# =============================================================================

SYSTEM_PROMPT = """
You are a senior procurement analyst specializing in African Development Bank (AfDB),
World Bank, UNDP, UNGM, and GCF-funded tenders. You extract structured intelligence
from tender notice texts to help firms make fast, accurate bid/no-bid decisions.

You will receive a cleaned notice text. Extract ONLY the 8 fields listed below.
Return a single valid JSON object — no markdown fences, no preamble, no explanation.

STRICT RULES:
- Return null for any field you cannot find or confidently infer from the text.
- Never guess or hallucinate values. If unsure, return null.
- Do not re-extract fields already in the database: budget amount, currency,
  deadline date, contact details (phone, email, address), procurement_group.
- For text fields: write in the same language as the notice
  (French notices → French output, English → English).

FIELDS TO EXTRACT:

{
  "scope_summary": string | null,
  // 5-6 sentences describing exactly what is being procured.
  // Cover: (1) what the deliverable is, (2) main tasks or components,
  // (3) geographic scope or location if relevant, (4) any important
  // constraints such as timeline pressure, language requirements, or
  // special standards to comply with, (5) who the beneficiary or client is.
  // Focus on the deliverable — not background or financing context.
  // A firm should be able to make a bid/no-bid decision from this alone.

  "project_program": string | null,
  // Full name and code/ID of the parent project or program this tender sits under.
  // Include the loan/grant/project number if stated.
  // Example: "Suriname Competitiveness Project — P166187 (IBRD Loan 8985-SR)"
  // Example: "PASECII — Don FAD 2100155039670"

  "financing_instrument": "grant" | "loan" | "trust_fund" | "own_funds" | null,
  // grant      = don / subvention / IDA grant / We-Fi
  // loan       = pret / IBRD loan / IDA credit
  // trust_fund = fonds fiduciaire / GCF / trust fund
  // own_funds  = AfDB own resources without external financing

  "bid_process_type": "eoi_only" | "two_stage" | "two_envelope" | "single_envelope" | null,
  // eoi_only        = only an Expression of Interest is requested now
  // two_stage       = EOI shortlist then RFP/full proposal
  // two_envelope    = technical envelope first, financial held separately
  // single_envelope = standard one-step sealed bid

  "contract_duration_months": integer | null,
  // Duration in months. Convert: 8 weeks = 2 months, 1 year = 12 months.
  // Use contract duration not overall project duration. null if not stated.

  "eligibility_summary": string | null,
  // Short bulleted list of the most important eligibility criteria.
  // Format: each criterion on its own line starting with "• ".
  // Shortest possible wording — no full sentences, just key facts.
  // Cover: min years experience, similar projects required, mandatory
  // certifications, team requirements, language requirements, geographic preference.
  // Max 7 bullets. null if no criteria stated.
  // Example: "• Min 10 yrs HSES in mining\\n• 3 similar projects last 10 yrs\\n• Dutch preferred"

  "specific_areas": {
    "tags": array of strings,
    // 1-4 tags from this fixed list ONLY — do not invent new tags:
    // ["energy", "water_sanitation", "transport", "digital", "financial_management",
    //  "environment_social", "agriculture", "health", "education", "governance",
    //  "extractives", "trade_standards", "capacity_building", "urban_development"]
    // IMPORTANT: water_sanitation = drinking water supply, sewerage, sanitation ONLY.
    // Do NOT use water_sanitation for agricultural irrigation — use agriculture instead.

    "lots": array of objects | null
    // Only if notice explicitly defines multiple lots/packages/clusters.
    // Each object: {"lot_number": integer, "lot_ref": string | null, "description": string}
    // null if single-lot or lots not mentioned.
  },

  "submission_process": string | null
  // 2-4 sentences on HOW to submit — deduced from the notice.
  // Cover: format (physical/electronic/both), language of submission,
  // JV or associations permitted, whether this leads to a subsequent RFP stage,
  // any page limits or document caps.
  // Do NOT include contact names, emails, phone numbers, or addresses.
  // null if submission mechanics cannot be deduced.
}
""".strip()

# =============================================================================
# DB QUERY
# =============================================================================

def fetch_eligible_tenders(
    session,
    portals:   list[str],
    limit:     int | None = None,
    target_id: int | None = None,
    re_run:    bool = False,
) -> list[dict]:
    """
    Fetch tenders that:
      - Have notice_text_clean in normalized_tenders (Step 1 done)
      - Have a matching enriched_tenders row (to write results)
      - Have not yet been LLM-enriched (llm_scope_summary IS NULL) unless re_run
      - Have a future deadline:
          * WorldBank / UNGM / UNDP → tenders.deadline_date
          * AfDB                    → enriched_tenders.deadline_datetime
    """
    from sqlalchemy import text

    today    = date.today().isoformat()
    today_ts = datetime.now(timezone.utc).isoformat()

    portal_list = ", ".join(f"'{p}'" for p in portals)

    already_enriched_filter = "" if re_run else \
        "AND et.llm_scope_summary IS NULL"

    id_filter = f"AND t.id = {target_id}" if target_id else ""

    sql = f"""
        SELECT
            t.id            AS tender_db_id,
            t.title,
            t.source_portal,
            t.deadline_date,
            nt.notice_text_clean,
            et.id               AS enriched_id,
            et.deadline_datetime
        FROM tenders t
        INNER JOIN normalized_tenders nt ON nt.tender_id = t.id
        INNER JOIN enriched_tenders   et ON et.tender_id = t.id
        WHERE
            t.source_portal IN ({portal_list})
            AND nt.notice_text_clean IS NOT NULL
            AND LENGTH(nt.notice_text_clean) > 100
            AND (
                -- WorldBank / UNGM / UNDP use tenders.deadline_date
                t.deadline_date >= :today
                OR
                -- AfDB uses enriched_tenders.deadline_datetime
                et.deadline_datetime >= :today_ts
            )
            {already_enriched_filter}
            {id_filter}
        ORDER BY COALESCE(t.deadline_date::text, et.deadline_datetime::date::text) ASC
        {f'LIMIT {limit}' if limit else ''}
    """

    rows = session.execute(
        text(sql),
        {"today": today, "today_ts": today_ts},
    ).mappings().all()
    return [dict(row) for row in rows]


# =============================================================================
# OPENAI API CALL
# =============================================================================

def call_llm(cleaned_text: str, model_key: str = "gpt4o_mini") -> dict:
    """Call OpenAI and return parsed JSON result or error dict."""
    try:
        from openai import OpenAI
    except ImportError:
        log.error("openai not installed. Run: pip install openai")
        return {"status": "error", "error": "openai not installed"}

    client = OpenAI()  # reads OPENAI_API_KEY from environment
    model  = MODELS.get(model_key, MODELS["gpt4o_mini"])

    try:
        response = client.chat.completions.create(
            model=model,
            max_tokens=1500,
            temperature=0.1,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": (
                    "Extract structured fields from this tender notice.\n"
                    "Return ONLY a valid JSON object — no markdown, no explanation.\n\n"
                    f"NOTICE TEXT:\n{cleaned_text}"
                )},
            ],
        )
        raw = response.choices[0].message.content.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$",          "", raw)

        data   = json.loads(raw)
        tokens = response.usage.total_tokens
        return {"status": "ok", "data": data, "tokens": tokens}

    except json.JSONDecodeError as e:
        return {"status": "parse_error", "error": str(e), "raw": raw[:300]}
    except Exception as e:
        return {"status": "api_error", "error": str(e)}


# =============================================================================
# VALUE COERCION
# =============================================================================

def coerce_value(field: str, val):
    """Validate and coerce each extracted value to the correct DB type."""
    if val is None:
        return None

    if field == "contract_duration_months":
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return None

    if field == "specific_areas":
        if isinstance(val, dict):
            return json.dumps(val, ensure_ascii=False)
        if isinstance(val, str):
            try:
                parsed = json.loads(val)
                return json.dumps(parsed, ensure_ascii=False)
            except json.JSONDecodeError:
                return None
        return None

    if field == "financing_instrument":
        allowed = {"grant", "loan", "trust_fund", "own_funds"}
        return val if val in allowed else None

    if field == "bid_process_type":
        allowed = {"eoi_only", "two_stage", "two_envelope", "single_envelope"}
        return val if val in allowed else None

    return str(val) if val else None


# =============================================================================
# DB WRITE
# =============================================================================

def write_to_db(session, enriched_id: int, data: dict) -> bool:
    """Write all 8 LLM fields to enriched_tenders."""
    from models import EnrichedTender

    enriched = session.query(EnrichedTender).filter_by(id=enriched_id).first()
    if not enriched:
        log.warning("  No enriched_tender row for id=%s", enriched_id)
        return False

    for csv_field, db_col in LLM_FIELD_MAP.items():
        val = coerce_value(csv_field, data.get(csv_field))
        setattr(enriched, db_col, val)

    return True


# =============================================================================
# MAIN
# =============================================================================

def run(
    portals:    list[str] | None = None,
    limit:      int | None = None,
    target_id:  int | None = None,
    model_key:  str = "gpt4o_mini",
    dry_run:    bool = False,
    re_run:     bool = False,
    audit_path: str = "enricher/llm_step2_output.csv",
) -> None:

    portals = portals or ALL_PORTALS

    try:
        from db import SessionLocal
    except ImportError as e:
        log.error("Import error: %s", e)
        return

    if dry_run:
        log.info("DRY RUN — no DB writes, no API calls")

    session = SessionLocal()

    total_tokens = 0
    written      = 0
    no_match     = 0
    api_errors   = 0
    audit_rows   = []
    notices      = []

    try:
        log.info(
            "Fetching eligible tenders (portals=%s, re_run=%s)",
            portals, re_run,
        )
        notices = fetch_eligible_tenders(
            session,
            portals   = portals,
            limit     = limit,
            target_id = target_id,
            re_run    = re_run,
        )
        log.info("Found %d tenders ready for LLM enrichment", len(notices))

        for i, notice in enumerate(notices, 1):
            tender_id   = notice["tender_db_id"]
            enriched_id = notice["enriched_id"]
            title       = (notice["title"] or "")[:70]
            cleaned     = notice["notice_text_clean"]

            log.info(
                "  [%d/%d] id=%-6s  portal=%-10s  %s",
                i, len(notices), tender_id, notice["source_portal"], title,
            )

            if dry_run:
                written += 1
                continue

            # Call OpenAI with retry on rate limit
            result = None
            for attempt in range(3):
                result = call_llm(cleaned, model_key=model_key)
                if result["status"] == "ok":
                    break
                if "rate" in result.get("error", "").lower():
                    wait = 20 * (attempt + 1)
                    log.warning("  Rate limit hit — waiting %ds", wait)
                    time.sleep(wait)
                else:
                    break

            if result["status"] != "ok":
                log.error(
                    "  API error id=%s: %s",
                    tender_id, result.get("error", "unknown"),
                )
                api_errors += 1
                audit_rows.append({
                    "id":     tender_id,
                    "portal": notice["source_portal"],
                    "title":  notice["title"] or "",
                    "status": result["status"],
                    "tokens": 0,
                    "error":  result.get("error", ""),
                    **{f: "" for f in FIELDS},
                })
                continue

            data         = result["data"]
            tokens       = result.get("tokens", 0)
            total_tokens += tokens

            # Write to DB
            try:
                ok = write_to_db(session, enriched_id, data)
                if ok:
                    session.commit()
                    written += 1
                    log.info(
                        "  ✓ id=%s  tokens=%d  fields=%d/%d",
                        tender_id, tokens,
                        sum(1 for f in FIELDS if data.get(f) is not None),
                        len(FIELDS),
                    )
                else:
                    no_match += 1
            except Exception as e:
                log.error("  DB write error id=%s: %s", tender_id, e)
                session.rollback()
                api_errors += 1

            # Build audit row
            audit_row = {
                "id":     tender_id,
                "portal": notice["source_portal"],
                "title":  notice["title"] or "",
                "status": result["status"],
                "tokens": tokens,
                "error":  "",
            }
            for field in FIELDS:
                val = data.get(field)
                if val is None:
                    audit_row[field] = ""
                elif isinstance(val, (dict, list)):
                    audit_row[field] = json.dumps(val, ensure_ascii=False)
                else:
                    audit_row[field] = str(val)
            audit_rows.append(audit_row)

            # Polite delay every 10 calls
            if i % 10 == 0:
                time.sleep(0.5)

    except Exception as e:
        session.rollback()
        log.error("Fatal error: %s", e, exc_info=True)
    finally:
        session.close()

    # Save audit CSV
    if audit_rows and not dry_run:
        os.makedirs(os.path.dirname(audit_path) or ".", exist_ok=True)
        fieldnames = ["id", "portal", "title", "status", "tokens", "error"] + FIELDS
        with open(audit_path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(audit_rows)
        log.info("Audit CSV saved → %s", audit_path)

    cost_usd = (total_tokens / 1_000_000) * (
        0.30 if model_key == "gpt4o_mini" else 2.50
    )

    print(f"\n{'='*60}")
    print(f"  STEP 2 SUMMARY {'(DRY RUN)' if dry_run else ''}")
    print(f"{'='*60}")
    print(f"  Tenders found          : {len(notices)}")
    print(f"  Written to DB          : {written}")
    print(f"  No enriched row (skip) : {no_match}")
    print(f"  API / DB errors        : {api_errors}")
    print(f"  Total tokens used      : {total_tokens:,}")
    print(f"  Estimated cost         : ${cost_usd:.4f}")
    print(f"  Model                  : {MODELS.get(model_key)}")
    if not dry_run:
        print(f"  Audit CSV              : {audit_path}")
    print(f"{'='*60}\n")

    if no_match > 0:
        print(
            "  NOTE: Some tenders had no enriched_tenders row.\n"
            "  Run your enricher pipeline on those tenders first.\n"
        )


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Step 2: Call OpenAI on cleaned notices and write to enriched_tenders."
    )
    parser.add_argument(
        "--portal", choices=ALL_PORTALS, default=None,
        help="Process one portal only (default: all four).",
    )
    parser.add_argument(
        "--limit", type=int, default=None, metavar="N",
        help="Process at most N tenders.",
    )
    parser.add_argument(
        "--id", type=int, default=None, metavar="ID",
        help="Process only the tender with this tenders.id.",
    )
    parser.add_argument(
        "--model", choices=["gpt4o", "gpt4o_mini"], default="gpt4o_mini",
        help="OpenAI model to use (default: gpt4o_mini).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Count eligible tenders without calling API or writing to DB.",
    )
    parser.add_argument(
        "--all", action="store_true", dest="re_run",
        help="Re-enrich tenders that already have llm_scope_summary.",
    )
    parser.add_argument(
        "--audit", default="enricher/llm_step2_output.csv",
        help="Path for the audit CSV output.",
    )
    args = parser.parse_args()

    portals = [args.portal] if args.portal else ALL_PORTALS

    run(
        portals    = portals,
        limit      = args.limit,
        target_id  = args.id,
        model_key  = args.model,
        dry_run    = args.dry_run,
        re_run     = args.re_run,
        audit_path = args.audit,
    )