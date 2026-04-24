"""
enricher/normalisationseed.py
==============================
PIPELINE STEP 2a — Seed EnrichedTender from NormalizedTender.

Run:
    python enricher/normalisationseed.py               # all pending
    python enricher/normalisationseed.py --dry-run     # print only, no DB writes
    python enricher/normalisationseed.py --limit 50    # first 50 only
    python enricher/normalisationseed.py --portals afdb worldbank
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timezone

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

DEFAULT_PORTALS = ["afdb", "worldbank", "undp", "ungm"]
BATCH_SIZE = 100

INSERT_SQL = """
INSERT INTO enriched_tenders (
    tender_id, source_portal, notice_id, source_url,
    title_clean, description_clean,
    publication_datetime, deadline_datetime, days_to_deadline,
    country_name_normalized, is_multi_country, countries_list,
    notice_type_normalized, lifecycle_stage, status_normalized,
    project_id, procurement_method_name, procurement_group,
    budget, currency,
    pdf_path, has_pdf,
    contact_name, contact_email, contact_phone,
    sector, keywords, language, organisation_name, funding_agency, summary,
    enrichment_status, enriched_at
)
VALUES (
    :tender_id, :source_portal, :notice_id, :source_url,
    :title_clean, :description_clean,
    :publication_datetime, :deadline_datetime, :days_to_deadline,
    :country_name_normalized, :is_multi_country, :countries_list,
    :notice_type_normalized, :lifecycle_stage, :status_normalized,
    :project_id, :procurement_method_name, :procurement_group,
    :budget, :currency,
    :pdf_path, :has_pdf,
    :contact_name, :contact_email, :contact_phone,
    :sector, :keywords, :language, :organisation_name, :funding_agency, :summary,
    :enrichment_status, :enriched_at
)
ON CONFLICT (tender_id) DO NOTHING
"""

SELECT_SQL = """
SELECT
    n.tender_id,
    n.source_portal,
    n.notice_id,
    n.source_url,
    n.title_clean,
    n.description_clean,
    n.publication_datetime,
    n.deadline_datetime,
    n.days_to_deadline,
    n.country_name_normalized,
    n.is_multi_country,
    n.countries_list,
    n.notice_type_normalized,
    n.lifecycle_stage,
    n.status_normalized,
    n.project_id,
    n.procurement_method_name,
    n.procurement_group_normalized  AS procurement_group,
    n.budget_numeric                AS budget,
    n.currency_iso                  AS currency,
    n.pdf_path,
    n.has_pdf,
    n.contact_name,
    n.contact_email,
    n.contact_phone,
    n.organisation_name_normalized  AS organisation_name,
    n.funding_agency
FROM normalized_tenders n
LEFT JOIN enriched_tenders e ON n.tender_id = e.tender_id
WHERE e.tender_id IS NULL
  AND n.source_portal = ANY(:portals)
ORDER BY n.tender_id
{limit_clause}
"""


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────

def seed_enriched_from_normalized(
    dry_run: bool = False,
    limit:   int | None = None,
    portals: list[str] | None = None,
) -> None:

    try:
        from db import SessionLocal
        from sqlalchemy import text
    except ImportError as e:
        log.error("Import error — run from project root: %s", e)
        return

    portals  = [p.lower() for p in (portals or DEFAULT_PORTALS)]
    counters = dict(total=0, created=0, failed=0)
    now      = datetime.now(timezone.utc)

    limit_clause = f"LIMIT {limit}" if limit else ""
    select_sql   = SELECT_SQL.format(limit_clause=limit_clause)

    session = SessionLocal()
    try:
        # ── Fetch pending rows via raw SQL ────────────────────────────────────
        rows = session.execute(
            text(select_sql),
            {"portals": portals},
        ).mappings().all()

        counters["total"] = len(rows)
        log.info(
            "Found %d NormalizedTender rows pending seeding (portals: %s)",
            counters["total"], ", ".join(portals),
        )

        if counters["total"] == 0:
            log.info("Nothing to do — all normalized tenders already seeded.")
            return

        if dry_run:
            for row in rows:
                log.info(
                    "  [DRY-RUN] Would seed tender_id=%s  title=%s",
                    row["tender_id"],
                    (row["title_clean"] or "")[:80],
                )
                counters["created"] += 1
            return

        # ── Insert in batches ─────────────────────────────────────────────────
        batch = []
        for row in rows:
            batch.append({
                "tender_id":               row["tender_id"],
                "source_portal":           row["source_portal"],
                "notice_id":               row["notice_id"],
                "source_url":              row["source_url"],
                "title_clean":             row["title_clean"],
                "description_clean":       row["description_clean"],
                "publication_datetime":    row["publication_datetime"],
                "deadline_datetime":       row["deadline_datetime"],
                "days_to_deadline":        row["days_to_deadline"],
                "country_name_normalized": row["country_name_normalized"],
                "is_multi_country":        row["is_multi_country"],
                "countries_list":          row["countries_list"],
                "notice_type_normalized":  row["notice_type_normalized"],
                "lifecycle_stage":         row["lifecycle_stage"],
                "status_normalized":       row["status_normalized"],
                "project_id":              row["project_id"],
                "procurement_method_name": row["procurement_method_name"],
                "procurement_group":       row["procurement_group"],
                "budget":                  row["budget"],
                "currency":                row["currency"],
                "pdf_path":                row["pdf_path"],
                "has_pdf":                 row["has_pdf"],
                "contact_name":            row["contact_name"],
                "contact_email":           row["contact_email"],
                "contact_phone":           row["contact_phone"],
                "sector":                  None,
                "keywords":                None,
                "language":                None,
                "organisation_name":       row["organisation_name"],
                "funding_agency":          row["funding_agency"],
                "summary":                 None,
                "enrichment_status":       "seeded",
                "enriched_at":             now,
            })

            if len(batch) >= BATCH_SIZE:
                try:
                    session.execute(text(INSERT_SQL), batch)
                    session.commit()
                    counters["created"] += len(batch)
                    log.info(
                        "  ... committed %d rows (last tender_id=%s)",
                        len(batch), batch[-1]["tender_id"],
                    )
                except Exception as e:
                    session.rollback()
                    log.error(
                        "  ✗ Batch failed — error_type=%s  detail=%s",
                        type(e).__name__, str(e),
                    )
                    counters["failed"] += len(batch)
                batch = []

        # ── Final partial batch ───────────────────────────────────────────────
        if batch:
            try:
                session.execute(text(INSERT_SQL), batch)
                session.commit()
                counters["created"] += len(batch)
                log.info(
                    "  ... committed final %d rows (last tender_id=%s)",
                    len(batch), batch[-1]["tender_id"],
                )
            except Exception as e:
                session.rollback()
                log.error(
                    "  ✗ Final batch failed — error_type=%s  detail=%s",
                    type(e).__name__, str(e),
                )
                counters["failed"] += len(batch)

    except Exception as e:
        session.rollback()
        log.error("Fatal error: %s — %s", type(e).__name__, str(e), exc_info=True)
    finally:
        session.close()

    log.info(
        "Seeding done — total=%d  created=%d  failed=%d%s",
        counters["total"],
        counters["created"],
        counters["failed"],
        "  [DRY-RUN]" if dry_run else "",
    )


# ─────────────────────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Seed EnrichedTender rows from NormalizedTender."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be seeded without writing to DB.",
    )
    parser.add_argument(
        "--limit", type=int, default=None, metavar="N",
        help="Process at most N tenders.",
    )
    parser.add_argument(
        "--portals", nargs="+", default=DEFAULT_PORTALS, metavar="PORTAL",
        help="Source portals to process (default: all four — afdb worldbank undp ungm).",
    )
    args = parser.parse_args()

    seed_enriched_from_normalized(
        dry_run = args.dry_run,
        limit   = args.limit,
        portals = args.portals,
    )