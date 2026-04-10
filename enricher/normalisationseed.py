"""
enricher/stage2a_seed_from_normalized.py
=========================================
PIPELINE STEP 1 — Seed EnrichedTender from NormalizedTender.

For every tender that has a NormalizedTender row but no EnrichedTender row yet,
this script creates an EnrichedTender row pre-populated with all the clean,
validated values from NormalizedTender.

After this runs, stage2b_rule_extraction.py can pick up those rows and
fill in any fields that are still NULL using rule-based extraction on
tenders.notice_text.

Processes all four portals: AfDB, World Bank, UNDP, UNGM.
Incremental by design — the outerjoin filter ensures only new tenders
(those without an EnrichedTender row yet) are processed on every run,
making it safe to schedule as a daily automated job.

Run:
    python enricher/stage2a_seed_from_normalized.py               # all pending (incremental)
    python enricher/stage2a_seed_from_normalized.py --dry-run     # print only, no DB writes
    python enricher/stage2a_seed_from_normalized.py --limit 50    # first 50 only
    python enricher/stage2a_seed_from_normalized.py --portals afdb worldbank  # override portals
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

# All four monitored portals
DEFAULT_PORTALS = ["afdb", "worldbank", "undp", "ungm"]


def seed_enriched_from_normalized(
    dry_run: bool = False,
    limit: int | None = None,
    portals: list[str] | None = None,
) -> None:
    """
    For each NormalizedTender row (filtered by portal) that has no matching
    EnrichedTender row, create a new EnrichedTender pre-populated with all
    available normalized values.

    Sets enrichment_status = 'seeded' so that stage2b can pick it up.
    """
    try:
        from db import get_session
        from models import EnrichedTender, NormalizedTender, Tender
        from sqlalchemy import select
    except ImportError as e:
        log.error("Import error — run from project root: %s", e)
        return

    portals = [p.lower() for p in (portals or DEFAULT_PORTALS)]
    counters = dict(total=0, created=0, skipped=0, failed=0)

    with get_session() as session:
        # Find NormalizedTender rows for our portals that have no EnrichedTender yet
        stmt = (
            select(NormalizedTender, Tender)
            .join(Tender, NormalizedTender.tender_id == Tender.id)
            .outerjoin(EnrichedTender, EnrichedTender.tender_id == NormalizedTender.tender_id)
            .where(
                NormalizedTender.source_portal.in_(portals),
                EnrichedTender.tender_id.is_(None),          # not yet seeded
            )
            .order_by(NormalizedTender.tender_id)
        )
        if limit:
            stmt = stmt.limit(limit)

        rows = session.execute(stmt).all()
        counters["total"] = len(rows)
        log.info(
            "Found %d NormalizedTender rows pending seeding (portals: %s)",
            counters["total"],
            ", ".join(portals),
        )

        if counters["total"] == 0:
            log.info("Nothing to seed — all tenders already have EnrichedTender rows.")
            return

        for norm, tender in rows:
            try:
                if dry_run:
                    log.info(
                        "  [DRY-RUN] Would seed tender_id=%s  title=%s",
                        norm.tender_id,
                        (norm.title_clean or "")[:80],
                    )
                    counters["created"] += 1
                    continue

                enriched = EnrichedTender(
                    tender_id    = norm.tender_id,
                    source_portal= norm.source_portal,
                    notice_id    = norm.notice_id,
                    source_url   = norm.source_url,

                    # ── Text ─────────────────────────────────────────────────
                    title_clean       = norm.title_clean,
                    description_clean = norm.description_clean,

                    # ── Dates ────────────────────────────────────────────────
                    publication_datetime = norm.publication_datetime,
                    deadline_datetime    = norm.deadline_datetime,
                    days_to_deadline     = norm.days_to_deadline,

                    # ── Geography ────────────────────────────────────────────
                    country_name_normalized = norm.country_name_normalized,
                    is_multi_country        = norm.is_multi_country,
                    countries_list          = norm.countries_list,

                    # ── Procurement ──────────────────────────────────────────
                    notice_type_normalized  = norm.notice_type_normalized,
                    lifecycle_stage         = norm.lifecycle_stage,
                    status_normalized       = norm.status_normalized,
                    project_id              = norm.project_id,
                    procurement_method_name = norm.procurement_method_name,
                    procurement_group       = norm.procurement_group_normalized,

                    # ── Budget ───────────────────────────────────────────────
                    budget   = norm.budget_numeric,
                    currency = norm.currency_iso,

                    # ── Documents ────────────────────────────────────────────
                    pdf_path = norm.pdf_path,
                    has_pdf  = norm.has_pdf,

                    # ── Contact ──────────────────────────────────────────────
                    contact_name  = norm.contact_name,
                    contact_email = norm.contact_email,
                    contact_phone = norm.contact_phone,

                    # ── Organisation ─────────────────────────────────────────
                    organisation_name = norm.organisation_name_normalized,

                    # ── Status ───────────────────────────────────────────────
                    # 'seeded' = ready for rule-based extraction in stage2b
                    enrichment_status = "seeded",
                    enriched_at       = datetime.now(timezone.utc),
                )

                session.add(enriched)
                session.commit()
                log.info("  ✓ Seeded EnrichedTender for tender_id=%s", norm.tender_id)
                counters["created"] += 1

            except Exception as e:
                session.rollback()
                log.error("  ✗ Failed tender_id=%s: %s", norm.tender_id, e, exc_info=True)
                counters["failed"] += 1

    log.info(
        "Seeding done — total=%d  created=%d  skipped=%d  failed=%d%s",
        counters["total"],
        counters["created"],
        counters["skipped"],
        counters["failed"],
        "  [DRY-RUN]" if dry_run else "",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Seed EnrichedTender rows from NormalizedTender."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be seeded without writing to DB.")
    parser.add_argument("--limit", type=int, default=None, metavar="N",
                        help="Process at most N tenders.")
    parser.add_argument("--portals", nargs="+", default=DEFAULT_PORTALS, metavar="PORTAL",
                        help="Source portals to process (default: all four — afdb worldbank undp ungm).")
    args = parser.parse_args()

    seed_enriched_from_normalized(
        dry_run=args.dry_run,
        limit=args.limit,
        portals=args.portals,
    )