"""
daily_cleanup.py
================
KPMG Tender Pipeline — Daily Cleanup
Runs as Step 1 in the orchestrator, before scrapers.

What it does:
1. Recomputes days_to_deadline for ALL enriched tenders
   (deadline_datetime is stored, days_to_deadline goes stale daily)
2. Removes tender_scores rows where deadline has passed
3. Resets p_go to NULL for expired tenders so they don't show up in alerts
4. Prints a summary

Run standalone:
    cd C:\\projects\\tenders
    python daily_cleanup.py
"""

import sys
import os
import logging
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db import get_session
from models import EnrichedTender, TenderScore  # adjust if TenderScore is named differently

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

NOW = datetime.now(timezone.utc)


def run_daily_cleanup():
    logger.info("=" * 55)
    logger.info("  KPMG DAILY CLEANUP — Starting")
    logger.info(f"  Run date: {NOW.strftime('%Y-%m-%d %H:%M UTC')}")
    logger.info("=" * 55)

    stats = {
        "deadlines_refreshed": 0,
        "expired_scores_deleted": 0,
        "expired_p_go_reset": 0,
        "errors": 0,
    }

    with get_session() as session:

        # ── Step 1: Refresh days_to_deadline for all tenders ─────────────
        logger.info("  Step 1 — Refreshing days_to_deadline...")

        tenders = (
            session.query(EnrichedTender)
            .filter(EnrichedTender.deadline_datetime.isnot(None))
            .all()
        )

        for tender in tenders:
            try:
                dl = tender.deadline_datetime
                # Make timezone-aware if naive
                if dl.tzinfo is None:
                    dl = dl.replace(tzinfo=timezone.utc)

                new_days = (dl - NOW).days
                tender.days_to_deadline = new_days
                stats["deadlines_refreshed"] += 1

            except Exception as e:
                logger.error(f"  Error refreshing tender {tender.id}: {e}")
                stats["errors"] += 1

        session.commit()
        logger.info(f"  ✅ Refreshed {stats['deadlines_refreshed']} deadlines")

        # ── Step 2: Delete tender_scores where deadline has passed ────────
        logger.info("  Step 2 — Removing expired recommendations...")

        try:
            # Find enriched_tender_ids where deadline has passed (days < 0)
            expired_ids = [
                t.id for t in tenders
                if t.days_to_deadline is not None and t.days_to_deadline < 0
            ]

            if expired_ids:
                deleted = (
                    session.query(TenderScore)
                    .filter(TenderScore.enriched_tender_id.in_(expired_ids))
                    .delete(synchronize_session=False)
                )
                stats["expired_scores_deleted"] = deleted
                session.commit()
                logger.info(f"  ✅ Deleted {deleted} expired recommendations")
            else:
                logger.info("  ✅ No expired recommendations to delete")

        except Exception as e:
            logger.error(f"  Error deleting expired scores: {e}")
            stats["errors"] += 1

        # ── Step 3: Reset p_go for tenders with deadline < 2 days ─────────
        # So they won't be shown in alerts or re-scored
        logger.info("  Step 3 — Resetting p_go for imminent/expired tenders...")

        try:
            imminent = (
                session.query(EnrichedTender)
                .filter(
                    EnrichedTender.days_to_deadline < 2,
                    EnrichedTender.p_go.isnot(None),
                )
                .all()
            )

            for tender in imminent:
                tender.p_go            = None
                tender.score_breakdown = None
                tender.model_version   = None
                stats["expired_p_go_reset"] += 1

            session.commit()
            logger.info(f"  ✅ Reset p_go for {stats['expired_p_go_reset']} expired tenders")

        except Exception as e:
            logger.error(f"  Error resetting p_go: {e}")
            stats["errors"] += 1

    # ── Summary ───────────────────────────────────────────────────────────
    logger.info("=" * 55)
    logger.info("  CLEANUP COMPLETE")
    logger.info(f"  Deadlines refreshed    : {stats['deadlines_refreshed']}")
    logger.info(f"  Expired scores deleted : {stats['expired_scores_deleted']}")
    logger.info(f"  Expired p_go reset     : {stats['expired_p_go_reset']}")
    logger.info(f"  Errors                 : {stats['errors']}")
    logger.info("=" * 55)


if __name__ == "__main__":
    run_daily_cleanup()