"""
scoring/prepare_simulation_data.py
====================================
Extracts the first 1000 rows from enriched_tenders,
preprocesses them for GO/NO GO simulation:

    - Flips status: closed → open
    - Flips days_to_deadline: abs() all values
    - Keeps only scoring-relevant fields
    - Exports to CSV

Run:
    python scoring/prepare_simulation_data.py
    python scoring/prepare_simulation_data.py --limit 500
    python scoring/prepare_simulation_data.py --output data/simulation_input.csv
"""

import argparse
import csv
import logging
import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

DEFAULT_OUTPUT = os.path.join(ROOT_DIR, "scoring", "simulation_input.csv")
DEFAULT_LIMIT  = 2000

FIELDS = [
    "id",
    "tender_id",
    "source_portal",
    "title_clean",
    "country_name_normalized",
    "sector",
    "procurement_group",
    "budget",
    "currency",
    "days_to_deadline",
    "status_normalized",
    "lifecycle_stage",
    "notice_type_normalized",
    "procurement_method_name",
    "organisation_name",
    "funding_agency",
    "enrichment_status",
]

SELECT_SQL = f"""
    SELECT
        {", ".join(FIELDS)}
    FROM enriched_tenders
    WHERE enrichment_status = 'nlp_complete'
    ORDER BY id
    LIMIT :limit
"""


def run(limit: int = DEFAULT_LIMIT, output: str = DEFAULT_OUTPUT) -> None:
    try:
        from db import SessionLocal
        from sqlalchemy import text
    except ImportError as e:
        log.error("Import error: %s", e)
        return

    session = SessionLocal()
    try:
        log.info("Fetching first %d rows from enriched_tenders...", limit)
        rows = session.execute(text(SELECT_SQL), {"limit": limit}).mappings().all()
        log.info("Fetched %d rows.", len(rows))
    except Exception as e:
        log.error("Query failed: %s", e)
        session.close()
        return
    finally:
        session.close()

    # ── Preprocess ────────────────────────────────────────────────────────────
    processed = []
    for row in rows:
        r = dict(row)

        # Flip status: closed → open, keep open as open
        if r.get("status_normalized") == "closed":
            r["status_normalized"] = "open"

        # Flip days_to_deadline: abs() all values
        if r.get("days_to_deadline") is not None:
            r["days_to_deadline"] = abs(r["days_to_deadline"])

        processed.append(r)

    # ── Write CSV ─────────────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(output), exist_ok=True)

    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(processed)

    log.info("Saved %d rows to %s", len(processed), output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Prepare simulation input CSV from enriched_tenders."
    )
    parser.add_argument(
        "--limit", type=int, default=DEFAULT_LIMIT, metavar="N",
        help=f"Number of rows to extract (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--output", type=str, default=DEFAULT_OUTPUT, metavar="PATH",
        help=f"Output CSV path (default: {DEFAULT_OUTPUT}).",
    )
    args = parser.parse_args()
    run(limit=args.limit, output=args.output)