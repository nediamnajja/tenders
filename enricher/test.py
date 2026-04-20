"""
enricher/test.py
================
Standalone NLP test using EMBEDDING similarity.

Reads tenders directly from the raw tenders table,
runs sector classification + keyword extraction,
and prints results to terminal.

NO database writes. Safe to run at any time.

Usage:
    python enricher/test.py
    python enricher/test.py --limit 20
    python enricher/test.py --portal afdb
    python enricher/test.py --portal worldbank --limit 30
"""

import argparse
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

from stage3 import (
    _load_classifier,
    _load_keybert,
    build_classification_text,
    build_keyword_text,
    classify_sectors,
    extract_keywords,
)


def run_test(
    portal: str | None = None,
    limit: int = 50,
    device: str = "cpu",
) -> None:
    try:
        from db import get_session
        from models import Tender
        from sqlalchemy import select, func
    except ImportError as e:
        log.error("Import error — run from project root: %s", e)
        return

    portals = [portal] if portal else ["afdb", "worldbank", "ungm", "undp"]

    classifier = _load_classifier(device)
    keybert = _load_keybert(device)

    with get_session() as session:
        stmt = (
            select(
                Tender.id,
                Tender.source_portal,
                Tender.title,
                Tender.description,
            )
            .where(Tender.source_portal.in_(portals))
            .order_by(func.random())
            .limit(limit)
        )
        rows = session.execute(stmt).all()

    if not rows:
        log.info("No tenders found for portals: %s", portals)
        return

    log.info("Testing EMBEDDING NLP on %d tenders from portals: %s", len(rows), portals)
    log.info("=" * 72)

    total = len(rows)
    others_count = 0
    two_sector = 0

    for i, row in enumerate(rows, 1):
        title = (row.title or "").strip()
        description = row.description
        portal_name = row.source_portal

        classification_text = build_classification_text(title)
        keyword_text = build_keyword_text(title, description, portal_name)

        primary, secondary, debug_scores = classify_sectors(
            classification_text,
            classifier,
            return_debug=True,
        )
        keywords = extract_keywords(keyword_text, keybert)

        if primary == "Others":
            others_count += 1
        if secondary:
            two_sector += 1

        sectors = [s for s in [primary, secondary] if s]
        sector_str = " + ".join(sectors)

        title_preview = title[:90] + ("..." if len(title) > 90 else "")
        print(f"\n[{i:02d}/{total}] {portal_name.upper():10s} [EMBED]")
        print(f"  title   : {title_preview}")
        print(f"  sectors : {sector_str}")

        if debug_scores:
            top_debug = ", ".join(
                [f"{name}={score:.3f}" for name, score in debug_scores[:3]]
            )
            print(f"  scores  : {top_debug}")

        if keywords:
            print(f"  keywords: {', '.join(keywords[:5])}")

    print("\n" + "=" * 72)
    print(f"  SUMMARY — {total} tenders tested")
    print(f"  Got 'Others'            : {others_count:3d}  ({100*others_count//total}%)")
    print(f"  Got 2 sectors           : {two_sector:3d}  ({100*two_sector//total}%)")
    print("=" * 72)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Test embedding-based NLP classification on real tenders — no DB writes."
    )
    parser.add_argument(
        "--portal",
        choices=["afdb", "worldbank", "ungm", "undp"],
        default=None,
        help="Test one portal only (default: all four)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Number of tenders to test (default: 50)",
    )
    parser.add_argument(
        "--device",
        default="cpu",
        choices=["cpu", "cuda"],
        help="Device for model inference (default: cpu)",
    )
    args = parser.parse_args()

    run_test(
        portal=args.portal,
        limit=args.limit,
        device=args.device,
    )