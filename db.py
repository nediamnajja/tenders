"""
db.py — Database connection, session factory, and helpers
==========================================================
Single source of truth for the SQLAlchemy engine and session.
Uses DATABASE_URL from .env (postgresql+psycopg2://...).

Usage:
    from db import get_session, init_db

    with get_session() as session:
        session.add(...)        # commit happens automatically on clean exit
"""

import logging
import os
import re
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from models import (
    Base, Organisation, Contact, Tender,
    ScraperState, ScraperRunLog,
    EnrichedTender, TenderScore, WeightsHistory          # ← NEW
)

load_dotenv()

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
#  CONNECTION
# ─────────────────────────────────────────────────────────────

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///tenders.db")

_connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

engine = create_engine(
    DATABASE_URL,
    connect_args=_connect_args,
    echo=False,
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


# ─────────────────────────────────────────────────────────────
#  SESSION CONTEXT MANAGER
# ─────────────────────────────────────────────────────────────

@contextmanager
def get_session():
    """
    Provide a transactional database session.
    Commits on success, rolls back on exception.

    Usage:
        with get_session() as session:
            session.add(obj)
            # auto-commit here
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ─────────────────────────────────────────────────────────────
#  INIT
# ─────────────────────────────────────────────────────────────

def init_db() -> None:
    """
    Create all tables if they don't exist, then apply any missing-column migrations.
    Safe to call on every startup — fully idempotent.
    """
    Base.metadata.create_all(bind=engine)
    _migrate_columns()
    logger.info(f"Database ready — {DATABASE_URL}")


def _migrate_columns() -> None:
    """
    Add columns that were introduced after the initial schema creation.
    Uses IF NOT EXISTS (Postgres) — safe to run repeatedly.
    Falls back to PRAGMA-based check for SQLite.
    """
    is_sqlite = DATABASE_URL.startswith("sqlite")

    # (table, column, postgres_type)
    migrations = [
        # ── tenders ──────────────────────────────────────────
        ("tenders",            "notice_text",                  "TEXT"),
        ("tenders",            "pdf_path",                     "TEXT"),
        ("tenders",            "updated_at",                   "TIMESTAMPTZ"),
        ("tenders",            "created_at",                   "TIMESTAMPTZ"),
        ("tenders",            "keywords",                     "TEXT"),
        ("tenders",            "summary",                      "TEXT"),
        ("tenders",            "sector",                       "TEXT"),
        ("tenders",            "status_id",                    "INTEGER"),
        ("tenders",            "deadline_time",                "TEXT"),
        ("tenders",            "project_id",                   "TEXT"),
        ("tenders",            "procurement_group",            "TEXT"),
        ("tenders",            "procurement_method_code",      "TEXT"),
        ("tenders",            "procurement_method_name",      "TEXT"),

        # ── enriched_tenders ─────────────────────────────────
        ("enriched_tenders",   "emails_found",                 "TEXT"),
        ("enriched_tenders",   "phones_found",                 "TEXT"),
        ("enriched_tenders",   "urls_found",                   "TEXT"),
        ("enriched_tenders",   "amounts_found",                "TEXT"),
        ("enriched_tenders",   "notice_text_clean",            "TEXT"),

        # ── enriched_tenders SCORING (NEW) ───────────────────
        # Populated by the scoring engine when it runs.
        # NULL until scored.
        ("enriched_tenders",   "p_go",                        "FLOAT"),
        ("enriched_tenders",   "score_breakdown",             "TEXT"),
        ("enriched_tenders",   "model_version",               "INTEGER"),

        # ── organisations ────────────────────────────────────
        ("organisations",      "name_normalised",              "TEXT"),
        ("organisations",      "country_iso2",                 "TEXT"),
        ("organisations",      "updated_at",                   "TIMESTAMPTZ"),
        ("organisations",      "created_at",                   "TIMESTAMPTZ"),

        # ── scraper_run_log ──────────────────────────────────
        ("scraper_run_log",    "already_existed",              "INTEGER"),
        ("scraper_run_log",    "notes",                        "TEXT"),

        # ── normalized_tenders ───────────────────────────────
        ("normalized_tenders", "created_at",                   "TIMESTAMPTZ DEFAULT NOW()"),
        ("normalized_tenders", "updated_at",                   "TIMESTAMPTZ DEFAULT NOW()"),
        ("normalized_tenders", "language_normalized",          "VARCHAR(20)"),
        ("normalized_tenders", "lifecycle_stage",              "VARCHAR(50)"),
        ("normalized_tenders", "status_normalized",            "VARCHAR(50)"),
        ("normalized_tenders", "procurement_group_normalized", "VARCHAR(100)"),
        ("normalized_tenders", "budget_missing",               "BOOLEAN DEFAULT FALSE"),
        ("normalized_tenders", "has_pdf",                      "BOOLEAN DEFAULT FALSE"),
        ("normalized_tenders", "document_count",               "INTEGER DEFAULT 0"),
        ("normalized_tenders", "is_multi_country",             "BOOLEAN DEFAULT FALSE"),
        ("normalized_tenders", "countries_list",               "TEXT"),
        ("normalized_tenders", "missing_fields",               "TEXT"),
        ("normalized_tenders", "validation_flags",             "TEXT"),
        ("normalized_tenders", "normalized_at",                "TIMESTAMPTZ DEFAULT NOW()"),
    ]

    with engine.connect() as conn:
        for table, column, col_type in migrations:
            try:
                if is_sqlite:
                    rows     = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
                    existing = [row[1] for row in rows]
                    if column not in existing:
                        sqlite_type = col_type.split()[0]
                        conn.execute(text(
                            f"ALTER TABLE {table} ADD COLUMN {column} {sqlite_type}"
                        ))
                        conn.commit()
                        logger.info(f"Migration: added {table}.{column}")
                else:
                    conn.execute(text(
                        f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {col_type}"
                    ))
                    conn.commit()
                    logger.info(f"Migration: ensured {table}.{column}")
            except Exception as e:
                logger.warning(f"Migration skipped for {table}.{column}: {e}")

        # ── Create tender_scores table if it doesn't exist ───
        # SQLAlchemy's create_all handles this, but we also do it
        # explicitly here so init_db() is fully self-contained.
        if is_sqlite:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS tender_scores (
                    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                    enriched_tender_id    INTEGER NOT NULL UNIQUE
                                          REFERENCES enriched_tenders(id)
                                          ON DELETE CASCADE,
                    p_go                  REAL    NOT NULL,
                    recommendation        TEXT    NOT NULL,
                    justification         TEXT,
                    scored_at             TEXT,
                    partner_decision      TEXT,
                    partner_justification TEXT,
                    decided_at            TEXT
                )
            """))
        else:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS tender_scores (
                    id                    SERIAL PRIMARY KEY,
                    enriched_tender_id    INTEGER NOT NULL UNIQUE
                                          REFERENCES enriched_tenders(id)
                                          ON DELETE CASCADE,
                    p_go                  FLOAT   NOT NULL,
                    recommendation        VARCHAR(20) NOT NULL,
                    justification         TEXT,
                    scored_at             TIMESTAMPTZ DEFAULT NOW(),
                    partner_decision      VARCHAR(10),
                    partner_justification TEXT,
                    decided_at            TIMESTAMPTZ
                )
            """))
        conn.commit()
        logger.info("Migration: ensured tender_scores table")


# ─────────────────────────────────────────────────────────────
#  SCORING HELPERS  (NEW)
# ─────────────────────────────────────────────────────────────

def get_unscored_enriched_tenders(session) -> list:
    """
    Return all enriched tenders that:
      - have not been scored yet (p_go IS NULL)
      - have a valid deadline (days_to_deadline >= 2)
    These are the tenders the scoring engine will process.
    """
    return (
        session.query(EnrichedTender)
        .filter(
            EnrichedTender.p_go.is_(None),
            EnrichedTender.days_to_deadline >= 2,
        )
        .all()
    )


def save_score_to_enriched(
    session,
    enriched_tender_id: int,
    p_go:               float,
    score_breakdown:    str,    # JSON string
    model_version:      int,
) -> None:
    """
    Write p_go + score_breakdown + model_version to enriched_tenders.
    Called for EVERY tender regardless of score.
    """
    tender = session.query(EnrichedTender).filter_by(id=enriched_tender_id).first()
    if tender:
        tender.p_go            = p_go
        tender.score_breakdown = score_breakdown
        tender.model_version   = model_version


def save_tender_score(
    session,
    enriched_tender_id: int,
    p_go:               float,
    recommendation:     str,
    justification:      str,
) -> None:
    """
    Write a row to tender_scores.
    Called ONLY for tenders where p_go >= 0.70.
    Skips silently if a score row already exists for this tender
    (prevents duplicates on re-runs).
    """
    existing = (
        session.query(TenderScore)
        .filter_by(enriched_tender_id=enriched_tender_id)
        .first()
    )
    if existing:
        logger.info(f"Score already exists for enriched_tender {enriched_tender_id} — skipping")
        return

    session.add(TenderScore(
        enriched_tender_id = enriched_tender_id,
        p_go               = p_go,
        recommendation     = recommendation,
        justification      = justification,
    ))


def get_top_tenders(session, limit: int = 5, offset: int = 0) -> list:
    """
    Return the top GO tenders for platform display.
    Ordered by p_go descending.
    Only returns tenders with valid deadline (days_to_deadline >= 2).

    Usage:
        # First 5
        tenders = get_top_tenders(session, limit=5, offset=0)
        # Next 5 (Show More)
        tenders = get_top_tenders(session, limit=5, offset=5)
    """
    return (
        session.query(TenderScore, EnrichedTender)
        .join(EnrichedTender, TenderScore.enriched_tender_id == EnrichedTender.id)
        .filter(EnrichedTender.days_to_deadline >= 2)
        .order_by(TenderScore.p_go.desc())
        .limit(limit)
        .offset(offset)
        .all()
    )


# ─────────────────────────────────────────────────────────────
#  SCRAPER RUN LOG
# ─────────────────────────────────────────────────────────────

def log_scraper_run(
    portal:          str,
    started_at:      datetime,
    status:          str,
    saved:           int           = 0,
    expired_skipped: int           = 0,
    year_filtered:   int           = 0,
    cursor_stopped:  int           = 0,
    already_existed: int           = 0,
    errors:          int           = 0,
    new_cursor:      Optional[str] = None,
    notes:           Optional[str] = None,
) -> None:
    """
    Persist one run's stats to scraper_run_log.
    Always call this — even on failure — so the dashboard has a complete history.
    finished_at is set to now() automatically.
    """
    finished_at = datetime.now(timezone.utc)
    with get_session() as session:
        session.add(ScraperRunLog(
            portal          = portal,
            started_at      = started_at,
            finished_at     = finished_at,
            saved           = saved,
            expired_skipped = expired_skipped,
            year_filtered   = year_filtered,
            cursor_stopped  = cursor_stopped,
            already_existed = already_existed,
            errors          = errors,
            status          = status,
            new_cursor      = new_cursor,
            notes           = notes,
        ))
    logger.info(
        f"Run logged — portal:{portal} status:{status} "
        f"saved:{saved} errors:{errors} cursor:{new_cursor}"
    )


# ─────────────────────────────────────────────────────────────
#  ORGANISATION
# ─────────────────────────────────────────────────────────────

def upsert_organisation(
    name: str,
    organisation_type: Optional[str] = None,
    country: Optional[str] = None,
) -> int:
    """
    Insert or update an organisation.
    Returns the organisation's integer primary key.
    """
    name_normalised = re.sub(r"\s+", " ", name.strip().lower())

    with get_session() as session:
        org = session.query(Organisation).filter_by(
            name=name,
            organisation_type=organisation_type,
        ).first()

        if org:
            if country and org.country != country:
                org.country = country
            org.name_normalised = name_normalised
        else:
            org = Organisation(
                name=name,
                name_normalised=name_normalised,
                organisation_type=organisation_type,
                country=country,
            )
            session.add(org)
            session.flush()

        return org.id


# ─────────────────────────────────────────────────────────────
#  CONTACTS
# ─────────────────────────────────────────────────────────────

def add_contact(
    organisation_id: int,
    name:    Optional[str] = None,
    email:   Optional[str] = None,
    phone:   Optional[str] = None,
    address: Optional[str] = None,
) -> Optional[int]:
    """
    Add a contact for an organisation.
    Deduplicates on email (preferred) or name.
    Skips if both name and email are empty.
    Returns contact ID or None if skipped.
    """
    if not name and not email:
        return None

    with get_session() as session:
        existing = None
        if email:
            existing = session.query(Contact).filter_by(
                organisation_id=organisation_id,
                email=email,
            ).first()
        elif name:
            existing = session.query(Contact).filter_by(
                organisation_id=organisation_id,
                name=name,
            ).first()

        if existing:
            return existing.id

        contact = Contact(
            organisation_id=organisation_id,
            name=name,
            email=email,
            phone=phone,
            address=address,
        )
        session.add(contact)
        session.flush()
        return contact.id


# ─────────────────────────────────────────────────────────────
#  TENDERS
# ─────────────────────────────────────────────────────────────

def upsert_tender(data: dict) -> dict:
    """
    Insert or update a tender record.
    Uses (source_portal, tender_id) as the unique key.
    Returns a plain dict with the saved tender's fields.
    """
    with get_session() as session:
        tender = session.query(Tender).filter_by(
            source_portal=data["source_portal"],
            tender_id=data["tender_id"],
        ).first()

        if tender:
            for key, value in data.items():
                if hasattr(tender, key):
                    setattr(tender, key, value)
        else:
            tender = Tender(**{k: v for k, v in data.items() if hasattr(Tender, k)})
            session.add(tender)
            session.flush()

        session.refresh(tender)
        return _tender_to_dict(tender)


def get_tender_by_ref(portal: str, tender_id: str) -> Optional[dict]:
    """
    Look up a tender by (source_portal, tender_id).
    Returns a plain dict or None if not found.
    """
    with get_session() as session:
        tender = session.query(Tender).filter_by(
            source_portal=portal,
            tender_id=tender_id,
        ).first()
        return _tender_to_dict(tender) if tender else None


# ─────────────────────────────────────────────────────────────
#  SCRAPER STATE
# ─────────────────────────────────────────────────────────────

def get_state(portal: str) -> dict:
    with get_session() as session:
        state = session.query(ScraperState).filter_by(portal=portal).first()
        if state:
            return {
                "last_run":       state.last_run,
                "last_notice_id": state.last_notice_id,
            }
        return {"last_run": None, "last_notice_id": None}


def save_state(portal: str, last_notice_id: str, last_run: Optional[str] = None) -> None:
    from datetime import date
    run_date = last_run or date.today().isoformat()
    with get_session() as session:
        state = session.query(ScraperState).filter_by(portal=portal).first()
        if state:
            state.last_run       = run_date
            state.last_notice_id = last_notice_id
        else:
            state = ScraperState(
                portal=portal,
                last_run=run_date,
                last_notice_id=last_notice_id,
            )
            session.add(state)


def reset_state(portal: str) -> dict:
    from datetime import date
    with get_session() as session:
        state = session.query(ScraperState).filter_by(portal=portal).first()
        if state:
            state.last_notice_id = None
            state.last_run       = date.today().isoformat()
        else:
            state = ScraperState(portal=portal, last_run=date.today().isoformat())
            session.add(state)
    return {"last_run": None, "last_notice_id": None}


def get_newest_id_from_db(portal: str) -> Optional[str]:
    with get_session() as session:
        row = session.execute(text("""
            SELECT tender_id FROM tenders
            WHERE source_portal = :portal
            ORDER BY publication_date DESC, id DESC
            LIMIT 1
        """), {"portal": portal}).fetchone()
        return row[0] if row else None


# ─────────────────────────────────────────────────────────────
#  INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────

def _tender_to_dict(tender: Tender) -> dict:
    """Convert a Tender ORM object to a plain dict."""
    return {
        "id":                       tender.id,
        "source_portal":            tender.source_portal,
        "tender_id":                tender.tender_id,
        "organisation_id":          tender.organisation_id,
        "title":                    tender.title,
        "description":              tender.description,
        "notice_text":              tender.notice_text,
        "pdf_path":                 tender.pdf_path,
        "country":                  tender.country,
        "notice_type":              tender.notice_type,
        "language":                 tender.language,
        "publication_date":         tender.publication_date,
        "deadline_date":            tender.deadline_date,
        "deadline_time":            tender.deadline_time,
        "budget":                   tender.budget,
        "currency":                 tender.currency,
        "project_id":               tender.project_id,
        "procurement_group":        tender.procurement_group,
        "procurement_method_code":  tender.procurement_method_code,
        "procurement_method_name":  tender.procurement_method_name,
        "sector":                   tender.sector,
        "keywords":                 tender.keywords,
        "summary":                  tender.summary,
        "status_id":                tender.status_id,
        "source_url":               tender.source_url,
        "scraped_at":               tender.created_at.isoformat() if tender.created_at else None,
        "updated_at":               tender.updated_at.isoformat() if tender.updated_at else None,
    }
def get_unscored_enriched_tenders(session):
    return (
        session.query(EnrichedTender)
        .filter(
            EnrichedTender.p_go.is_(None),
            EnrichedTender.days_to_deadline >= 2,
        )
        .all()
    )

def save_score_to_enriched(session, enriched_tender_id, p_go, score_breakdown, model_version):
    tender = session.query(EnrichedTender).filter_by(id=enriched_tender_id).first()
    if tender:
        tender.p_go             = p_go
        tender.score_breakdown  = score_breakdown
        tender.model_version    = model_version

def save_tender_score(session, enriched_tender_id, p_go, recommendation, justification):
    existing = (
        session.query(TenderScore)
        .filter_by(enriched_tender_id=enriched_tender_id)
        .first()
    )
    if existing:
        return
    session.add(TenderScore(
        enriched_tender_id = enriched_tender_id,
        p_go               = p_go,
        recommendation     = recommendation,
        justification      = justification,
    ))

def get_top_tenders(session, limit=5, offset=0):
    return (
        session.query(TenderScore, EnrichedTender)
        .join(EnrichedTender, TenderScore.enriched_tender_id == EnrichedTender.id)
        .filter(EnrichedTender.days_to_deadline >= 2)
        .order_by(TenderScore.p_go.desc())
        .limit(limit)
        .offset(offset)
        .all()
    )

# ─────────────────────────────────────────────────────────────
#  LEGACY SHIM
# ─────────────────────────────────────────────────────────────

def _connect():
    """Legacy shim — returns a raw SQLAlchemy connection. Use get_session() instead."""
    return engine.connect()