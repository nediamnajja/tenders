"""
models.py — SQLAlchemy ORM models
==================================
Single source of truth for all table definitions.
Works with SQLite now, Postgres later — just change DATABASE_URL in db.py.

Tables:
    organisations       Procuring entities
    contacts            Contact persons per organisation
    tenders             Raw scraped tender records
    enriched_tenders    LLM-extracted fields from notice_text
    normalized_tenders  Clean, validated, standardised records
    scraper_state       Cursor state per portal
    normalization_log   Audit log per normalization run

NOTE on sector mapping:
    cpv_code, cpv_label, unspsc_code, unspsc_label, sector_source
    are present in NormalizedTender but intentionally left NULL.
    They will be populated by a future NLP pipeline.
"""

from datetime import datetime, timezone
from sqlalchemy import (
    Boolean, Column, DateTime, Float, ForeignKey,
    Integer, String, Text, UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, relationship


def _now():
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


# ─────────────────────────────────────────────────────────────
#  ORGANISATIONS
# ─────────────────────────────────────────────────────────────

class Organisation(Base):
    __tablename__ = "organisations"

    id                = Column(Integer, primary_key=True, autoincrement=True)
    name              = Column(String(500), nullable=False)
    name_normalised   = Column(String(500))      # lowercase stripped — used for dedup
    organisation_type = Column(String(100))      # International, NGO, Government…
    country           = Column(String(200))
    country_iso2      = Column(String(2))        # populated by normalizer
    created_at        = Column(DateTime(timezone=True), default=_now)
    updated_at        = Column(DateTime(timezone=True), default=_now, onupdate=_now)

    contacts = relationship("Contact", back_populates="organisation", cascade="all, delete-orphan")
    tenders  = relationship("Tender",  back_populates="organisation")

    __table_args__ = (
        UniqueConstraint("name", "organisation_type", name="uq_org_name_type"),
    )


# ─────────────────────────────────────────────────────────────
#  CONTACTS
# ─────────────────────────────────────────────────────────────

class Contact(Base):
    __tablename__ = "contacts"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    organisation_id = Column(Integer, ForeignKey("organisations.id", ondelete="CASCADE"), nullable=False)
    name            = Column(String(300))
    email           = Column(String(300))
    phone           = Column(String(100))
    address         = Column(Text)
    created_at      = Column(DateTime(timezone=True), default=_now)

    organisation = relationship("Organisation", back_populates="contacts")


# ─────────────────────────────────────────────────────────────
#  TENDERS  (raw scraped data — written by scrapers)
# ─────────────────────────────────────────────────────────────

class Tender(Base):
    __tablename__ = "tenders"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    source_portal   = Column(String(100), nullable=False)
    tender_id       = Column(String(200), nullable=False)
    organisation_id = Column(Integer, ForeignKey("organisations.id"))

    title            = Column(String(1000))
    description      = Column(Text)
    notice_text      = Column(Text)
    pdf_path         = Column(String(500)) 
    country          = Column(String(200))
    notice_type      = Column(String(200))
    language         = Column(String(100))

    publication_date = Column(String(20))
    deadline_date    = Column(String(20))
    deadline_time    = Column(String(10))

    budget           = Column(String(200))
    currency         = Column(String(20))

    project_id              = Column(String(200))
    procurement_group       = Column(String(200))
    procurement_method_code = Column(String(100))
    procurement_method_name = Column(String(300))

    # TODO: NLP sector mapping — leave NULL until pipeline is built
    sector   = Column(String(200))
    keywords = Column(Text)
    summary  = Column(Text)

    status_id  = Column(Integer)
    source_url = Column(String(2000))

    created_at = Column(DateTime(timezone=True), default=_now)
    updated_at = Column(DateTime(timezone=True), default=_now, onupdate=_now)

    organisation = relationship("Organisation",     back_populates="tenders")
    enriched     = relationship("EnrichedTender",   back_populates="tender", uselist=False)
    normalized   = relationship("NormalizedTender", back_populates="tender", uselist=False)

    __table_args__ = (
        UniqueConstraint("source_portal", "tender_id", name="uq_portal_tender"),
    )


# ─────────────────────────────────────────────────────────────
#  ENRICHED TENDERS  (LLM-extracted from notice_text)
# ─────────────────────────────────────────────────────────────
class EnrichedTender(Base):
    __tablename__ = "enriched_tenders"
    # ───────────────────────────────
    # Identification & Source
    # ───────────────────────────────
    id = Column(Integer, primary_key=True, autoincrement=True)
    tender_id     = Column(Integer, ForeignKey("tenders.id", ondelete="CASCADE"), nullable=False, unique=True)
    source_portal = Column(String(100))
    notice_id     = Column(String(200))
    source_url    = Column(String(2000))

    # ───────────────────────────────
    # Text & Title
    # ───────────────────────────────
    title_clean       = Column(String(1000))
    description_clean = Column(Text)

    # ───────────────────────────────
    # Dates
    # ───────────────────────────────
    publication_datetime = Column(DateTime(timezone=True))
    deadline_datetime    = Column(DateTime(timezone=True))  # final, normalized or extracted
    days_to_deadline     = Column(Integer)

    # ───────────────────────────────
    # Geography
    # ───────────────────────────────
    country_name_normalized = Column(String(200))
    is_multi_country        = Column(Boolean, default=False)
    countries_list          = Column(Text)  # JSON or pipe-separated

    # ───────────────────────────────
    # Procurement Context
    # ───────────────────────────────
    notice_type_normalized  = Column(String(100))
    lifecycle_stage         = Column(String(50))
    status_normalized       = Column(String(50))
    project_id              = Column(String(200))
    procurement_method_name = Column(String(300))
    procurement_group       = Column(String(50))  # final enriched

    # ───────────────────────────────
    # Budget & Currency (final)
    # ───────────────────────────────
    budget   = Column(Float)
    currency = Column(String(3))

    # ───────────────────────────────
    # Documents
    # ───────────────────────────────
    pdf_path = Column(String(500))
    has_pdf  = Column(Boolean, default=False)

    # ───────────────────────────────
    # Contact (final)
    # ───────────────────────────────
    contact_name  = Column(String(300))
    contact_email = Column(String(300))
    contact_phone = Column(String(100))

    # ───────────────────────────────
    # Enrichment (NLP / rules)
    # ───────────────────────────────
    sector            = Column(Text)  # JSON list for multiple sectors
    keywords          = Column(Text)  # JSON list or comma-separated
    language          = Column(String(50))
    organisation_name = Column(String(500))  # final authoritative

    # ───────────────────────────────
    # LLM-generated Summary
    # ───────────────────────────────
    summary = Column(Text)

    # ───────────────────────────────
    # Metadata
    # ───────────────────────────────
    enriched_at       = Column(DateTime(timezone=True), default=_now)
    enrichment_status = Column(String(50))  # success / partial / failed

    # ───────────────────────────────
    # Relationships
    # ───────────────────────────────
    tender = relationship("Tender", back_populates="enriched")

# ─────────────────────────────────────────────────────────────
#  NORMALIZED TENDERS  (clean, validated, standardised)
# ─────────────────────────────────────────────────────────────

class NormalizedTender(Base):
    __tablename__ = "normalized_tenders"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    tender_id     = Column(Integer, ForeignKey("tenders.id", ondelete="CASCADE"), nullable=False, unique=True)
    source_portal = Column(String(100))
    notice_id     = Column(String(200))
    source_url    = Column(String(2000))

    # ─────────────────────────────────────────────────────────
    # Core Identification
    # ─────────────────────────────────────────────────────────
    organisation_id              = Column(Integer, ForeignKey("organisations.id"))
    organisation_name_normalized = Column(String(500))

    # ─────────────────────────────────────────────────────────
    # Title & Text
    # ─────────────────────────────────────────────────────────
    title_clean         = Column(String(1000))
    description_clean   = Column(Text)


    # ─────────────────────────────────────────────────────────
    # Dates
    # ─────────────────────────────────────────────────────────
    publication_datetime = Column(DateTime(timezone=True))
    deadline_datetime    = Column(DateTime(timezone=True))
    days_to_deadline     = Column(Integer)
    created_at           = Column(DateTime(timezone=True), default=_now)
    updated_at           = Column(DateTime(timezone=True), default=_now, onupdate=_now)

    # ─────────────────────────────────────────────────────────
    # Country & Geography
    # ─────────────────────────────────────────────────────────
    country_name_normalized = Column(String(200))
    is_multi_country        = Column(Boolean, default=False)
    countries_list          = Column(Text)   # e.g. JSON string or pipe-separated list

    # ─────────────────────────────────────────────────────────
    # Notice Type 
    # ─────────────────────────────────────────────────────────
    notice_type_normalized = Column(String(100))

    # ─────────────────────────────────────────────────────────
    # Procurement Stage
    # Values:  forecast | exploration | tendering | implementation
      # status_normalized values:
    #   planned | open | closed | shortlisted | awarded | cancelled
    # ─────────────────────────────────────────────────────────
    lifecycle_stage   = Column(String(50))
    status_normalized = Column(String(50))
    # ─────────────────────────────────────────────────────────
    # Procurement / Project Info
    # ─────────────────────────────────────────────────────────
    project_id                   = Column(String(200))
    procurement_group_normalized = Column(String(100))
    procurement_method_name      = Column(String(300))

    # ─────────────────────────────────────────────────────────
    # Budget
    # ─────────────────────────────────────────────────────────
    budget_numeric = Column(Float)
    currency_iso   = Column(String(3))
    budget_missing = Column(Boolean, default=False)

    # ─────────────────────────────────────────────────────────
    # Documents
    # ─────────────────────────────────────────────────────────
    pdf_path       = Column(String(500))
    has_pdf        = Column(Boolean, default=False)
    document_count = Column(Integer, default=0)

    # ─────────────────────────────────────────────────────────
    # Contact (standardized, not inferred)
    # ─────────────────────────────────────────────────────────
    contact_name  = Column(String(300))
    contact_email = Column(String(300))
    contact_phone = Column(String(100))

    # ─────────────────────────────────────────────────────────
    # Status
    # ─────────────────────────────────────────────────────────
    status_id         = Column(Integer)
    source_status_raw = Column(String(200))

    # ─────────────────────────────────────────────────────────
    # Sector (structure only — enrichment later)
    # ─────────────────────────────────────────────────────────
    cpv_code      = Column(String(20))
    cpv_label     = Column(String(300))
    unspsc_code   = Column(String(20))
    unspsc_label  = Column(String(300))
    sector_source = Column(String(50))

    # ─────────────────────────────────────────────────────────
    # Data Integrity
    # ─────────────────────────────────────────────────────────
    is_valid             = Column(Boolean, default=True)
    missing_fields       = Column(Text)   # pipe-separated or JSON string
    validation_flags     = Column(Text)   # pipe-separated or JSON string
    normalization_status = Column(String(50), default="success")  # success | failed | flagged
    normalized_at        = Column(DateTime(timezone=True), default=_now)

    tender = relationship("Tender", back_populates="normalized")
    organisation = relationship("Organisation")


# ─────────────────────────────────────────────────────────────
#  SCRAPER STATE
# ─────────────────────────────────────────────────────────────

class ScraperState(Base):
    __tablename__ = "scraper_state"

    portal         = Column(String(100), primary_key=True)
    last_run       = Column(String(20))
    last_notice_id = Column(String(200))
    updated_at     = Column(DateTime(timezone=True), default=_now, onupdate=_now)


 
# ─────────────────────────────────────────────────────────────
#  SCRAPER RUN LOG  ← NEW
#  One row per scraper run. Dashboard reads from this table.
#  Written at the end of every run (success or failure).
# ─────────────────────────────────────────────────────────────
 
class ScraperRunLog(Base):
    __tablename__ = "scraper_run_log"
 
    id         = Column(Integer, primary_key=True, autoincrement=True)
    portal     = Column(String(100), nullable=False)
 
    started_at  = Column(DateTime(timezone=True), nullable=False)
    finished_at = Column(DateTime(timezone=True))
 
    # Counts
    saved            = Column(Integer, default=0)   # new rows inserted / updated
    expired_skipped  = Column(Integer, default=0)   # deadline already passed
    year_filtered    = Column(Integer, default=0)   # wrong publication year
    cursor_stopped   = Column(Integer, default=0)   # rows seen after cursor hit
    already_existed  = Column(Integer, default=0)   # dedup skips
    errors           = Column(Integer, default=0)   # parse / save failures
 
    # Outcome
    # "success" | "partial" | "failed" | "empty_db_abort"
    status     = Column(String(50), nullable=False, default="success")
    new_cursor = Column(String(200))   # last_notice_id written at end of run
    notes      = Column(Text)          # any extra info, warnings, error messages

# ─────────────────────────────────────────────────────────────
#  NORMALIZATION LOG
# ─────────────────────────────────────────────────────────────

class NormalizationLog(Base):
    __tablename__ = "normalization_log"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    portal      = Column(String(100))
    run_at      = Column(DateTime(timezone=True), default=_now)
    total_input = Column(Integer, default=0)
    success     = Column(Integer, default=0)
    flagged     = Column(Integer, default=0)
    failed      = Column(Integer, default=0)
    skipped     = Column(Integer, default=0)
    notes       = Column(Text)