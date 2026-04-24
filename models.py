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
    funding_agency = Column(String(500))
    # ───────────────────────────────
    # LLM-generated Summary
    # ───────────────────────────────
    summary = Column(Text)

    # ───────────────────────────────
    # Metadata
    # ───────────────────────────────
    enriched_at       = Column(DateTime(timezone=True), default=_now)
    enrichment_status = Column(String(50))  # success / partial / failed

    p_go            = Column(Float,   nullable=True)
    score_breakdown = Column(Text,    nullable=True)
    model_version   = Column(Integer, nullable=True)

    # ───────────────────────────────
    # Relationships
    # ───────────────────────────────
    tender = relationship("Tender", back_populates="enriched")
    tender_score = relationship("TenderScore", back_populates="enriched_tender", uselist=False)
# ─────────────────────────────────────────────────────────────
#  TENDER SCORES
#  Only GO tenders (p_go ≥ 0.70) land here.
#  Reads p_go and score_breakdown directly from enriched_tenders
#  — nothing is recalculated.
#  This is the table the platform reads from to display rankings.
# ─────────────────────────────────────────────────────────────
 
class TenderScore(Base):
    __tablename__ = "tender_scores"
 
    id                 = Column(Integer, primary_key=True, autoincrement=True)
 
    enriched_tender_id = Column(
        Integer,
        ForeignKey("enriched_tenders.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        # unique=True because one tender has one score row — never duplicated
    )
 
    # ───────────────────────────────
    # Score (copied from enriched_tenders at scoring time — never recalculated)
    # ───────────────────────────────
    p_go           = Column(Float,        nullable=False)
    # Direct copy of enriched_tenders.p_go — stored here for fast ORDER BY queries
    # without joining enriched_tenders every time
 
    # ───────────────────────────────
    # Recommendation
    # ───────────────────────────────
    recommendation = Column(String(20),   nullable=False)
    # "STRONG GO" if p_go >= 0.80
    # "GO"        if p_go >= 0.70
    # Derived at scoring time and stored — never recomputed
 
    # ───────────────────────────────
    # Justification (auto-generated by scoring engine, GO tenders only)
    # ───────────────────────────────
    justification  = Column(Text,         nullable=True)
    # Auto-written by the scoring engine at scoring time.
    # Format (Option C):
    #   STRONG SIGNALS:
    #   ✅ CONSULTING        +3.10  Core KPMG business
    #   ✅ Risk & Compliance +2.03  Strong sector expertise
    #   WEAK SIGNALS:
    #   ⚠️  Deadline over 40  -1.03  Low urgency
    #   WHY: Strong consulting engagement in known Maghreb market...
 
    # ───────────────────────────────
    # Timestamp
    # ───────────────────────────────
    scored_at      = Column(DateTime(timezone=True), default=_now)
    # When the scoring engine wrote this row
    # Used to know if score was produced before or after an SGD update
 
    # ───────────────────────────────
    # Partner Decision (filled in Step 3 — SGD)
    # NULL until partner acts on this tender
    # ───────────────────────────────
    partner_decision      = Column(String(10),  nullable=True)
    # "GO" or "NO GO" — filled when partner decides
    # Triggers SGD weight update when written
 
    partner_justification = Column(Text,        nullable=True)
    # JSON of partner's j-values per feature
    # e.g. '{"proc_CONSULTING": 0.5, "budget_large": 0.0, "tier_1": -0.5}'
    # "drove it" → 0.5 | "neutral" → 0.0 | "not the reason" → -0.5
    # Used by SGD engine to update weights
 
    decided_at            = Column(DateTime(timezone=True), nullable=True)
    # When partner made their decision — NULL until decided
 
    # ───────────────────────────────
    # Relationships
    # ───────────────────────────────
    enriched_tender = relationship("EnrichedTender", back_populates="tender_score")
 
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
    funding_agency = Column(String(500))
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


    """
ADD THIS CLASS TO models.py
Place it after the TenderScore class.
Replace the previous WeightsHistory class if it exists.
"""

class WeightsHistory(Base):
    __tablename__ = "weights_history"

    # ───────────────────────────────
    # Primary Key — version number
    # Increments every training or SGD update
    # Matches model_version in enriched_tenders
    # ───────────────────────────────
    version       = Column(Integer, primary_key=True)

    # ───────────────────────────────
    # Training Info
    # ───────────────────────────────
    trained_at    = Column(String(50),              nullable=True)
    updated_at    = Column(DateTime(timezone=True), default=_now)
    



    # ───────────────────────────────
    # SGD Trigger (NULL for initial training)
    # ───────────────────────────────
    triggered_by_tender_score_id = Column(
        Integer,
        ForeignKey("tender_scores.id", ondelete="SET NULL"),
        nullable=True,
    )
    triggered_by_partner = Column(String(200), nullable=True)
    notes                = Column(Text,        nullable=True)

    # ───────────────────────────────
    # BASELINE
    # ───────────────────────────────
    baseline = Column(Float, nullable=False)

    # ───────────────────────────────
    # COUNTRY TIERS
    # ───────────────────────────────
    tier_1 = Column(Float, nullable=True)
    tier_2 = Column(Float, nullable=True)
    tier_3 = Column(Float, nullable=True)
    tier_4 = Column(Float, nullable=True)
    tier_5 = Column(Float, nullable=True)
    tier_6 = Column(Float, nullable=True)

    # ───────────────────────────────
    # SECTORS
    # ───────────────────────────────
    sector_environment_climate                  = Column(Float, nullable=True)
    sector_social_protection_poverty_reduction  = Column(Float, nullable=True)
    sector_water_sanitation_waste               = Column(Float, nullable=True)
    sector_digital_transformation               = Column(Float, nullable=True)
    sector_others                               = Column(Float, nullable=True)
    sector_agriculture_food_security            = Column(Float, nullable=True)
    sector_risk_compliance                      = Column(Float, nullable=True)
    sector_health_life_sciences                 = Column(Float, nullable=True)
    sector_construction_infrastructure          = Column(Float, nullable=True)
    sector_energy_utilities                     = Column(Float, nullable=True)
    sector_education_training                   = Column(Float, nullable=True)
    sector_transport_logistics                  = Column(Float, nullable=True)
    sector_enterprise_it_systems_implementation = Column(Float, nullable=True)
    sector_business_strategy_performance        = Column(Float, nullable=True)
    sector_government_reform_public_admin       = Column(Float, nullable=True)
    sector_financial_services                   = Column(Float, nullable=True)
    sector_marketing_customer_experience        = Column(Float, nullable=True)
    sector_mining_natural_resources             = Column(Float, nullable=True)
    sector_data_ai_analytics                    = Column(Float, nullable=True)
    sector_employment_skills_development        = Column(Float, nullable=True)
    sector_telecommunications                   = Column(Float, nullable=True)
    sector_organizational_reform_hr_management  = Column(Float, nullable=True)
    sector_cybersecurity_data_security          = Column(Float, nullable=True)
    sector_justice_rule_of_law                  = Column(Float, nullable=True)

    # ───────────────────────────────
    # PROCUREMENT
    # ───────────────────────────────
    proc_consulting     = Column(Float, nullable=True)
    proc_non_consulting = Column(Float, nullable=True)
    proc_works          = Column(Float, nullable=True)
    proc_goods          = Column(Float, nullable=True)
    proc_others         = Column(Float, nullable=True)

    # ───────────────────────────────
    # FUNDING AGENCY
    # ───────────────────────────────
    agency_world_bank = Column(Float, nullable=True)
    agency_afdb       = Column(Float, nullable=True)
    agency_undp       = Column(Float, nullable=True)
    agency_fao        = Column(Float, nullable=True)
    agency_ilo        = Column(Float, nullable=True)
    agency_unicef     = Column(Float, nullable=True)
    agency_iom        = Column(Float, nullable=True)
    agency_unops      = Column(Float, nullable=True)
    agency_unido      = Column(Float, nullable=True)
    agency_other_un   = Column(Float, nullable=True)

    # ───────────────────────────────
    # BUDGET
    # ───────────────────────────────
    budget_large  = Column(Float, nullable=True)
    budget_medium = Column(Float, nullable=True)
    budget_small  = Column(Float, nullable=True)

    # ───────────────────────────────
    # DEADLINE
    # ───────────────────────────────
    deadline_2_20    = Column(Float, nullable=True)
    deadline_20_40   = Column(Float, nullable=True)
    deadline_over_40 = Column(Float, nullable=True)