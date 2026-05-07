"""
back/models/db_models.py
"""

from datetime import datetime, timezone
from sqlalchemy import (
    Boolean, Column, DateTime, Float, ForeignKey,
    Integer, String, Text,
)
from sqlalchemy.orm import relationship
from database import Base


def _now():
    return datetime.now(timezone.utc)


class PlatformUser(Base):
    __tablename__ = "platform_users"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    email           = Column(String(300), unique=True, nullable=False)
    full_name       = Column(String(300))
    hashed_password = Column(String(500), nullable=False)
    role            = Column(String(50), default="manager")
    is_active       = Column(Boolean, default=True)
    created_at      = Column(DateTime(timezone=True), default=_now)
    last_login      = Column(DateTime(timezone=True), nullable=True)


class Tender(Base):
    __tablename__ = "tenders"

    id               = Column(Integer, primary_key=True)
    source_portal    = Column(String(100))
    tender_id        = Column(String(200))
    title            = Column(String(1000))
    notice_text      = Column(Text)
    source_url       = Column(String(2000))
    deadline_date    = Column(String(20))
    publication_date = Column(String(20))
    country          = Column(String(200))

    enriched   = relationship("EnrichedTender",  back_populates="tender", uselist=False)
    normalized = relationship("NormalizedTender", back_populates="tender", uselist=False)


class EnrichedTender(Base):
    __tablename__ = "enriched_tenders"

    id                       = Column(Integer, primary_key=True)
    tender_id                = Column(Integer, ForeignKey("tenders.id"))
    source_portal            = Column(String(100))
    source_url               = Column(String(2000))
    title_clean              = Column(String(1000))
    description_clean        = Column(Text)
    deadline_datetime        = Column(DateTime(timezone=True))
    days_to_deadline         = Column(Integer)
    country_name_normalized  = Column(String(200))
    is_multi_country         = Column(Boolean)
    countries_list           = Column(Text)
    procurement_group        = Column(String(50))
    budget                   = Column(Float)
    currency                 = Column(String(3))
    sector                   = Column(Text)
    funding_agency           = Column(String(500))
    organisation_name        = Column(String(500))
    language                 = Column(String(50))
    has_pdf                  = Column(Boolean)
    pdf_path                 = Column(String(500))
    enrichment_status        = Column(String(50))
    enriched_at              = Column(DateTime(timezone=True))
    p_go                     = Column(Float)
    score_breakdown          = Column(Text)
    model_version            = Column(Integer)

    llm_scope_summary            = Column(Text)
    llm_project_program          = Column(String(500))
    llm_financing_instrument     = Column(String(50))
    llm_bid_process_type         = Column(String(50))
    llm_contract_duration_months = Column(Integer)
    llm_eligibility_summary      = Column(Text)
    llm_specific_areas           = Column(Text)
    llm_submission_process       = Column(Text)

    tender       = relationship("Tender",      back_populates="enriched")
    tender_score = relationship("TenderScore", back_populates="enriched_tender", uselist=False)


class NormalizedTender(Base):
    __tablename__ = "normalized_tenders"

    id                = Column(Integer, primary_key=True)
    tender_id         = Column(Integer, ForeignKey("tenders.id"))
    notice_text_clean = Column(Text)
    description_clean = Column(Text)

    tender = relationship("Tender", back_populates="normalized")


class TenderScore(Base):
    __tablename__ = "tender_scores"

    id                    = Column(Integer, primary_key=True)
    enriched_tender_id    = Column(Integer, ForeignKey("enriched_tenders.id"))
    p_go                  = Column(Float)
    recommendation        = Column(String(20))
    justification         = Column(Text)
    scored_at             = Column(DateTime(timezone=True))
    partner_decision      = Column(String(10))
    partner_justification = Column(Text)
    decided_at            = Column(DateTime(timezone=True))

    enriched_tender = relationship("EnrichedTender", back_populates="tender_score")
    decisions       = relationship("PartnerDecision", back_populates="tender_score")


class PartnerDecision(Base):
    __tablename__ = "partner_decisions"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    tender_score_id = Column(Integer, ForeignKey("tender_scores.id"), nullable=False)
    user_id         = Column(Integer, ForeignKey("platform_users.id"), nullable=False)
    decision        = Column(String(10), nullable=False)
    justification   = Column(Text)
    decided_at      = Column(DateTime(timezone=True), default=_now)

    tender_score = relationship("TenderScore", back_populates="decisions")
    user         = relationship("PlatformUser")