# """
# back/models/db_models.py
# """

# from datetime import datetime, timezone
# from sqlalchemy import (
#     Boolean, Column, DateTime, Float, ForeignKey,
#     Integer, String, Text,
# )
# from sqlalchemy.orm import relationship
# from database import Base


# def _now():
#     return datetime.now(timezone.utc)


# class PlatformUser(Base):
#     __tablename__ = "platform_users"

#     id              = Column(Integer, primary_key=True, autoincrement=True)
#     email           = Column(String(300), unique=True, nullable=False)
#     full_name       = Column(String(300))
#     hashed_password = Column(String(500), nullable=False)
#     role            = Column(String(50), default="manager")
#     is_active       = Column(Boolean, default=True)
#     created_at      = Column(DateTime(timezone=True), default=_now)
#     last_login      = Column(DateTime(timezone=True), nullable=True)


# class Tender(Base):
#     __tablename__ = "tenders"

#     id               = Column(Integer, primary_key=True)
#     source_portal    = Column(String(100))
#     tender_id        = Column(String(200))
#     title            = Column(String(1000))
#     notice_text      = Column(Text)
#     source_url       = Column(String(2000))
#     deadline_date    = Column(String(20))
#     publication_date = Column(String(20))
#     country          = Column(String(200))

#     enriched   = relationship("EnrichedTender",  back_populates="tender", uselist=False)
#     normalized = relationship("NormalizedTender", back_populates="tender", uselist=False)


# class EnrichedTender(Base):
#     __tablename__ = "enriched_tenders"

#     id                       = Column(Integer, primary_key=True)
#     tender_id                = Column(Integer, ForeignKey("tenders.id"))
#     source_portal            = Column(String(100))
#     source_url               = Column(String(2000))
#     title_clean              = Column(String(1000))
#     description_clean        = Column(Text)
#     deadline_datetime        = Column(DateTime(timezone=True))
#     publication_datetime = Column(DateTime(timezone=True), nullable=True)
#     days_to_deadline         = Column(Integer)
#     country_name_normalized  = Column(String(200))
#     is_multi_country         = Column(Boolean)
#     countries_list           = Column(Text)
#     procurement_group        = Column(String(50))
#     budget                   = Column(Float)
#     currency                 = Column(String(3))
#     sector                   = Column(Text)
#     funding_agency           = Column(String(500))
#     organisation_name        = Column(String(500))
#     language                 = Column(String(50))
#     has_pdf                  = Column(Boolean)
#     pdf_path                 = Column(String(500))
#     enrichment_status        = Column(String(50))
#     enriched_at              = Column(DateTime(timezone=True))
#     p_go                     = Column(Float)
#     score_breakdown          = Column(Text)
#     model_version            = Column(Integer)

#     llm_scope_summary            = Column(Text)
#     llm_project_program          = Column(String(500))
#     llm_financing_instrument     = Column(String(50))
#     llm_bid_process_type         = Column(String(50))
#     llm_contract_duration_months = Column(Integer)
#     llm_eligibility_summary      = Column(Text)
#     llm_specific_areas           = Column(Text)
#     llm_submission_process       = Column(Text)

#     tender       = relationship("Tender",      back_populates="enriched")
#     tender_score = relationship("TenderScore", back_populates="enriched_tender", uselist=False)


# class NormalizedTender(Base):
#     __tablename__ = "normalized_tenders"

#     id                = Column(Integer, primary_key=True)
#     tender_id         = Column(Integer, ForeignKey("tenders.id"))
#     notice_text_clean = Column(Text)
#     description_clean = Column(Text)

#     tender = relationship("Tender", back_populates="normalized")


# class TenderScore(Base):
#     __tablename__ = "tender_scores"

#     id                    = Column(Integer, primary_key=True)
#     enriched_tender_id    = Column(Integer, ForeignKey("enriched_tenders.id"))
#     p_go                  = Column(Float)
#     recommendation        = Column(String(20))
#     justification         = Column(Text)
#     scored_at             = Column(DateTime(timezone=True))
#     partner_decision      = Column(String(10))
#     partner_justification = Column(Text)
#     decided_at            = Column(DateTime(timezone=True))

#     enriched_tender = relationship("EnrichedTender", back_populates="tender_score")
#     decisions       = relationship("PartnerDecision", back_populates="tender_score")


# class SavedTender(Base):
#     __tablename__ = "saved_tenders"
 
#     id        = Column(Integer, primary_key=True, autoincrement=True)
#     user_id   = Column(Integer, ForeignKey("platform_users.id"), nullable=False)
#     tender_id = Column(Integer, ForeignKey("enriched_tenders.id"), nullable=False)
#     saved_at  = Column(DateTime(timezone=True), default=_now)
 
#     user   = relationship("PlatformUser")
#     tender = relationship("EnrichedTender")

# class PartnerDecision(Base):
#     __tablename__ = "partner_decisions"

#     id              = Column(Integer, primary_key=True, autoincrement=True)
#     tender_score_id = Column(Integer, ForeignKey("tender_scores.id"), nullable=False)
#     user_id         = Column(Integer, ForeignKey("platform_users.id"), nullable=False)
#     decision        = Column(String(10), nullable=False)
#     justification   = Column(Text)
#     decided_at      = Column(DateTime(timezone=True), default=_now)

#     tender_score = relationship("TenderScore", back_populates="decisions")
#     user         = relationship("PlatformUser")

    

"""
back/models/db_models.py
"""

from datetime import datetime, timezone
from sqlalchemy import (
    Boolean, Column, DateTime, Float, ForeignKey,
    Integer, String, Text,
)
from sqlalchemy.orm import relationship
from back.database import Base


def _now():
    return datetime.now(timezone.utc)


# ─────────────────────────────────────────────────────────────
#  PLATFORM USERS
# ─────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────
#  TENDERS  (raw — read only from pipeline)
# ─────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────
#  ENRICHED TENDERS  (read only from pipeline)
# ─────────────────────────────────────────────────────────────

class EnrichedTender(Base):
    __tablename__ = "enriched_tenders"

    id                      = Column(Integer, primary_key=True)
    tender_id               = Column(Integer, ForeignKey("tenders.id"))
    source_portal           = Column(String(100))
    source_url              = Column(String(2000))
    title_clean             = Column(String(1000))
    description_clean       = Column(Text)
    deadline_datetime       = Column(DateTime(timezone=True))
    publication_datetime    = Column(DateTime(timezone=True), nullable=True)
    days_to_deadline        = Column(Integer)
    country_name_normalized = Column(String(200))
    is_multi_country        = Column(Boolean)
    countries_list          = Column(Text)
    procurement_group       = Column(String(50))
    procurement_method_name = Column(String(300))   # needed by scoring engine filter
    budget                  = Column(Float)
    currency                = Column(String(3))
    sector                  = Column(Text)
    funding_agency          = Column(String(500))
    organisation_name       = Column(String(500))
    language                = Column(String(50))
    has_pdf                 = Column(Boolean)
    pdf_path                = Column(String(500))
    enrichment_status       = Column(String(50))
    enriched_at             = Column(DateTime(timezone=True))
    p_go                    = Column(Float)
    score_breakdown         = Column(Text)
    model_version           = Column(Integer)

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


# ─────────────────────────────────────────────────────────────
#  NORMALIZED TENDERS  (read only from pipeline)
# ─────────────────────────────────────────────────────────────

class NormalizedTender(Base):
    __tablename__ = "normalized_tenders"

    id                = Column(Integer, primary_key=True)
    tender_id         = Column(Integer, ForeignKey("tenders.id"))
    notice_text_clean = Column(Text)
    description_clean = Column(Text)

    tender = relationship("Tender", back_populates="normalized")


# ─────────────────────────────────────────────────────────────
#  TENDER SCORES
# ─────────────────────────────────────────────────────────────

class TenderScore(Base):
    __tablename__ = "tender_scores"

    id                    = Column(Integer, primary_key=True)
    enriched_tender_id    = Column(Integer, ForeignKey("enriched_tenders.id"))
    p_go                  = Column(Float)
    recommendation        = Column(String(20))
    justification         = Column(Text)        # auto-generated by scoring engine
    scored_at             = Column(DateTime(timezone=True))

    # ── Partner decision fields ───────────────────────────────
    partner_decision      = Column(String(10),              nullable=True)  # "GO" or "NO GO"
    partner_justification = Column(Text,                    nullable=True)  # plain text snapshot
    decided_at            = Column(DateTime(timezone=True), nullable=True)

    enriched_tender = relationship("EnrichedTender", back_populates="tender_score")
    decisions       = relationship("PartnerDecision", back_populates="tender_score")


# ─────────────────────────────────────────────────────────────
#  SAVED TENDERS
# ─────────────────────────────────────────────────────────────

class SavedTender(Base):
    __tablename__ = "saved_tenders"

    id        = Column(Integer, primary_key=True, autoincrement=True)
    user_id   = Column(Integer, ForeignKey("platform_users.id"), nullable=False)
    tender_id = Column(Integer, ForeignKey("enriched_tenders.id"), nullable=False)
    saved_at  = Column(DateTime(timezone=True), default=_now)

    user   = relationship("PlatformUser")
    tender = relationship("EnrichedTender")


# ─────────────────────────────────────────────────────────────
#  PARTNER DECISIONS  (full log — every decision by every user)
# ─────────────────────────────────────────────────────────────

class PartnerDecision(Base):
    __tablename__ = "partner_decisions"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    tender_score_id = Column(Integer, ForeignKey("tender_scores.id"), nullable=False)
    user_id         = Column(Integer, ForeignKey("platform_users.id"), nullable=False)
    decision        = Column(String(10), nullable=False)   # "GO" or "NO GO"
    justification   = Column(Text)                         # plain text — shown in frontend
    j_values        = Column(Text, nullable=True)          # JSON dict — used by SGD
    decided_at      = Column(DateTime(timezone=True), default=_now)

    tender_score = relationship("TenderScore", back_populates="decisions")
    user         = relationship("PlatformUser")


# ─────────────────────────────────────────────────────────────
#  WEIGHTS HISTORY  (one row per SGD update)
# ─────────────────────────────────────────────────────────────

class WeightsHistory(Base):
    __tablename__ = "weights_history"

    version    = Column(Integer, primary_key=True)
    baseline   = Column(Float,                   nullable=False)
    trained_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), default=_now)
    notes      = Column(Text,                    nullable=True)

    triggered_by_tender_score_id = Column(
        Integer,
        ForeignKey("tender_scores.id", ondelete="SET NULL"),
        nullable=True,
    )
    triggered_by_partner = Column(String(200), nullable=True)

    # ── Country tiers ─────────────────────────────────────────
    tier_1 = Column(Float, nullable=True)
    tier_2 = Column(Float, nullable=True)
    tier_3 = Column(Float, nullable=True)
    tier_4 = Column(Float, nullable=True)
    tier_5 = Column(Float, nullable=True)
    tier_6 = Column(Float, nullable=True)

    # ── Sectors ───────────────────────────────────────────────
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

    # ── Procurement ───────────────────────────────────────────
    proc_consulting     = Column(Float, nullable=True)
    proc_non_consulting = Column(Float, nullable=True)
    proc_works          = Column(Float, nullable=True)
    proc_goods          = Column(Float, nullable=True)
    proc_others         = Column(Float, nullable=True)

    # ── Agencies ──────────────────────────────────────────────
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

    # ── Budget ────────────────────────────────────────────────
    budget_large  = Column(Float, nullable=True)
    budget_medium = Column(Float, nullable=True)
    budget_small  = Column(Float, nullable=True)

    # ── Deadline ──────────────────────────────────────────────
    deadline_2_20    = Column(Float, nullable=True)
    deadline_20_40   = Column(Float, nullable=True)
    deadline_over_40 = Column(Float, nullable=True)