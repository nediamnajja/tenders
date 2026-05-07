"""
back/routers/tenders.py
"""

import ast
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import and_, or_
from sqlalchemy.orm import Session, joinedload

from database import get_db
from models.db_models import (
    EnrichedTender, PartnerDecision,
    PlatformUser, TenderScore,
)
from routers.auth import get_current_user
from schemas.schemas import (
    DecisionItem, DecisionRequest, DecisionResponse,
    TenderDetail, TenderListItem, TenderListResponse,
)

router = APIRouter(prefix="/tenders", tags=["tenders"])


def _parse_sector(sector_raw: Optional[str]) -> Optional[str]:
    if not sector_raw:
        return None
    try:
        parsed = ast.literal_eval(sector_raw)
        if isinstance(parsed, list):
            return ", ".join(parsed)
    except Exception:
        pass
    return sector_raw


def _build_list_item(et: EnrichedTender) -> TenderListItem:
    ts = et.tender_score
    return TenderListItem(
        id                      = et.id,
        title_clean             = et.title_clean,
        country_name_normalized = et.country_name_normalized,
        funding_agency          = et.funding_agency,
        sector                  = _parse_sector(et.sector),
        procurement_group       = et.procurement_group,
        budget                  = et.budget,
        currency                = et.currency,
        days_to_deadline        = et.days_to_deadline,
        source_portal           = et.source_portal,
        source_url              = et.source_url,
        p_go                    = ts.p_go             if ts else et.p_go,
        recommendation          = ts.recommendation   if ts else None,
        partner_decision        = ts.partner_decision if ts else None,
        enriched_at             = et.enriched_at,
    )


@router.get("/today")
def get_today_recommendations(
    db:           Session = Depends(get_db),
    current_user: PlatformUser = Depends(get_current_user),
):
    tenders = (
        db.query(EnrichedTender)
        .join(TenderScore, TenderScore.enriched_tender_id == EnrichedTender.id)
        .options(joinedload(EnrichedTender.tender_score))
        .filter(
            TenderScore.p_go >= 0.70,
            EnrichedTender.days_to_deadline >= 2,
        )
        .order_by(TenderScore.p_go.desc())
        .all()
    )

    strong_go = [_build_list_item(t) for t in tenders if t.tender_score and t.tender_score.p_go >= 0.80]
    go        = [_build_list_item(t) for t in tenders if t.tender_score and 0.70 <= t.tender_score.p_go < 0.80]

    return {
        "strong_go": [i.model_dump() for i in strong_go],
        "go":        [i.model_dump() for i in go],
        "total":     len(tenders),
    }


@router.get("", response_model=TenderListResponse)
def list_tenders(
    page:          int = Query(default=1, ge=1),
    per_page:      int = Query(default=25, ge=1, le=100),
    search:        Optional[str]   = Query(default=None),
    portal:        Optional[str]   = Query(default=None),
    sector:        Optional[str]   = Query(default=None),
    country:       Optional[str]   = Query(default=None),
    agency:        Optional[str]   = Query(default=None),
    procurement:   Optional[str]   = Query(default=None),
    recommendation: Optional[str]  = Query(default=None),
    sort_by:       str = Query(default="p_go"),
    db:            Session = Depends(get_db),
    current_user:  PlatformUser = Depends(get_current_user),
):
    query = (
        db.query(EnrichedTender)
        .outerjoin(TenderScore, TenderScore.enriched_tender_id == EnrichedTender.id)
        .options(joinedload(EnrichedTender.tender_score))
        .filter(EnrichedTender.days_to_deadline >= 2)
    )

    if search:
        like = f"%{search}%"
        query = query.filter(or_(
            EnrichedTender.title_clean.ilike(like),
            EnrichedTender.country_name_normalized.ilike(like),
            EnrichedTender.funding_agency.ilike(like),
        ))

    if portal:
        query = query.filter(EnrichedTender.source_portal == portal.lower())

    if sector:
        query = query.filter(EnrichedTender.sector.ilike(f"%{sector}%"))

    if country:
        query = query.filter(EnrichedTender.country_name_normalized.ilike(f"%{country}%"))

    if agency:
        query = query.filter(EnrichedTender.funding_agency.ilike(f"%{agency}%"))

    if procurement:
        query = query.filter(EnrichedTender.procurement_group == procurement)

    if recommendation == "STRONG GO":
        query = query.filter(TenderScore.p_go >= 0.80)
    elif recommendation == "GO":
        query = query.filter(and_(TenderScore.p_go >= 0.70, TenderScore.p_go < 0.80))
    elif recommendation == "scored":
        query = query.filter(TenderScore.p_go.isnot(None))

    if sort_by == "deadline":
        query = query.order_by(EnrichedTender.days_to_deadline.asc())
    elif sort_by == "enriched_at":
        query = query.order_by(EnrichedTender.enriched_at.desc())
    else:
        query = query.order_by(TenderScore.p_go.desc().nullslast())

    total  = query.count()
    offset = (page - 1) * per_page
    items  = query.offset(offset).limit(per_page).all()
    pages  = max(1, (total + per_page - 1) // per_page)

    return TenderListResponse(
        items = [_build_list_item(t) for t in items],
        total = total,
        page  = page,
        pages = pages,
    )


@router.get("/{tender_id}", response_model=TenderDetail)
def get_tender(
    tender_id:    int,
    db:           Session = Depends(get_db),
    current_user: PlatformUser = Depends(get_current_user),
):
    et = (
        db.query(EnrichedTender)
        .options(
            joinedload(EnrichedTender.tender_score)
            .joinedload(TenderScore.decisions)
            .joinedload(PartnerDecision.user),
        )
        .filter(EnrichedTender.id == tender_id)
        .first()
    )

    if not et:
        raise HTTPException(status_code=404, detail="Tender not found")

    ts = et.tender_score

    decisions = []
    if ts and ts.decisions:
        for d in sorted(ts.decisions, key=lambda x: x.decided_at or datetime.min):
            decisions.append(DecisionItem(
                user_full_name = d.user.full_name if d.user else "Unknown",
                decision       = d.decision,
                justification  = d.justification,
                decided_at     = d.decided_at,
            ))

    return TenderDetail(
        id                      = et.id,
        title_clean             = et.title_clean,
        country_name_normalized = et.country_name_normalized,
        is_multi_country        = et.is_multi_country,
        countries_list          = et.countries_list,
        funding_agency          = et.funding_agency,
        organisation_name       = et.organisation_name,
        sector                  = _parse_sector(et.sector),
        procurement_group       = et.procurement_group,
        budget                  = et.budget,
        currency                = et.currency,
        days_to_deadline        = et.days_to_deadline,
        deadline_datetime       = et.deadline_datetime,
        source_portal           = et.source_portal,
        source_url              = et.source_url,
        language                = et.language,
        has_pdf                 = et.has_pdf,
        description_clean       = et.description_clean,
        p_go                    = ts.p_go           if ts else et.p_go,
        recommendation          = ts.recommendation if ts else None,
        justification           = ts.justification  if ts else None,
        score_breakdown         = et.score_breakdown,
        llm_scope_summary            = et.llm_scope_summary,
        llm_project_program          = et.llm_project_program,
        llm_financing_instrument     = et.llm_financing_instrument,
        llm_bid_process_type         = et.llm_bid_process_type,
        llm_contract_duration_months = et.llm_contract_duration_months,
        llm_eligibility_summary      = et.llm_eligibility_summary,
        llm_specific_areas           = et.llm_specific_areas,
        llm_submission_process       = et.llm_submission_process,
        partner_decision      = ts.partner_decision      if ts else None,
        partner_justification = ts.partner_justification if ts else None,
        decided_at            = ts.decided_at            if ts else None,
        decisions             = decisions,
    )


@router.post("/{tender_id}/decide", response_model=DecisionResponse)
def submit_decision(
    tender_id:    int,
    request:      DecisionRequest,
    db:           Session = Depends(get_db),
    current_user: PlatformUser = Depends(get_current_user),
):
    if request.decision not in ("GO", "NO GO"):
        raise HTTPException(status_code=422, detail="Decision must be 'GO' or 'NO GO'")

    et = db.query(EnrichedTender).filter_by(id=tender_id).first()
    if not et:
        raise HTTPException(status_code=404, detail="Tender not found")

    ts = db.query(TenderScore).filter_by(enriched_tender_id=tender_id).first()
    if not ts:
        raise HTTPException(status_code=404, detail="Tender has not been scored yet")

    now = datetime.now(timezone.utc)
    ts.partner_decision      = request.decision
    ts.partner_justification = request.justification
    ts.decided_at            = now

    db.add(PartnerDecision(
        tender_score_id = ts.id,
        user_id         = current_user.id,
        decision        = request.decision,
        justification   = request.justification,
        decided_at      = now,
    ))
    db.commit()

    return DecisionResponse(ok=True, message=f"Decision '{request.decision}' recorded")