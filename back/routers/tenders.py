"""
back/routers/tenders.py
"""

import ast
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import and_, distinct, func, or_
from sqlalchemy.orm import Session, joinedload, selectinload

from back.database import get_db
from back.models.db_models import (
    EnrichedTender, PartnerDecision,
    PlatformUser, SavedTender, TenderScore,
)
from back.routers.auth import get_current_user
from back.routers.sgd import run_sgd_update
from back.schemas.schemas import (
    DecisionItem, DecisionRequest, DecisionResponse,
    TenderDetail, TenderListItem, TenderListResponse,
)

router = APIRouter(prefix="/tenders", tags=["tenders"])
logger = logging.getLogger(__name__)


# =============================================================================
# HELPERS
# =============================================================================

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

    # ── Build decisions list — needed for Decisions.jsx ──────────────────────
    decisions = []
    if ts and ts.decisions:
        for d in sorted(ts.decisions, key=lambda x: x.decided_at or datetime.min):
            decisions.append(DecisionItem(
                user_full_name = d.user.full_name if d.user else "Unknown",
                decision       = d.decision,
                justification  = d.justification,
                j_values       = d.j_values,
                decided_at     = d.decided_at,
            ))

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
        publication_datetime    = et.publication_datetime,
        deadline_datetime       = et.deadline_datetime,
        language                = et.language,
        decided_at = ts.decided_at if ts else None,
        decisions               = decisions,
    )


# =============================================================================
# FILTER OPTIONS
# =============================================================================

@router.get("/filters")
def get_filter_options(
    db:           Session     = Depends(get_db),
    current_user: PlatformUser = Depends(get_current_user),
):
    countries = [
        r[0] for r in
        db.query(distinct(EnrichedTender.country_name_normalized))
        .filter(EnrichedTender.country_name_normalized.isnot(None))
        .order_by(EnrichedTender.country_name_normalized)
        .all()
    ]

    agencies = [
        r[0] for r in
        db.query(distinct(EnrichedTender.funding_agency))
        .filter(EnrichedTender.funding_agency.isnot(None))
        .order_by(EnrichedTender.funding_agency)
        .all()
    ]

    raw_sectors = [
        r[0] for r in
        db.query(EnrichedTender.sector)
        .filter(EnrichedTender.sector.isnot(None))
        .all()
    ]
    sector_set = set()
    for raw in raw_sectors:
        try:
            parsed = ast.literal_eval(raw)
            if isinstance(parsed, list):
                sector_set.update(parsed)
            else:
                sector_set.add(str(raw))
        except Exception:
            sector_set.add(str(raw))
    sectors = sorted(sector_set)

    budget_range = db.query(
        func.min(EnrichedTender.budget),
        func.max(EnrichedTender.budget),
    ).filter(EnrichedTender.budget.isnot(None)).first()

    return {
        "countries":  countries,
        "agencies":   agencies,
        "sectors":    sectors,
        "budget_min": budget_range[0] or 0,
        "budget_max": budget_range[1] or 100_000_000,
    }


# =============================================================================
# TODAY RECOMMENDATIONS
# =============================================================================

@router.get("/today")
def get_today_recommendations(
    db:           Session     = Depends(get_db),
    current_user: PlatformUser = Depends(get_current_user),
):
    now = datetime.now(timezone.utc)
    tenders = (
        db.query(EnrichedTender)
        .join(TenderScore, TenderScore.enriched_tender_id == EnrichedTender.id)
        .options(
            joinedload(EnrichedTender.tender_score)
            .joinedload(TenderScore.decisions)
            .joinedload(PartnerDecision.user)
        )
        .filter(
            TenderScore.p_go >= 0.70,
            EnrichedTender.deadline_datetime >= now,
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


# =============================================================================
# SAVED TENDERS
# =============================================================================

@router.get("/saved")
def get_saved_tenders(
    db:           Session     = Depends(get_db),
    current_user: PlatformUser = Depends(get_current_user),
):
    rows = (
        db.query(SavedTender.tender_id)
        .filter(SavedTender.user_id == current_user.id)
        .all()
    )
    return {"saved_ids": [r[0] for r in rows]}


@router.post("/{tender_id}/save")
def save_tender(
    tender_id:    int,
    db:           Session     = Depends(get_db),
    current_user: PlatformUser = Depends(get_current_user),
):
    et = db.query(EnrichedTender).filter_by(id=tender_id).first()
    if not et:
        raise HTTPException(status_code=404, detail="Tender not found")

    existing = db.query(SavedTender).filter_by(
        user_id=current_user.id, tender_id=tender_id
    ).first()

    if not existing:
        db.add(SavedTender(user_id=current_user.id, tender_id=tender_id))
        db.commit()

    return {"ok": True, "saved": True}


@router.delete("/{tender_id}/save")
def unsave_tender(
    tender_id:    int,
    db:           Session     = Depends(get_db),
    current_user: PlatformUser = Depends(get_current_user),
):
    row = db.query(SavedTender).filter_by(
        user_id=current_user.id, tender_id=tender_id
    ).first()

    if row:
        db.delete(row)
        db.commit()

    return {"ok": True, "saved": False}


# =============================================================================
# LIST TENDERS
# =============================================================================

@router.get("", response_model=TenderListResponse)
def list_tenders(
    page:         int           = Query(default=1,    ge=1),
    per_page:     int           = Query(default=25,   ge=1, le=100),
    search:       Optional[str] = Query(default=None),
    portal:       Optional[str] = Query(default=None),
    sector:       Optional[str] = Query(default=None),
    country:      Optional[str] = Query(default=None),
    agency:       Optional[str] = Query(default=None),
    procurement:  Optional[str] = Query(default=None),
    language:     Optional[str] = Query(default=None),
    posted_from:  Optional[str] = Query(default=None),
    posted_till:  Optional[str] = Query(default=None),
    budget_min:   Optional[float] = Query(default=None),
    budget_max:   Optional[float] = Query(default=None),
    status:       Optional[str] = Query(default="open"),
    decided:      Optional[bool] = Query(default=None),
    sort_by:      str           = Query(default="publication_datetime"),
    db:           Session       = Depends(get_db),
    current_user: PlatformUser  = Depends(get_current_user),
):
    now = datetime.now(timezone.utc)

    query = (
        db.query(EnrichedTender)
        .outerjoin(TenderScore, TenderScore.enriched_tender_id == EnrichedTender.id)
        .options(
            joinedload(EnrichedTender.tender_score)
            .selectinload(TenderScore.decisions)
            .joinedload(PartnerDecision.user)
        )
    )

    if status == "open":
        query = query.filter(EnrichedTender.deadline_datetime >= now)
    elif status == "closed":
        query = query.filter(EnrichedTender.deadline_datetime < now)

    if search:
        like = f"%{search}%"
        query = query.filter(or_(
            EnrichedTender.title_clean.ilike(like),
            EnrichedTender.country_name_normalized.ilike(like),
            EnrichedTender.funding_agency.ilike(like),
        ))

    if portal:
        portals = [p.strip().lower() for p in portal.split(",")]
        query = query.filter(EnrichedTender.source_portal.in_(portals))

    if sector:
        sectors = [s.strip() for s in sector.split(",")]
        query = query.filter(or_(*[EnrichedTender.sector.ilike(f"%{s}%") for s in sectors]))

    if country:
        countries = [c.strip() for c in country.split(",")]
        query = query.filter(EnrichedTender.country_name_normalized.in_(countries))

    if agency:
        agencies = [a.strip() for a in agency.split(",")]
        query = query.filter(EnrichedTender.funding_agency.in_(agencies))

    if procurement:
        procs = [p.strip() for p in procurement.split(",")]
        query = query.filter(EnrichedTender.procurement_group.in_(procs))

    LANG_MAP = {
        "en": "English", "fr": "French", "pt": "Portuguese",
        "es": "Spanish", "ar": "Arabic",
    }
    KNOWN_LANGUAGES = list(LANG_MAP.values())

    if language:
        if language == "other":
            query = query.filter(
                and_(
                    EnrichedTender.language.isnot(None),
                    EnrichedTender.language.notin_(KNOWN_LANGUAGES),
                )
            )
        else:
            langs = [LANG_MAP.get(l.strip(), l.strip()) for l in language.split(",")]
            query = query.filter(EnrichedTender.language.in_(langs))

    if decided is True:
        query = query.filter(TenderScore.partner_decision.isnot(None))
    elif decided is False:
        query = query.filter(TenderScore.partner_decision.is_(None))

    if posted_from:
        try:
            query = query.filter(
                EnrichedTender.publication_datetime >= datetime.fromisoformat(posted_from)
            )
        except ValueError:
            pass

    if posted_till:
        try:
            query = query.filter(
                EnrichedTender.publication_datetime <= datetime.fromisoformat(posted_till)
            )
        except ValueError:
            pass

    if budget_min is not None:
        query = query.filter(EnrichedTender.budget >= budget_min)
    if budget_max is not None:
        query = query.filter(EnrichedTender.budget <= budget_max)

    if sort_by == "deadline":
        query = query.order_by(EnrichedTender.deadline_datetime.asc().nullslast())
    elif sort_by == "publication_datetime":
        query = query.order_by(EnrichedTender.publication_datetime.desc().nullslast())
    elif sort_by == "decided_at":
        query = query.order_by(TenderScore.decided_at.desc().nullslast())
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


# =============================================================================
# TENDER DETAIL
# =============================================================================

@router.get("/{tender_id}", response_model=TenderDetail)
def get_tender(
    tender_id:    int,
    db:           Session     = Depends(get_db),
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
                j_values       = d.j_values,
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
        publication_datetime    = et.publication_datetime,
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


# =============================================================================
# SUBMIT DECISION
# =============================================================================

@router.post("/{tender_id}/decide", response_model=DecisionResponse)
def submit_decision(
    tender_id:    int,
    request:      DecisionRequest,
    db:           Session     = Depends(get_db),
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
    y   = 1 if request.decision == "GO" else 0

    ts.partner_decision      = request.decision
    ts.partner_justification = request.justification
    ts.decided_at            = now

    existing_decision = (
        db.query(PartnerDecision)
        .filter_by(tender_score_id=ts.id, user_id=current_user.id)
        .first()
    )

    if existing_decision:
        existing_decision.decision      = request.decision
        existing_decision.justification = request.justification
        existing_decision.j_values      = json.dumps(request.j_values) if request.j_values else None
        existing_decision.decided_at    = now
    else:
        db.add(PartnerDecision(
            tender_score_id = ts.id,
            user_id         = current_user.id,
            decision        = request.decision,
            justification   = request.justification,
            j_values        = json.dumps(request.j_values) if request.j_values else None,
            decided_at      = now,
        ))

    if request.j_values:
        try:
            new_version = run_sgd_update(
                db           = db,
                tender_score = ts,
                partner_name = current_user.full_name or current_user.email,
                j_values     = request.j_values,
                y            = y,
            )
            db.commit()
            logger.info(
                f"Decision saved + SGD complete | "
                f"tender={tender_id} decision={request.decision} "
                f"new_weights_version={new_version}"
            )
            return DecisionResponse(
                ok      = True,
                message = f"Decision '{request.decision}' recorded. Model updated to v{new_version}.",
            )
        except Exception as e:
            db.commit()
            logger.error(f"SGD failed for tender {tender_id}: {e}")
            return DecisionResponse(
                ok      = True,
                message = f"Decision '{request.decision}' recorded. Model update failed: {str(e)}",
            )
    else:
        db.commit()
        return DecisionResponse(
            ok      = True,
            message = f"Decision '{request.decision}' recorded.",
        )


# =============================================================================
# DROP DECISION
# =============================================================================

@router.delete("/{tender_id}/decide", response_model=DecisionResponse)
def drop_decision(
    tender_id:    int,
    db:           Session     = Depends(get_db),
    current_user: PlatformUser = Depends(get_current_user),
):
    ts = db.query(TenderScore).filter_by(enriched_tender_id=tender_id).first()
    if not ts:
        raise HTTPException(status_code=404, detail="Tender not found")

    decision_row = (
        db.query(PartnerDecision)
        .filter_by(tender_score_id=ts.id, user_id=current_user.id)
        .order_by(PartnerDecision.decided_at.desc())
        .first()
    )
    if not decision_row:
        raise HTTPException(status_code=404, detail="No decision found for this user")

    db.delete(decision_row)

    remaining = (
        db.query(PartnerDecision)
        .filter(
            PartnerDecision.tender_score_id == ts.id,
            PartnerDecision.id != decision_row.id,
        )
        .order_by(PartnerDecision.decided_at.desc())
        .first()
    )

    if remaining:
        ts.partner_decision      = remaining.decision
        ts.partner_justification = remaining.justification
        ts.decided_at            = remaining.decided_at
    else:
        ts.partner_decision      = None
        ts.partner_justification = None
        ts.decided_at            = None

    db.commit()
    logger.info(f"Decision dropped | tender={tender_id} user={current_user.email}")
    return DecisionResponse(ok=True, message="Decision dropped successfully.")