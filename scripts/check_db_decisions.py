import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from back.database import SessionLocal, engine
from back.models import db_models
from sqlalchemy import select, func

session = SessionLocal()
try:
    total_scores = session.query(func.count(db_models.TenderScore.id)).scalar()
    decided_scores = session.query(func.count(db_models.TenderScore.id)).filter(db_models.TenderScore.partner_decision.isnot(None)).scalar()
    total_partner_decisions = session.query(func.count(db_models.PartnerDecision.id)).scalar()

    print('total_tender_scores:', total_scores)
    print('tender_scores_with_partner_decision:', decided_scores)
    print('partner_decisions_rows:', total_partner_decisions)

    if decided_scores:
        rows = session.query(db_models.TenderScore).filter(db_models.TenderScore.partner_decision.isnot(None)).limit(5).all()
        print('\nSample TenderScores with partner_decision:')
        for r in rows:
            print(r.id, r.enriched_tender_id, r.partner_decision, r.decided_at)

    if total_partner_decisions:
        rows = session.query(db_models.PartnerDecision).limit(5).all()
        print('\nSample PartnerDecision rows:')
        for r in rows:
            print(r.id, r.tender_score_id, r.user_id, r.decision, (r.decided_at and r.decided_at.isoformat()))

finally:
    session.close()
