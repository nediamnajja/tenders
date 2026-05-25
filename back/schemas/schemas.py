# """
# back/schemas/schemas.py
# """

# from datetime import datetime
# from typing import Optional
# from pydantic import BaseModel


# class LoginRequest(BaseModel):
#     email:    str
#     password: str


# class UserMe(BaseModel):
#     id:        int
#     email:     str
#     full_name: Optional[str] = None
#     role:      str

#     class Config:
#         from_attributes = True


# class TokenResponse(BaseModel):
#     access_token: str
#     token_type:   str = "bearer"
#     user:         UserMe


# class TenderListItem(BaseModel):
#     id:                      int
#     title_clean:             Optional[str] = None
#     country_name_normalized: Optional[str] = None
#     funding_agency:          Optional[str] = None
#     sector:                  Optional[str] = None
#     procurement_group:       Optional[str] = None
#     budget:                  Optional[float] = None
#     currency:                Optional[str] = None
#     days_to_deadline:        Optional[int] = None
#     source_portal:           Optional[str] = None
#     source_url:              Optional[str] = None
#     p_go:                    Optional[float] = None
#     recommendation:          Optional[str] = None
#     partner_decision:        Optional[str] = None
#     enriched_at:             Optional[datetime] = None
#     publication_datetime: Optional[datetime] = None
#     deadline_datetime:       Optional[datetime] = None
#     language:                Optional[str] = None 
#     decisions:               list = []

#     class Config:
#         from_attributes = True


# class TenderListResponse(BaseModel):
#     items: list[TenderListItem]
#     total: int
#     page:  int
#     pages: int


# class DecisionItem(BaseModel):
#     user_full_name: Optional[str] = None
#     user_email:     Optional[str] = None
#     decision:       str
#     justification:  Optional[str] = None
#     decided_at:     Optional[datetime] = None

#     class Config:
#         from_attributes = True


# class TenderDetail(BaseModel):
#     id:                      int
#     title_clean:             Optional[str] = None
#     country_name_normalized: Optional[str] = None
#     is_multi_country:        Optional[bool] = None
#     countries_list:          Optional[str] = None
#     funding_agency:          Optional[str] = None
#     organisation_name:       Optional[str] = None
#     sector:                  Optional[str] = None
#     procurement_group:       Optional[str] = None
#     budget:                  Optional[float] = None
#     currency:                Optional[str] = None
#     days_to_deadline:        Optional[int] = None
#     deadline_datetime:       Optional[datetime] = None
#     source_portal:           Optional[str] = None
#     source_url:              Optional[str] = None
#     language:                Optional[str] = None
#     has_pdf:                 Optional[bool] = None
#     description_clean:       Optional[str] = None
#     p_go:                    Optional[float] = None
#     recommendation:          Optional[str] = None
#     justification:           Optional[str] = None
#     score_breakdown:         Optional[str] = None
#     llm_scope_summary:            Optional[str] = None
#     llm_project_program:          Optional[str] = None
#     llm_financing_instrument:     Optional[str] = None
#     llm_bid_process_type:         Optional[str] = None
#     llm_contract_duration_months: Optional[int] = None
#     llm_eligibility_summary:      Optional[str] = None
#     llm_specific_areas:           Optional[str] = None
#     llm_submission_process:       Optional[str] = None
#     partner_decision:      Optional[str] = None
#     partner_justification: Optional[str] = None
#     decided_at:            Optional[datetime] = None
#     decisions:             list[DecisionItem] = []
#     publication_datetime: Optional[datetime] = None

#     class Config:
#         from_attributes = True


# class DecisionRequest(BaseModel):
#     decision:      str
#     justification: Optional[str] = None


# class DecisionResponse(BaseModel):
#     ok:      bool
#     message: str


# class DashboardStats(BaseModel):
#     total_tenders_today:  int
#     strong_go_today:      int
#     go_today:             int
#     total_active:         int
#     decisions_pending:    int
#     by_sector:            list[dict]
#     by_agency:            list[dict]
#     by_country:           list[dict]
#     by_procurement:       list[dict]
#     score_distribution:   list[dict]
#     daily_trend:          list[dict]

"""
back/schemas/schemas.py
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel


class LoginRequest(BaseModel):
    email:    str
    password: str


class UserMe(BaseModel):
    id:        int
    email:     str
    full_name: Optional[str] = None
    role:      str

    class Config:
        from_attributes = True


class TokenResponse(BaseModel):
    access_token: str
    token_type:   str = "bearer"
    user:         UserMe


class TenderListItem(BaseModel):
    id:                      int
    title_clean:             Optional[str]   = None
    country_name_normalized: Optional[str]   = None
    funding_agency:          Optional[str]   = None
    sector:                  Optional[str]   = None
    procurement_group:       Optional[str]   = None
    budget:                  Optional[float] = None
    currency:                Optional[str]   = None
    days_to_deadline:        Optional[int]   = None
    source_portal:           Optional[str]   = None
    source_url:              Optional[str]   = None
    p_go:                    Optional[float] = None
    recommendation:          Optional[str]   = None
    partner_decision:        Optional[str]   = None
    enriched_at:             Optional[datetime] = None
    publication_datetime:    Optional[datetime] = None
    deadline_datetime:       Optional[datetime] = None
    language:                Optional[str]   = None
    decided_at:              Optional[datetime] = None
    decisions:               list = []

    class Config:
        from_attributes = True


class TenderListResponse(BaseModel):
    items: list[TenderListItem]
    total: int
    page:  int
    pages: int


class DecisionItem(BaseModel):
    user_full_name: Optional[str]  = None
    user_email:     Optional[str]  = None
    decision:       str
    justification:  Optional[str]  = None
    j_values:       Optional[str]  = None  # JSON string — raw per-feature answers
    decided_at:     Optional[datetime] = None

    class Config:
        from_attributes = True


class TenderDetail(BaseModel):
    id:                      int
    title_clean:             Optional[str]   = None
    country_name_normalized: Optional[str]   = None
    is_multi_country:        Optional[bool]  = None
    countries_list:          Optional[str]   = None
    funding_agency:          Optional[str]   = None
    organisation_name:       Optional[str]   = None
    sector:                  Optional[str]   = None
    procurement_group:       Optional[str]   = None
    budget:                  Optional[float] = None
    currency:                Optional[str]   = None
    days_to_deadline:        Optional[int]   = None
    deadline_datetime:       Optional[datetime] = None
    publication_datetime:    Optional[datetime] = None
    source_portal:           Optional[str]   = None
    source_url:              Optional[str]   = None
    language:                Optional[str]   = None
    has_pdf:                 Optional[bool]  = None
    description_clean:       Optional[str]   = None
    p_go:                    Optional[float] = None
    recommendation:          Optional[str]   = None
    justification:           Optional[str]   = None
    score_breakdown:         Optional[str]   = None
    llm_scope_summary:            Optional[str] = None
    llm_project_program:          Optional[str] = None
    llm_financing_instrument:     Optional[str] = None
    llm_bid_process_type:         Optional[str] = None
    llm_contract_duration_months: Optional[int] = None
    llm_eligibility_summary:      Optional[str] = None
    llm_specific_areas:           Optional[str] = None
    llm_submission_process:       Optional[str] = None
    partner_decision:      Optional[str]      = None
    partner_justification: Optional[str]      = None
    decided_at:            Optional[datetime] = None
    decisions:             list[DecisionItem] = []

    class Config:
        from_attributes = True


class DecisionRequest(BaseModel):
    decision:      str                        # "GO" or "NO GO"
    justification: Optional[str]  = None      # plain text — shown to other users
    j_values:      dict[str, float] = {}      # {"tier_1": 1.0, "proc_CONSULTING": 0.1, ...}


class DecisionResponse(BaseModel):
    ok:      bool
    message: str


class DashboardStats(BaseModel):
    total_tenders_today:  int
    strong_go_today:      int
    go_today:             int
    total_active:         int
    decisions_pending:    int
    by_sector:            list[dict]
    by_agency:            list[dict]
    by_country:           list[dict]
    by_procurement:       list[dict]
    score_distribution:   list[dict]
    daily_trend:          list[dict]