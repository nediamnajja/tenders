"""
email_alert.py
==============
KPMG Tender Pipeline — Email Alert
Sends a daily digest of GO and STRONG GO tenders after scoring.

Two sections in the email:
  1. STRONG GO  (p_go >= 0.80) — top priority
  2. GO         (p_go >= 0.70) — worth reviewing

Configuration:
  Fill in SENDER_EMAIL, SENDER_PASSWORD, and RECIPIENTS below.
  Uses Gmail SMTP with App Password (not your real Gmail password).

How to get a Gmail App Password:
  1. Go to myaccount.google.com → Security
  2. Enable 2-Step Verification
  3. Search "App passwords" → generate one for "Mail"
  4. Paste the 16-character password below as SENDER_PASSWORD

Run standalone:
    cd C:\\projects\\tenders
    python alerts\\email_alert.py
"""

import ast
import os
import sys
import smtplib
import logging
from datetime import date, datetime, timezone, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db import get_session
from models import EnrichedTender

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s  %(levelname)s  %(message)s",
    datefmt= "%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
#  CONFIGURATION — fill these in
# ─────────────────────────────────────────────────────────────

SENDER_EMAIL    = "nediamnajja2009@gmail.com"       # <- your Gmail address
SENDER_PASSWORD = "hhlp yuoe vztq vhjg"        # <- Gmail App Password (16 chars)

RECIPIENTS = [
    "nediamnajja.tbs@gmail.com",                  # <- add recipients here
    "recipient2@example.com",
]

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

# Only alert on tenders scored in the last N hours (avoids re-alerting old ones)
ALERT_WINDOW_HOURS = 26   # slightly more than 24h to handle timing drift

# ─────────────────────────────────────────────────────────────
#  FETCH TODAY'S GO TENDERS
#  All data loaded INSIDE the session to avoid DetachedInstanceError
# ─────────────────────────────────────────────────────────────

def fetch_go_tenders() -> tuple[list, list]:
    """
    Fetch tenders scored in the last ALERT_WINDOW_HOURS.
    Returns (strong_go_list, go_list) — each item is a plain dict,
    not a SQLAlchemy object, so no session needed after this call.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=ALERT_WINDOW_HOURS)

    with get_session() as session:
        tenders = (
            session.query(EnrichedTender)
            .filter(
                EnrichedTender.p_go >= 0.70,
                EnrichedTender.enriched_at >= cutoff,
            )
            .order_by(EnrichedTender.p_go.desc())
            .all()
        )

        # Load all attributes INSIDE the session before it closes
        result = []
        for t in tenders:
            result.append({
                "p_go":                    t.p_go,
                "title_clean":             t.title_clean,
                "country_name_normalized": t.country_name_normalized,
                "funding_agency":          t.funding_agency,
                "sector":                  t.sector,
                "budget":                  t.budget,
                "days_to_deadline":        t.days_to_deadline,
                "source_url":              t.source_url,
            })

    strong_go = [t for t in result if t["p_go"] >= 0.80]
    go        = [t for t in result if 0.70 <= t["p_go"] < 0.80]

    logger.info(f"Found {len(strong_go)} STRONG GO and {len(go)} GO tenders")
    return strong_go, go

# ─────────────────────────────────────────────────────────────
#  BUILD EMAIL HTML
# ─────────────────────────────────────────────────────────────

def _tender_row_html(tender: dict, rank: int) -> str:
    """Build one tender card as HTML. tender is a plain dict."""

    # Clean sector display
    sector_raw = tender["sector"] or "N/A"
    try:
        parsed = ast.literal_eval(sector_raw)
        if isinstance(parsed, list):
            sector_str = ", ".join(parsed)
        else:
            sector_str = sector_raw
    except Exception:
        sector_str = sector_raw

    budget       = tender["budget"]
    days         = tender["days_to_deadline"]
    budget_str   = f"{budget:,.0f} EUR" if budget else "N/A"
    deadline_str = f"{int(days)} days"  if days   else "N/A"
    p_pct        = f"{tender['p_go']:.1%}"
    agency       = tender["funding_agency"]          or "N/A"
    country      = tender["country_name_normalized"] or "N/A"
    title        = tender["title_clean"]             or "No title"
    url          = tender["source_url"]              or "#"

    return f"""
    <tr style="border-bottom:1px solid #e5e7eb;">
      <td style="padding:12px 8px;font-weight:600;color:#374151;">#{rank}</td>
      <td style="padding:12px 8px;">
        <a href="{url}" style="color:#1d4ed8;text-decoration:none;font-weight:600;">
          {title[:90]}{"..." if len(title) > 90 else ""}
        </a><br>
        <span style="color:#6b7280;font-size:12px;">{agency} &nbsp;|&nbsp; {country}</span>
      </td>
      <td style="padding:12px 8px;color:#374151;">{sector_str[:50]}</td>
      <td style="padding:12px 8px;color:#374151;">{budget_str}</td>
      <td style="padding:12px 8px;color:#374151;">{deadline_str}</td>
      <td style="padding:12px 8px;font-weight:700;color:#15803d;">{p_pct}</td>
    </tr>
    """


def build_html(strong_go: list, go: list) -> str:
    today_str = date.today().strftime("%B %d, %Y")
    total     = len(strong_go) + len(go)

    strong_rows = "".join(_tender_row_html(t, i+1) for i, t in enumerate(strong_go))
    go_rows     = "".join(_tender_row_html(t, i+1) for i, t in enumerate(go))

    table_header = """
    <tr style="background:#f3f4f6;">
      <th style="padding:10px 8px;text-align:left;color:#374151;">#</th>
      <th style="padding:10px 8px;text-align:left;color:#374151;">Tender</th>
      <th style="padding:10px 8px;text-align:left;color:#374151;">Sector</th>
      <th style="padding:10px 8px;text-align:left;color:#374151;">Budget</th>
      <th style="padding:10px 8px;text-align:left;color:#374151;">Deadline</th>
      <th style="padding:10px 8px;text-align:left;color:#374151;">P(GO)</th>
    </tr>
    """

    no_data_row = '<tr><td colspan="6" style="padding:16px;color:#9ca3af;text-align:center;">None today</td></tr>'

    strong_section = f"""
    <h2 style="color:#15803d;margin-top:32px;">
      STRONG GO &nbsp;<span style="font-size:14px;color:#6b7280;">p_go &ge; 80%</span>
      &nbsp;&nbsp;<span style="background:#dcfce7;color:#15803d;padding:2px 10px;
      border-radius:12px;font-size:13px;">{len(strong_go)} tenders</span>
    </h2>
    <table width="100%" cellpadding="0" cellspacing="0"
           style="border-collapse:collapse;border:1px solid #e5e7eb;border-radius:8px;">
      {table_header}
      {strong_rows or no_data_row}
    </table>
    """

    go_section = f"""
    <h2 style="color:#1d4ed8;margin-top:32px;">
      GO &nbsp;<span style="font-size:14px;color:#6b7280;">p_go 70-79%</span>
      &nbsp;&nbsp;<span style="background:#dbeafe;color:#1d4ed8;padding:2px 10px;
      border-radius:12px;font-size:13px;">{len(go)} tenders</span>
    </h2>
    <table width="100%" cellpadding="0" cellspacing="0"
           style="border-collapse:collapse;border:1px solid #e5e7eb;border-radius:8px;">
      {table_header}
      {go_rows or no_data_row}
    </table>
    """

    no_results = "" if total > 0 else """
    <div style="background:white;border:1px solid #e5e7eb;border-radius:8px;
                padding:32px;text-align:center;color:#6b7280;">
      <p style="font-size:18px;margin:0;">No GO tenders found today.</p>
      <p style="margin:8px 0 0;font-size:14px;">
        The pipeline ran successfully — no tenders met the threshold.
      </p>
    </div>
    """

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="font-family:Arial,sans-serif;max-width:900px;margin:0 auto;
             padding:24px;background:#f9fafb;">

  <div style="background:#1e3a5f;color:white;padding:24px;border-radius:8px;
              margin-bottom:24px;">
    <h1 style="margin:0;font-size:22px;">KPMG Tender Intelligence</h1>
    <p style="margin:8px 0 0;opacity:0.8;font-size:14px;">
      Daily Digest — {today_str}
    </p>
  </div>

  <div style="background:white;border:1px solid #e5e7eb;border-radius:8px;
              padding:16px;margin-bottom:24px;">
    <table width="100%"><tr>
      <td style="text-align:center;">
        <div style="font-size:28px;font-weight:700;color:#15803d;">{len(strong_go)}</div>
        <div style="font-size:12px;color:#6b7280;">STRONG GO</div>
      </td>
      <td style="text-align:center;">
        <div style="font-size:28px;font-weight:700;color:#1d4ed8;">{len(go)}</div>
        <div style="font-size:12px;color:#6b7280;">GO</div>
      </td>
      <td style="text-align:center;">
        <div style="font-size:28px;font-weight:700;color:#374151;">{total}</div>
        <div style="font-size:12px;color:#6b7280;">TOTAL</div>
      </td>
    </tr></table>
  </div>

  {no_results}
  {strong_section}
  {go_section}

  <div style="margin-top:32px;padding-top:16px;border-top:1px solid #e5e7eb;
              color:#9ca3af;font-size:12px;text-align:center;">
    Generated automatically by KPMG Tender Intelligence Pipeline<br>
    {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
  </div>

</body>
</html>"""


def build_plain_text(strong_go: list, go: list) -> str:
    """Plain text fallback. tender is a plain dict."""
    lines = [
        f"KPMG TENDER INTELLIGENCE — {date.today().isoformat()}",
        "=" * 60,
        f"STRONG GO : {len(strong_go)}",
        f"GO        : {len(go)}",
        "=" * 60,
    ]

    if strong_go:
        lines.append("\nSTRONG GO (p_go >= 80%)")
        lines.append("-" * 40)
        for i, t in enumerate(strong_go, 1):
            lines.append(f"{i}. {t['title_clean'] or 'N/A'}")
            lines.append(f"   P(GO): {t['p_go']:.1%}  |  {t['country_name_normalized'] or 'N/A'}  |  {t['funding_agency'] or 'N/A'}")
            lines.append(f"   {t['source_url'] or ''}")

    if go:
        lines.append("\nGO (p_go 70-79%)")
        lines.append("-" * 40)
        for i, t in enumerate(go, 1):
            lines.append(f"{i}. {t['title_clean'] or 'N/A'}")
            lines.append(f"   P(GO): {t['p_go']:.1%}  |  {t['country_name_normalized'] or 'N/A'}  |  {t['funding_agency'] or 'N/A'}")
            lines.append(f"   {t['source_url'] or ''}")

    if not strong_go and not go:
        lines.append("\nNo GO tenders found today.")

    return "\n".join(lines)

# ─────────────────────────────────────────────────────────────
#  SEND EMAIL
# ─────────────────────────────────────────────────────────────

def send_alert(strong_go: list, go: list) -> bool:
    total   = len(strong_go) + len(go)
    subject = (
        f"KPMG Tenders — {total} recommendation{'s' if total != 1 else ''} "
        f"({len(strong_go)} STRONG GO, {len(go)} GO) — {date.today().isoformat()}"
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SENDER_EMAIL
    msg["To"]      = ", ".join(RECIPIENTS)

    plain_part = MIMEText(build_plain_text(strong_go, go), "plain", "utf-8")
    html_part  = MIMEText(build_html(strong_go, go),       "html",  "utf-8")

    msg.attach(plain_part)
    msg.attach(html_part)

    try:
        logger.info(f"Connecting to {SMTP_HOST}:{SMTP_PORT}...")
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, RECIPIENTS, msg.as_string())

        logger.info(f"Email sent to {len(RECIPIENTS)} recipient(s)")
        logger.info(f"Subject: {subject}")
        return True

    except smtplib.SMTPAuthenticationError:
        logger.error("Authentication failed — check SENDER_EMAIL and SENDER_PASSWORD")
        logger.error("Make sure you are using a Gmail App Password, not your real password")
        return False
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False

# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────

def run():
    logger.info("=" * 55)
    logger.info("  KPMG EMAIL ALERT")
    logger.info("=" * 55)

    if "your.email@gmail.com" in SENDER_EMAIL:
        logger.error("SENDER_EMAIL not configured — edit alerts/email_alert.py")
        sys.exit(1)
    if "xxxx" in SENDER_PASSWORD:
        logger.error("SENDER_PASSWORD not configured — edit alerts/email_alert.py")
        sys.exit(1)
    if not RECIPIENTS or "example.com" in RECIPIENTS[0]:
        logger.error("RECIPIENTS not configured — edit alerts/email_alert.py")
        sys.exit(1)

    strong_go, go = fetch_go_tenders()

    logger.info(f"  STRONG GO : {len(strong_go)}")
    logger.info(f"  GO        : {len(go)}")

    if not strong_go and not go:
        logger.info("  No GO tenders today — sending empty digest anyway")

    success = send_alert(strong_go, go)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    run()