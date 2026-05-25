
import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import api from '../lib/api'
import { Spinner } from '../components/ui'
import IntelStrip from '../components/layout/IntelStrip'
import PlatformFooter from '../components/layout/PlatformFooter'
import { TrendingUp, Users, ThumbsUp, ThumbsDown } from 'lucide-react'

const C = {
  blue:       '#00338D', accent:    '#0091DA', navy:      '#0D1F6B',
  text:       '#111827', textMid:   '#374151', textMuted: '#6B7280', textFaint: '#9CA3AF',
  border:     '#E5E7EB', divider:   '#F3F4F6', pageBg:    '#F8FAFC', surface:   '#FFFFFF',
  green:      '#15803D', greenFill: '#F0FDF4', greenBord: '#BBF7D0',
  red:        '#B91C1C', redFill:   '#FFF1F2', redBord:   '#FECDD3',
  amber:      '#92400E', amberFill: '#FEF3C7', amberBord: '#FCD34D',
  blueTint:   '#EBF2FB', blueBorder:'#C3D9F2',
}
const F    = "'Inter', system-ui, -apple-system, sans-serif"
const MONO = "'DM Mono', 'JetBrains Mono', ui-monospace, monospace"

const COL_LINE = '1px solid rgba(0,0,0,0.05)'

function fmtDate(dt) {
  if (!dt) return '—'
  try { return new Date(dt.replace(' ', 'T')).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' }) }
  catch { return '—' }
}

function scoreMeta(p) {
  if (p == null) return { color: C.textFaint, label: '—' }
  if (p >= 0.85) return { color: C.blue,    label: 'STRONG' }
  if (p >= 0.70) return { color: C.blue,    label: 'GO'     }
  if (p >= 0.50) return { color: '#B45309', label: 'REVIEW' }
  return               { color: C.red,      label: 'LOW'    }
}

function acronym(name) {
  if (!name) return '?'
  const w = name.trim().split(/\s+/)
  return w.length === 1 ? name.slice(0, 3).toUpperCase() : w.slice(0, 3).map(x => x[0]).join('').toUpperCase()
}

// ── SectionCard ───────────────────────────────────────────────────────────────
function SectionCard({ label, meta, action, children, noPad }) {
  return (
    <div style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 4, overflow: 'visible', boxShadow: '0 1px 3px rgba(0,0,0,.05)' }}>
      <div style={{ background: C.pageBg, borderBottom: `1px solid ${C.border}`, padding: '9px 24px', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <span style={{ fontSize: 11, fontWeight: 600, color: C.blue, letterSpacing: '0.05em', textTransform: 'uppercase', fontFamily: F }}>{label}</span>
          {meta && (
            <>
              <span style={{ width: 1, height: 10, background: C.border, flexShrink: 0 }} />
              <span style={{ fontSize: 11, color: C.textFaint, fontFamily: F }}>{meta}</span>
            </>
          )}
        </div>
        {action && <ActionLink label={action.label} onClick={action.onClick} />}
      </div>
      <div style={{ padding: noPad ? 0 : '24px' }}>{children}</div>
    </div>
  )
}

function ActionLink({ label, onClick }) {
  const [hov, setHov] = useState(false)
  return (
    <button onClick={onClick} onMouseEnter={() => setHov(true)} onMouseLeave={() => setHov(false)}
      style={{ fontSize: 11, fontWeight: 500, color: hov ? C.blue : C.textMuted, background: 'none', border: 'none', cursor: 'pointer', fontFamily: F, flexShrink: 0, transition: 'color .12s' }}>
      {label}
    </button>
  )
}

// ── KpiCard ───────────────────────────────────────────────────────────────────
function KpiCard({ icon: Icon, label, value, sub, color }) {
  const [hov, setHov] = useState(false)
  return (
    <div onMouseEnter={() => setHov(true)} onMouseLeave={() => setHov(false)}
      style={{ background: hov ? C.pageBg : C.surface, border: `1px solid ${hov ? C.blueBorder : C.border}`, borderLeft: `3px solid ${color || C.blue}`, borderRadius: 4, padding: '10px 12px 10px 10px', transition: 'background 0.13s, border-color 0.13s', boxShadow: hov ? '0 2px 8px rgba(0,51,141,.07)' : '0 1px 3px rgba(0,0,0,.04)', fontFamily: F }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6 }}>
        <span style={{ fontSize: 9, fontWeight: 600, color: C.textFaint, textTransform: 'uppercase', letterSpacing: '0.07em' }}>{label}</span>
        {Icon && <Icon style={{ width: 10, height: 10, color: C.textFaint }} />}
      </div>
      <div style={{ fontSize: 18, fontWeight: 700, color: C.text, lineHeight: 1, fontFamily: MONO, letterSpacing: '-0.03em' }}>{value}</div>
      {sub && <div style={{ fontSize: 10, color: C.textFaint, marginTop: 3 }}>{sub}</div>}
    </div>
  )
}

// ── DecisionRow ───────────────────────────────────────────────────────────────
function DecisionRow({ tender, isLast }) {
  const navigate   = useNavigate()
  const [hov, setHov] = useState(false)
  const pct        = tender.p_go != null ? Math.round(tender.p_go * 100) : null
  const decisions  = tender.decisions || []
  const latestDate = decisions
    .map(d => d.decided_at).filter(Boolean)
    .sort((a, b) => new Date(b) - new Date(a))[0] || tender.decided_at
  const isGo       = tender.partner_decision === 'GO'

  const cell = (extra = {}) => ({
    padding:       '14px 16px',
    borderBottom:  isLast ? 'none' : `1px solid ${C.divider}`,
    borderRight:   COL_LINE,
    verticalAlign: 'middle',
    fontSize:      12,
    fontFamily:    F,
    color:         C.textMid,
    ...extra,
  })

  return (
    <tr onClick={() => navigate(`/tenders/${tender.id}`)}
      onMouseEnter={() => setHov(true)} onMouseLeave={() => setHov(false)}
      style={{ background: hov ? C.pageBg : C.surface, cursor: 'pointer', transition: 'background .12s' }}>

      {/* Opportunity */}
      <td style={cell()}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <span style={{ width: 7, height: 7, borderRadius: '50%', flexShrink: 0, background: tender.partner_decision ? (isGo ? C.green : C.red) : C.textFaint }} />
          <div>
            <div style={{ fontSize: 13, fontWeight: 500, color: C.text, fontFamily: F, marginBottom: 2 }}>
              {(tender.title_clean || 'Untitled').slice(0, 55)}{(tender.title_clean || '').length > 55 ? '…' : ''}
            </div>
            <div style={{ fontSize: 11, color: C.textFaint, fontFamily: F }}>
              {[tender.funding_agency, tender.country_name_normalized].filter(Boolean).join(' · ')}
            </div>
          </div>
        </div>
      </td>

      {/* Funding Agency */}
      <td style={cell({ textAlign: 'center' })}>
        {tender.funding_agency || '—'}
      </td>

      {/* Score */}
      <td style={cell({ textAlign: 'center' })}>
        <span style={{ fontSize: 13, fontWeight: 700, color: C.blue, fontFamily: MONO }}>
          {pct != null ? `${pct}%` : '—'}
        </span>
      </td>

      {/* Partner Decision */}
      <td style={cell({ textAlign: 'center' })}>
        {tender.partner_decision ? (
          <span style={{ fontSize: 12, fontWeight: 600, color: isGo ? C.green : C.red, fontFamily: F }}>
            {isGo ? '✓ Endorsed' : '✗ Declined'}
          </span>
        ) : (
          <span style={{ color: C.textFaint }}>—</span>
        )}
      </td>

      {/* Reviewed By — full name */}
      <td style={cell({ textAlign: 'center' })}>
        {decisions.length > 0 ? (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 2, alignItems: 'center' }}>
            {decisions.map((d, i) => (
              <span key={i} style={{ color: C.textMid }}>{d.user_full_name || '—'}</span>
            ))}
          </div>
        ) : <span style={{ color: C.textFaint }}>—</span>}
      </td>

      {/* Justification */}
      <td style={cell({ maxWidth: 200 })}>
        <span style={{
          color: C.textMuted, lineHeight: 1.5,
          display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical', overflow: 'hidden',
        }}>
          {decisions.find(d => d.justification)?.justification || '—'}
        </span>
      </td>

      {/* Date */}
      <td style={cell({ textAlign: 'center', whiteSpace: 'nowrap', borderRight: 'none', color: C.textFaint })}>
        {fmtDate(latestDate)}
      </td>
    </tr>
  )
}

// ── PendingRow ────────────────────────────────────────────────────────────────
function PendingRow({ tender, isLast }) {
  const navigate = useNavigate()
  const [hov, setHov] = useState(false)
  const pct  = tender.p_go != null ? Math.round(tender.p_go * 100) : null
  const meta = scoreMeta(tender.p_go)

  return (
    <div
      onClick={() => navigate(`/tenders/${tender.id}`)}
      onMouseEnter={() => setHov(true)}
      onMouseLeave={() => setHov(false)}
      style={{ display: 'flex', alignItems: 'center', gap: 14, padding: '13px 16px', borderBottom: isLast ? 'none' : `1px solid ${C.divider}`, background: hov ? C.pageBg : C.surface, cursor: 'pointer', transition: 'background .12s', fontFamily: F }}
    >
      <span style={{ fontSize: 10, fontWeight: 700, color: C.blue, background: C.blueTint, border: `1px solid ${C.blueBorder}`, padding: '2px 7px', borderRadius: 3, fontFamily: MONO, flexShrink: 0, width: 44, textAlign: 'center' }}>
        {acronym(tender.funding_agency)}
      </span>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontSize: 13, fontWeight: 500, color: C.text, lineHeight: 1.35, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', marginBottom: 3 }}>
          {tender.title_clean || 'Untitled'}
        </div>
        <div style={{ fontSize: 11, color: C.textFaint }}>
          {[tender.funding_agency, tender.country_name_normalized].filter(Boolean).join(' · ')}
          {tender.days_to_deadline != null && (
            <span style={{ color: tender.days_to_deadline <= 7 ? C.red : C.textFaint }}> · {tender.days_to_deadline}d remaining</span>
          )}
        </div>
      </div>
      {pct != null && (
        <span style={{ fontSize: 13, fontWeight: 700, color: meta.color, fontFamily: MONO, flexShrink: 0 }}>
          {pct}%<span style={{ fontSize: 10, fontWeight: 600, marginLeft: 4, textTransform: 'uppercase', letterSpacing: '.05em', fontFamily: F }}>{meta.label}</span>
        </span>
      )}
      <span style={{ fontSize: 13, fontWeight: 500, color: hov ? C.blue : C.textFaint, flexShrink: 0, transition: 'color .12s' }}>
        Review →
      </span>
    </div>
  )
}

// ── Main ──────────────────────────────────────────────────────────────────────
export default function Decisions() {
  const navigate = useNavigate()
  const [loading, setLoading] = useState(true)
  const [decided, setDecided] = useState([])
  const [pending, setPending] = useState([])
  const [error, setError] = useState(null)

  useEffect(() => {
    const urlParams = new URLSearchParams(window.location.search)
    const debugMode = urlParams.get('debug') === '1'
    let cancelled = false

    async function loadDecisions() {
      try {
        const scoredRes = await api.get('/tenders', { params: { status: 'open', decided: false, per_page: 100, sort_by: 'p_go' } })
        const scored = scoredRes.data.items || []

        const decidedRes = await api.get('/tenders', { params: { status: 'all', decided: true, per_page: 100, sort_by: 'decided_at', page: 1 } })
        let decidedItems = decidedRes.data.items || []
        const totalPages = decidedRes.data.pages || 1

        for (let page = 2; page <= totalPages; page += 1) {
          const pageRes = await api.get('/tenders', { params: { status: 'all', decided: true, per_page: 100, sort_by: 'decided_at', page } })
          decidedItems = decidedItems.concat(pageRes.data.items || [])
        }

        if (cancelled) return

        const pendingItems = scored.filter(t => !t.partner_decision && t.p_go != null && (debugMode ? true : t.p_go >= 0.70))

        setDecided(decidedItems)
        setPending(pendingItems)
      } catch (err) {
        console.error(err)
        if (!cancelled) setError(err.response?.data?.detail || err.message || 'Failed to load decisions')
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    loadDecisions()
    return () => { cancelled = true }
  }, [])

  const goCount   = decided.filter(t => t.partner_decision === 'GO').length
  const nogoCount = decided.filter(t => t.partner_decision === 'NO GO').length
  const scored    = decided.filter(t => t.p_go != null)
  const avgScore  = scored.length ? Math.round(scored.reduce((s, t) => s + t.p_go, 0) / scored.length * 100) : null

  if (loading) return (
    <div style={{ display: 'flex', justifyContent: 'center', padding: '80px 0' }}>
      <Spinner size="lg" />
    </div>
  )

  if (error) return (
    <div style={{ display: 'flex', justifyContent: 'center', padding: '80px 0', color: '#B91C1C' }}>
      <div style={{ maxWidth: 520, textAlign: 'center', fontSize: 14, lineHeight: 1.6 }}>
        <div style={{ fontWeight: 700, marginBottom: 8 }}>Unable to load decisions</div>
        <div>{error}</div>
      </div>
    </div>
  )

  const TABLE_HEADERS = ['Opportunity', 'Funding Agency', 'Score', 'Partner Decision', 'Reviewed By', 'Justification', 'Date']

  return (
    <div style={{ display: 'flex', flexDirection: 'column', minHeight: '100%', background: C.surface, fontFamily: F }}>
      <IntelStrip />
      <div style={{ flex: 1, padding: '36px 48px 56px', maxWidth: 1100, width: '100%', margin: '0 auto' }}>

        {/* Page header */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr auto', gap: 40, alignItems: 'start', marginBottom: 32 }}>
          <div>
            <div style={{ fontSize: 11, fontWeight: 700, color: C.navy, letterSpacing: '0.10em', textTransform: 'uppercase', marginBottom: 8 }}>
              Strategic Workspace
            </div>
            <h1 style={{ fontSize: 28, fontWeight: 700, color: C.text, margin: '0 0 12px', letterSpacing: '-0.02em' }}>
              Team Decisions
            </h1>
            <p style={{ fontSize: 13, color: C.textMuted, margin: 0, lineHeight: 1.75, borderLeft: `3px solid ${C.navy}`, paddingLeft: 14, maxWidth: 480 }}>
              Track all partner decisions, justifications, and approval statuses across the active pipeline.
              Endorsed opportunities move to bid preparation — conflicting decisions are flagged for Practice Lead review.
            </p>
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10, minWidth: 380 }}>
            <KpiCard icon={Users}      label="Total Reviewed" value={decided.length}  sub="Partner decisions"   color={C.blue} />
            <KpiCard icon={ThumbsUp}   label="Endorsed"       value={goCount}         sub="Approved to pursue"  color={C.blue} />
            <KpiCard icon={ThumbsDown} label="Declined"       value={nogoCount}       sub="Not pursued"         color={C.blue} />
            <KpiCard icon={TrendingUp} label="Avg. Score"     value={avgScore != null ? `${avgScore}%` : '—'}   sub="Across reviewed" color={C.blue} />
          </div>
        </div>

        {/* Collaboration note */}
        <div style={{ marginBottom: 24 }}>
          <SectionCard label="Collaboration Workflow">
            <p style={{ fontSize: 12, color: C.textMuted, lineHeight: 1.65, margin: 0 }}>
              Decisions submitted on the Opportunity Detail page are aggregated here for team oversight. The Pursuit Lead reviews all GO decisions before bid preparation begins.
              Conflicting decisions (e.g., one GO and one NO GO from different partners) are flagged for senior partner arbitration within 24 hours.
            </p>
          </SectionCard>
        </div>

        {/* Reviewed Opportunities */}
        <div style={{ marginBottom: 24 }}>
          <SectionCard
            label="Reviewed Opportunities"
            meta={decided.length === 0 ? 'No decisions recorded yet' : `${decided.length} decision${decided.length !== 1 ? 's' : ''} recorded`}
            noPad
          >
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontFamily: F }}>
                <thead>
                  <tr style={{ background: C.pageBg }}>
                    {TABLE_HEADERS.map((h, i) => (
                      <th key={h} style={{
                        padding: '10px 16px',
                        fontSize: 10, fontWeight: 700,
                        textTransform: 'uppercase', letterSpacing: '.08em',
                        color: C.textFaint,
                        textAlign: i === 0 ? 'left' : 'center',
                        borderBottom: `1px solid ${C.border}`,
                        borderRight: i < TABLE_HEADERS.length - 1 ? COL_LINE : 'none',
                        whiteSpace: 'nowrap',
                      }}>
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {decided.length > 0 ? (
                    decided.map((t, i) => (
                      <DecisionRow key={t.id} tender={t} isLast={i === decided.length - 1} />
                    ))
                  ) : (
                    <tr>
                      <td colSpan={TABLE_HEADERS.length} style={{ padding: '48px 16px', textAlign: 'center' }}>
                        <div style={{ fontSize: 13, color: C.textFaint, fontStyle: 'italic', fontFamily: F }}>
                          No decisions have been recorded yet.
                        </div>
                        <div style={{ fontSize: 12, color: C.textFaint, marginTop: 6, fontFamily: F }}>
                          Open an opportunity and submit a GO / NO GO decision — it will appear here.
                        </div>
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </SectionCard>
        </div>

        {/* Pending Review */}
       {pending.length > 0 && (
         <div style={{ marginBottom: 24, border: `1px solid ${C.border}`, borderRadius: 4, boxShadow: '0 1px 3px rgba(0,0,0,.05)' }}>
           <div style={{ background: C.pageBg, borderBottom: `1px solid ${C.border}`, padding: '9px 24px', display: 'flex', alignItems: 'center', gap: 12 }}>
           <span style={{ fontSize: 11, fontWeight: 600, color: C.blue, letterSpacing: '0.05em', textTransform: 'uppercase', fontFamily: F }}>Pending Review</span>
           <span style={{ width: 1, height: 10, background: C.border }} />
           <span style={{ fontSize: 11, color: C.textFaint, fontFamily: F }}>{pending.length} awaiting decision</span>
        </div>
           <div style={{ height: 300, overflowY: 'scroll' }}>
             {pending.map((t, i) => (
              <PendingRow key={t.id} tender={t} isLast={i === pending.length - 1} />
              ))}
            </div>
        </div>
        )}
        {/* Governance note */}
        <SectionCard label="GO / NO GO Governance">
          <p style={{ fontSize: 12, color: C.textMuted, lineHeight: 1.65, margin: 0 }}>
            All GO decisions with a score ≥ 80% require at minimum one Partner sign-off before proposal resources can be allocated.
            Conflicting decisions are escalated to the Practice Lead within 24 hours.
            Decision records are retained for 24 months for quality review and bid performance analysis.
          </p>
        </SectionCard>

      </div>
      <PlatformFooter />
    </div>
  )
}