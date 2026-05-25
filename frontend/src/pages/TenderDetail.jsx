// src/pages/TenderDetail.jsx
import { useEffect, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  ArrowLeft, ExternalLink, Clock, Building2,
  CheckCircle, AlertCircle, Bookmark, BookmarkPlus,
  MapPin, Zap, Users, Brain,
} from 'lucide-react'
import api from '../lib/api'
import { Spinner } from '../components/ui'

const C = {
  blue:      '#00338D', accent:    '#0091DA', navy:      '#0D1F6B',
  teal:      '#0F766E', green:     '#15803D', red:       '#B91C1C', amber: '#B45309',
  text:      '#111827', textMid:   '#374151', textMuted: '#6B7280', textFaint: '#9CA3AF',
  border:    '#E5E7EB', divider:   '#F3F4F6', pageBg:    '#F8FAFC', surface:   '#FFFFFF',
  blueTint:  '#EBF2FB', blueBorder:'#C3D9F2',
  greenFill: '#F0FDF4', greenBord: '#BBF7D0',
  redFill:   '#FFF1F2', redBord:   '#FECDD3',
}
const F    = "'Inter', system-ui, -apple-system, sans-serif"
const MONO = "'DM Mono', 'JetBrains Mono', ui-monospace, monospace"

const FEATURE_LABELS = {
  tier_1: 'Country: Maghreb / North Africa',
  tier_2: 'Country: Sub-Saharan Africa',
  tier_3: 'Country: Other Africa / Fragile',
  tier_4: 'Country: Europe',
  tier_5: 'Country: Americas',
  tier_6: 'Country: Asia & Pacific',
  budget_large:             'Budget > 500k€',
  budget_medium:            'Budget 100k–500k€',
  budget_small:             'Budget < 100k€',
  deadline_2_20:            'Deadline 2–20 days',
  deadline_20_40:           'Deadline 20–40 days',
  deadline_over_40:         'Deadline > 40 days',
  'proc_CONSULTING':        'Procurement: Consulting',
  'proc_NON-CONSULTING':    'Procurement: Non-Consulting',
  'proc_WORKS':             'Procurement: Works',
  'proc_GOODS':             'Procurement: Goods',
  'proc_Others':            'Procurement: Other',
  'sector_Energy & Utilities':                        'Sector: Energy & Utilities',
  'sector_Risk & Compliance':                         'Sector: Risk & Compliance',
  'sector_Digital Transformation':                    'Sector: Digital Transformation',
  'sector_Financial Services':                        'Sector: Financial Services',
  'sector_Data, AI & Analytics':                      'Sector: Data, AI & Analytics',
  'sector_Construction & Infrastructure':             'Sector: Construction & Infrastructure',
  'sector_Health & Life Sciences':                    'Sector: Health & Life Sciences',
  'sector_Education & Training':                      'Sector: Education & Training',
  'sector_Government Reform & Public Administration': 'Sector: Government Reform',
  'sector_Agriculture & Food Security':               'Sector: Agriculture & Food Security',
  'sector_Environment & Climate':                     'Sector: Environment & Climate',
  'sector_Transport & Logistics':                     'Sector: Transport & Logistics',
  'sector_Water, Sanitation & Waste':                 'Sector: Water & Sanitation',
  'sector_Enterprise IT & Systems Implementation':    'Sector: Enterprise IT',
  'sector_Business Strategy & Performance':           'Sector: Business Strategy',
  'sector_Employment & Skills Development':           'Sector: Employment & Skills',
  'sector_Telecommunications':                        'Sector: Telecommunications',
  'sector_Organizational Reform & HR Management':     'Sector: Org Reform & HR',
  'sector_Cybersecurity & Data Security':             'Sector: Cybersecurity',
  'sector_Justice & Rule of Law':                     'Sector: Justice & Rule of Law',
  'sector_Mining & Natural Resources':                'Sector: Mining & Resources',
  'sector_Marketing & Customer Experience':           'Sector: Marketing & CX',
  'sector_Social Protection & Poverty Reduction':     'Sector: Social Protection',
  sector_Others:                                      'Sector: Other',
  'agency_World Bank':                                     'Agency: World Bank',
  'agency_African Development Bank (AfDB)':                'Agency: AfDB',
  'agency_United Nations Development Programme (UNDP)':    'Agency: UNDP',
  agency_FAO:             'Agency: FAO',
  agency_ILO:             'Agency: ILO',
  agency_UNICEF:          'Agency: UNICEF',
  agency_IOM:             'Agency: IOM',
  agency_UNOPS:           'Agency: UNOPS',
  agency_UNIDO:           'Agency: UNIDO',
  agency_Other_UN_Agency: 'Agency: Other UN',
}

function fmtDate(dt) {
  if (!dt) return null
  try { return new Date(dt.replace(' ', 'T')).toLocaleDateString('en-GB', { day: 'numeric', month: 'long', year: 'numeric' }) }
  catch { return null }
}
function fmtDateShort(dt) {
  if (!dt) return null
  try { return new Date(dt.replace(' ', 'T')).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' }) }
  catch { return null }
}
function fmtBudget(b, cur) {
  if (!b) return null
  if (b >= 1e6) return `$${(b / 1e6).toFixed(1)}M ${cur || ''}`.trim()
  if (b >= 1e3) return `$${(b / 1e3).toFixed(0)}K ${cur || ''}`.trim()
  return `$${b.toLocaleString()}`
}
function scoreMeta(p) {
  if (p == null) return { label: '—' }
  if (p >= 0.85) return { label: 'Strong GO' }
  if (p >= 0.70) return { label: 'GO' }
  if (p >= 0.50) return { label: 'Review' }
  return               { label: 'Low' }
}
function acronym(name) {
  if (!name) return '?'
  const w = name.trim().split(/\s+/)
  return w.length === 1 ? name.slice(0, 3).toUpperCase() : w.slice(0, 3).map(x => x[0]).join('').toUpperCase()
}

// ── Agency logo ───────────────────────────────────────────────────────────────
const AGENCY_MAP = {
  'world bank':'worldbank','the world bank':'worldbank','world bank group':'worldbank',
  'african development bank':'afdb','afdb':'afdb','undp':'undp','unicef':'unicef',
  'who':'who','wfp':'wfp','fao':'fao','adb':'adb','ifc':'ifc','ebrd':'ebrd',
}
function agencySlug(a) {
  if (!a) return ''
  return AGENCY_MAP[a.toLowerCase().trim()] || a.toLowerCase().replace(/\s+/g,'-').replace(/[^a-z0-9-]/g,'')
}
function AgencyLogo({ agency, portal, size = 44 }) {
  const [imgOk, setImgOk] = useState(true)
  const slug  = agencySlug(agency) || agencySlug(portal)
  const label = acronym(agency || portal)
  return (
    <div style={{ width:size,height:size,flexShrink:0,borderRadius:6,border:'1.5px solid rgba(255,255,255,.2)',background:'white',display:'flex',alignItems:'center',justifyContent:'center',overflow:'hidden' }}>
      {imgOk && slug
        ? <img src={imgOk==='svg'?`/agencies/${slug}.svg`:`/agencies/${slug}.png`} alt={label}
            style={{ width:size-10,height:size-10,objectFit:'contain' }}
            onError={()=>{ if(imgOk===true) setImgOk('svg'); else setImgOk(false) }} />
        : <span style={{ fontSize:10,fontWeight:700,color:C.blue,fontFamily:MONO }}>{label}</span>}
    </div>
  )
}

// ── Lifecycle Stepper ─────────────────────────────────────────────────────────
const LIFECYCLE = [
  { key:'programming', label:'Programming',  group:'early' },
  { key:'formulation', label:'Formulation',  group:'early' },
  { key:'approval',    label:'Approval',     group:'early' },
  { key:'forecast',    label:'Forecast',     group:'early' },
  { key:'open',        label:'Open',         group:'procurement' },
  { key:'closed',      label:'Closed',       group:'procurement' },
  { key:'shortlisted', label:'Shortlisted',  group:'procurement' },
  { key:'awarded',     label:'Awarded',      group:'procurement' },
]
function Stepper({ status }) {
  const cur    = LIFECYCLE.findIndex(s => s.key === (status || 'open').toLowerCase())
  const active = cur >= 0 ? cur : 4
  return (
    <div style={{ display:'flex',flexDirection:'column',gap:0,fontFamily:F }}>
      {LIFECYCLE.map((s, i) => {
        const done    = i < active
        const current = i === active
        const last    = i === LIFECYCLE.length - 1
        const groupChange = i > 0 && LIFECYCLE[i].group !== LIFECYCLE[i-1].group
        return (
          <div key={s.key}>
            {groupChange && (
              <div style={{ display:'flex',alignItems:'center',gap:8,margin:'6px 0 5px',paddingLeft:20 }}>
                <div style={{ flex:1,height:1,background:C.divider }} />
                <span style={{ fontSize:9,fontWeight:600,color:C.textFaint,textTransform:'uppercase',letterSpacing:'0.08em' }}>Procurement</span>
                <div style={{ flex:1,height:1,background:C.divider }} />
              </div>
            )}
            {i === 0 && (
              <div style={{ display:'flex',alignItems:'center',gap:8,marginBottom:5,paddingLeft:20 }}>
                <div style={{ flex:1,height:1,background:C.divider }} />
                <span style={{ fontSize:9,fontWeight:600,color:C.textFaint,textTransform:'uppercase',letterSpacing:'0.08em' }}>Early</span>
                <div style={{ flex:1,height:1,background:C.divider }} />
              </div>
            )}
            <div style={{ display:'flex',alignItems:'center',gap:9,position:'relative',paddingLeft:6 }}>
              {!last && (
                <div style={{ position:'absolute',left:11,top:18,width:2,height:22,background:done?C.blue:C.border,zIndex:0 }} />
              )}
              <div style={{ width:12,height:12,borderRadius:'50%',flexShrink:0,zIndex:1,
                background: current ? C.blue : done ? C.blue : C.surface,
                border: current ? `3px solid ${C.blue}` : done ? `2px solid ${C.blue}` : `2px solid ${C.border}`,
                boxShadow: current ? `0 0 0 3px rgba(0,51,141,.12)` : 'none',
              }} />
              <span style={{ fontSize:12,padding:'5px 0',
                color: current ? C.blue : done ? C.textMid : C.textFaint,
                fontWeight: current ? 600 : done ? 400 : 400,
                fontFamily: F,
              }}>
                {s.label}
                {current && (
                  <span style={{ marginLeft:7,fontSize:9,background:C.blue,color:'white',padding:'1px 6px',borderRadius:10,fontWeight:600 }}>
                    Now
                  </span>
                )}
              </span>
            </div>
          </div>
        )
      })}
    </div>
  )
}

// ── Card ──────────────────────────────────────────────────────────────────────
function Card({ title, children, noPad }) {
  return (
    <div style={{ background:C.surface,border:`1px solid ${C.border}`,borderRadius:6,overflow:'hidden',fontFamily:F }}>
      {title && (
        <div style={{ padding:'9px 16px',borderBottom:`1px solid ${C.border}`,background:C.pageBg }}>
          <span style={{ fontSize:10,fontWeight:600,color:C.textMuted,textTransform:'uppercase',letterSpacing:'0.07em' }}>{title}</span>
        </div>
      )}
      <div style={{ padding:noPad?0:'14px 16px' }}>{children}</div>
    </div>
  )
}

// ── Meta row ──────────────────────────────────────────────────────────────────
function MetaRow({ label, value, last }) {
  if (!value) return null
  return (
    <div style={{ display:'flex',justifyContent:'space-between',alignItems:'flex-start',gap:12,padding:'7px 0',borderBottom:last?'none':`1px solid ${C.divider}`,fontFamily:F }}>
      <span style={{ fontSize:12,color:C.textFaint,flexShrink:0 }}>{label}</span>
      <span style={{ fontSize:12,fontWeight:500,color:C.textMid,textAlign:'right' }}>{value}</span>
    </div>
  )
}

// ── Deadline ring ─────────────────────────────────────────────────────────────
function DeadlineRing({ days, date }) {
  const total = 90
  const used  = days != null ? Math.max(0, Math.min(total, days)) : total
  const r     = 20
  const circ  = 2 * Math.PI * r
  const dash  = (used / total) * circ
  const color = days == null ? C.textFaint : days <= 7 ? C.red : days <= 21 ? C.amber : C.teal
  return (
    <div style={{ display:'flex',alignItems:'center',gap:12,padding:'13px 16px' }}>
      <div style={{ position:'relative',width:48,height:48,flexShrink:0 }}>
        <svg width={48} height={48} style={{ transform:'rotate(-90deg)' }}>
          <circle cx={24} cy={24} r={r} fill="none" stroke={C.divider} strokeWidth={4} />
          <circle cx={24} cy={24} r={r} fill="none" stroke={color} strokeWidth={4}
            strokeDasharray={`${dash} ${circ-dash}`} strokeLinecap="round" />
        </svg>
        <div style={{ position:'absolute',inset:0,display:'flex',flexDirection:'column',alignItems:'center',justifyContent:'center' }}>
          <span style={{ fontSize:12,fontWeight:700,color,lineHeight:1,fontFamily:MONO }}>{days != null ? days : '—'}</span>
          {days != null && <span style={{ fontSize:8,color:C.textFaint }}>days</span>}
        </div>
      </div>
      <div>
        <div style={{ fontSize:10,color:C.textFaint,textTransform:'uppercase',letterSpacing:'0.05em',marginBottom:2 }}>Deadline</div>
        <div style={{ fontSize:13,fontWeight:500,color:C.text }}>{date || '—'}</div>
      </div>
    </div>
  )
}

// ── Score panel ───────────────────────────────────────────────────────────────
function ScorePanel({ p_go, scoreBreakdown }) {
  const pct  = p_go != null ? Math.round(p_go * 100) : null
  const meta = scoreMeta(p_go)

  const entries = Object.entries(scoreBreakdown?.contributions || scoreBreakdown || {})
    .map(([k, v]) => ({ label: FEATURE_LABELS[k] || k.replace(/_/g,' ').replace(/\b\w/g,l=>l.toUpperCase()), value: Number(v) }))
    .filter(e => !isNaN(e.value) && e.value !== 0)
    .sort((a, b) => Math.abs(b.value) - Math.abs(a.value))
    .slice(0, 5)

  const maxAbs = entries.length ? Math.max(...entries.map(e => Math.abs(e.value))) : 1

  return (
    <div style={{ background:C.surface,border:`1px solid ${C.border}`,borderRadius:6,overflow:'hidden' }}>
      {/* Header */}
      <div style={{ background:`linear-gradient(135deg, ${C.navy} 0%, ${C.blue} 100%)`,padding:'16px 18px',display:'flex',alignItems:'center',justifyContent:'space-between' }}>
        <div>
          <div style={{ fontSize:9,fontWeight:600,color:'rgba(255,255,255,.4)',textTransform:'uppercase',letterSpacing:'0.10em',marginBottom:4 }}>AI Recommendation</div>
          <div style={{ fontSize:17,fontWeight:600,color:'#fff',fontFamily:F }}>{meta.label}</div>
        </div>
        <div style={{ fontSize:34,fontWeight:700,color:'#fff',fontFamily:MONO,letterSpacing:'-0.03em',lineHeight:1 }}>
          {pct != null ? `${pct}%` : '—'}
        </div>
      </div>

      {/* Bars — no numbers, just lines */}
      {entries.length > 0 && (
        <div style={{ padding:'14px 16px' }}>
          {entries.map((e, i) => {
            const w   = Math.round((Math.abs(e.value) / maxAbs) * 100)
            const col = e.value >= 0 ? C.blue : C.red
            return (
              <div key={i} style={{ marginBottom: i < entries.length - 1 ? 10 : 0 }}>
                <div style={{ fontSize:11,color:C.textMuted,marginBottom:4 }}>{e.label}</div>
                <div style={{ height:3,background:C.divider,borderRadius:2 }}>
                  <div style={{ height:3,borderRadius:2,background:col,width:`${w}%`,transition:'width .4s' }} />
                </div>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

// ── Team decisions ────────────────────────────────────────────────────────────
function TeamDecisions({ decisions }) {
  if (!decisions?.length) return null
  return (
    <div style={{ marginTop:14,paddingTop:12,borderTop:`1px solid ${C.border}` }}>
      <div style={{ fontSize:10,fontWeight:600,color:C.textFaint,textTransform:'uppercase',letterSpacing:'0.07em',marginBottom:10,fontFamily:F }}>
        Team assessments ({decisions.length})
      </div>
      <div style={{ display:'flex',flexDirection:'column',gap:10 }}>
        {decisions.map((d, i) => (
          <div key={i} style={{ display:'flex',alignItems:'flex-start',gap:8 }}>
            <span style={{ fontSize:10,fontWeight:500,padding:'2px 7px',borderRadius:4,flexShrink:0,marginTop:1,
              background:d.decision==='GO'?C.greenFill:C.redFill,
              color:d.decision==='GO'?C.green:C.red,
              border:`0.5px solid ${d.decision==='GO'?C.greenBord:C.redBord}` }}>
              {d.decision==='GO'?'Endorsed':'Declined'}
            </span>
            <div style={{ flex:1,minWidth:0 }}>
              <div style={{ display:'flex',alignItems:'center',gap:6,marginBottom:d.justification?3:0 }}>
                <span style={{ fontSize:12,fontWeight:500,color:C.text,fontFamily:F }}>{d.user_full_name||'Unknown'}</span>
                {d.decided_at && (
                  <span style={{ fontSize:10,color:C.textFaint,fontFamily:F }}>
                    · {fmtDateShort(d.decided_at)}
                  </span>
                )}
              </div>
              {d.justification && (
                <p style={{ fontSize:11,color:C.textMuted,margin:0,lineHeight:1.5,fontFamily:F }}>{d.justification}</p>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

// ── Decision panel ────────────────────────────────────────────────────────────
function DecisionPanel({ tender, onDecisionMade }) {
  const [decision,      setDecision]      = useState(tender.partner_decision || '')
  const [justification, setJustification] = useState(tender.partner_justification || '')
  const [jValues,       setJValues]       = useState({})
  const [submitting,    setSubmitting]    = useState(false)
  const [dropping,      setDropping]      = useState(false)
  const [error,         setError]         = useState('')
  const [submitted,     setSubmitted]     = useState(!!tender.partner_decision)

  const activeFeatures = (() => {
    if (!tender.score_breakdown) return []
    try {
      const bd = typeof tender.score_breakdown === 'string' ? JSON.parse(tender.score_breakdown) : tender.score_breakdown
      return Object.keys(bd.contributions || {}).filter(k => FEATURE_LABELS[k])
    } catch { return [] }
  })()

  function buildInitJValues() {
    const init = {}
    activeFeatures.forEach(f => { init[f] = 0.1 })
    return init
  }

  function handleDecision(val) {
    setDecision(val)
    if (Object.keys(jValues).length === 0) setJValues(buildInitJValues())
  }

  function toggleFeature(feature) {
    setJValues(prev => ({ ...prev, [feature]: prev[feature] === 1.0 ? 0.1 : 1.0 }))
  }

  async function submit() {
    if (!decision) return
    setSubmitting(true); setError('')
    try {
      await api.post(`/tenders/${tender.id}/decide`, { decision, justification, j_values: jValues })
      setSubmitted(true)
      onDecisionMade()
    } catch(err) { setError(err.response?.data?.detail || 'Failed to submit') }
    finally { setSubmitting(false) }
  }

  async function dropDecision() {
    setDropping(true); setError('')
    try {
      await api.delete(`/tenders/${tender.id}/decide`)
      setDecision(''); setJustification(''); setJValues({}); setSubmitted(false)
      onDecisionMade()
    } catch(err) { setError(err.response?.data?.detail || 'Failed to drop decision') }
    finally { setDropping(false) }
  }

  // ── Submitted ────────────────────────────────────────────────────────────────
  if (submitted) {
    return (
      <Card title="Partner Assessment">
        <div style={{ padding:'10px 12px',background:decision==='GO'?C.greenFill:C.redFill,borderRadius:4,border:`0.5px solid ${decision==='GO'?C.greenBord:C.redBord}`,marginBottom:12 }}>
          <div style={{ fontSize:13,fontWeight:600,color:decision==='GO'?C.green:C.red,marginBottom:2 }}>
            {decision==='GO'?'✓ Endorsed':'✗ Declined'}
          </div>
          {justification && <p style={{ fontSize:12,color:C.textMuted,margin:'4px 0 0',lineHeight:1.5 }}>{justification}</p>}
          <div style={{ fontSize:11,color:C.textFaint,marginTop:5 }}>
            {new Date().toLocaleDateString('en-GB', { day:'numeric', month:'short', year:'numeric' })}
          </div>
          <div style={{ display:'flex',gap:8,marginTop:8 }}>
            <button onClick={() => { setSubmitted(false); setJValues(buildInitJValues()) }}
              style={{ fontSize:11,color:C.blue,background:'none',border:'none',cursor:'pointer',fontFamily:F,textDecoration:'underline',padding:0 }}>
              Revise
            </button>
            <span style={{ fontSize:11,color:C.textFaint }}>·</span>
            <button onClick={dropDecision} disabled={dropping}
              style={{ fontSize:11,color:dropping?C.textFaint:C.red,background:'none',border:'none',cursor:dropping?'not-allowed':'pointer',fontFamily:F,textDecoration:'underline',padding:0 }}>
              {dropping?'Dropping…':'Drop'}
            </button>
          </div>
        </div>
        {error && <p style={{ fontSize:12,color:C.red,margin:'0 0 8px',fontFamily:F }}>{error}</p>}
        <TeamDecisions decisions={tender.decisions} />
      </Card>
    )
  }

  // ── Form ──────────────────────────────────────────────────────────────────────
  return (
    <Card title="Partner Assessment">

      {/* GO / NO GO */}
      <div style={{ display:'flex',gap:8,marginBottom:12 }}>
        {[
          { val:'GO',    label:'✓ Endorse', on:{ bg:C.green,  tc:'white',bdr:C.green  }, off:{ bg:C.greenFill, tc:C.green, bdr:C.greenBord } },
          { val:'NO GO', label:'✗ Decline', on:{ bg:C.red,    tc:'white',bdr:C.red    }, off:{ bg:C.redFill,   tc:C.red,   bdr:C.redBord   } },
        ].map(btn => {
          const a = decision === btn.val, col = a ? btn.on : btn.off
          return (
            <button key={btn.val} onClick={() => handleDecision(btn.val)}
              style={{ flex:1,padding:'9px 0',borderRadius:4,fontSize:12,fontWeight:500,cursor:'pointer',background:col.bg,color:col.tc,border:`1px solid ${col.bdr}`,transition:'all .12s',fontFamily:F }}>
              {btn.label}
            </button>
          )
        })}
      </div>

      {/* Feature toggles */}
      {decision && activeFeatures.length > 0 && (
        <div style={{ marginBottom:12,padding:'10px 12px',background:C.pageBg,border:`1px solid ${C.border}`,borderRadius:4 }}>
          <div style={{ display:'flex',alignItems:'center',gap:5,marginBottom:6 }}>
            <Brain style={{ width:10,height:10,color:C.blue }} />
            <span style={{ fontSize:10,fontWeight:600,color:C.blue,textTransform:'uppercase',letterSpacing:'0.07em',fontFamily:F }}>What drove your decision?</span>
          </div>
          <div style={{ display:'flex',flexDirection:'column',gap:4 }}>
            {activeFeatures.map(feature => {
              const isYes = jValues[feature] === 1.0
              return (
                <div key={feature} onClick={() => toggleFeature(feature)}
                  style={{ display:'flex',alignItems:'center',justifyContent:'space-between',padding:'6px 10px',borderRadius:3,cursor:'pointer',border:`0.5px solid ${isYes?C.blueBorder:C.border}`,background:isYes?C.blueTint:C.surface,transition:'all .12s' }}>
                  <span style={{ fontSize:11,color:isYes?C.blue:C.textMid,fontFamily:F }}>{FEATURE_LABELS[feature]||feature}</span>
                  <div style={{ width:28,height:16,borderRadius:8,background:isYes?C.blue:C.border,position:'relative',transition:'background .12s',flexShrink:0 }}>
                    <div style={{ position:'absolute',top:2,left:isYes?12:2,width:12,height:12,borderRadius:'50%',background:'white',transition:'left .12s' }} />
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      )}

      {/* Justification */}
      <div style={{ marginBottom:12 }}>
        <div style={{ fontSize:10,fontWeight:600,color:C.textFaint,textTransform:'uppercase',letterSpacing:'0.07em',marginBottom:5 }}>
          Justification
        </div>
        <textarea value={justification} onChange={e => setJustification(e.target.value)}
          placeholder="Strategic rationale…" rows={3}
          style={{ width:'100%',borderRadius:4,border:`1px solid ${C.border}`,padding:'8px 10px',fontSize:12,fontFamily:F,color:C.textMid,resize:'vertical',outline:'none',boxSizing:'border-box',lineHeight:1.55,background:C.pageBg }}
          onFocus={e => e.target.style.borderColor = C.blue}
          onBlur={e  => e.target.style.borderColor = C.border} />
      </div>

      {error && <p style={{ fontSize:12,color:C.red,margin:'0 0 8px',fontFamily:F }}>{error}</p>}

      <button onClick={submit} disabled={!decision||submitting}
        style={{ width:'100%',padding:'9px 0',borderRadius:4,border:'none',cursor:(!decision||submitting)?'not-allowed':'pointer',fontSize:12,fontWeight:500,fontFamily:F,background:(!decision||submitting)?C.divider:C.blue,color:(!decision||submitting)?C.textFaint:'white',transition:'background .12s' }}>
        {submitting?'Submitting…':'Submit Decision'}
      </button>

      <TeamDecisions decisions={tender.decisions} />
    </Card>
  )
}

// ── Main ──────────────────────────────────────────────────────────────────────
export default function TenderDetail() {
  const { id }    = useParams()
  const navigate  = useNavigate()
  const [tender,  setTender]  = useState(null)
  const [loading, setLoading] = useState(true)
  const [error,   setError]   = useState(null)
  const [saved,   setSaved]   = useState(false)
  const [tab,     setTab]     = useState('overview')

  function load() {
    setLoading(true)
    api.get(`/tenders/${id}`)
      .then(r => setTender(r.data))
      .catch(() => setError('Tender not found'))
      .finally(() => setLoading(false))
  }

  useEffect(() => {
    load()
    api.get('/tenders/saved')
      .then(r => setSaved((r.data.saved_ids || []).includes(Number(id))))
      .catch(() => {})
  }, [id])

  async function handleSave() {
    const next = !saved; setSaved(next)
    try {
      if (next) await api.post(`/tenders/${id}/save`)
      else      await api.delete(`/tenders/${id}/save`)
    } catch { setSaved(!next) }
  }

  if (loading) return <div style={{ display:'flex',justifyContent:'center',padding:'80px 0' }}><Spinner size="lg" /></div>
  if (error)   return <div style={{ textAlign:'center',padding:'80px 0',color:C.red,fontSize:14,fontFamily:F }}>{error}</div>

  const t      = tender
  const isOpen = !t.deadline_datetime || new Date(t.deadline_datetime.replace(' ','T')) > new Date()
  const days   = t.days_to_deadline
  let scoreBreakdown = {}
  if (t.score_breakdown) {
    try { scoreBreakdown = typeof t.score_breakdown === 'string' ? JSON.parse(t.score_breakdown) : t.score_breakdown }
    catch {}
  }

  const tabs = [
    { key:'overview',   label:'Overview'   },
    t.llm_submission_process && { key:'submission', label:'Submission' },
  ].filter(Boolean)

  return (
    <div style={{ display:'flex',flexDirection:'column',minHeight:'100%',background:C.surface,fontFamily:F }}>

      {/* Sticky top bar */}
      <div style={{ background:C.surface,borderBottom:`1px solid ${C.border}`,padding:'10px 40px',display:'flex',alignItems:'center',justifyContent:'space-between',position:'sticky',top:0,zIndex:20 }}>
        <button onClick={()=>navigate(-1)} style={{ display:'flex',alignItems:'center',gap:6,fontSize:13,color:C.textMuted,background:'none',border:'none',cursor:'pointer',fontFamily:F,transition:'color .12s' }}
          onMouseEnter={e=>e.currentTarget.style.color=C.blue}
          onMouseLeave={e=>e.currentTarget.style.color=C.textMuted}>
          <ArrowLeft style={{ width:14,height:14 }} /> Back
        </button>
        <div style={{ display:'flex',alignItems:'center',gap:8 }}>
          {t.source_url && (
            <a href={t.source_url} target="_blank" rel="noopener noreferrer"
              style={{ display:'flex',alignItems:'center',gap:5,fontSize:12,color:C.accent,textDecoration:'none',border:`1px solid ${C.blueBorder}`,borderRadius:4,padding:'5px 10px',background:C.blueTint,fontFamily:F }}>
              <ExternalLink style={{ width:12,height:12 }} /> Source
            </a>
          )}
          <button onClick={handleSave} style={{ display:'flex',alignItems:'center',gap:6,fontSize:12,color:C.textMuted,background:saved?C.pageBg:C.surface,border:`1px solid ${C.border}`,borderRadius:4,padding:'5px 10px',cursor:'pointer',fontFamily:F }}>
            {saved ? <Bookmark style={{ width:13,height:13,fill:C.textMuted,color:C.textMuted }} /> : <BookmarkPlus style={{ width:13,height:13 }} />}
            {saved?'Saved':'Save'}
          </button>
        </div>
      </div>

      {/* Hero */}
      <div style={{ background:`linear-gradient(135deg, ${C.navy} 0%, #001A5C 60%, ${C.blue} 100%)` }}>
        <div style={{ height:3,background:`linear-gradient(90deg, ${C.blue}, ${C.accent})` }} />
        <div style={{ padding:'16px 40px 0',maxWidth:1100,margin:'0 auto' }}>
          <div style={{ display:'flex',alignItems:'flex-start',gap:12,marginBottom:12 }}>
            <AgencyLogo agency={t.funding_agency} portal={t.source_portal} size={44} />
            <div style={{ flex:1,minWidth:0 }}>
              <div style={{ display:'flex',alignItems:'center',gap:7,flexWrap:'wrap',marginBottom:7 }}>
                <span style={{ fontSize:10,fontWeight:600,color:'rgba(255,255,255,.35)',textTransform:'uppercase',letterSpacing:'.08em' }}>{(t.source_portal||'').toUpperCase()}</span>
                <span style={{ color:'rgba(255,255,255,.2)' }}>·</span>
                <span style={{ fontSize:11,fontWeight:500,padding:'2px 8px',borderRadius:20,background:isOpen?'rgba(15,118,110,.25)':'rgba(185,28,28,.20)',color:isOpen?'#5EEAD4':'#FCA5A5' }}>
                  {isOpen?'● Open':'● Closed'}
                </span>
                {t.procurement_group && (
                  <span style={{ fontSize:11,padding:'2px 8px',borderRadius:20,background:'rgba(255,255,255,.1)',color:'rgba(255,255,255,.6)' }}>
                    {t.procurement_group}
                  </span>
                )}
              </div>
              <h1 style={{ fontSize:16,fontWeight:600,color:'white',lineHeight:1.4,margin:'0 0 8px',letterSpacing:'-.01em' }}>
                {t.title_clean || 'Untitled'}
              </h1>
              <div style={{ display:'flex',alignItems:'center',gap:14,flexWrap:'wrap',fontSize:12,color:'rgba(255,255,255,.5)' }}>
                {t.country_name_normalized && <span style={{ display:'flex',alignItems:'center',gap:4 }}><MapPin style={{ width:11,height:11 }} />{t.country_name_normalized}</span>}
                {t.funding_agency          && <span style={{ display:'flex',alignItems:'center',gap:4 }}><Building2 style={{ width:11,height:11 }} />{t.funding_agency}</span>}
                {days != null              && <span style={{ display:'flex',alignItems:'center',gap:4,color:days<=7?'#FCA5A5':days<=21?'#FDE68A':'rgba(255,255,255,.5)' }}><Clock style={{ width:11,height:11 }} />{days}d remaining</span>}
              </div>
            </div>
          </div>

          {/* KPI strip */}
          <div style={{ display:'flex',flexWrap:'wrap',borderTop:'1px solid rgba(255,255,255,.08)' }}>
            {[
              t.budget                       && { k:'Budget',   v:fmtBudget(t.budget,t.currency) },
              t.sector                       && { k:'Sector',   v:t.sector.split(',')[0].trim()   },
              t.language                     && { k:'Language', v:t.language                      },
              t.llm_contract_duration_months && { k:'Duration', v:`${t.llm_contract_duration_months} months` },
              t.deadline_datetime            && { k:'Deadline', v:fmtDate(t.deadline_datetime)    },
            ].filter(Boolean).map((item,i,arr) => (
              <div key={item.k} style={{ padding:'8px 16px',borderRight:i<arr.length-1?'1px solid rgba(255,255,255,.08)':'none' }}>
                <div style={{ fontSize:9,color:'rgba(255,255,255,.35)',textTransform:'uppercase',letterSpacing:'.07em',marginBottom:2 }}>{item.k}</div>
                <div style={{ fontSize:11,color:'white',fontWeight:500 }}>{item.v}</div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Body — 2 columns */}
      <div style={{ flex:1,padding:'24px 40px 56px',maxWidth:1100,width:'100%',margin:'0 auto',boxSizing:'border-box' }}>
        <div style={{ display:'grid',gridTemplateColumns:'1fr 320px',gap:20,alignItems:'start' }}>

          {/* ── LEFT ── */}
          <div>
            {/* Tabs */}
            <div style={{ display:'flex',gap:0,borderBottom:`1px solid ${C.border}`,marginBottom:16 }}>
              {tabs.map(tab_ => (
                <button key={tab_.key} onClick={() => setTab(tab_.key)}
                  style={{ fontSize:13,padding:'8px 16px',color:tab===tab_.key?C.blue:C.textMuted,background:'none',border:'none',borderBottom:tab===tab_.key?`2px solid ${C.blue}`:'2px solid transparent',marginBottom:-1,cursor:'pointer',fontFamily:F,fontWeight:tab===tab_.key?500:400,transition:'color .12s' }}>
                  {tab_.label}
                </button>
              ))}
            </div>

            {/* Tab content */}
            {tab === 'overview' && (
              <div style={{ display:'flex',flexDirection:'column',gap:14 }}>

                {/* Intelligence Brief */}
                {t.llm_scope_summary && (
                  <div style={{ background:C.blueTint,borderLeft:`3px solid ${C.blue}`,borderRadius:'0 6px 6px 0',padding:'12px 14px' }}>
                    <div style={{ fontSize:9,fontWeight:600,color:C.blue,textTransform:'uppercase',letterSpacing:'.08em',marginBottom:6,display:'flex',alignItems:'center',gap:4 }}>
                      <Zap style={{ width:10,height:10 }} /> Intelligence Brief
                    </div>
                    <p style={{ fontSize:13,color:C.textMid,lineHeight:1.7,margin:0 }}>{t.llm_scope_summary}</p>
                  </div>
                )}

                {/* Procurement Details */}
                <Card title="Procurement Details" noPad>
                  <div style={{ padding:'6px 16px 10px' }}>
                    <MetaRow label="Status"       value={isOpen?'Open':'Closed'} />
                    <MetaRow label="Country"      value={t.country_name_normalized} />
                    <MetaRow label="Organisation" value={t.organisation_name} />
                    <MetaRow label="Category"     value={t.procurement_group} />
                    <MetaRow label="Language"     value={t.language} />
                    <MetaRow label="Financing"    value={t.llm_financing_instrument} />
                    <MetaRow label="Bid process"  value={t.llm_bid_process_type} />
                    <MetaRow label="Published"    value={fmtDate(t.publication_datetime)} />
                    <MetaRow label="Deadline"     value={fmtDate(t.deadline_datetime)} last />
                  </div>
                </Card>

                {/* Description */}
                {t.description_clean && (
                  <Card title="Description">
                    <p style={{ fontSize:13,color:C.textMid,lineHeight:1.75,margin:0,whiteSpace:'pre-line' }}>{t.description_clean}</p>
                  </Card>
                )}

                {/* Eligibility — merged into overview */}
                {t.llm_eligibility_summary && (
                  <Card title="Eligibility Criteria">
                    <p style={{ fontSize:13,color:C.textMid,lineHeight:1.75,margin:0 }}>{t.llm_eligibility_summary}</p>
                    {t.llm_specific_areas && (
                      <div style={{ marginTop:12,paddingTop:12,borderTop:`1px solid ${C.divider}` }}>
                        <div style={{ fontSize:10,fontWeight:600,color:C.textFaint,textTransform:'uppercase',letterSpacing:'.07em',marginBottom:6 }}>Specific areas</div>
                        <p style={{ fontSize:12,color:C.textMid,lineHeight:1.65,margin:0 }}>{t.llm_specific_areas}</p>
                      </div>
                    )}
                  </Card>
                )}
              </div>
            )}

            {tab === 'submission' && t.llm_submission_process && (
              <Card title="Submission Process">
                <p style={{ fontSize:13,color:C.textMid,lineHeight:1.75,margin:0 }}>{t.llm_submission_process}</p>
              </Card>
            )}
          </div>

          {/* ── RIGHT ── */}
          <div style={{ display:'flex',flexDirection:'column',gap:14 }}>

            {/* Lifecycle */}
            <Card title="Project Lifecycle">
              <Stepper status={isOpen ? 'open' : 'closed'} />
            </Card>

            {/* Score */}
            <ScorePanel p_go={t.p_go} scoreBreakdown={scoreBreakdown} />

            {/* Decision */}
            <DecisionPanel tender={t} onDecisionMade={load} />

          </div>
        </div>
      </div>
    </div>
  )
}