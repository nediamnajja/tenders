// src/pages/Today.jsx
import { useEffect, useState, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { useAuth } from '../lib/auth'
import api from '../lib/api'
import { Spinner } from '../components/ui'
import IntelStrip from '../components/layout/IntelStrip'
import PlatformFooter from '../components/layout/PlatformFooter'
import {
  AlertTriangle, ChevronLeft, ChevronRight, ArrowRight,
  TrendingUp, Clock, AlertCircle, DollarSign,
  CheckCircle, XCircle,
} from 'lucide-react'

// ── Tokens ────────────────────────────────────────────────────────────────────
const C = {
  blue:      '#00338D',
  accent:    '#0091DA',
  navy:      '#0D1F6B',
  text:      '#111827',
  textMid:   '#374151',
  textMuted: '#6B7280',
  textFaint: '#9CA3AF',
  border:    '#E5E7EB',
  divider:   '#F3F4F6',
  surface:   '#FFFFFF',
  pageBg:    '#F8FAFC',
  blueBorder:'#C3D9F2',
}
const F    = "'Inter', system-ui, -apple-system, sans-serif"
const MONO = "'DM Mono', 'JetBrains Mono', ui-monospace, monospace"

// ── Helpers ───────────────────────────────────────────────────────────────────
function fmtM(v) {
  if (!v) return '—'
  if (v >= 1e9) return `$${(v / 1e9).toFixed(1)}B`
  if (v >= 1e6) return `$${(v / 1e6).toFixed(1)}M`
  if (v >= 1e3) return `$${(v / 1e3).toFixed(0)}K`
  return `$${v}`
}
function greeting() {
  const h = new Date().getHours()
  if (h < 12) return 'Good morning'
  if (h < 18) return 'Good afternoon'
  return 'Good evening'
}
function todayLabel() {
  return new Date().toLocaleDateString('en-GB', {
    weekday: 'long', day: 'numeric', month: 'long', year: 'numeric',
  }).toUpperCase()
}
function buildInsight(stats) {
  if (stats.closingThisWeek > 0 && stats.pendingDecisions > 0) {
    const pct = stats.bestScore ? Math.round(stats.bestScore * 100) : null
    return {
      headline: 'Your pipeline needs attention.',
      detail: pct && stats.bestTitle
        ? `${stats.closingThisWeek} opportunit${stats.closingThisWeek > 1 ? 'ies close' : 'y closes'} this week. The ${stats.bestTitle.slice(0, 60)}${stats.bestTitle.length > 60 ? '…' : ''} mandate scores ${pct}% — the highest-ranked opportunity this quarter. No GO decision has been submitted yet.`
        : `${stats.closingThisWeek} opportunit${stats.closingThisWeek > 1 ? 'ies close' : 'y closes'} this week with ${stats.pendingDecisions} decision${stats.pendingDecisions > 1 ? 's' : ''} still pending. Review and submit before deadlines close.`,
    }
  }
  return {
    headline: stats.strongGo > 0
      ? `${stats.strongGo} high-priority signal${stats.strongGo > 1 ? 's' : ''} identified today.`
      : 'Your pipeline is active and up to date.',
    detail: stats.pipelineValue
      ? `The pipeline holds ${stats.active?.toLocaleString()} active tenders with a combined value of ${fmtM(stats.pipelineValue)}. ${stats.awaitingDecision > 0 ? `${stats.awaitingDecision} high-priority opportunit${stats.awaitingDecision > 1 ? 'ies are' : 'y is'} awaiting a decision — review before the week closes.` : 'All high-priority opportunities have been assessed by the team.'}`
      : 'Use today to review high-priority matches and align your team on GO / NO GO positions.',
  }
}
function scoreGradient(p) {
  if (p == null) return `${C.navy}`
  if (p >= 0.80) return `linear-gradient(150deg, ${C.navy} 0%, ${C.blue} 100%)`
  if (p >= 0.60) return `linear-gradient(150deg, ${C.blue} 0%, ${C.accent} 100%)`
  if (p >= 0.40) return `linear-gradient(150deg, #78350F 0%, #B45309 100%)`
  return `linear-gradient(150deg, #1F2937 0%, #374151 100%)`
}
function scoreLabel(p) {
  if (p == null) return '—'
  if (p >= 0.80) return 'Priority'
  if (p >= 0.60) return 'GO'
  if (p >= 0.40) return 'Marginal'
  return 'Low'
}

// ── Country coords (Africa) ───────────────────────────────────────────────────
const COUNTRY_COORDS = {
  'Nigeria':{ lng:8.68, lat:9.08 }, 'Kenya':{ lng:37.91, lat:-0.02 },
  'Ethiopia':{ lng:40.49, lat:9.15 }, 'Ghana':{ lng:-1.02, lat:7.95 },
  'Tanzania':{ lng:34.89, lat:-6.37 }, 'Uganda':{ lng:32.29, lat:1.37 },
  'Senegal':{ lng:-14.45, lat:14.50 }, 'Cameroon':{ lng:12.35, lat:3.85 },
  "Côte d'Ivoire":{ lng:-5.55, lat:7.54 }, 'Ivory Coast':{ lng:-5.55, lat:7.54 },
  'Mozambique':{ lng:35.53, lat:-18.67 }, 'Zambia':{ lng:27.85, lat:-13.13 },
  'Zimbabwe':{ lng:29.15, lat:-19.02 }, 'Rwanda':{ lng:29.87, lat:-1.94 },
  'Mali':{ lng:-2.00, lat:17.57 }, 'Niger':{ lng:8.08, lat:17.61 },
  'Burkina Faso':{ lng:-1.56, lat:12.36 }, 'Guinea':{ lng:-11.31, lat:11.80 },
  'Congo':{ lng:15.83, lat:-0.23 }, 'DRC':{ lng:23.66, lat:-4.04 },
  'Democratic Republic of the Congo':{ lng:23.66, lat:-4.04 },
  'Angola':{ lng:17.87, lat:-11.20 }, 'Malawi':{ lng:34.30, lat:-13.25 },
  'South Africa':{ lng:22.94, lat:-30.56 }, 'Egypt':{ lng:30.80, lat:26.82 },
  'Morocco':{ lng:-7.09, lat:31.79 }, 'Tunisia':{ lng:9.54, lat:33.89 },
  'Sudan':{ lng:30.22, lat:12.86 }, 'Somalia':{ lng:46.20, lat:5.15 },
  'Madagascar':{ lng:46.87, lat:-18.77 }, 'Togo':{ lng:0.82, lat:8.62 },
  'Benin':{ lng:2.32, lat:9.31 }, 'Chad':{ lng:18.73, lat:15.45 },
  'Liberia':{ lng:-9.43, lat:6.43 }, 'Sierra Leone':{ lng:-11.78, lat:8.46 },
  'Namibia':{ lng:18.49, lat:-22.96 }, 'Botswana':{ lng:24.68, lat:-22.33 },
  'Mauritania':{ lng:-10.94, lat:21.01 }, 'Libya':{ lng:17.23, lat:26.34 },
  'Algeria':{ lng:1.66, lat:28.03 }, 'Gabon':{ lng:11.61, lat:-0.80 },
}
const AFRICA = { minLng:-25, maxLng:55, minLat:-40, maxLat:42 }

function toXY(lng, lat, w, h) {
  return {
    x: ((lng - AFRICA.minLng) / (AFRICA.maxLng - AFRICA.minLng)) * w,
    y: ((AFRICA.maxLat - lat) / (AFRICA.maxLat - AFRICA.minLat)) * h,
  }
}
function pinColor(score) {
  if (score >= 0.80) return C.blue
  if (score >= 0.60) return C.accent
  return '#F59E0B'
}

// ── Africa canvas map ─────────────────────────────────────────────────────────
function AfricaMap({ tenders, onPinClick }) {
  const canvasRef    = useRef(null)
  const maskDataRef  = useRef(null)
  const maskReadyRef = useRef(false)
  const animRef      = useRef(null)
  const pinsRef      = useRef([])
  const [tooltip, setTooltip] = useState(null)
  const [pins, setPins] = useState([])

  // Keep ref in sync so draw loop always reads latest pins
  useEffect(() => { pinsRef.current = pins }, [pins])

  useEffect(() => {
    if (!tenders?.length) return
    async function buildPins() {
      const resolved = await Promise.all(
        tenders.map(async t => {
          const name = t.country_name_normalized
          if (!name) return null
          const coords = COUNTRY_COORDS[name] || await (async () => {
            try {
              const res = await fetch(`https://restcountries.com/v3.1/name/${encodeURIComponent(name)}?fields=latlng`)
              const data = await res.json()
              if (Array.isArray(data) && data[0]?.latlng?.length === 2) {
                const c = { lat: data[0].latlng[0], lng: data[0].latlng[1] }
                COUNTRY_COORDS[name] = c
                return c
              }
            } catch {}
            return null
          })()
          if (!coords) return null
          if (coords.lng < -20 || coords.lng > 52 || coords.lat < -37 || coords.lat > 38) return null
          return { tender: t, score: t.p_go ?? 0, ...coords }
        })
      )
      setPins(resolved.filter(Boolean).slice(0, 30))
    }
    buildPins()
  }, [tenders?.length])

  useEffect(() => {
    const canvas = canvasRef.current; if (!canvas) return
    const ctx = canvas.getContext('2d')
    const MASK_W = 1000, MASK_H = 500

    function resize() {
      const rect = canvas.getBoundingClientRect()
      if (rect.width > 0 && rect.height > 0) {
        canvas.width  = rect.width
        canvas.height = rect.height
      }
    }
    resize()
    const ro = new ResizeObserver(resize)
    ro.observe(canvas)
    window.addEventListener('resize', resize)

    // Pre-render the Africa-cropped mask into an offscreen canvas
    // We sample the world mask at Africa's exact lng/lat bounds
    const offMask = document.createElement('canvas')
    const OMASK_W = 600, OMASK_H = 400
    offMask.width = OMASK_W; offMask.height = OMASK_H
    const omc = offMask.getContext('2d')
    let maskReady = false

    const img = new Image(); img.crossOrigin = 'anonymous'; img.src = '/world-map-mask.png'
    img.onload = () => {
      const iw = img.naturalWidth  || img.width
      const ih = img.naturalHeight || img.height
      // Shift bounds south — world-map-mask may not be full equirectangular
      // so we add a correction to push the crop below Mediterranean
      const sx = ((AFRICA.minLng + 180) / 360) * iw
      const sy = ((90 - AFRICA.maxLat) / 180) * ih + (ih * 0.14) // push down 6%
      const sw = ((AFRICA.maxLng - AFRICA.minLng) / 360) * iw
      const sh = ((AFRICA.maxLat - AFRICA.minLat) / 180) * ih
      omc.drawImage(img, sx, sy, sw, sh, 0, 0, OMASK_W, OMASK_H)
      maskDataRef.current = omc.getImageData(0, 0, OMASK_W, OMASK_H)
      maskReadyRef.current = true
      maskReady = true
    }
    img.onerror = () => { maskReadyRef.current = true; maskReady = true }

    function isLand(px, py, w, h) {
      if (!maskDataRef.current) return false
      // px,py are canvas pixels → map directly to offscreen mask
      const mx = Math.round((px / w) * OMASK_W)
      const my = Math.round((py / h) * OMASK_H)
      if (mx < 0 || mx >= OMASK_W || my < 0 || my >= OMASK_H) return false
      return maskDataRef.current.data[(my * OMASK_W + mx) * 4] > 128
    }

    let t0 = null
    function draw(ts) {
      if (!t0) t0 = ts
      const w = canvas.width, h = canvas.height
      ctx.clearRect(0, 0, w, h)

      if (maskReadyRef.current) {
        ctx.fillStyle = 'rgba(0,51,141,0.22)'
        for (let px = 0; px < w; px += 4)
          for (let py = 0; py < h; py += 4)
            if (isLand(px, py, w, h)) {
              ctx.beginPath(); ctx.arc(px, py, 1.4, 0, Math.PI * 2); ctx.fill()
            }
      }

      pinsRef.current.forEach(pin => {
        const {x,y} = toXY(pin.lng, pin.lat, w, h)
        const col = pinColor(pin.score), pulse = 0.5 + 0.5 * Math.sin((ts - t0) / 900 + pin.lng)
        ctx.beginPath(); ctx.arc(x, y, 7 + pulse * 4, 0, Math.PI * 2)
        ctx.strokeStyle = col + '44'; ctx.lineWidth = 1; ctx.stroke()
        ctx.beginPath(); ctx.arc(x, y, 5, 0, Math.PI * 2)
        ctx.fillStyle = col; ctx.fill(); ctx.strokeStyle = '#fff'; ctx.lineWidth = 1.5; ctx.stroke()
      })

      animRef.current = requestAnimationFrame(draw)
    }
    animRef.current = requestAnimationFrame(draw)
    return () => { cancelAnimationFrame(animRef.current); ro.disconnect(); window.removeEventListener('resize', resize) }
  }, [])

  function onMouseMove(e) {
    const canvas=canvasRef.current; if (!canvas) return
    const rect=canvas.getBoundingClientRect()
    const mx=e.clientX-rect.left, my=e.clientY-rect.top
    const w=canvas.width, h=canvas.height
    let found=null
    for (const pin of pins) {
      const {x,y}=toXY(pin.lng,pin.lat,w,h)
      if (Math.hypot(mx-x,my-y)<10) { found={x:e.clientX,y:e.clientY,tender:pin.tender}; break }
    }
    setTooltip(found); canvas.style.cursor=found?'pointer':'default'
  }

  return (
    <div style={{ position:'relative', width:'100%', height:'100%' }}>
      <canvas ref={canvasRef} onMouseMove={onMouseMove} onMouseLeave={()=>setTooltip(null)}
        onClick={()=>tooltip?.tender&&onPinClick(tooltip.tender)}
        style={{ width:'100%', height:'100%', display:'block' }} />
      {/* Legend */}
      <div style={{ position:'absolute',bottom:12,left:14,display:'flex',gap:10,background:'rgba(255,255,255,.90)',borderRadius:4,padding:'5px 10px',border:`1px solid ${C.border}` }}>
        {[{color:C.blue,label:'≥ 80%'},{color:C.accent,label:'60–79%'},{color:'#F59E0B',label:'Marginal'}].map(l=>(
          <div key={l.label} style={{ display:'flex',alignItems:'center',gap:4 }}>
            <div style={{ width:7,height:7,borderRadius:'50%',background:l.color }} />
            <span style={{ fontSize:9,color:C.textMuted,fontFamily:F }}>{l.label}</span>
          </div>
        ))}
      </div>
      {/* Tooltip */}
      {tooltip && (
        <div style={{ position:'fixed',left:tooltip.x+12,top:tooltip.y-8,zIndex:999,background:C.navy,color:'#fff',borderRadius:4,padding:'8px 12px',fontSize:11,fontFamily:F,maxWidth:220,pointerEvents:'none',boxShadow:'0 4px 16px rgba(0,0,0,.22)' }}>
          <div style={{ fontWeight:600,marginBottom:3,lineHeight:1.4 }}>
            {(tooltip.tender.title_clean||'Untitled').slice(0,55)}{(tooltip.tender.title_clean||'').length>55?'…':''}
          </div>
          <div style={{ color:'rgba(255,255,255,.55)',fontSize:10 }}>
            {tooltip.tender.country_name_normalized} · {tooltip.tender.p_go!=null?Math.round(tooltip.tender.p_go*100)+'%':'—'}
          </div>
          <div style={{ marginTop:4,fontSize:10,color:C.accent }}>Click to open →</div>
        </div>
      )}
    </div>
  )
}

// ── Endorsement badge + panel ─────────────────────────────────────────────────
function EndorsementBadge({ decision }) {
  const ok = decision === 'GO'
  return (
    <span style={{ display:'inline-flex',alignItems:'center',gap:3,fontSize:10,fontWeight:700,color:ok?'#15803D':'#B91C1C',background:ok?'#F0FDF4':'#FFF1F2',border:`1px solid ${ok?'#BBF7D0':'#FECDD3'}`,padding:'2px 7px',borderRadius:3,whiteSpace:'nowrap',fontFamily:F }}>
      {ok?<><CheckCircle style={{width:9,height:9}}/> Endorsed</>:<><XCircle style={{width:9,height:9}}/> Declined</>}
    </span>
  )
}

function EndorsementsPanel({ decided, navigate }) {
  const recent=[...decided].sort((a,b)=>new Date(b.decided_at||0)-new Date(a.decided_at||0)).slice(0,7)
  if (!recent.length) return <div style={{ padding:'20px 16px',fontSize:12,color:C.textFaint,fontStyle:'italic',textAlign:'center' }}>No endorsements yet.</div>
  return (
    <>
      {recent.map((t,i)=>{
        const [hov,setHov]=useState(false)
        const pct=t.p_go!=null?Math.round(t.p_go*100):null
        return (
          <div key={t.id} onClick={()=>navigate(`/tenders/${t.id}`)}
            onMouseEnter={()=>setHov(true)} onMouseLeave={()=>setHov(false)}
            style={{ display:'flex',flexDirection:'column',gap:5,padding:'11px 16px',borderBottom:i<recent.length-1?`1px solid ${C.divider}`:'none',background:hov?C.pageBg:C.surface,cursor:'pointer',transition:'background .12s' }}>
            <div style={{ fontSize:12,fontWeight:500,color:C.text,lineHeight:1.35,overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap' }}>
              {(t.title_clean||'Untitled').slice(0,46)}{(t.title_clean||'').length>46?'…':''}
            </div>
            <div style={{ display:'flex',alignItems:'center',gap:6,flexWrap:'wrap' }}>
              <EndorsementBadge decision={t.partner_decision} />
              {pct!=null&&<span style={{ fontSize:10,fontWeight:700,color:C.textMuted,fontFamily:MONO }}>{pct}%</span>}
              <span style={{ fontSize:10,color:C.textFaint }}>{t.country_name_normalized||'—'}</span>
            </div>
          </div>
        )
      })}
    </>
  )
}

// ── SectionCard (shared pattern) ──────────────────────────────────────────────
function SectionCard({ label, meta, action, children, noPad }) {
  return (
    <div style={{ background:C.surface,border:`1px solid ${C.border}`,borderRadius:4,overflow:'hidden',boxShadow:'0 1px 3px rgba(0,0,0,.05)' }}>
      <div style={{ background:C.pageBg,borderBottom:`1px solid ${C.border}`,padding:'9px 24px',display:'flex',alignItems:'center',justifyContent:'space-between' }}>
        <div style={{ display:'flex',alignItems:'center',gap:12 }}>
          <span style={{ fontSize:11,fontWeight:600,color:C.textMuted,letterSpacing:'0.07em',textTransform:'uppercase',fontFamily:F }}>{label}</span>
          {meta&&<><span style={{ width:1,height:10,background:C.border,flexShrink:0 }}/><span style={{ fontSize:11,color:C.textFaint,fontFamily:F }}>{meta}</span></>}
        </div>
        {action&&<ActionBtn label={action.label} onClick={action.onClick} />}
      </div>
      <div style={{ padding:noPad?0:'20px 24px' }}>{children}</div>
    </div>
  )
}
function ActionBtn({ label, onClick }) {
  const [hov,setHov]=useState(false)
  return (
    <button onClick={onClick} onMouseEnter={()=>setHov(true)} onMouseLeave={()=>setHov(false)}
      style={{ fontSize:11,fontWeight:500,color:hov?C.textMid:C.textFaint,background:'none',border:'none',cursor:'pointer',fontFamily:F,flexShrink:0,transition:'color .12s' }}>
      {label}
    </button>
  )
}

// ── Live bar ──────────────────────────────────────────────────────────────────
function LiveBar({ stats }) {
  const [tick, setTick] = useState(0)
  useEffect(() => {
    const id = setInterval(() => setTick(t => t + 1), 30000)
    return () => clearInterval(id)
  }, [])
  const items = [
    { label: 'Active opps.',      value: stats.active?.toLocaleString() ?? '—' },
    { label: 'Strategic matches', value: stats.strongGo ?? '—'                 },
    { label: 'Pipeline value',    value: fmtM(stats.pipelineValue)             },
    { label: 'Pending decisions', value: stats.pendingDecisions ?? '—'         },
  ]
  return (
    <>
      <style>{`
        @keyframes livePulse { 0%{box-shadow:0 0 0 0 rgba(34,197,94,.70)} 60%{box-shadow:0 0 0 6px rgba(34,197,94,.00)} 100%{box-shadow:0 0 0 0 rgba(34,197,94,.00)} }
        @keyframes barShimmer { 0%{background-position:-600px 0} 100%{background-position:600px 0} }
        @keyframes fadeNum { 0%{opacity:.4;transform:translateY(2px)} 100%{opacity:1;transform:translateY(0)} }
      `}</style>
      <div style={{ position:'relative',overflow:'hidden',background:'linear-gradient(90deg,#0D1F6B 0%,#00338D 32%,#5B4EA0 66%,#8C3075 100%)',display:'flex',alignItems:'stretch',borderBottom:'2px solid rgba(0,0,0,.18)',fontFamily:F,flexShrink:0 }}>
        <div style={{ position:'absolute',inset:0,pointerEvents:'none',background:'linear-gradient(90deg,transparent 0%,rgba(255,255,255,.04) 50%,transparent 100%)',backgroundSize:'600px 100%',animation:'barShimmer 3s linear infinite' }} />
        <div style={{ display:'flex',alignItems:'center',gap:8,padding:'0 22px',borderRight:'1px solid rgba(255,255,255,.14)',flexShrink:0,position:'relative',zIndex:1 }}>
          <span style={{ display:'block',width:8,height:8,borderRadius:'50%',background:'#22C55E',flexShrink:0,animation:'livePulse 2s ease-out infinite' }} />
          <span style={{ fontSize:11,fontWeight:700,color:'white',letterSpacing:'.10em',fontFamily:F }}>LIVE</span>
        </div>
        {items.map((item,i) => (
          <div key={i} style={{ display:'flex',alignItems:'center',gap:10,padding:'11px 24px',borderRight:i<items.length-1?'1px solid rgba(255,255,255,.11)':'none',flex:1,position:'relative',zIndex:1 }}>
            <span style={{ fontSize:11,color:'rgba(255,255,255,.48)',whiteSpace:'nowrap',fontFamily:F }}>{item.label}</span>
            <span key={`${item.value}-${tick}`} style={{ fontSize:16,fontWeight:600,color:'white',fontFamily:MONO,letterSpacing:'-.01em',animation:'fadeNum .4s ease' }}>{item.value}</span>
          </div>
        ))}
        <div style={{ display:'flex',alignItems:'center',padding:'0 18px',borderLeft:'1px solid rgba(255,255,255,.10)',flexShrink:0,position:'relative',zIndex:1 }}>
          <span style={{ fontSize:10,color:'rgba(255,255,255,.32)',fontFamily:MONO }}>{new Date().toLocaleTimeString('en-GB',{hour:'2-digit',minute:'2-digit'})}</span>
        </div>
      </div>
    </>
  )
}

// ── KpiCard ───────────────────────────────────────────────────────────────────
// CHANGED: added left accent border (3px solid C.blue) to mirror OpCard's
// left-border treatment. Tightened border-radius from 6→4 to match OpCard.
// Increased padding slightly for better visual weight parity.
function KpiCard({ icon: Icon, value, label }) {
  const [hov, setHov] = useState(false)
  return (
    <div
      onMouseEnter={() => setHov(true)}
      onMouseLeave={() => setHov(false)}
      style={{
        background: hov ? C.pageBg : C.surface,
        border: `1px solid ${hov ? C.blueBorder : C.border}`,
        borderLeft: `3px solid ${C.blue}`,       // ← mirrors OpCard accent strip
        borderRadius: 4,                          // ← matches OpCard radius
        padding: '14px 14px 14px 12px',
        transition: 'background 0.13s, border-color 0.13s',
        fontFamily: F,
        boxShadow: hov ? '0 2px 8px rgba(0,51,141,.07)' : '0 1px 3px rgba(0,0,0,.04)', // ← subtle shadow matching OpCard at rest
      }}
    >
      <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between', marginBottom: 10 }}>
        <span style={{ fontSize:10, fontWeight:600, color:C.textFaint, textTransform:'uppercase', letterSpacing:'0.07em' }}>
          {label}
        </span>
        <Icon style={{ width:12, height:12, color:C.textFaint }} />
      </div>
      <div style={{ fontSize:22, fontWeight:700, color:C.text, lineHeight:1, fontFamily:MONO, letterSpacing:'-0.03em' }}>
        {value}
      </div>
    </div>
  )
}

// ── QuickRow ──────────────────────────────────────────────────────────────────
function QuickRow({ title, sub, count, onClick, isLast }) {
  const [hov, setHov] = useState(false)
  return (
    <div
      onClick={onClick}
      onMouseEnter={() => setHov(true)}
      onMouseLeave={() => setHov(false)}
      style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        padding: '13px 0',
        borderBottom: isLast ? 'none' : `1px solid ${C.divider}`,
        cursor: 'pointer',
        fontFamily: F,
      }}
    >
      <div style={{ flex:1, minWidth:0 }}>
        <div style={{
          fontSize: 13,
          fontWeight: 500,
          color: hov ? C.blue : C.text,
          transition: 'color 0.13s',
          marginBottom: 2,
          fontFamily: F,
        }}>
          {title}
        </div>
        <div style={{ fontSize:11, color:C.textFaint, lineHeight:1.4, fontFamily:F }}>
          {sub}
        </div>
      </div>
      <div style={{ display:'flex', alignItems:'center', gap:8, flexShrink:0, marginLeft:16 }}>
        {count != null && (
          <span style={{ fontSize:12, fontWeight:700, color:C.blue, fontFamily:MONO }}>
            {count}
          </span>
        )}
        <span style={{ fontSize:12, color:C.textFaint }}>→</span>
      </div>
    </div>
  )
}

// ── OpCard ────────────────────────────────────────────────────────────────────
function OpCard({ tender }) {
  const navigate = useNavigate()
  const [hov, setHov] = useState(false)
  const p      = tender.p_go
  const pct    = p != null ? Math.round(p * 100) : null
  const days   = tender.days_to_deadline
  const urgent = days != null && days <= 8
  const budget = tender.budget
    ? tender.budget >= 1e6 ? `$${(tender.budget/1e6).toFixed(1)}M` : `$${(tender.budget/1e3).toFixed(0)}K`
    : null

  return (
    <div onClick={() => navigate(`/tenders/${tender.id}`)}
      onMouseEnter={() => setHov(true)} onMouseLeave={() => setHov(false)}
      style={{ width:258,flexShrink:0,border:`1px solid ${hov?C.blueBorder:C.border}`,borderRadius:4,cursor:'pointer',background:C.surface,boxShadow:hov?'0 4px 16px rgba(0,51,141,.10)':'0 1px 3px rgba(0,0,0,.05)',display:'flex',flexDirection:'column',transition:'all .16s ease',fontFamily:F,overflow:'hidden' }}>
      <div style={{ background:scoreGradient(p),height:140,display:'flex',flexDirection:'column',justifyContent:'space-between',padding:'14px 16px',position:'relative',overflow:'hidden' }}>
        <div style={{ display:'flex',alignItems:'flex-start',justifyContent:'space-between',position:'relative',zIndex:1 }}>
          <div style={{ fontSize:9,fontWeight:700,color:'rgba(255,255,255,.55)',textTransform:'uppercase',letterSpacing:'.10em',fontFamily:F,maxWidth:140,overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap' }}>
            {(tender.funding_agency||tender.source_portal||'').toUpperCase()}
          </div>
          {pct != null && (
            <div style={{ textAlign:'right' }}>
              <div style={{ fontSize:28,fontWeight:700,color:'white',lineHeight:1,fontFamily:MONO,letterSpacing:'-.03em' }}>{pct}%</div>
              <div style={{ fontSize:9,color:'rgba(255,255,255,.55)',textTransform:'uppercase',letterSpacing:'.07em',marginTop:2,fontFamily:F }}>{scoreLabel(p)}</div>
            </div>
          )}
        </div>
        <div style={{ fontSize:11,color:'rgba(255,255,255,.70)',position:'relative',zIndex:1,fontFamily:F }}>
          {tender.country_name_normalized||'—'}{tender.source_portal?` · ${tender.source_portal.toUpperCase()}`:''}
        </div>
      </div>
      <div style={{ flex:1,padding:'14px 16px 20px',display:'flex',flexDirection:'column',gap:8,borderLeft:`3px solid ${C.blue}` }}>
        <h3 style={{ fontSize:13,fontWeight:600,color:C.text,lineHeight:1.4,margin:0,fontFamily:F,display:'-webkit-box',WebkitLineClamp:2,WebkitBoxOrient:'vertical',overflow:'hidden' }}>
          {tender.title_clean||'Untitled'}
        </h3>
        {(tender.llm_scope_summary || tender.description_clean) && (
          <p style={{ fontSize:11,color:C.textMuted,lineHeight:1.6,margin:0,fontFamily:F,display:'-webkit-box',WebkitLineClamp:5,WebkitBoxOrient:'vertical',overflow:'hidden' }}>
            {tender.llm_scope_summary || tender.description_clean}
          </p>
        )}
        <div style={{ flex:1 }} />
        <div style={{ display:'flex',alignItems:'center',gap:6,flexWrap:'wrap',paddingTop:8,borderTop:`1px solid ${C.divider}` }}>
          {tender.procurement_group && (
            <span style={{ fontSize:10,fontWeight:500,color:C.textMuted,background:C.pageBg,padding:'2px 7px',borderRadius:2,border:`1px solid ${C.border}`,textTransform:'uppercase',letterSpacing:'.05em' }}>
              {tender.procurement_group}
            </span>
          )}
          {budget && <span style={{ fontSize:11,fontWeight:600,color:C.textMid,fontFamily:MONO }}>{budget}</span>}
          {days != null && (
            <span style={{ display:'flex',alignItems:'center',gap:3,fontSize:11,fontWeight:urgent?600:400,color:urgent?'#B91C1C':C.textFaint,marginLeft:'auto',fontFamily:F }}>
              {urgent && <AlertTriangle style={{ width:10,height:10,color:'#B91C1C' }} />}{days}d left
            </span>
          )}
        </div>
        <div style={{ display:'flex',alignItems:'center',gap:4,fontSize:12,fontWeight:500,color:hov?C.blue:C.textMuted,fontFamily:F,transition:'color .14s',marginTop:2 }}>
          Review opportunity <ArrowRight style={{ width:13,height:13 }} />
        </div>
      </div>
    </div>
  )
}

// ── PriorityCarousel ──────────────────────────────────────────────────────────
function PriorityCarousel({ items }) {
  const trackRef = useRef(null)
  const [canLeft,  setCanLeft]  = useState(false)
  const [canRight, setCanRight] = useState(false)

  function updateArrows() {
    const el = trackRef.current; if (!el) return
    setCanLeft(el.scrollLeft > 8)
    setCanRight(el.scrollLeft + el.clientWidth < el.scrollWidth - 8)
  }
  useEffect(() => {
    const el = trackRef.current; if (!el) return
    el.addEventListener('scroll', updateArrows, { passive: true })
    updateArrows()
    return () => el.removeEventListener('scroll', updateArrows)
  }, [items])
  function scroll(dir) { trackRef.current?.scrollBy({ left: dir * 290, behavior: 'smooth' }) }

  const Btn = ({ dir, on }) => (
    <button onClick={() => scroll(dir)} disabled={!on}
      style={{ width:30,height:30,borderRadius:'50%',border:`1px solid ${on?C.border:'transparent'}`,background:on?C.surface:'transparent',display:'flex',alignItems:'center',justifyContent:'center',cursor:on?'pointer':'default',color:on?C.textMid:C.border,boxShadow:on?'0 1px 4px rgba(0,0,0,.08)':'none',transition:'all .12s',flexShrink:0 }}
      onMouseEnter={e=>{ if(on) e.currentTarget.style.borderColor=C.blueBorder }}
      onMouseLeave={e=>{ if(on) e.currentTarget.style.borderColor=C.border }}>
      {dir===-1?<ChevronLeft style={{ width:13,height:13 }} />:<ChevronRight style={{ width:13,height:13 }} />}
    </button>
  )

  return (
    <div style={{ position:'relative' }}>
      <div style={{ position:'absolute',top:-40,right:0,display:'flex',gap:6 }}>
        <Btn dir={-1} on={canLeft} /><Btn dir={1} on={canRight} />
      </div>
      <div ref={trackRef}
        style={{ display:'flex',gap:16,overflowX:'auto',overflowY:'visible',paddingBottom:12,scrollbarWidth:'none',msOverflowStyle:'none',WebkitOverflowScrolling:'touch',cursor:'grab' }}
        onMouseDown={e => {
          const el=trackRef.current; if(!el) return
          const startX=e.pageX-el.offsetLeft, sl=el.scrollLeft
          el.style.cursor='grabbing'
          const mv=ev=>{ el.scrollLeft=sl-(ev.pageX-el.offsetLeft-startX) }
          const up=()=>{ el.style.cursor='grab'; document.removeEventListener('mousemove',mv); document.removeEventListener('mouseup',up) }
          document.addEventListener('mousemove',mv); document.addEventListener('mouseup',up)
        }}>
        {items.map(t => <OpCard key={t.id} tender={t} />)}
      </div>
      {canRight && (
        <div style={{ position:'absolute',right:0,top:0,bottom:12,width:14,background:'linear-gradient(270deg,#ffffff,transparent)',pointerEvents:'none' }} />
      )}
    </div>
  )
}

// ── Main ──────────────────────────────────────────────────────────────────────
export default function Today() {
  const { user }    = useAuth()
  const navigate    = useNavigate()
  const priorityRef = useRef(null)

  const [loading,   setLoading]   = useState(true)
  const [stats,     setStats]     = useState({})
  const [todayData, setTodayData] = useState(null)

  useEffect(() => {
    async function load() {
      try {
        const now=new Date(), in7=new Date(now.getTime()+7*86400000)
        const [todayRes,openRes,closedRes] = await Promise.all([
          api.get('/tenders/today'),
          api.get('/tenders',{params:{status:'open',per_page:100,sort_by:'p_go'}}),
          api.get('/tenders',{params:{status:'closed', per_page:100, sort_by:'publication_datetime'}}),
        ])
        const openItems=openRes.data.items
        const closedItems = closedRes.data.items
        const seen = new Set()
        const allItems = [...openItems, ...closedItems].filter(t => {
          if (seen.has(t.id)) return false
          seen.add(t.id)
          return true
         })
        const today = todayRes.data
        const pipelineValue    = openItems.reduce((s,t)=>s+(t.budget||0),0)
        const scored           = openItems.filter(t=>t.p_go!=null).sort((a,b)=>b.p_go-a.p_go)
        const bestItem         = scored[0]
        const closingThisWeek  = openItems.filter(t=>{ if(!t.deadline_datetime) return false; const d=new Date(t.deadline_datetime.replace(' ','T')); return d>=now&&d<=in7 }).length
        const closestDays      = (()=>{ const w=openItems.filter(t=>t.deadline_datetime).sort((a,b)=>new Date(a.deadline_datetime)-new Date(b.deadline_datetime)); return w[0]?Math.ceil((new Date(w[0].deadline_datetime.replace(' ','T'))-now)/86400000):null })()
        const pendingDecisions = allItems.filter(t=>t.partner_decision&&t.deadline_datetime&&new Date(t.deadline_datetime.replace(' ','T'))>=now).length
        const awaitingDecision = openItems.filter(t=>!t.partner_decision&&t.p_go>=0.7).length
        const mapTenders       = openItems.filter(t=>t.p_go>=0.60&&t.country_name_normalized).sort((a,b)=>b.p_go-a.p_go).slice(0,40)
        const decidedTenders   = allItems.filter(t=>t.partner_decision).sort((a,b)=>new Date(b.decided_at||0)-new Date(a.decided_at||0))
        setTodayData(today)
        setStats({ active:openRes.data.total, strongGo:today.strong_go?.length??0, pipelineValue, pendingDecisions, closingThisWeek, closestDays, bestScore:bestItem?.p_go??null, bestTitle:bestItem?.title_clean??null, awaitingDecision, topBudget:bestItem?.budget??null, mapTenders, decidedTenders })
      } catch(e){ console.error(e) } finally { setLoading(false) }
    }
    load()
  }, [])

  const firstName   = user?.full_name?.split(' ')[1] || user?.full_name?.split(' ')[0] || user?.email?.split('@')[0] || 'there'
  const insight     = !loading ? buildInsight(stats) : null
  const bestPct     = stats.bestScore != null ? Math.round(stats.bestScore * 100) : null
  const priorityAll = todayData ? [...(todayData.strong_go||[]),...(todayData.go||[])] : []

  return (
    <div style={{ display:'flex',flexDirection:'column',minHeight:'100%',background:C.surface,fontFamily:F }}>

      {/* Intel strip */}
      <IntelStrip />

      {/* Body */}
      {/* CHANGED: reduced top padding 40→32, bottom 64→48 to tighten overall density */}
      <div style={{ flex:1,background:C.surface,padding:'36px 48px 56px' }}>
        {loading ? (
          <div style={{ display:'flex',justifyContent:'center',padding:'80px 0' }}>
            <Spinner size="lg" />
          </div>
        ) : (
          <div style={{ maxWidth:1060 }}>

            {/* ── Section 1: Morning Briefing ── */}
            <div style={{ background:C.surface, border:`1px solid ${C.border}`, borderRadius:4, overflow:'hidden', marginBottom:32, boxShadow:'0 1px 3px rgba(0,0,0,.05)' }}>

              {/* Header strip */}
              <div style={{ background:'#EEF2FF', borderBottom:'1px solid #BFDBFE', padding:'11px 28px', display:'flex', alignItems:'center', justifyContent:'space-between' }}>
                <div style={{ display:'flex', alignItems:'center', gap:12 }}>
                  <span style={{ fontSize:11, fontWeight:700, color:C.blue, letterSpacing:'0.07em', textTransform:'uppercase', fontFamily:F }}>Morning Briefing</span>
                  <span style={{ width:1, height:10, background:'#BFDBFE', flexShrink:0 }} />
                  <span style={{ fontSize:11, color:C.textMuted, fontFamily:F }}>{todayLabel()}</span>
                </div>
                <div style={{ display:'flex', alignItems:'center', gap:6 }}>
                  <span style={{ width:6, height:6, borderRadius:'50%', background:'#22C55E', display:'block' }} />
                  <span style={{ fontSize:10, color:C.textMuted, fontFamily:F }}>Live pipeline</span>
                </div>
              </div>

              {/* Body — 3 columns */}
              <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr 220px', gap:0 }}>

                {/* Col 1 — Insight */}
                <div style={{ padding:'28px', borderRight:`1px solid ${C.border}` }}>
                  <div style={{ fontSize:10, fontWeight:700, color:C.textFaint, textTransform:'uppercase', letterSpacing:'0.08em', marginBottom:14, fontFamily:F }}>
                    Pipeline Insight
                  </div>
                  {insight && (
                    <>
                      <div style={{ fontSize:13, fontWeight:600, color:C.blue, lineHeight:1.4, marginBottom:12, fontFamily:F }}>
                        {insight.headline}
                      </div>
                      <p style={{ fontSize:12, color:C.textMuted, lineHeight:1.85, margin:0, fontFamily:F }}>
                        {insight.detail}
                      </p>
                    </>
                  )}
                  <div style={{ marginTop:20, paddingTop:16, borderTop:`1px solid ${C.divider}` }}>
                    <span style={{ fontSize:11, color:C.textFaint, fontFamily:F }}>
                      {greeting()}, <span style={{ fontWeight:600, color:C.text }}>{firstName}</span>.
                    </span>
                  </div>
                </div>

                {/* Col 2 — KPI grid */}
                <div style={{ padding:'28px', borderRight:`1px solid ${C.border}` }}>
                  <div style={{ fontSize:10, fontWeight:700, color:C.textFaint, textTransform:'uppercase', letterSpacing:'0.08em', marginBottom:14, fontFamily:F }}>
                    Today's KPIs
                  </div>
                  <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:12 }}>
                    {bestPct != null && <KpiCard icon={TrendingUp} value={`${bestPct}%`} label="Top score" />}
                    {stats.closestDays != null && <KpiCard icon={Clock} value={`${stats.closestDays}d`} label="Next deadline" />}
                    {stats.awaitingDecision != null && <KpiCard icon={AlertCircle} value={stats.awaitingDecision} label="Awaiting review" />}
                    {stats.topBudget > 0 && <KpiCard icon={DollarSign} value={fmtM(stats.topBudget)} label="Top budget" />}
                  </div>
                </div>

                {/* Col 3 — Quick Access */}
                <div style={{ display:'flex', flexDirection:'column' }}>
                  <div style={{ padding:'28px 24px 12px' }}>
                    <div style={{ fontSize:10, fontWeight:700, color:C.textFaint, textTransform:'uppercase', letterSpacing:'0.08em', fontFamily:F }}>Quick Access</div>
                  </div>
                  <div style={{ padding:'0 24px', flex:1, display:'flex', flexDirection:'column', justifyContent:'center' }}>
                    <QuickRow
                      title="Today's recommendations"
                      sub={priorityAll.length > 0 ? `${priorityAll.length} priorit${priorityAll.length > 1 ? 'y opportunities' : 'y opportunity'}` : 'No priority opportunities'}
                      count={priorityAll.length > 0 ? priorityAll.length : null}
                      onClick={() => priorityRef.current?.scrollIntoView({ behavior:'smooth', block:'start' })}
                    />
                    <QuickRow
                      title="Team decisions"
                      sub={stats.awaitingDecision > 0 ? `${stats.awaitingDecision} pending review` : 'All reviewed'}
                      count={stats.awaitingDecision > 0 ? stats.awaitingDecision : null}
                      onClick={() => navigate('/decisions')}
                    />
                    <QuickRow
                      title="Intelligence dashboard"
                      sub="Pipeline KPIs & market trends"
                      onClick={() => navigate('/dashboard')}
                      isLast
                    />
                  </div>
                </div>

              </div>
            </div>

            {/* ── Section 1.5: Africa map + Recent Endorsements ── */}
            {(stats.mapTenders?.length > 0 || stats.decidedTenders?.length > 0) && (
              <div style={{ display:'grid', gridTemplateColumns:'1fr 320px', gap:24, marginBottom:32 }}>

                {/* Africa map */}
                <SectionCard label="Pipeline Map" meta="Africa · Score ≥ 60%" noPad>
                  <div style={{ height:320 }}>
                    <AfricaMap
                      tenders={stats.mapTenders||[]}
                      onPinClick={t=>navigate(`/tenders/${t.id}`)}
                    />
                  </div>
                </SectionCard>

                {/* Recent Endorsements */}
                <SectionCard
                  label="Recent Endorsements"
                  meta={`${stats.decidedTenders?.length||0} total`}
                  action={{ label:'All ›', onClick:()=>navigate('/decisions') }}
                  noPad
                >
                  <div style={{ maxHeight:320, overflowY:'auto' }}>
                    <EndorsementsPanel decided={stats.decidedTenders||[]} navigate={navigate} />
                  </div>
                </SectionCard>
              </div>
            )}

            {/* ── Section 2: Priority Opportunities ── */}
            {priorityAll.length > 0 && (
              <div ref={priorityRef} style={{ scrollMarginTop:32 }}>
                {/*
                  CHANGED: replaced the heavy <hr>+marginBottom:32 divider with a
                  lighter container that uses the same border/bg/strip treatment as
                  Section 1 — giving both sections a unified card-within-page feel.
                  The section header now mirrors the Morning Briefing context strip.
                */}
                <div style={{
                  background: C.surface,
                  border: `1px solid ${C.border}`,
                  borderRadius: 4,
                  overflow: 'hidden',
                  boxShadow: '0 1px 3px rgba(0,0,0,.05)',
                }}>
                  {/* Section header strip — light blue, matching tenders panel style */}
                  <div style={{ background:'#EEF2FF', borderBottom:`1px solid #BFDBFE`, padding:'9px 24px', display:'flex', alignItems:'center', justifyContent:'space-between' }}>
                    <div style={{ display:'flex', alignItems:'center', gap:12 }}>
                      <span style={{ fontSize:11,fontWeight:600,color:C.blue,letterSpacing:'0.07em',textTransform:'uppercase',fontFamily:F }}>
                        Priority Opportunities
                      </span>
                      <span style={{ width:1,height:10,background:'#BFDBFE',flexShrink:0 }} />
                      <span style={{ fontSize:11,color:C.textMuted,fontFamily:F }}>
                        Ranked by strategic fit · {priorityAll.length} total
                      </span>
                    </div>
                    <button onClick={() => navigate('/tenders')}
                      style={{ fontSize:11,fontWeight:500,color:C.textMuted,background:'none',border:'none',cursor:'pointer',fontFamily:F,flexShrink:0,transition:'color .12s' }}
                      onMouseEnter={e=>e.currentTarget.style.color=C.blue}
                      onMouseLeave={e=>e.currentTarget.style.color=C.textMuted}>
                      View all ›
                    </button>
                  </div>

                  {/* Carousel body — same horizontal padding as briefing inner padding */}
                  <div style={{ padding:'28px 28px 20px' }}>
                    <PriorityCarousel items={priorityAll} />
                  </div>
                </div>
              </div>
            )}

          </div>
        )}
      </div>

      <PlatformFooter />

    </div>
  )
}