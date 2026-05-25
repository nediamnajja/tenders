// // src/pages/Dashboard.jsx
// import { useEffect, useState, useRef } from 'react'
// import { useNavigate } from 'react-router-dom'
// import { TrendingUp, Globe, Zap, Clock, CheckCircle, XCircle } from 'lucide-react'
// import api from '../lib/api'
// import { Spinner } from '../components/ui'
// import PlatformFooter from '../components/layout/PlatformFooter'
// import IntelStrip from '../components/layout/IntelStrip'

// const C = {
//   blue:      '#00338D',
//   accent:    '#0091DA',
//   navy:      '#0D1F6B',
//   teal:      '#0F766E',
//   amber:     '#342471',
//   purple:    '#7E22CE',
//   green:     '#15803D',
//   red:       '#B91C1C',
//   text:      '#111827',
//   textMid:   '#374151',
//   textMuted: '#6B7280',
//   textFaint: '#9CA3AF',
//   border:    '#E5E7EB',
//   divider:   '#F3F4F6',
//   surface:   '#FFFFFF',
//   pageBg:    '#F8FAFC',
//   blueBorder:'#C3D9F2',
// }
// const F    = "'Inter', system-ui, -apple-system, sans-serif"
// const MONO = "'DM Mono', 'JetBrains Mono', ui-monospace, monospace"
// const NOTES_CACHE_KEY = 'dashboard_market_notes'
// const NOTES_TTL_MS    = 7 * 24 * 60 * 60 * 1000

// function fmtM(v) {
//   if (!v) return '—'
//   if (v >= 1e9) return `$${(v / 1e9).toFixed(1)}B`
//   if (v >= 1e6) return `$${(v / 1e6).toFixed(1)}M`
//   if (v >= 1e3) return `$${(v / 1e3).toFixed(0)}K`
//   return `$${v}`
// }

// // ── Country coords ─────────────────────────────────────────────────────────────
// const COUNTRY_COORDS = {
//   'Nigeria':{ lng:8.68, lat:9.08 }, 'Kenya':{ lng:37.91, lat:-0.02 },
//   'Ethiopia':{ lng:40.49, lat:9.15 }, 'Ghana':{ lng:-1.02, lat:7.95 },
//   'Tanzania':{ lng:34.89, lat:-6.37 }, 'Uganda':{ lng:32.29, lat:1.37 },
//   'Senegal':{ lng:-14.45, lat:14.50 }, 'Cameroon':{ lng:12.35, lat:3.85 },
//   "Côte d'Ivoire":{ lng:-5.55, lat:7.54 }, 'Ivory Coast':{ lng:-5.55, lat:7.54 },
//   'Mozambique':{ lng:35.53, lat:-18.67 }, 'Zambia':{ lng:27.85, lat:-13.13 },
//   'Zimbabwe':{ lng:29.15, lat:-19.02 }, 'Rwanda':{ lng:29.87, lat:-1.94 },
//   'Mali':{ lng:-2.00, lat:17.57 }, 'Niger':{ lng:8.08, lat:17.61 },
//   'Burkina Faso':{ lng:-1.56, lat:12.36 }, 'Guinea':{ lng:-11.31, lat:11.80 },
//   'Congo':{ lng:15.83, lat:-0.23 }, 'DRC':{ lng:23.66, lat:-4.04 },
//   'Democratic Republic of the Congo':{ lng:23.66, lat:-4.04 },
//   'Angola':{ lng:17.87, lat:-11.20 }, 'Malawi':{ lng:34.30, lat:-13.25 },
//   'South Africa':{ lng:22.94, lat:-30.56 }, 'Egypt':{ lng:30.80, lat:26.82 },
//   'Morocco':{ lng:-7.09, lat:31.79 }, 'Tunisia':{ lng:9.54, lat:33.89 },
//   'Sudan':{ lng:30.22, lat:12.86 }, 'Somalia':{ lng:46.20, lat:5.15 },
//   'Madagascar':{ lng:46.87, lat:-18.77 }, 'Togo':{ lng:0.82, lat:8.62 },
//   'Benin':{ lng:2.32, lat:9.31 }, 'Chad':{ lng:18.73, lat:15.45 },
//   'Liberia':{ lng:-9.43, lat:6.43 }, 'Sierra Leone':{ lng:-11.78, lat:8.46 },
//   'Namibia':{ lng:18.49, lat:-22.96 }, 'Botswana':{ lng:24.68, lat:-22.33 },
//   'Mauritania':{ lng:-10.94, lat:21.01 }, 'Libya':{ lng:17.23, lat:26.34 },
//   'Algeria':{ lng:1.66, lat:28.03 }, 'Gabon':{ lng:11.61, lat:-0.80 },
//   'Guinea-Bissau':{ lng:-15.18, lat:11.80 }, 'Eritrea':{ lng:39.78, lat:15.18 },
//   'South Sudan':{ lng:31.30, lat:6.88 }, 'Burundi':{ lng:29.92, lat:-3.37 },
//   'Central African Republic':{ lng:20.94, lat:6.61 },
// }
// const AFRICA = { minLng:-25, maxLng:55, minLat:-40, maxLat:42 }

// function toXY(lng, lat, w, h) {
//   return {
//     x: ((lng - AFRICA.minLng) / (AFRICA.maxLng - AFRICA.minLng)) * w,
//     y: ((AFRICA.maxLat - lat) / (AFRICA.maxLat - AFRICA.minLat)) * h,
//   }
// }
// function pinColor(score) {
//   if (score >= 0.80) return C.blue
//   if (score >= 0.60) return C.accent
//   return '#F59E0B'
// }

// // ── Africa Map ─────────────────────────────────────────────────────────────────
// function AfricaMap({ tenders, onPinClick }) {
//   const canvasRef    = useRef(null)
//   const maskDataRef  = useRef(null)
//   const maskReadyRef = useRef(false)
//   const animRef      = useRef(null)
//   const pinsRef      = useRef([])
//   const [tooltip, setTooltip] = useState(null)
//   const [pins, setPins]       = useState([])

//   useEffect(() => { pinsRef.current = pins }, [pins])

//   useEffect(() => {
//     if (!tenders?.length) return
//     async function buildPins() {
//       const resolved = await Promise.all(tenders.map(async t => {
//         const name = t.country_name_normalized; if (!name) return null
//         const coords = COUNTRY_COORDS[name] || await (async () => {
//           try {
//             const res = await fetch(`https://restcountries.com/v3.1/name/${encodeURIComponent(name)}?fields=latlng`)
//             const d = await res.json()
//             if (Array.isArray(d) && d[0]?.latlng?.length === 2) {
//               const c = { lat: d[0].latlng[0], lng: d[0].latlng[1] }
//               COUNTRY_COORDS[name] = c; return c
//             }
//           } catch {} return null
//         })()
//         if (!coords) return null
//         if (coords.lng < AFRICA.minLng || coords.lng > AFRICA.maxLng || coords.lat < AFRICA.minLat || coords.lat > AFRICA.maxLat) return null
//         return { tender: t, score: t.p_go ?? 0, ...coords }
//       }))
//       setPins(resolved.filter(Boolean).slice(0, 30))
//     }
//     buildPins()
//   }, [tenders?.length])

//   useEffect(() => {
//     const canvas = canvasRef.current; if (!canvas) return
//     const ctx = canvas.getContext('2d')
//     const OMASK_W = 600, OMASK_H = 400
//     function resize() {
//       const rect = canvas.getBoundingClientRect()
//       if (rect.width > 0 && rect.height > 0) { canvas.width = rect.width; canvas.height = rect.height }
//     }
//     resize()
//     const ro = new ResizeObserver(resize); ro.observe(canvas)
//     window.addEventListener('resize', resize)
//     const offMask = document.createElement('canvas')
//     offMask.width = OMASK_W; offMask.height = OMASK_H
//     const omc = offMask.getContext('2d')
//     const img = new Image(); img.crossOrigin = 'anonymous'; img.src = '/world-map-mask.png'
//     img.onload = () => {
//       const iw = img.naturalWidth || img.width, ih = img.naturalHeight || img.height
//       const sx = ((AFRICA.minLng + 180) / 360) * iw
//       const sy = ((90 - AFRICA.maxLat) / 180) * ih + (ih * 0.14)
//       const sw = ((AFRICA.maxLng - AFRICA.minLng) / 360) * iw
//       const sh = ((AFRICA.maxLat - AFRICA.minLat) / 180) * ih
//       omc.drawImage(img, sx, sy, sw, sh, 0, 0, OMASK_W, OMASK_H)
//       maskDataRef.current = omc.getImageData(0, 0, OMASK_W, OMASK_H)
//       maskReadyRef.current = true
//     }
//     img.onerror = () => { maskReadyRef.current = true }
//     function isLand(px, py, w, h) {
//       if (!maskDataRef.current) return false
//       const mx = Math.round((px / w) * OMASK_W), my = Math.round((py / h) * OMASK_H)
//       if (mx < 0 || mx >= OMASK_W || my < 0 || my >= OMASK_H) return false
//       return maskDataRef.current.data[(my * OMASK_W + mx) * 4] > 128
//     }
//     let t0 = null
//     function draw(ts) {
//       if (!t0) t0 = ts
//       const w = canvas.width, h = canvas.height
//       ctx.clearRect(0, 0, w, h)
//       if (maskReadyRef.current) {
//         ctx.fillStyle = 'rgba(0,51,141,0.22)'
//         for (let px = 0; px < w; px += 4)
//           for (let py = 0; py < h; py += 4)
//             if (isLand(px, py, w, h)) { ctx.beginPath(); ctx.arc(px, py, 1.4, 0, Math.PI * 2); ctx.fill() }
//       }
//       pinsRef.current.forEach(pin => {
//         const { x, y } = toXY(pin.lng, pin.lat, w, h)
//         const col = pinColor(pin.score), pulse = 0.5 + 0.5 * Math.sin((ts - t0) / 900 + pin.lng)
//         ctx.beginPath(); ctx.arc(x, y, 7 + pulse * 4, 0, Math.PI * 2)
//         ctx.strokeStyle = col + '44'; ctx.lineWidth = 1; ctx.stroke()
//         ctx.beginPath(); ctx.arc(x, y, 5, 0, Math.PI * 2)
//         ctx.fillStyle = col; ctx.fill(); ctx.strokeStyle = '#fff'; ctx.lineWidth = 1.5; ctx.stroke()
//       })
//       animRef.current = requestAnimationFrame(draw)
//     }
//     animRef.current = requestAnimationFrame(draw)
//     return () => { cancelAnimationFrame(animRef.current); ro.disconnect(); window.removeEventListener('resize', resize) }
//   }, [])

//   function onMouseMove(e) {
//     const canvas = canvasRef.current; if (!canvas) return
//     const rect = canvas.getBoundingClientRect()
//     const mx = e.clientX - rect.left, my = e.clientY - rect.top
//     const w = canvas.width, h = canvas.height
//     let found = null
//     for (const pin of pinsRef.current) {
//       const { x, y } = toXY(pin.lng, pin.lat, w, h)
//       if (Math.hypot(mx - x, my - y) < 10) { found = { x: e.clientX, y: e.clientY, tender: pin.tender }; break }
//     }
//     setTooltip(found); canvas.style.cursor = found ? 'pointer' : 'default'
//   }

//   return (
//     <div style={{ position: 'relative', width: '100%', height: '100%' }}>
//       <canvas ref={canvasRef} onMouseMove={onMouseMove} onMouseLeave={() => setTooltip(null)}
//         onClick={() => tooltip?.tender && onPinClick(tooltip.tender)}
//         style={{ width: '100%', height: '100%', display: 'block' }} />
//       <div style={{ position: 'absolute', bottom: 10, left: 12, display: 'flex', gap: 8, background: 'rgba(255,255,255,.92)', borderRadius: 4, padding: '4px 10px', border: `1px solid ${C.border}` }}>
//         {[{ color: C.blue, label: '≥ 80%' }, { color: C.accent, label: '60–79%' }, { color: '#F59E0B', label: 'Marginal' }].map(l => (
//           <div key={l.label} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
//             <div style={{ width: 6, height: 6, borderRadius: '50%', background: l.color }} />
//             <span style={{ fontSize: 9, color: C.textMuted, fontFamily: F }}>{l.label}</span>
//           </div>
//         ))}
//       </div>
//       {tooltip && (
//         <div style={{ position: 'fixed', left: tooltip.x + 12, top: tooltip.y - 8, zIndex: 999, background: C.navy, color: '#fff', borderRadius: 4, padding: '8px 12px', fontSize: 11, fontFamily: F, maxWidth: 220, pointerEvents: 'none', boxShadow: '0 4px 16px rgba(0,0,0,.22)' }}>
//           <div style={{ fontWeight: 600, marginBottom: 3, lineHeight: 1.4 }}>{(tooltip.tender.title_clean || 'Untitled').slice(0, 55)}{(tooltip.tender.title_clean || '').length > 55 ? '…' : ''}</div>
//           <div style={{ color: 'rgba(255,255,255,.55)', fontSize: 10 }}>{tooltip.tender.country_name_normalized} · {tooltip.tender.p_go != null ? Math.round(tooltip.tender.p_go * 100) + '%' : '—'}</div>
//           <div style={{ marginTop: 4, fontSize: 10, color: C.accent }}>Click to open →</div>
//         </div>
//       )}
//     </div>
//   )
// }

// // ── Monthly Activity Bar Chart ─────────────────────────────────────────────────
// function MonthlyChart({ data }) {
//   const canvasRef = useRef(null)
//   const chartRef  = useRef(null)

//   useEffect(() => {
//     if (!canvasRef.current || !data?.length) return
//     // Lazy-load Chart.js
//     const script = document.createElement('script')
//     script.src = 'https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js'
//     script.onload = () => {
//       if (chartRef.current) chartRef.current.destroy()
//       chartRef.current = new window.Chart(canvasRef.current, {
//         type: 'bar',
//         data: {
//           labels: data.map(d => d.label),
//           datasets: [
//             {
//               label: 'New tenders',
//               data: data.map(d => d.new),
//               backgroundColor: '#BFDBFE',
//               borderRadius: 3,
//               borderSkipped: false,
//             },
//             {
//               label: 'Endorsed',
//               data: data.map(d => d.endorsed),
//               backgroundColor: '#00338D',
//               borderRadius: 3,
//               borderSkipped: false,
//             },
//           ],
//         },
//         options: {
//           responsive: true,
//           maintainAspectRatio: false,
//           plugins: { legend: { display: false }, tooltip: { mode: 'index', intersect: false } },
//           scales: {
//             x: { grid: { display: false }, ticks: { font: { size: 10 }, color: '#9CA3AF' } },
//             y: { grid: { color: '#F3F4F6' }, ticks: { font: { size: 10 }, color: '#9CA3AF' }, border: { display: false } },
//           },
//         },
//       })
//     }
//     document.head.appendChild(script)
//     return () => { if (chartRef.current) { chartRef.current.destroy(); chartRef.current = null } }
//   }, [data])

//   return (
//     <div style={{ position: 'relative', height: 160 }}>
//       <canvas ref={canvasRef} role="img" aria-label="Monthly pipeline activity bar chart showing new tenders and endorsements">Monthly activity data</canvas>
//     </div>
//   )
// }

// // ── KPI Card — compact version for header grid ────────────────────────────────
// function KpiCard({ icon: Icon, label, value, sub, color }) {
//   const [hov, setHov] = useState(false)
//   return (
//     <div onMouseEnter={() => setHov(true)} onMouseLeave={() => setHov(false)}
//       style={{ background: hov ? C.pageBg : C.surface, border: `1px solid ${hov ? C.blueBorder : C.border}`, borderLeft: `3px solid ${color}`, borderRadius: 4, padding: '10px 12px 10px 10px', transition: 'background 0.13s, border-color 0.13s', boxShadow: hov ? '0 2px 8px rgba(0,51,141,.07)' : '0 1px 3px rgba(0,0,0,.04)', fontFamily: F }}>
//       <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6 }}>
//         <span style={{ fontSize: 9, fontWeight: 600, color: C.textFaint, textTransform: 'uppercase', letterSpacing: '0.07em' }}>{label}</span>
//         <Icon style={{ width: 10, height: 10, color: C.textFaint }} />
//       </div>
//       <div style={{ fontSize: 18, fontWeight: 700, color: C.text, lineHeight: 1, fontFamily: MONO, letterSpacing: '-0.03em' }}>{value}</div>
//       {sub && <div style={{ fontSize: 10, color: C.textFaint, marginTop: 3 }}>{sub}</div>}
//     </div>
//   )
// }

// // ── Section Card — same as Today/Tenders ──────────────────────────────────────
// function SectionCard({ label, meta, action, hint, children, noPad }) {
//   return (
//     <div style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 4, overflow: 'hidden', boxShadow: '0 1px 3px rgba(0,0,0,.05)' }}>
//       <div style={{ background: C.pageBg, borderBottom: `1px solid ${C.border}`, padding: '9px 24px', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
//         <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
//           <span style={{ fontSize: 11, fontWeight: 600, color: C.blue, letterSpacing: '0.07em', textTransform: 'uppercase', fontFamily: F }}>{label}</span>
//           {meta && <><span style={{ width: 1, height: 10, background: C.border, flexShrink: 0 }} /><span style={{ fontSize: 11, color: C.textMuted, fontFamily: F }}>{meta}</span></>}
//         </div>
//         {action && <ActionBtn label={action.label} onClick={action.onClick} />}
//       </div>
//       {hint && <div style={{ padding: '10px 24px 0', fontSize: 11, color: C.textFaint, fontStyle: 'italic', fontFamily: F }}>{hint}</div>}
//       <div style={{ padding: noPad ? 0 : '16px 24px' }}>{children}</div>
//     </div>
//   )
// }

// function ActionBtn({ label, onClick }) {
//   const [hov, setHov] = useState(false)
//   return (
//     <button onClick={onClick} onMouseEnter={() => setHov(true)} onMouseLeave={() => setHov(false)}
//       style={{ fontSize: 11, fontWeight: 500, color: hov ? C.blue : C.textMuted, background: 'none', border: 'none', cursor: 'pointer', fontFamily: F, transition: 'color .12s' }}>
//       {label}
//     </button>
//   )
// }

// // ── Priority Row ───────────────────────────────────────────────────────────────
// function PriorityRow({ tender, isLast }) {
//   const navigate = useNavigate()
//   const [hov, setHov] = useState(false)
//   const pct = tender.p_go != null ? Math.round(tender.p_go * 100) : null
//   const col = pct >= 80 ? C.blue : pct >= 60 ? C.accent : '#F59E0B'
//   return (
//     <div onClick={() => navigate(`/tenders/${tender.id}`)} onMouseEnter={() => setHov(true)} onMouseLeave={() => setHov(false)}
//       style={{ display: 'flex', alignItems: 'center', gap: 12, padding: '10px 24px', borderBottom: isLast ? 'none' : `1px solid ${C.divider}`, background: hov ? C.pageBg : C.surface, cursor: 'pointer', transition: 'background .12s' }}>
//       <div style={{ width: 6, height: 6, borderRadius: '50%', flexShrink: 0, background: col }} />
//       <div style={{ flex: 1, minWidth: 0 }}>
//         <div style={{ fontSize: 12, fontWeight: 500, color: C.text, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{tender.title_clean || 'Untitled'}</div>
//         <div style={{ fontSize: 10, color: C.textFaint, marginTop: 1 }}>{tender.funding_agency || '—'} · {tender.country_name_normalized || '—'}</div>
//       </div>
//       <span style={{ fontSize: 12, fontWeight: 700, color: col, fontFamily: MONO, flexShrink: 0 }}>{pct != null ? `${pct}%` : '—'}</span>
//     </div>
//   )
// }

// // ── Endorsement Badge ──────────────────────────────────────────────────────────
// function EndBadge({ decision }) {
//   const ok = decision === 'GO'
//   return (
//     <span style={{ display: 'inline-flex', alignItems: 'center', gap: 3, fontSize: 10, fontWeight: 700, color: ok ? C.green : C.red, background: ok ? '#F0FDF4' : '#FFF1F2', border: `1px solid ${ok ? '#BBF7D0' : '#FECDD3'}`, padding: '2px 7px', borderRadius: 3, whiteSpace: 'nowrap' }}>
//       {ok ? <><CheckCircle style={{ width: 9, height: 9 }} />Endorsed</> : <><XCircle style={{ width: 9, height: 9 }} />Declined</>}
//     </span>
//   )
// }

// // ── Donut Chart ────────────────────────────────────────────────────────────────
// function DonutChart({ scoreDist, total }) {
//   const C2 = 2 * Math.PI * 38
//   const tiers = [
//     { label: 'Priority ≥ 80%',    count: scoreDist.strong,                  color: C.blue      },
//     { label: 'GO 60–79%',         count: scoreDist.go,                      color: C.accent    },
//     { label: 'Marginal 40–59%',   count: scoreDist.review,                  color: '#F59E0B'   },
//     { label: 'Low / Unscored',    count: scoreDist.low + scoreDist.noScore, color: C.textFaint },
//   ]
//   let offset = 0
//   const segs = tiers.map(t => {
//     const dash = (total > 0 ? t.count / total : 0) * C2
//     const s = { ...t, dash, offset }; offset += dash; return s
//   })
//   const pct = total > 0 ? Math.round((scoreDist.strong / total) * 100) : 0
//   return (
//     <div style={{ display: 'flex', alignItems: 'center', gap: 24 }}>
//       <svg width={110} height={110} viewBox="0 0 100 100" style={{ flexShrink: 0 }} role="img" aria-label="Score distribution donut chart">
//         <circle cx={50} cy={50} r={38} fill="none" stroke={C.divider} strokeWidth={14} />
//         {segs.map((s, i) => (
//           <circle key={i} cx={50} cy={50} r={38} fill="none" stroke={s.color} strokeWidth={14}
//             strokeDasharray={`${s.dash} ${C2 - s.dash}`} strokeDashoffset={-s.offset} transform="rotate(-90 50 50)" />
//         ))}
//         <text x={50} y={46} textAnchor="middle" fontSize={16} fontWeight={700} fill={C.text} fontFamily={MONO}>{pct}%</text>
//         <text x={50} y={58} textAnchor="middle" fontSize={9} fill={C.textFaint} fontFamily={F}>priority</text>
//       </svg>
//       <div style={{ flex: 1 }}>
//         {tiers.map(t => (
//           <div key={t.label} style={{ display: 'flex', alignItems: 'center', gap: 7, marginBottom: 8 }}>
//             <div style={{ width: 8, height: 8, borderRadius: '50%', background: t.color, flexShrink: 0 }} />
//             <span style={{ flex: 1, fontSize: 11, color: C.textMid }}>{t.label}</span>
//             <span style={{ fontSize: 11, fontWeight: 700, color: C.text, fontFamily: MONO }}>{t.count}</span>
//           </div>
//         ))}
//       </div>
//     </div>
//   )
// }

// // ── Market Notes ───────────────────────────────────────────────────────────────
// function MarketNotesPanel({ data, cacheKey }) {
//   const [notes,   setNotes]   = useState(null)
//   const [loading, setLoading] = useState(false)
//   const [error,   setError]   = useState(null)
//   const [lastGen, setLastGen] = useState(null)

//   useEffect(() => {
//     try {
//       const raw = localStorage.getItem(NOTES_CACHE_KEY)
//       if (raw) {
//         const cached = JSON.parse(raw)
//         if (Date.now() - new Date(cached.generatedAt).getTime() < NOTES_TTL_MS) {
//           setNotes(cached.notes); setLastGen(cached.generatedAt); return
//         }
//       }
//     } catch {} generate()
//   }, [cacheKey])

//   async function generate() {
//     if (!data) return
//     setLoading(true); setError(null)
//     const prompt = `You are a senior KPMG procurement analyst. Write exactly 3 concise market signals from this pipeline data — patterns, risks, or opportunities. Be specific and actionable.
// Pipeline: ${data.total} tenders, value ${fmtM(data.pipelineValue)}, top sectors: ${data.sectors.slice(0,4).map(([n,c])=>`${n}(${c})`).join(', ')}, top countries: ${data.countries.slice(0,4).map(([n,c])=>`${n}(${c})`).join(', ')}, priority signals: ${data.strongGo}, closing this week: ${data.closingThisWeek}.
// Respond ONLY with JSON, no markdown: [{"text":"...","tag":"Source · Region"},{"text":"...","tag":"Source · Region"},{"text":"...","tag":"Source · Region"}]`
//     try {
//       const res = await fetch('https://api.anthropic.com/v1/messages', {
//         method: 'POST', headers: { 'Content-Type': 'application/json' },
//         body: JSON.stringify({ model: 'claude-sonnet-4-20250514', max_tokens: 800, messages: [{ role: 'user', content: prompt }] }),
//       })
//       const json = await res.json()
//       const raw = json.content?.find(b => b.type === 'text')?.text || '[]'
//       const parsed = JSON.parse(raw.replace(/```json|```/g, '').trim())
//       const generatedAt = new Date().toISOString()
//       localStorage.setItem(NOTES_CACHE_KEY, JSON.stringify({ notes: parsed, generatedAt }))
//       setNotes(parsed); setLastGen(generatedAt)
//     } catch (e) { console.error(e); setError('Could not generate. Try refreshing.') }
//     finally { setLoading(false) }
//   }

//   const TC = [
//     { bg: '#EEF2FF', color: C.blue,   border: '#BFDBFE' },
//     { bg: '#FEF3C7', color: '#92400E', border: '#FCD34D' },
//     { bg: '#F0FDF4', color: C.green,  border: '#BBF7D0' },
//   ]

//   if (loading) return <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '24px', justifyContent: 'center' }}><Spinner size="sm" /><span style={{ fontSize: 12, color: C.textMuted }}>Analysing pipeline…</span></div>
//   if (error)   return <div style={{ fontSize: 12, color: C.red, padding: '12px', background: '#FFF1F2', borderRadius: 4 }}>{error}</div>
//   if (!notes)  return null

//   return (
//     <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 14 }}>
//       {notes.map((note, i) => {
//         const tc = TC[i % TC.length]
//         return (
//           <div key={i} style={{ padding: '14px', background: C.pageBg, border: `1px solid ${C.border}`, borderLeft: `3px solid ${C.blue}`, borderRadius: '0 4px 4px 0' }}>
//             <span style={{ fontSize: 9, fontWeight: 700, padding: '2px 7px', borderRadius: 3, background: tc.bg, color: tc.color, border: `1px solid ${tc.border}`, display: 'inline-block', marginBottom: 8 }}>{note.tag}</span>
//             <p style={{ fontSize: 12, color: C.textMid, lineHeight: 1.75, margin: 0 }}>{note.text}</p>
//           </div>
//         )
//       })}
//     </div>
//   )
// }

// // ── Activity Feed ──────────────────────────────────────────────────────────────
// function ActivityFeed({ decided, all }) {
//   const now = new Date()
//   const events = []
//   ;[...decided].sort((a, b) => new Date(b.decided_at || 0) - new Date(a.decided_at || 0)).slice(0, 4).forEach(t => {
//     const ok = t.partner_decision === 'GO'
//     events.push({ icon: ok ? '✓' : '✗', iconBg: ok ? '#F0FDF4' : '#FFF1F2', iconColor: ok ? C.green : C.red, text: `${ok ? 'Endorsed' : 'Declined'} — ${(t.title_clean || 'Untitled').slice(0, 38)}`, time: t.decided_at ? new Date(t.decided_at) : null })
//   })
//   all.filter(t => { if (!t.deadline_datetime) return false; const d = new Date(t.deadline_datetime.replace(' ', 'T')); const days = Math.ceil((d - now) / 86400000); return days >= 0 && days <= 3 }).slice(0, 2).forEach(t => {
//     const days = Math.ceil((new Date(t.deadline_datetime.replace(' ', 'T')) - now) / 86400000)
//     events.push({ icon: '!', iconBg: '#FFF7ED', iconColor: C.amber, text: `Deadline in ${days}d — ${(t.title_clean || 'Untitled').slice(0, 36)}`, time: null })
//   })
//   function timeAgo(d) {
//     if (!d) return 'Today'
//     const mins = Math.floor((now - d) / 60000)
//     if (mins < 60) return `${mins}m ago`
//     const hrs = Math.floor(mins / 60)
//     if (hrs < 24) return `${hrs}h ago`
//     return `${Math.floor(hrs / 24)}d ago`
//   }
//   if (!events.length) return <div style={{ fontSize: 12, color: C.textFaint, fontStyle: 'italic' }}>No recent activity.</div>
//   return (
//     <>
//       {events.slice(0, 5).map((ev, i) => (
//         <div key={i} style={{ display: 'flex', gap: 10, padding: '9px 0', borderBottom: i < events.length - 1 ? `1px solid ${C.divider}` : 'none', alignItems: 'flex-start' }}>
//           <div style={{ width: 22, height: 22, borderRadius: '50%', background: ev.iconBg, color: ev.iconColor, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 10, fontWeight: 700, flexShrink: 0, marginTop: 1 }}>{ev.icon}</div>
//           <div style={{ flex: 1 }}>
//             <div style={{ fontSize: 11, color: C.text, fontWeight: 500, lineHeight: 1.4 }}>{ev.text}</div>
//             <div style={{ fontSize: 10, color: C.textFaint, marginTop: 2 }}>{timeAgo(ev.time)}</div>
//           </div>
//         </div>
//       ))}
//     </>
//   )
// }

// // ── Main ───────────────────────────────────────────────────────────────────────
// export default function Dashboard() {
//   const navigate   = useNavigate()
//   const [loading,  setLoading]  = useState(true)
//   const [data,     setData]     = useState(null)
//   const [notesKey, setNotesKey] = useState(0)

//   useEffect(() => {
//     async function load() {
//       try {
//         const now = new Date(), in7 = new Date(now.getTime() + 7 * 86400000)
//         const [todayRes, openRes, allRes] = await Promise.all([
//           api.get('/tenders/today'),
//           api.get('/tenders', { params: { status: 'open', per_page: 100, sort_by: 'p_go' } }),
//           api.get('/tenders', { params: { status: 'all',  per_page: 100, sort_by: 'publication_datetime' } }),
//         ])
//         const open = openRes.data.items, all = allRes.data.items, today = todayRes.data
//         const pipelineValue   = open.reduce((s, t) => s + (t.budget || 0), 0)
//         const closingThisWeek = open.filter(t => { if (!t.deadline_datetime) return false; const d = new Date(t.deadline_datetime.replace(' ', 'T')); return d >= now && d <= in7 }).length
//         const decided         = all.filter(t => t.partner_decision)
//         const goCount         = decided.filter(t => t.partner_decision === 'GO').length
//         const scored          = open.filter(t => t.p_go != null)
//         const avgScore        = scored.length ? Math.round(scored.reduce((s, t) => s + t.p_go, 0) / scored.length * 100) : null
//         const priority        = open.filter(t => t.p_go >= 0.7).slice(0, 8)
//         const mapTenders      = open.filter(t => t.p_go >= 0.60 && t.country_name_normalized).sort((a, b) => b.p_go - a.p_go).slice(0, 40)

//         // Monthly activity — last 6 months
//         const months = []
//         for (let i = 5; i >= 0; i--) {
//           const d = new Date(now.getFullYear(), now.getMonth() - i, 1)
//           const label = d.toLocaleDateString('en-GB', { month: 'short' })
//           const nextD = new Date(d.getFullYear(), d.getMonth() + 1, 1)
//           const newCount = open.filter(t => { if (!t.publication_datetime) return false; const pd = new Date(t.publication_datetime.replace(' ', 'T')); return pd >= d && pd < nextD }).length
//           const endorsedCount = decided.filter(t => { if (!t.decided_at) return false; const dd = new Date(t.decided_at.replace(' ', 'T')); return dd >= d && dd < nextD && t.partner_decision === 'GO' }).length
//           months.push({ label, new: newCount, endorsed: endorsedCount })
//         }

//         const sectorMap = {}, portalMap = {}, countryMap = {}, sectorScoreMap = {}
//         open.forEach(t => {
//           if (t.sector) {
//             const s = t.sector.split(',')[0].trim()
//             sectorMap[s] = (sectorMap[s] || 0) + 1
//             if (t.p_go != null) { if (!sectorScoreMap[s]) sectorScoreMap[s] = []; sectorScoreMap[s].push(t.p_go) }
//           }
//           if (t.source_portal) portalMap[t.source_portal] = (portalMap[t.source_portal] || 0) + 1
//           if (t.country_name_normalized) countryMap[t.country_name_normalized] = (countryMap[t.country_name_normalized] || 0) + 1
//         })
//         const sectors       = Object.entries(sectorMap).sort((a, b) => b[1] - a[1]).slice(0, 8)
//         const portals       = Object.entries(portalMap).sort((a, b) => b[1] - a[1]).slice(0, 6)
//         const countries     = Object.entries(countryMap).sort((a, b) => b[1] - a[1]).slice(0, 8)
//         const sectorsByScore = Object.entries(sectorScoreMap)
//           .map(([name, scores]) => [name, Math.round(scores.reduce((a, b) => a + b, 0) / scores.length * 100)])
//           .sort((a, b) => b[1] - a[1]).slice(0, 3)

//         const strong  = open.filter(t => t.p_go >= 0.80).length
//         const go      = open.filter(t => t.p_go >= 0.60 && t.p_go < 0.80).length
//         const review  = open.filter(t => t.p_go >= 0.40 && t.p_go < 0.60).length
//         const low     = open.filter(t => t.p_go != null && t.p_go < 0.40).length
//         const noScore = open.filter(t => t.p_go == null).length

//         setData({
//           total: openRes.data.total, pipelineValue, strongGo: today.strong_go?.length ?? 0,
//           closingThisWeek, decided, goCount, avgScore, priority, mapTenders,
//           sectors, portals, countries, sectorsByScore, months,
//           scoreDist: { strong, go, review, low, noScore }, all,
//         })
//       } catch (e) { console.error(e) } finally { setLoading(false) }
//     }
//     load()
//   }, [])

//   if (loading) return <div style={{ display: 'flex', justifyContent: 'center', padding: '80px 0' }}><Spinner size="lg" /></div>
//   if (!data)   return null

//   const distTotal = data.scoreDist.strong + data.scoreDist.go + data.scoreDist.review + data.scoreDist.low + data.scoreDist.noScore || 1
//   const SECTOR_COLORS = [C.blue, C.green, '#92400E']

//   return (
//     <div style={{ display: 'flex', flexDirection: 'column', minHeight: '100%', background: C.surface, fontFamily: F }}>
//       <IntelStrip />
//       <div style={{ flex: 1, padding: '36px 48px 56px', maxWidth: 1100, width: '100%', margin: '0 auto' }}>

//         {/* Page header — title left, 4 KPIs right */}
//         <div style={{ display: 'grid', gridTemplateColumns: '1fr auto', gap: 40, alignItems: 'start', marginBottom: 32 }}>
//           <div>
//             <div style={{ fontSize: 11, fontWeight: 700, color: C.navy, letterSpacing: '0.10em', textTransform: 'uppercase', marginBottom: 8 }}>Executive Overview</div>
//             <h1 style={{ fontSize: 28, fontWeight: 700, color: C.text, margin: '0 0 12px', letterSpacing: '-0.02em' }}>Analytics Dashboard</h1>
//             <p style={{ fontSize: 13, color: C.textMuted, margin: 0, lineHeight: 1.75, borderLeft: `3px solid ${C.navy}`, paddingLeft: 14, maxWidth: 480 }}>
//               A full read of your active pipeline — scores, team decisions, sector fit, and market signals.
//               Use this each morning to prioritise where to focus advisory capacity.
//             </p>
//           </div>
//           <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10, minWidth: 380 }}>
//             <KpiCard icon={CheckCircle} label="Endorsement Rate"  value={data.decided.length ? `${Math.round((data.goCount / data.decided.length) * 100)}%` : '—'} sub="GO ÷ total reviewed"   color={C.blue}  />
//             <KpiCard icon={Clock}       label="Expiring This Week" value={data.all.filter(t => { if (!t.deadline_datetime) return false; const d = new Date(t.deadline_datetime.replace(' ','T')); const days = Math.ceil((d - new Date()) / 86400000); return days >= 0 && days <= 7 }).length} sub="Review before close" color={C.blue}    />
//             <KpiCard icon={Globe}       label="New This Week"      value={data.all.filter(t => t.publication_datetime && (new Date() - new Date(t.publication_datetime.replace(' ','T'))) < 7 * 86400000).length} sub="Added in last 7 days" color={C.blue}   />
//             <KpiCard icon={TrendingUp}  label="Total Reviewed"     value={data.decided.length} sub="Partner decisions"    color={C.blue} />
//           </div>
//         </div>

//         {/* ── Pipeline Activity — full width ── */}
//         <div style={{ marginBottom: 32 }}>
//           <SectionCard label="Pipeline Activity" meta="New tenders & endorsements · last 6 months">
//             <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
//               <p style={{ fontSize: 11, color: C.textFaint, fontStyle: 'italic', margin: 0 }}>
//                 Track how the pipeline evolves month by month — new consulting opportunities scored alongside team endorsement volume.
//               </p>
//               <div style={{ display: 'flex', gap: 16, flexShrink: 0, marginLeft: 24 }}>
//                 <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}><div style={{ width: 10, height: 10, borderRadius: 2, background: '#BFDBFE' }} /><span style={{ fontSize: 11, color: C.textMuted }}>New tenders</span></div>
//                 <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}><div style={{ width: 10, height: 10, borderRadius: 2, background: C.blue }} /><span style={{ fontSize: 11, color: C.textMuted }}>Endorsed</span></div>
//               </div>
//             </div>
//             <MonthlyChart data={data.months} />
//           </SectionCard>
//         </div>

//         {/* ── Row 3: Map + Priority Pipeline ── */}
//         <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginBottom: 32 }}>
//           <SectionCard label="Pipeline Map" meta="Africa · Score ≥ 60% · click to open" noPad
//             hint="Each pin is a scored opportunity. Hover to preview, click to open.">
//             <div style={{ height: 240 }}>
//               <AfricaMap tenders={data.mapTenders} onPinClick={t => navigate(`/tenders/${t.id}`)} />
//             </div>
//           </SectionCard>

//           <SectionCard label="Priority Pipeline" meta="Top scored · ranked by strategic fit"
//             action={{ label: 'View all ›', onClick: () => navigate('/tenders') }} noPad
//             hint="Opportunities your team should act on first.">
//             {data.priority.length === 0
//               ? <div style={{ padding: '24px', fontSize: 12, color: C.textFaint, fontStyle: 'italic', textAlign: 'center' }}>No scored opportunities yet.</div>
//               : data.priority.map((t, i) => <PriorityRow key={t.id} tender={t} isLast={i === data.priority.length - 1} />)}
//           </SectionCard>
//         </div>

//         {/* ── Row 4: Activity Feed + Recent Endorsements ── */}
//         <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginBottom: 32 }}>
//           <SectionCard label="Recent Activity" meta="Last 24h across the team"
//             hint="Decisions submitted and deadlines approaching — a live record of what's happening.">
//             <ActivityFeed decided={data.decided} all={data.all} />
//           </SectionCard>

//           <SectionCard label="Recent Endorsements" meta={`${data.decided.length} total`}
//             action={{ label: 'All decisions ›', onClick: () => navigate('/decisions') }} noPad
//             hint="Partner decisions submitted on the Opportunity Detail page.">
//             {[...data.decided].sort((a, b) => new Date(b.decided_at || 0) - new Date(a.decided_at || 0)).slice(0, 6).map((t, i, arr) => {
//               const [hov, setHov] = useState(false)
//               const pct = t.p_go != null ? Math.round(t.p_go * 100) : null
//               return (
//                 <div key={t.id} onClick={() => navigate(`/tenders/${t.id}`)}
//                   onMouseEnter={() => setHov(true)} onMouseLeave={() => setHov(false)}
//                   style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '9px 24px', borderBottom: i < arr.length - 1 ? `1px solid ${C.divider}` : 'none', background: hov ? C.pageBg : C.surface, cursor: 'pointer', transition: 'background .12s' }}>
//                   <EndBadge decision={t.partner_decision} />
//                   <div style={{ flex: 1, minWidth: 0 }}>
//                     <div style={{ fontSize: 11, fontWeight: 500, color: C.text, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{(t.title_clean || 'Untitled').slice(0, 44)}</div>
//                     <div style={{ fontSize: 10, color: C.textFaint, marginTop: 1 }}>{t.country_name_normalized || '—'}{pct != null ? ` · ${pct}%` : ''}</div>
//                   </div>
//                 </div>
//               )
//             })}
//           </SectionCard>
//         </div>

//         {/* ── Row 5: Top 3 Sectors + Score Breakdown ── */}
//         <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginBottom: 32 }}>
//           <SectionCard label="Top 3 Sectors" meta="Avg. strategic fit score · KPMG positioning"
//             hint="Sectors where KPMG scores highest. Use to guide practice area focus and BD prioritisation.">
//             <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
//               {data.sectorsByScore.map(([name, avg], i) => (
//                 <div key={name}>
//                   <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
//                     <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
//                       <div style={{ width: 28, height: 28, borderRadius: 4, background: i === 0 ? '#EEF2FF' : i === 1 ? '#F0FDF4' : '#FEF3C7', display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0 }}>
//                         <span style={{ fontSize: 12, fontWeight: 700, color: SECTOR_COLORS[i], fontFamily: MONO }}>{i + 1}</span>
//                       </div>
//                       <span style={{ fontSize: 13, fontWeight: 600, color: C.text }}>{name}</span>
//                     </div>
//                     <span style={{ fontSize: 14, fontWeight: 700, color: SECTOR_COLORS[i], fontFamily: MONO }}>{avg}%</span>
//                   </div>
//                   <div style={{ height: 6, background: C.pageBg, borderRadius: 3 }}>
//                     <div style={{ height: 6, borderRadius: 3, background: SECTOR_COLORS[i], width: `${avg}%`, transition: 'width .6s ease' }} />
//                   </div>
//                 </div>
//               ))}
//             </div>
//           </SectionCard>

//           <SectionCard label="Score Breakdown" meta={`${distTotal} open tenders`}
//             hint="A healthy pipeline has ≥ 30% in Priority or GO. Use this to track overall pipeline quality over time.">
//             <DonutChart scoreDist={data.scoreDist} total={distTotal} />
//           </SectionCard>
//         </div>

//         {/* ── Row 6: Market Notes — full width ── */}
//         <SectionCard label="Market Notes" meta="AI-generated from your live pipeline · refreshes weekly"
//           hint="Patterns and signals detected from your pipeline data. Use these to inform positioning conversations and proposal strategy."
//           action={{ label: '↺ Refresh', onClick: () => { localStorage.removeItem(NOTES_CACHE_KEY); setNotesKey(k => k + 1) } }}>
//           <MarketNotesPanel key={notesKey} data={data} />
//         </SectionCard>

//       </div>

//       <PlatformFooter />
//     </div>
//   )
// }


// src/pages/Dashboard.jsx
import { useEffect, useState, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { TrendingUp, Globe, Zap, Clock, CheckCircle, XCircle } from 'lucide-react'
import api from '../lib/api'
import { Spinner } from '../components/ui'
import PlatformFooter from '../components/layout/PlatformFooter'
import IntelStrip from '../components/layout/IntelStrip'

const C = {
  blue:      '#00338D',
  accent:    '#0091DA',
  navy:      '#0D1F6B',
  teal:      '#0F766E',
  amber:     '#342471',
  purple:    '#7E22CE',
  green:     '#15803D',
  red:       '#B91C1C',
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
const NOTES_CACHE_KEY = 'dashboard_market_notes'
const NOTES_TTL_MS    = 7 * 24 * 60 * 60 * 1000

function fmtM(v) {
  if (!v) return '—'
  if (v >= 1e9) return `$${(v / 1e9).toFixed(1)}B`
  if (v >= 1e6) return `$${(v / 1e6).toFixed(1)}M`
  if (v >= 1e3) return `$${(v / 1e3).toFixed(0)}K`
  return `$${v}`
}

// ── Country coords ─────────────────────────────────────────────────────────────
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
  'Guinea-Bissau':{ lng:-15.18, lat:11.80 }, 'Eritrea':{ lng:39.78, lat:15.18 },
  'South Sudan':{ lng:31.30, lat:6.88 }, 'Burundi':{ lng:29.92, lat:-3.37 },
  'Central African Republic':{ lng:20.94, lat:6.61 },
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

// ── Africa Map ─────────────────────────────────────────────────────────────────
function AfricaMap({ tenders, onPinClick }) {
  const canvasRef    = useRef(null)
  const maskDataRef  = useRef(null)
  const maskReadyRef = useRef(false)
  const animRef      = useRef(null)
  const pinsRef      = useRef([])
  const [tooltip, setTooltip] = useState(null)
  const [pins, setPins]       = useState([])

  useEffect(() => { pinsRef.current = pins }, [pins])

  useEffect(() => {
    if (!tenders?.length) return
    async function buildPins() {
      const resolved = await Promise.all(tenders.map(async t => {
        const name = t.country_name_normalized; if (!name) return null
        const coords = COUNTRY_COORDS[name] || await (async () => {
          try {
            const res = await fetch(`https://restcountries.com/v3.1/name/${encodeURIComponent(name)}?fields=latlng`)
            const d = await res.json()
            if (Array.isArray(d) && d[0]?.latlng?.length === 2) {
              const c = { lat: d[0].latlng[0], lng: d[0].latlng[1] }
              COUNTRY_COORDS[name] = c; return c
            }
          } catch {} return null
        })()
        if (!coords) return null
        if (coords.lng < AFRICA.minLng || coords.lng > AFRICA.maxLng || coords.lat < AFRICA.minLat || coords.lat > AFRICA.maxLat) return null
        return { tender: t, score: t.p_go ?? 0, ...coords }
      }))
      setPins(resolved.filter(Boolean).slice(0, 30))
    }
    buildPins()
  }, [tenders?.length])

  useEffect(() => {
    const canvas = canvasRef.current; if (!canvas) return
    const ctx = canvas.getContext('2d')
    const OMASK_W = 600, OMASK_H = 400
    function resize() {
      const rect = canvas.getBoundingClientRect()
      if (rect.width > 0 && rect.height > 0) { canvas.width = rect.width; canvas.height = rect.height }
    }
    resize()
    const ro = new ResizeObserver(resize); ro.observe(canvas)
    window.addEventListener('resize', resize)
    const offMask = document.createElement('canvas')
    offMask.width = OMASK_W; offMask.height = OMASK_H
    const omc = offMask.getContext('2d')
    const img = new Image(); img.crossOrigin = 'anonymous'; img.src = '/world-map-mask.png'
    img.onload = () => {
      const iw = img.naturalWidth || img.width, ih = img.naturalHeight || img.height
      const sx = ((AFRICA.minLng + 180) / 360) * iw
      const sy = ((90 - AFRICA.maxLat) / 180) * ih + (ih * 0.14)
      const sw = ((AFRICA.maxLng - AFRICA.minLng) / 360) * iw
      const sh = ((AFRICA.maxLat - AFRICA.minLat) / 180) * ih
      omc.drawImage(img, sx, sy, sw, sh, 0, 0, OMASK_W, OMASK_H)
      maskDataRef.current = omc.getImageData(0, 0, OMASK_W, OMASK_H)
      maskReadyRef.current = true
    }
    img.onerror = () => { maskReadyRef.current = true }
    function isLand(px, py, w, h) {
      if (!maskDataRef.current) return false
      const mx = Math.round((px / w) * OMASK_W), my = Math.round((py / h) * OMASK_H)
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
            if (isLand(px, py, w, h)) { ctx.beginPath(); ctx.arc(px, py, 1.4, 0, Math.PI * 2); ctx.fill() }
      }
      pinsRef.current.forEach(pin => {
        const { x, y } = toXY(pin.lng, pin.lat, w, h)
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
    const canvas = canvasRef.current; if (!canvas) return
    const rect = canvas.getBoundingClientRect()
    const mx = e.clientX - rect.left, my = e.clientY - rect.top
    const w = canvas.width, h = canvas.height
    let found = null
    for (const pin of pinsRef.current) {
      const { x, y } = toXY(pin.lng, pin.lat, w, h)
      if (Math.hypot(mx - x, my - y) < 10) { found = { x: e.clientX, y: e.clientY, tender: pin.tender }; break }
    }
    setTooltip(found); canvas.style.cursor = found ? 'pointer' : 'default'
  }

  return (
    <div style={{ position: 'relative', width: '100%', height: '100%' }}>
      <canvas ref={canvasRef} onMouseMove={onMouseMove} onMouseLeave={() => setTooltip(null)}
        onClick={() => tooltip?.tender && onPinClick(tooltip.tender)}
        style={{ width: '100%', height: '100%', display: 'block' }} />
      <div style={{ position: 'absolute', bottom: 10, left: 12, display: 'flex', gap: 8, background: 'rgba(255,255,255,.92)', borderRadius: 4, padding: '4px 10px', border: `1px solid ${C.border}` }}>
        {[{ color: C.blue, label: '≥ 80%' }, { color: C.accent, label: '60–79%' }, { color: '#F59E0B', label: 'Marginal' }].map(l => (
          <div key={l.label} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            <div style={{ width: 6, height: 6, borderRadius: '50%', background: l.color }} />
            <span style={{ fontSize: 9, color: C.textMuted, fontFamily: F }}>{l.label}</span>
          </div>
        ))}
      </div>
      {tooltip && (
        <div style={{ position: 'fixed', left: tooltip.x + 12, top: tooltip.y - 8, zIndex: 999, background: C.navy, color: '#fff', borderRadius: 4, padding: '8px 12px', fontSize: 11, fontFamily: F, maxWidth: 220, pointerEvents: 'none', boxShadow: '0 4px 16px rgba(0,0,0,.22)' }}>
          <div style={{ fontWeight: 600, marginBottom: 3, lineHeight: 1.4 }}>{(tooltip.tender.title_clean || 'Untitled').slice(0, 55)}{(tooltip.tender.title_clean || '').length > 55 ? '…' : ''}</div>
          <div style={{ color: 'rgba(255,255,255,.55)', fontSize: 10 }}>{tooltip.tender.country_name_normalized} · {tooltip.tender.p_go != null ? Math.round(tooltip.tender.p_go * 100) + '%' : '—'}</div>
          <div style={{ marginTop: 4, fontSize: 10, color: C.accent }}>Click to open →</div>
        </div>
      )}
    </div>
  )
}

// ── Monthly Activity Bar Chart ─────────────────────────────────────────────────
function MonthlyChart({ data }) {
  const canvasRef = useRef(null)
  const chartRef  = useRef(null)

  useEffect(() => {
    if (!canvasRef.current || !data?.length) return
    const script = document.createElement('script')
    script.src = 'https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js'
    script.onload = () => {
      if (chartRef.current) chartRef.current.destroy()
      chartRef.current = new window.Chart(canvasRef.current, {
        type: 'bar',
        data: {
          labels: data.map(d => d.label),
          datasets: [
            { label: 'New tenders', data: data.map(d => d.new), backgroundColor: '#BFDBFE', borderRadius: 3, borderSkipped: false },
            { label: 'Endorsed',    data: data.map(d => d.endorsed), backgroundColor: '#00338D', borderRadius: 3, borderSkipped: false },
          ],
        },
        options: {
          responsive: true, maintainAspectRatio: false,
          plugins: { legend: { display: false }, tooltip: { mode: 'index', intersect: false } },
          scales: {
            x: { grid: { display: false }, ticks: { font: { size: 10 }, color: '#9CA3AF' } },
            y: { grid: { color: '#F3F4F6' }, ticks: { font: { size: 10 }, color: '#9CA3AF' }, border: { display: false } },
          },
        },
      })
    }
    document.head.appendChild(script)
    return () => { if (chartRef.current) { chartRef.current.destroy(); chartRef.current = null } }
  }, [data])

  return (
    <div style={{ position: 'relative', height: 160 }}>
      <canvas ref={canvasRef} role="img" aria-label="Monthly pipeline activity">Monthly activity data</canvas>
    </div>
  )
}

// ── KPI Card ───────────────────────────────────────────────────────────────────
function KpiCard({ icon: Icon, label, value, sub, color }) {
  const [hov, setHov] = useState(false)
  return (
    <div onMouseEnter={() => setHov(true)} onMouseLeave={() => setHov(false)}
      style={{ background: hov ? C.pageBg : C.surface, border: `1px solid ${hov ? C.blueBorder : C.border}`, borderLeft: `3px solid ${color}`, borderRadius: 4, padding: '10px 12px 10px 10px', transition: 'background 0.13s, border-color 0.13s', boxShadow: hov ? '0 2px 8px rgba(0,51,141,.07)' : '0 1px 3px rgba(0,0,0,.04)', fontFamily: F }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6 }}>
        <span style={{ fontSize: 9, fontWeight: 600, color: C.textFaint, textTransform: 'uppercase', letterSpacing: '0.07em' }}>{label}</span>
        <Icon style={{ width: 10, height: 10, color: C.textFaint }} />
      </div>
      <div style={{ fontSize: 18, fontWeight: 700, color: C.text, lineHeight: 1, fontFamily: MONO, letterSpacing: '-0.03em' }}>{value}</div>
      {sub && <div style={{ fontSize: 10, color: C.textFaint, marginTop: 3 }}>{sub}</div>}
    </div>
  )
}

// ── Section Card ───────────────────────────────────────────────────────────────
function SectionCard({ label, meta, action, hint, children, noPad }) {
  return (
    <div style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 4, overflow: 'hidden' }}>
      <div style={{ padding: '10px 20px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderBottom: `1px solid ${C.divider}` }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <span style={{ fontSize: 10, fontWeight: 600, color: C.textMuted, letterSpacing: '0.08em', textTransform: 'uppercase', fontFamily: F }}>{label}</span>
          {meta && <><span style={{ width: 1, height: 8, background: C.border, flexShrink: 0 }} /><span style={{ fontSize: 11, color: C.textFaint, fontFamily: F }}>{meta}</span></>}
        </div>
        {action && <ActionBtn label={action.label} onClick={action.onClick} />}
      </div>
      {hint && <div style={{ padding: '8px 20px 0', fontSize: 11, color: C.textFaint, fontStyle: 'italic', fontFamily: F }}>{hint}</div>}
      <div style={{ padding: noPad ? 0 : '14px 20px' }}>{children}</div>
    </div>
  )
}

function ActionBtn({ label, onClick }) {
  const [hov, setHov] = useState(false)
  return (
    <button onClick={onClick} onMouseEnter={() => setHov(true)} onMouseLeave={() => setHov(false)}
      style={{ fontSize: 11, fontWeight: 500, color: hov ? C.blue : C.textMuted, background: 'none', border: 'none', cursor: 'pointer', fontFamily: F, transition: 'color .12s' }}>
      {label}
    </button>
  )
}

// ── Priority Row ───────────────────────────────────────────────────────────────
function PriorityRow({ tender, isLast }) {
  const navigate = useNavigate()
  const [hov, setHov] = useState(false)
  const pct = tender.p_go != null ? Math.round(tender.p_go * 100) : null
  const col = pct >= 80 ? C.blue : pct >= 60 ? C.accent : '#F59E0B'
  return (
    <div onClick={() => navigate(`/tenders/${tender.id}`)} onMouseEnter={() => setHov(true)} onMouseLeave={() => setHov(false)}
      style={{ display: 'flex', alignItems: 'center', gap: 12, padding: '10px 24px', borderBottom: isLast ? 'none' : `1px solid ${C.divider}`, background: hov ? C.pageBg : C.surface, cursor: 'pointer', transition: 'background .12s' }}>
      <div style={{ width: 6, height: 6, borderRadius: '50%', flexShrink: 0, background: col }} />
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontSize: 12, fontWeight: 500, color: C.text, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{tender.title_clean || 'Untitled'}</div>
        <div style={{ fontSize: 10, color: C.textFaint, marginTop: 1 }}>{tender.funding_agency || '—'} · {tender.country_name_normalized || '—'}</div>
      </div>
      <span style={{ fontSize: 12, fontWeight: 700, color: col, fontFamily: MONO, flexShrink: 0 }}>{pct != null ? `${pct}%` : '—'}</span>
    </div>
  )
}

// ── Endorsement Badge ──────────────────────────────────────────────────────────
function EndBadge({ decision }) {
  const ok = decision === 'GO'
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 3, fontSize: 10, fontWeight: 700, color: ok ? C.green : C.red, background: ok ? '#F0FDF4' : '#FFF1F2', border: `1px solid ${ok ? '#BBF7D0' : '#FECDD3'}`, padding: '2px 7px', borderRadius: 3, whiteSpace: 'nowrap' }}>
      {ok ? <><CheckCircle style={{ width: 9, height: 9 }} />Endorsed</> : <><XCircle style={{ width: 9, height: 9 }} />Declined</>}
    </span>
  )
}

// ── Endorsement Row — extracted as proper component (fixes useState-in-map bug) ─
function EndorsementRow({ tender: t, isLast }) {
  const navigate = useNavigate()
  const [hov, setHov] = useState(false)
  const pct = t.p_go != null ? Math.round(t.p_go * 100) : null
  return (
    <div onClick={() => navigate(`/tenders/${t.id}`)}
      onMouseEnter={() => setHov(true)} onMouseLeave={() => setHov(false)}
      style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '9px 24px', borderBottom: isLast ? 'none' : `1px solid ${C.divider}`, background: hov ? C.pageBg : C.surface, cursor: 'pointer', transition: 'background .12s' }}>
      <EndBadge decision={t.partner_decision} />
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontSize: 11, fontWeight: 500, color: C.text, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{(t.title_clean || 'Untitled').slice(0, 44)}</div>
        <div style={{ fontSize: 10, color: C.textFaint, marginTop: 1 }}>{t.country_name_normalized || '—'}{pct != null ? ` · ${pct}%` : ''}</div>
      </div>
    </div>
  )
}

// ── Donut Chart ────────────────────────────────────────────────────────────────
function DonutChart({ scoreDist, total }) {
  const C2 = 2 * Math.PI * 38
  const tiers = [
    { label: 'Priority ≥ 80%',    count: scoreDist.strong,                  color: C.blue      },
    { label: 'GO 60–79%',         count: scoreDist.go,                      color: C.accent    },
    { label: 'Marginal 40–59%',   count: scoreDist.review,                  color: '#F59E0B'   },
    { label: 'Low / Unscored',    count: scoreDist.low + scoreDist.noScore, color: C.textFaint },
  ]
  let offset = 0
  const segs = tiers.map(t => {
    const dash = (total > 0 ? t.count / total : 0) * C2
    const s = { ...t, dash, offset }; offset += dash; return s
  })
  const pct = total > 0 ? Math.round((scoreDist.strong / total) * 100) : 0
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 24 }}>
      <svg width={110} height={110} viewBox="0 0 100 100" style={{ flexShrink: 0 }} role="img" aria-label="Score distribution donut chart">
        <circle cx={50} cy={50} r={38} fill="none" stroke={C.divider} strokeWidth={14} />
        {segs.map((s, i) => (
          <circle key={i} cx={50} cy={50} r={38} fill="none" stroke={s.color} strokeWidth={14}
            strokeDasharray={`${s.dash} ${C2 - s.dash}`} strokeDashoffset={-s.offset} transform="rotate(-90 50 50)" />
        ))}
        <text x={50} y={46} textAnchor="middle" fontSize={16} fontWeight={700} fill={C.text} fontFamily={MONO}>{pct}%</text>
        <text x={50} y={58} textAnchor="middle" fontSize={9} fill={C.textFaint} fontFamily={F}>priority</text>
      </svg>
      <div style={{ flex: 1 }}>
        {tiers.map(t => (
          <div key={t.label} style={{ display: 'flex', alignItems: 'center', gap: 7, marginBottom: 8 }}>
            <div style={{ width: 8, height: 8, borderRadius: '50%', background: t.color, flexShrink: 0 }} />
            <span style={{ flex: 1, fontSize: 11, color: C.textMid }}>{t.label}</span>
            <span style={{ fontSize: 11, fontWeight: 700, color: C.text, fontFamily: MONO }}>{t.count}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

// ── Market Notes ───────────────────────────────────────────────────────────────
function MarketNotesPanel({ data, cacheKey }) {
  const [notes,   setNotes]   = useState(null)
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState(null)

  useEffect(() => {
    try {
      const raw = localStorage.getItem(NOTES_CACHE_KEY)
      if (raw) {
        const cached = JSON.parse(raw)
        if (Date.now() - new Date(cached.generatedAt).getTime() < NOTES_TTL_MS) {
          setNotes(cached.notes); return
        }
      }
    } catch {} generate()
  }, [cacheKey])

  async function generate() {
    if (!data) return
    setLoading(true); setError(null)
    const prompt = `You are a senior KPMG procurement analyst. Write exactly 3 concise market signals from this pipeline data — patterns, risks, or opportunities. Be specific and actionable.
Pipeline: ${data.total} tenders, value ${fmtM(data.pipelineValue)}, top sectors: ${data.sectors.slice(0,4).map(([n,c])=>`${n}(${c})`).join(', ')}, top countries: ${data.countries.slice(0,4).map(([n,c])=>`${n}(${c})`).join(', ')}, priority signals: ${data.strongGo}, closing this week: ${data.closingThisWeek}.
Respond ONLY with JSON, no markdown: [{"text":"...","tag":"Source · Region"},{"text":"...","tag":"Source · Region"},{"text":"...","tag":"Source · Region"}]`
    try {
      const res = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ model: 'claude-sonnet-4-20250514', max_tokens: 800, messages: [{ role: 'user', content: prompt }] }),
      })
      const json = await res.json()
      const raw = json.content?.find(b => b.type === 'text')?.text || '[]'
      const parsed = JSON.parse(raw.replace(/```json|```/g, '').trim())
      const generatedAt = new Date().toISOString()
      localStorage.setItem(NOTES_CACHE_KEY, JSON.stringify({ notes: parsed, generatedAt }))
      setNotes(parsed)
    } catch (e) { console.error(e); setError('Could not generate. Try refreshing.') }
    finally { setLoading(false) }
  }

  const TC = [
    { bg: '#EEF2FF', color: C.blue,   border: '#BFDBFE' },
    { bg: '#FEF3C7', color: '#92400E', border: '#FCD34D' },
    { bg: '#F0FDF4', color: C.green,  border: '#BBF7D0' },
  ]

  if (loading) return <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '24px', justifyContent: 'center' }}><Spinner size="sm" /><span style={{ fontSize: 12, color: C.textMuted }}>Analysing pipeline…</span></div>
  if (error)   return <div style={{ fontSize: 12, color: C.red, padding: '12px', background: '#FFF1F2', borderRadius: 4 }}>{error}</div>
  if (!notes)  return null

  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 14 }}>
      {notes.map((note, i) => {
        const tc = TC[i % TC.length]
        return (
          <div key={i} style={{ padding: '14px', background: C.pageBg, border: `1px solid ${C.border}`, borderLeft: `3px solid ${C.blue}`, borderRadius: '0 4px 4px 0' }}>
            <span style={{ fontSize: 9, fontWeight: 700, padding: '2px 7px', borderRadius: 3, background: tc.bg, color: tc.color, border: `1px solid ${tc.border}`, display: 'inline-block', marginBottom: 8 }}>{note.tag}</span>
            <p style={{ fontSize: 12, color: C.textMid, lineHeight: 1.75, margin: 0 }}>{note.text}</p>
          </div>
        )
      })}
    </div>
  )
}

// ── Activity Feed ──────────────────────────────────────────────────────────────
function ActivityFeed({ decided, all }) {
  const now = new Date()
  const events = []
  ;[...decided].sort((a, b) => new Date(b.decided_at || 0) - new Date(a.decided_at || 0)).slice(0, 4).forEach(t => {
    const ok = t.partner_decision === 'GO'
    events.push({ icon: ok ? '✓' : '✗', iconBg: ok ? '#F0FDF4' : '#FFF1F2', iconColor: ok ? C.green : C.red, text: `${ok ? 'Endorsed' : 'Declined'} — ${(t.title_clean || 'Untitled').slice(0, 38)}`, time: t.decided_at ? new Date(t.decided_at) : null })
  })
  all.filter(t => {
    if (!t.deadline_datetime) return false
    const d = new Date(t.deadline_datetime.replace(' ', 'T'))
    const days = Math.ceil((d - now) / 86400000)
    return days >= 0 && days <= 3
  }).slice(0, 2).forEach(t => {
    const days = Math.ceil((new Date(t.deadline_datetime.replace(' ', 'T')) - now) / 86400000)
    events.push({ icon: '!', iconBg: '#FFF7ED', iconColor: C.amber, text: `Deadline in ${days}d — ${(t.title_clean || 'Untitled').slice(0, 36)}`, time: null })
  })
  function timeAgo(d) {
    if (!d) return 'Today'
    const mins = Math.floor((now - d) / 60000)
    if (mins < 60) return `${mins}m ago`
    const hrs = Math.floor(mins / 60)
    if (hrs < 24) return `${hrs}h ago`
    return `${Math.floor(hrs / 24)}d ago`
  }
  if (!events.length) return <div style={{ fontSize: 12, color: C.textFaint, fontStyle: 'italic' }}>No recent activity.</div>
  return (
    <>
      {events.slice(0, 5).map((ev, i) => (
        <div key={i} style={{ display: 'flex', gap: 10, padding: '9px 0', borderBottom: i < events.length - 1 ? `1px solid ${C.divider}` : 'none', alignItems: 'flex-start' }}>
          <div style={{ width: 22, height: 22, borderRadius: '50%', background: ev.iconBg, color: ev.iconColor, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 10, fontWeight: 700, flexShrink: 0, marginTop: 1 }}>{ev.icon}</div>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 11, color: C.text, fontWeight: 500, lineHeight: 1.4 }}>{ev.text}</div>
            <div style={{ fontSize: 10, color: C.textFaint, marginTop: 2 }}>{timeAgo(ev.time)}</div>
          </div>
        </div>
      ))}
    </>
  )
}

// ── Main ───────────────────────────────────────────────────────────────────────
export default function Dashboard() {
  const navigate   = useNavigate()
  const [loading,  setLoading]  = useState(true)
  const [data,     setData]     = useState(null)
  const [notesKey, setNotesKey] = useState(0)

  useEffect(() => {
    async function load() {
      try {
        const now = new Date(), in7 = new Date(now.getTime() + 7 * 86400000)
        const [todayRes, openRes, closedRes] = await Promise.all([
          api.get('/tenders/today'),
          api.get('/tenders', { params: { status: 'open',   per_page: 100, sort_by: 'p_go' } }),
          api.get('/tenders', { params: { status: 'closed', per_page: 100, sort_by: 'publication_datetime' } }),
        ])
        const open   = openRes.data.items
        const closed = closedRes.data.items

        // Deduplicate — a tender can appear in both open and closed
        const seen = new Set()
        const all = [...open, ...closed].filter(t => {
          if (seen.has(t.id)) return false
          seen.add(t.id)
          return true
        })

        const today = todayRes.data
        const pipelineValue   = open.reduce((s, t) => s + (t.budget || 0), 0)
        const closingThisWeek = open.filter(t => {
          if (!t.deadline_datetime) return false
          const d = new Date(t.deadline_datetime.replace(' ', 'T'))
          return d >= now && d <= in7
        }).length
        const decided  = all.filter(t => t.partner_decision)
        const goCount  = decided.filter(t => t.partner_decision === 'GO').length
        const scored   = open.filter(t => t.p_go != null)
        const avgScore = scored.length ? Math.round(scored.reduce((s, t) => s + t.p_go, 0) / scored.length * 100) : null
        const priority = open.filter(t => t.p_go != null).sort((a, b) => b.p_go - a.p_go).slice(0, 20)
        const mapTenders = open.filter(t => t.p_go >= 0.60 && t.country_name_normalized).sort((a, b) => b.p_go - a.p_go).slice(0, 40)

        // Monthly activity — last 6 months
        const months = []
        for (let i = 5; i >= 0; i--) {
          const d     = new Date(now.getFullYear(), now.getMonth() - i, 1)
          const label = d.toLocaleDateString('en-GB', { month: 'short' })
          const nextD = new Date(d.getFullYear(), d.getMonth() + 1, 1)
          const newCount = open.filter(t => {
            if (!t.publication_datetime) return false
            const pd = new Date(t.publication_datetime.replace(' ', 'T'))
            return pd >= d && pd < nextD
          }).length
          const endorsedCount = decided.filter(t => {
            if (!t.decided_at) return false
            const dd = new Date(typeof t.decided_at === 'string' ? t.decided_at.replace(' ', 'T') : t.decided_at)
            return dd >= d && dd < nextD && t.partner_decision === 'GO'
          }).length
          months.push({ label, new: newCount, endorsed: endorsedCount })
        }

        const sectorMap = {}, portalMap = {}, countryMap = {}, sectorScoreMap = {}
        open.forEach(t => {
          if (t.sector) {
            const s = t.sector.split(',')[0].trim()
            sectorMap[s] = (sectorMap[s] || 0) + 1
            if (t.p_go != null) { if (!sectorScoreMap[s]) sectorScoreMap[s] = []; sectorScoreMap[s].push(t.p_go) }
          }
          if (t.source_portal) portalMap[t.source_portal] = (portalMap[t.source_portal] || 0) + 1
          if (t.country_name_normalized) countryMap[t.country_name_normalized] = (countryMap[t.country_name_normalized] || 0) + 1
        })
        const sectors        = Object.entries(sectorMap).sort((a, b) => b[1] - a[1]).slice(0, 8)
        const portals        = Object.entries(portalMap).sort((a, b) => b[1] - a[1]).slice(0, 6)
        const countries      = Object.entries(countryMap).sort((a, b) => b[1] - a[1]).slice(0, 8)
        const sectorsByScore = Object.entries(sectorScoreMap)
          .map(([name, scores]) => [name, Math.round(scores.reduce((a, b) => a + b, 0) / scores.length * 100)])
          .sort((a, b) => b[1] - a[1]).slice(0, 3)

        const strong  = open.filter(t => t.p_go >= 0.80).length
        const go      = open.filter(t => t.p_go >= 0.60 && t.p_go < 0.80).length
        const review  = open.filter(t => t.p_go >= 0.40 && t.p_go < 0.60).length
        const low     = open.filter(t => t.p_go != null && t.p_go < 0.40).length
        const noScore = open.filter(t => t.p_go == null).length

        setData({
          total: openRes.data.total, pipelineValue, strongGo: today.strong_go?.length ?? 0,
          closingThisWeek, decided, goCount, avgScore, priority, mapTenders,
          sectors, portals, countries, sectorsByScore, months,
          scoreDist: { strong, go, review, low, noScore }, all,
        })
      } catch (e) { console.error(e) } finally { setLoading(false) }
    }
    load()
  }, [])

  if (loading) return <div style={{ display: 'flex', justifyContent: 'center', padding: '80px 0' }}><Spinner size="lg" /></div>
  if (!data)   return null

  const distTotal     = data.scoreDist.strong + data.scoreDist.go + data.scoreDist.review + data.scoreDist.low + data.scoreDist.noScore || 1
  const SECTOR_COLORS = [C.blue, C.navy, C.accent]
  const sortedDecided = [...data.decided].sort((a, b) => new Date(b.decided_at || 0) - new Date(a.decided_at || 0)).slice(0, 6)

  return (
    <div style={{ display: 'flex', flexDirection: 'column', minHeight: '100%', background: C.surface, fontFamily: F }}>
      <IntelStrip />
      <div style={{ flex: 1, padding: '36px 48px 56px', maxWidth: 1100, width: '100%', margin: '0 auto' }}>

        {/* Page header */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr auto', gap: 40, alignItems: 'start', marginBottom: 32 }}>
          <div>
            <div style={{ fontSize: 11, fontWeight: 700, color: C.navy, letterSpacing: '0.10em', textTransform: 'uppercase', marginBottom: 8 }}>Executive Overview</div>
            <h1 style={{ fontSize: 28, fontWeight: 700, color: C.text, margin: '0 0 12px', letterSpacing: '-0.02em' }}>Analytics Dashboard</h1>
            <p style={{ fontSize: 13, color: C.textMuted, margin: 0, lineHeight: 1.75, borderLeft: `3px solid ${C.navy}`, paddingLeft: 14, maxWidth: 480 }}>
              A full read of your active pipeline — scores, team decisions, sector fit, and market signals.
              Use this each morning to prioritise where to focus advisory capacity.
            </p>
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10, minWidth: 380 }}>
            <KpiCard icon={CheckCircle} label="Endorsement Rate"   value={data.decided.length ? `${Math.round((data.goCount / data.decided.length) * 100)}%` : '—'} sub="GO ÷ total reviewed"   color={C.blue} />
            <KpiCard icon={Clock}       label="Expiring This Week" value={data.all.filter(t => { if (!t.deadline_datetime) return false; const d = new Date(t.deadline_datetime.replace(' ','T')); const days = Math.ceil((d - new Date()) / 86400000); return days >= 0 && days <= 7 }).length} sub="Review before close" color={C.blue} />
            <KpiCard icon={Globe}       label="New This Week"      value={data.all.filter(t => t.publication_datetime && (new Date() - new Date(t.publication_datetime.replace(' ','T'))) < 7 * 86400000).length} sub="Added in last 7 days" color={C.blue} />
            <KpiCard icon={TrendingUp}  label="Total Reviewed"     value={data.decided.length} sub="Partner decisions"    color={C.blue} />
          </div>
        </div>

        {/* Pipeline Activity */}
        <div style={{ marginBottom: 32 }}>
          <SectionCard label="Pipeline Activity" meta="New tenders & endorsements · last 6 months">
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
              <p style={{ fontSize: 11, color: C.textFaint, fontStyle: 'italic', margin: 0 }}>
                Track how the pipeline evolves month by month — new consulting opportunities scored alongside team endorsement volume.
              </p>
              <div style={{ display: 'flex', gap: 16, flexShrink: 0, marginLeft: 24 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}><div style={{ width: 10, height: 10, borderRadius: 2, background: '#BFDBFE' }} /><span style={{ fontSize: 11, color: C.textMuted }}>New tenders</span></div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}><div style={{ width: 10, height: 10, borderRadius: 2, background: C.blue }} /><span style={{ fontSize: 11, color: C.textMuted }}>Endorsed</span></div>
              </div>
            </div>
            <MonthlyChart data={data.months} />
          </SectionCard>
        </div>

        {/* Map + Priority Pipeline */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginBottom: 32 }}>
          <SectionCard label="Pipeline Map" meta="Africa · Score ≥ 60% · click to open" noPad >
            <div style={{ height: 240 }}>
              <AfricaMap tenders={data.mapTenders} onPinClick={t => navigate(`/tenders/${t.id}`)} />
            </div>
          </SectionCard>

          <SectionCard label="Priority Pipeline" meta="Top scored · ranked by strategic fit"
            action={{ label: 'View all ›', onClick: () => navigate('/tenders') }} noPad>
            {data.priority.length === 0
              ? <div style={{ padding: '24px', fontSize: 12, color: C.textFaint, fontStyle: 'italic', textAlign: 'center' }}>No scored opportunities yet.</div>
              : <div style={{ height: 242, overflowY: 'scroll' }}>
                  {data.priority.map((t, i) => <PriorityRow key={t.id} tender={t} isLast={i === data.priority.length - 1} />)}
                </div>
            }
          </SectionCard>
        </div>
        {/* Activity Feed + Recent Endorsements */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginBottom: 32 }}>
          <SectionCard label="Recent Activity" meta="Last 24h across the team"
            hint="Decisions submitted and deadlines approaching — a live record of what's happening.">
            <ActivityFeed decided={data.decided} all={data.all} />
          </SectionCard>

          <SectionCard label="Recent Endorsements" meta={`${data.decided.length} total`}
            action={{ label: 'All decisions ›', onClick: () => navigate('/decisions') }} noPad
            hint="Partner decisions submitted on the Opportunity Detail page.">
            {sortedDecided.length > 0
              ? sortedDecided.map((t, i) => (
                  <EndorsementRow key={t.id} tender={t} isLast={i === sortedDecided.length - 1} />
                ))
              : <div style={{ padding: '24px', fontSize: 12, color: '#9CA3AF', fontStyle: 'italic', textAlign: 'center' }}>No endorsements yet.</div>
            }
          </SectionCard>
        </div>

        {/* Top 3 Sectors + Score Breakdown */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 20, marginBottom: 32 }}>
          <SectionCard label="Top 3 Sectors" meta="Avg. strategic fit score · KPMG positioning"
            hint="Sectors where KPMG scores highest. Use to guide practice area focus and BD prioritisation.">
            <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
              {data.sectorsByScore.map(([name, avg], i) => (
                <div key={name}>
                  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                      <div style={{ width: 28, height: 28, borderRadius: 4, background: i === 0 ? '#EEF2FF' : i === 1 ? '#F0FDF4' : '#FEF3C7', display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0 }}>
                        <span style={{ fontSize: 12, fontWeight: 700, color: SECTOR_COLORS[i], fontFamily: MONO }}>{i + 1}</span>
                      </div>
                      <span style={{ fontSize: 13, fontWeight: 600, color: C.text }}>{name}</span>
                    </div>
                    <span style={{ fontSize: 14, fontWeight: 700, color: SECTOR_COLORS[i], fontFamily: MONO }}>{avg}%</span>
                  </div>
                  <div style={{ height: 6, background: C.pageBg, borderRadius: 3 }}>
                    <div style={{ height: 6, borderRadius: 3, background: SECTOR_COLORS[i], width: `${avg}%`, transition: 'width .6s ease' }} />
                  </div>
                </div>
              ))}
            </div>
          </SectionCard>

          <SectionCard label="Score Breakdown" meta={`${distTotal} open tenders`}
            hint="A healthy pipeline has ≥ 30% in Priority or GO. Use this to track overall pipeline quality over time.">
            <DonutChart scoreDist={data.scoreDist} total={distTotal} />
          </SectionCard>
        </div>

        {/* Market Notes */}
        <SectionCard label="Market Notes" meta="AI-generated from your live pipeline · refreshes weekly"
          hint="Patterns and signals detected from your pipeline data. Use these to inform positioning conversations and proposal strategy."
          action={{ label: '↺ Refresh', onClick: () => { localStorage.removeItem(NOTES_CACHE_KEY); setNotesKey(k => k + 1) } }}>
          <MarketNotesPanel key={notesKey} data={data} />
        </SectionCard>

      </div>

      <PlatformFooter />
    </div>
  )
}