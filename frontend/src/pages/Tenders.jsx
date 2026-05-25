import { useEffect, useState, useCallback, useRef } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import {
  Search, SlidersHorizontal,
  X, BookmarkPlus, Bookmark,
  RotateCcw, ChevronDown,
  Globe, FileText, ToggleLeft, Languages, MapPin,
  Building2, Layers, Calendar, Clock,
} from 'lucide-react'
import api from '../lib/api'
import { Spinner, EmptyState } from '../components/ui'
import { Avatar } from '../components/layout/Navbar'
import IntelStrip from '../components/layout/IntelStrip'

const K = {
  navy:   '#0D1F6B',
  blue:   '#00338D',
  royal:  '#1B57C5',
  mauve:  '#8C3075',
  violet: '#5B4EA0',
  teal:   '#00B2A9',
}
const C = {
  text:      '#111827',
  textMid:   '#374151',
  textMuted: '#6B7280',
  textFaint: '#9CA3AF',
  border:    '#E5E7EB',
  divider:   '#F3F4F6',
  surface:   '#FFFFFF',
  pageBg:    '#F8FAFC',
}
const FONT = "'Inter', 'Segoe UI', system-ui, sans-serif"
const MONO = "'DM Mono', 'JetBrains Mono', ui-monospace, monospace"
const BUDGET_MAX = 20_000_000

// ── URL helpers ───────────────────────────────────────────────────────────────
function filtersToParams(filters, search, sortBy, page) {
  const p = {}
  if (search)                            p.q        = search
  if (sortBy !== 'publication_datetime') p.sort     = sortBy
  if (page > 1)                          p.page     = page
  if (filters.status !== 'open')         p.status   = filters.status
  if (filters.portals.length)            p.portals  = filters.portals.join(',')
  if (filters.procurements.length)       p.procs    = filters.procurements.join(',')
  if (filters.languages.length)          p.langs    = filters.languages.join(',')
  if (filters.countries.length)          p.countries = filters.countries.join(',')
  if (filters.agencies.length)           p.agencies  = filters.agencies.join(',')
  if (filters.sectors.length)            p.sectors   = filters.sectors.join(',')
  if (filters.posted_from)               p.from      = filters.posted_from
  if (filters.posted_till)               p.till      = filters.posted_till
  if (filters.budget_min != null)        p.bmin      = filters.budget_min
  if (filters.budget_max != null)        p.bmax      = filters.budget_max
  return p
}

function paramsToFilters(sp) {
  return {
    portals:      sp.get('portals')   ? sp.get('portals').split(',')   : [],
    // CHANGED: default to CONSULTING if no procs param in URL
    procurements: sp.get('procs')     ? sp.get('procs').split(',')     : ['CONSULTING'],
    languages:    sp.get('langs')     ? sp.get('langs').split(',')     : [],
    countries:    sp.get('countries') ? sp.get('countries').split(',') : [],
    agencies:     sp.get('agencies')  ? sp.get('agencies').split(',')  : [],
    sectors:      sp.get('sectors')   ? sp.get('sectors').split(',')   : [],
    status:       sp.get('status')    || 'open',
    posted_from:  sp.get('from')      || '',
    posted_till:  sp.get('till')      || '',
    budget_min:   sp.get('bmin')      ? Number(sp.get('bmin')) : null,
    budget_max:   sp.get('bmax')      ? Number(sp.get('bmax')) : null,
  }
}

// CHANGED: CONSULTING selected by default
const DEFAULT_FILTERS = {
  portals: [], procurements: ['CONSULTING'], status: 'open', languages: [],
  countries: [], agencies: [], sectors: [],
  posted_from: '', posted_till: '',
  budget_min: null, budget_max: null,
}

// ── Agency logo ───────────────────────────────────────────────────────────────
const AGENCY_SLUG_MAP = {
  'world bank':'worldbank','the world bank':'worldbank','world bank group':'worldbank','ibrd':'worldbank','ida':'worldbank',
  'ifc':'ifc','african development bank':'afdb','african development bank group':'afdb','afdb':'afdb',
  'undp':'undp','united nations development programme':'undp','unicef':'unicef','who':'who',
  'wfp':'wfp','world food programme':'wfp','unhcr':'unhcr','unfpa':'unfpa','unops':'unops','un women':'unwomen',
  'ilo':'ilo','international labour organization':'ilo','itc-ilo':'ilo','fao':'fao',
  'adb':'adb','asian development bank':'adb','idb':'iadb','iadb':'iadb','inter-american development bank':'iadb',
  'ebrd':'ebrd','eib':'eib','european investment bank':'eib',
}

function agencyToSlug(a) {
  if (!a) return ''
  const k = a.toLowerCase().trim()
  return AGENCY_SLUG_MAP[k] || k.replace(/\s+/g,'-').replace(/[^a-z0-9-]/g,'')
}

function agencyLabel(agency, portal) {
  if (!agency) return (portal||'?').slice(0,5).toUpperCase()
  const m = agency.match(/\(([A-Z]{2,6})\)/)
  if (m) return m[1]
  const words = agency.trim().split(/\s+/)
  return words.length > 1 ? words.map(w=>w[0]).join('').toUpperCase().slice(0,5) : agency.slice(0,4).toUpperCase()
}

function AgencyLogo({ agency, portal }) {
  const [imgOk, setImgOk] = useState(true)
  const slug  = agencyToSlug(agency) || agencyToSlug(portal)
  const label = agencyLabel(agency, portal)
  return (
    <div style={{ width:64,height:64,flexShrink:0,border:'1px solid #E5E7EB',display:'flex',alignItems:'center',justifyContent:'center',borderRadius:6,overflow:'hidden',background:'white' }}>
      {imgOk && slug
        ? <img src={imgOk==='svg'?`/agencies/${slug}.svg`:`/agencies/${slug}.png`} alt={agency||portal}
            style={{ width:54,height:54,objectFit:'contain',background:'white',padding:'2px' }}
            onError={()=>{ if(imgOk===true) setImgOk('svg'); else setImgOk(false) }} />
        : <span style={{ fontSize:11,fontWeight:700,color:K.blue,fontFamily:FONT }}>{label}</span>}
    </div>
  )
}

// ── Tender card ───────────────────────────────────────────────────────────────
function TenderCard({ tender, onSave, savedIds }) {
  const navigate   = useNavigate()
  const isSaved    = savedIds.has(tender.id)
  const now        = new Date()
  const deadlineDt = tender.deadline_datetime ? new Date(tender.deadline_datetime.replace(' ','T')) : null
  const isExpired  = deadlineDt ? deadlineDt < now : false
  const deadlineDate = deadlineDt ? deadlineDt.toLocaleDateString('en-GB',{day:'numeric',month:'short',year:'numeric'}) : '—'
  const postedDate   = tender.publication_datetime ? new Date(tender.publication_datetime.replace(' ','T')).toLocaleDateString('en-GB',{day:'numeric',month:'short',year:'numeric'}) : '—'

  return (
    <div onClick={()=>navigate(`/tenders/${tender.id}`)}
      style={{ background:'white',border:'1px solid #E5E7EB',cursor:'pointer',boxShadow:'0 1px 3px rgba(0,0,0,.06)',transition:'box-shadow .2s ease,transform .2s ease',fontFamily:FONT,borderLeft:'3px solid #E5E7EB' }}
      onMouseEnter={e=>{ e.currentTarget.style.boxShadow='0 4px 14px rgba(0,51,141,.09)'; e.currentTarget.style.transform='translateY(-1px)'; e.currentTarget.style.borderLeftColor=K.blue }}
      onMouseLeave={e=>{ e.currentTarget.style.boxShadow='0 1px 3px rgba(0,0,0,.06)'; e.currentTarget.style.transform='translateY(0)'; e.currentTarget.style.borderLeftColor='#E5E7EB' }}>
      <div style={{ display:'flex',alignItems:'stretch' }}>
        <div style={{ display:'flex',flexDirection:'column',alignItems:'center',justifyContent:'flex-start',padding:'14px 10px 0',flexShrink:0 }}>
          <button onClick={e=>{ e.stopPropagation(); onSave(tender.id,isSaved) }} title={isSaved?'Remove':'Save'}
            style={{ background:'none',border:'none',cursor:'pointer',padding:0,color:isSaved?'#CBD5E1':'#E2E8F0',transition:'color .15s' }}
            onMouseEnter={e=>e.currentTarget.style.color='#94A3B8'} onMouseLeave={e=>e.currentTarget.style.color=isSaved?'#CBD5E1':'#E2E8F0'}>
            {isSaved?<Bookmark style={{ width:17,height:17,fill:'#CBD5E1',color:'#CBD5E1' }} />:<BookmarkPlus style={{ width:17,height:17 }} />}
          </button>
        </div>
        <div style={{ display:'flex',alignItems:'center',padding:'12px 12px 12px 6px',flexShrink:0 }}>
          <AgencyLogo agency={tender.funding_agency} portal={tender.source_portal} />
        </div>
        <div style={{ flex:1,minWidth:0,padding:'12px 16px 12px 0',borderRight:'1px solid #F1F5F9' }}>
          <h3 style={{ fontSize:13,fontWeight:600,color:'#1E293B',lineHeight:1.35,display:'-webkit-box',WebkitLineClamp:2,WebkitBoxOrient:'vertical',overflow:'hidden',marginBottom:8,fontFamily:FONT }}>
            {tender.title_clean||'Untitled'}
          </h3>
          <div style={{ display:'flex',flexDirection:'column',gap:3 }}>
            <div style={{ display:'flex',alignItems:'center',gap:8,fontSize:11 }}>
              <span style={{ color:'#9CA3AF',width:96,flexShrink:0 }}>Funding agency</span>
              <span style={{ color:'#4B5563' }}>{tender.funding_agency||'—'}</span>
            </div>
            <div style={{ display:'flex',alignItems:'center',gap:8,fontSize:11 }}>
              <span style={{ color:'#9CA3AF',width:96,flexShrink:0 }}>Posted</span>
              <span style={{ color:'#4B5563' }}>{postedDate}</span>
            </div>
          </div>
        </div>
        <div style={{ width:176,flexShrink:0,padding:'12px 16px',borderRight:'1px solid #F1F5F9',display:'flex',flexDirection:'column',gap:4 }}>
          <div style={{ display:'flex',alignItems:'center',gap:8,fontSize:11 }}>
            <span style={{ color:'#9CA3AF',width:64,flexShrink:0 }}>Status</span>
            <span style={{ fontWeight:600,color:isExpired?K.mauve:K.teal }}>{isExpired?'Closed':'Open'}</span>
          </div>
          <div style={{ display:'flex',alignItems:'center',gap:8,fontSize:11 }}>
            <span style={{ color:'#9CA3AF',width:64,flexShrink:0 }}>Location</span>
            <span style={{ color:'#4B5563' }}>{tender.country_name_normalized||'N/A'}</span>
          </div>
          <div style={{ display:'flex',alignItems:'center',gap:8,fontSize:11 }}>
            <span style={{ color:'#9CA3AF',width:64,flexShrink:0 }}>Budget</span>
            <span style={{ color:'#4B5563' }}>{tender.budget?`${(tender.budget/1_000_000).toFixed(1)}M ${tender.currency||''}`:'N/A'}</span>
          </div>
          {tender.decisions?.length>0 && (
            <div style={{ display:'flex',alignItems:'center',gap:4,paddingTop:4 }}>
              {tender.decisions.slice(0,4).map((d,i)=>(
                <Avatar key={i} name={d.user_full_name} email={d.user_email||d.user_full_name} size="xs" className={`border ${d.decision==='GO'?'border-green-400':'border-red-400'}`} />
              ))}
              {tender.decisions.length>4 && <span style={{ fontSize:10,color:'#9CA3AF' }}>+{tender.decisions.length-4}</span>}
            </div>
          )}
        </div>
        <div style={{ width:112,flexShrink:0,padding:'12px 16px',display:'flex',flexDirection:'column',gap:4 }}>
          <div style={{ fontSize:10,color:'#9CA3AF',textTransform:'uppercase',letterSpacing:'.06em' }}>Deadline</div>
          <div style={{ fontSize:11,fontWeight:600,color:'#4B5563' }}>{deadlineDate}</div>
        </div>
      </div>
    </div>
  )
}

// ── Checkbox & dropdowns ──────────────────────────────────────────────────────
function CbB({ checked }) {
  return (
    <div style={{ width:14,height:14,flexShrink:0,border:`1.5px solid ${checked?K.blue:'#D1D5DB'}`,background:checked?K.blue:'white',borderRadius:2,display:'flex',alignItems:'center',justifyContent:'center',transition:'all .15s' }}>
      {checked && <div style={{ width:7,height:4,borderLeft:'1.5px solid white',borderBottom:'1.5px solid white',transform:'rotate(-45deg) translateY(-1px)' }} />}
    </div>
  )
}

function CheckDropdown({ options, selected, onChange }) {
  const [open,setOpen]=useState(false); const ref=useRef(null)
  useEffect(()=>{ const h=e=>{ if(ref.current&&!ref.current.contains(e.target)) setOpen(false) }; document.addEventListener('mousedown',h); return()=>document.removeEventListener('mousedown',h) },[])
  const toggle=val=>onChange(selected.includes(val)?selected.filter(x=>x!==val):[...selected,val])
  const active=selected.length>0
  return (
    <div ref={ref} style={{ position:'relative' }}>
      <button onClick={()=>setOpen(!open)} style={{ width:'100%',display:'flex',alignItems:'center',justifyContent:'space-between',border:`1px solid ${active?'#BFDBFE':'#E5E7EB'}`,borderRadius:4,padding:'5px 9px',background:active?'#EEF2FF':'white',cursor:'pointer',fontFamily:FONT }}>
        <span style={{ fontSize:11,color:active?K.blue:'#6B7280' }}>{active?`${selected.length} selected`:'All'}</span>
        <ChevronDown style={{ width:11,height:11,color:'#9CA3AF' }} />
      </button>
      {open && (
        <div style={{ position:'absolute',top:'calc(100% + 4px)',left:0,zIndex:999,minWidth:150,background:'white',border:'1px solid #E5E7EB',borderRadius:6,boxShadow:'0 6px 20px rgba(0,0,0,.08)' }}>
          {options.map(o=>{ const val=typeof o==='object'?o.val:o,lbl=typeof o==='object'?o.label:o; return (
            <div key={val} style={{ display:'flex',alignItems:'center',gap:8,padding:'6px 10px',cursor:'pointer' }}
              onMouseEnter={e=>e.currentTarget.style.background='#F8FAFC'} onMouseLeave={e=>e.currentTarget.style.background='white'}
              onMouseDown={e=>{ e.preventDefault();e.stopPropagation();toggle(val) }}>
              <CbB checked={selected.includes(val)} /><span style={{ fontSize:11,color:'#374151',fontFamily:FONT }}>{lbl}</span>
            </div>
          )})}
        </div>
      )}
    </div>
  )
}

function SearchCheckDropdown({ options, selected, onChange, placeholder='Search…' }) {
  const [open,setOpen]=useState(false);const [q,setQ]=useState('');const ref=useRef(null)
  useEffect(()=>{ const h=e=>{ if(ref.current&&!ref.current.contains(e.target)) setOpen(false) }; document.addEventListener('mousedown',h); return()=>document.removeEventListener('mousedown',h) },[])
  const toggle=o=>onChange(selected.includes(o)?selected.filter(x=>x!==o):[...selected,o])
  const filtered=options.filter(o=>o.toLowerCase().includes(q.toLowerCase()))
  const active=selected.length>0
  return (
    <div ref={ref} style={{ position:'relative' }}>
      <button onClick={()=>setOpen(!open)} style={{ width:'100%',display:'flex',alignItems:'center',justifyContent:'space-between',border:`1px solid ${active?'#BFDBFE':'#E5E7EB'}`,borderRadius:4,padding:'5px 9px',background:active?'#EEF2FF':'white',cursor:'pointer',fontFamily:FONT }}>
        <span style={{ fontSize:11,color:active?K.blue:'#6B7280' }}>{active?`${selected.length} selected`:'All'}</span>
        <ChevronDown style={{ width:11,height:11,color:'#9CA3AF' }} />
      </button>
      {open && (
        <div style={{ position:'absolute',top:'calc(100% + 4px)',left:0,zIndex:999,maxHeight:200,overflowY:'auto',minWidth:190,background:'white',border:'1px solid #E5E7EB',borderRadius:6,boxShadow:'0 6px 20px rgba(0,0,0,.08)' }}>
          <div style={{ padding:7,borderBottom:'1px solid #F1F5F9',position:'sticky',top:0,background:'white' }}>
            <input value={q} onChange={e=>setQ(e.target.value)} placeholder={placeholder} autoFocus style={{ width:'100%',fontSize:11,border:'1px solid #E5E7EB',borderRadius:4,padding:'4px 7px',outline:'none',boxSizing:'border-box',fontFamily:FONT }} />
          </div>
          {filtered.length===0?<div style={{ padding:'7px 10px',fontSize:11,color:'#9CA3AF' }}>No results</div>
            :filtered.map(o=>(
              <div key={o} style={{ display:'flex',alignItems:'center',gap:8,padding:'6px 10px',cursor:'pointer' }}
                onMouseEnter={e=>e.currentTarget.style.background='#F8FAFC'} onMouseLeave={e=>e.currentTarget.style.background='white'}
                onMouseDown={e=>{ e.preventDefault();e.stopPropagation();toggle(o) }}>
                <CbB checked={selected.includes(o)} /><span style={{ fontSize:11,color:'#374151',fontFamily:FONT }}>{o}</span>
              </div>
            ))}
        </div>
      )}
    </div>
  )
}

function SelectedTags({ items, onRemove }) {
  if (!items.length) return null
  return (
    <div style={{ display:'flex',flexWrap:'wrap',gap:4,marginTop:5 }}>
      {items.map(item=>(
        <span key={item} style={{ display:'flex',alignItems:'center',gap:3,fontSize:10,fontWeight:500,background:'#EEF2FF',color:K.blue,padding:'2px 6px',borderRadius:3,fontFamily:FONT }}>
          {item.length>14?item.slice(0,14)+'…':item}
          <button onClick={()=>onRemove(item)} style={{ background:'none',border:'none',padding:0,cursor:'pointer',color:K.blue,display:'flex' }}><X style={{ width:9,height:9 }} /></button>
        </span>
      ))}
    </div>
  )
}

function DualSlider({ minVal, maxVal, onChange }) {
  const trackRef=useRef(null);const dragging=useRef(null)
  const mn=minVal??0;const mx=maxVal??BUDGET_MAX
  const pct=v=>Math.round((v/BUDGET_MAX)*100)
  const fmt=v=>v>=BUDGET_MAX?'20M+':v>=1e6?`${(v/1e6).toFixed(1).replace('.0','')}M`:v>=1e3?`${(v/1e3).toFixed(0)}K`:`$${v}`
  const getV=useCallback(e=>{ const r=trackRef.current.getBoundingClientRect(),cx=e.touches?e.touches[0].clientX:e.clientX; return Math.round((Math.max(0,Math.min(1,(cx-r.left)/r.width))*BUDGET_MAX)/5e5)*5e5 },[])
  useEffect(()=>{
    const mv=e=>{ if(!dragging.current) return; const v=getV(e); dragging.current==='min'?onChange(Math.min(v,mx-5e5),mx):onChange(mn,Math.max(v,mn+5e5)) }
    const up=()=>{ dragging.current=null }
    document.addEventListener('mousemove',mv);document.addEventListener('mouseup',up);document.addEventListener('touchmove',mv);document.addEventListener('touchend',up)
    return()=>{ document.removeEventListener('mousemove',mv);document.removeEventListener('mouseup',up);document.removeEventListener('touchmove',mv);document.removeEventListener('touchend',up) }
  },[mn,mx,getV,onChange])
  return (
    <div>
      <div style={{ display:'flex',justifyContent:'space-between',marginBottom:5 }}>
        <span style={{ fontSize:10,color:'#9CA3AF',textTransform:'uppercase',letterSpacing:'.05em',fontFamily:FONT }}>Budget</span>
        <span style={{ fontSize:11,fontWeight:600,color:K.blue,fontFamily:FONT }}>{mn===0&&mx>=BUDGET_MAX?'Any':fmt(mn)+' – '+fmt(mx)}</span>
      </div>
      <div ref={trackRef} style={{ position:'relative',height:18,display:'flex',alignItems:'center',cursor:'pointer',userSelect:'none' }}>
        <div style={{ position:'absolute',left:0,right:0,height:3,background:'#E5E7EB',borderRadius:2 }} />
        <div style={{ position:'absolute',left:`${pct(mn)}%`,width:`${pct(mx)-pct(mn)}%`,height:3,background:K.blue,borderRadius:2 }} />
        {['min','max'].map(h=>(
          <div key={h} onMouseDown={e=>{ dragging.current=h;e.preventDefault() }} onTouchStart={()=>{ dragging.current=h }}
            style={{ position:'absolute',left:`${pct(h==='min'?mn:mx)}%`,transform:'translateX(-50%)',width:13,height:13,borderRadius:'50%',background:'white',border:`2px solid ${K.blue}`,cursor:'pointer',zIndex:2 }} />
        ))}
      </div>
    </div>
  )
}

function FS({ title, Icon, children }) {
  return (
    <div>
      <div style={{ display:'flex',alignItems:'center',gap:4,marginBottom:5 }}>
        {Icon && <Icon style={{ width:10,height:10,color:'#9CA3AF' }} />}
        <span style={{ fontSize:10,fontWeight:600,color:'#9CA3AF',textTransform:'uppercase',letterSpacing:'.06em',fontFamily:FONT }}>{title}</span>
      </div>
      {children}
    </div>
  )
}

const PORTAL_OPTS      = [{val:'afdb',label:'AFDB'},{val:'worldbank',label:'World Bank'},{val:'undp',label:'UNDP'},{val:'ungm',label:'UNGM'}]
const PROCUREMENT_OPTS = [{val:'CONSULTING',label:'Consulting'},{val:'WORKS',label:'Works'},{val:'GOODS',label:'Goods'},{val:'NON-CONSULTING',label:'Non-Consulting'}]
const STATUS_OPTS      = [{val:'open',label:'Open'},{val:'closed',label:'Closed'},{val:'all',label:'All'}]
const LANG_OPTS        = [{val:'en',label:'English'},{val:'fr',label:'French'},{val:'other',label:'Other'}]

function FiltersPanel({ filters, onChange, onApply, onReset, onClose, filterOptions }) {
  const { countries=[], agencies=[], sectors=[] } = filterOptions
  const ss=[...sectors].sort((a,b)=>a.toLowerCase()==='other'?1:b.toLowerCase()==='other'?-1:a.localeCompare(b))
  return (
    <div style={{ background:'white',border:'1px solid #E5E7EB',borderRadius:6,boxShadow:'0 4px 16px rgba(0,0,0,.06)',fontFamily:FONT }}>
      <div style={{ display:'flex',alignItems:'center',justifyContent:'space-between',padding:'9px 16px',borderBottom:'1px solid #F1F5F9' }}>
        <span style={{ fontSize:12,fontWeight:600,color:'#374151' }}>Filter Opportunities</span>
        <button onClick={onClose} style={{ color:'#9CA3AF',cursor:'pointer',background:'none',border:'none',display:'flex' }}><X style={{ width:13,height:13 }} /></button>
      </div>
      <div style={{ padding:'12px 16px',display:'grid',gridTemplateColumns:'repeat(5,1fr)',gap:'10px 14px' }}>
        <FS title="Portal"         Icon={Globe}      ><CheckDropdown options={PORTAL_OPTS} selected={filters.portals} onChange={v=>onChange({...filters,portals:v})} /><SelectedTags items={filters.portals.map(v=>PORTAL_OPTS.find(o=>o.val===v)?.label||v)} onRemove={l=>{const v=PORTAL_OPTS.find(o=>o.label===l)?.val||l;onChange({...filters,portals:filters.portals.filter(x=>x!==v)})}} /></FS>
        <FS title="Procurement"    Icon={FileText}   ><CheckDropdown options={PROCUREMENT_OPTS} selected={filters.procurements} onChange={v=>onChange({...filters,procurements:v})} /><SelectedTags items={filters.procurements.map(v=>PROCUREMENT_OPTS.find(o=>o.val===v)?.label||v)} onRemove={l=>{const v=PROCUREMENT_OPTS.find(o=>o.label===l)?.val||l;onChange({...filters,procurements:filters.procurements.filter(x=>x!==v)})}} /></FS>
        <FS title="Status"         Icon={ToggleLeft} ><CheckDropdown options={STATUS_OPTS} selected={filters.status?[filters.status]:[]} onChange={v=>onChange({...filters,status:v[v.length-1]||'open'})} /></FS>
        <FS title="Language"       Icon={Languages}  ><CheckDropdown options={LANG_OPTS} selected={filters.languages} onChange={v=>onChange({...filters,languages:v})} /></FS>
        <FS title="Country"        Icon={MapPin}     ><SearchCheckDropdown options={countries} selected={filters.countries} onChange={v=>onChange({...filters,countries:v})} placeholder="Search country…" /><SelectedTags items={filters.countries} onRemove={c=>onChange({...filters,countries:filters.countries.filter(x=>x!==c)})} /></FS>
        <FS title="Funding Agency" Icon={Building2}  ><SearchCheckDropdown options={agencies} selected={filters.agencies} onChange={v=>onChange({...filters,agencies:v})} placeholder="Search agency…" /><SelectedTags items={filters.agencies} onRemove={a=>onChange({...filters,agencies:filters.agencies.filter(x=>x!==a)})} /></FS>
        <FS title="Sector"         Icon={Layers}     ><SearchCheckDropdown options={ss} selected={filters.sectors} onChange={v=>onChange({...filters,sectors:v})} placeholder="Search sector…" /><SelectedTags items={filters.sectors} onRemove={s=>onChange({...filters,sectors:filters.sectors.filter(x=>x!==s)})} /></FS>
        <FS title="Posted From"    Icon={Calendar}   ><input type="date" value={filters.posted_from} onChange={e=>onChange({...filters,posted_from:e.target.value})} style={{ width:'100%',fontSize:11,border:'1px solid #E5E7EB',borderRadius:4,padding:'5px 7px',outline:'none',color:'#374151',boxSizing:'border-box',fontFamily:FONT }} /></FS>
        <FS title="Until"          Icon={Calendar}   ><input type="date" value={filters.posted_till} onChange={e=>onChange({...filters,posted_till:e.target.value})} style={{ width:'100%',fontSize:11,border:'1px solid #E5E7EB',borderRadius:4,padding:'5px 7px',outline:'none',color:'#374151',boxSizing:'border-box',fontFamily:FONT }} /></FS>
        <div style={{ gridColumn:'span 2' }}><DualSlider minVal={filters.budget_min} maxVal={filters.budget_max} onChange={(mn,mx)=>onChange({...filters,budget_min:mn||null,budget_max:mx>=BUDGET_MAX?null:mx})} /></div>
      </div>
      <div style={{ display:'flex',alignItems:'center',justifyContent:'space-between',padding:'9px 16px',borderTop:'1px solid #F1F5F9',background:'#F8FAFC',borderRadius:'0 0 6px 6px' }}>
        <button onClick={onReset} style={{ display:'flex',alignItems:'center',gap:4,fontSize:11,color:'#9CA3AF',cursor:'pointer',background:'none',border:'none',fontFamily:FONT }}><RotateCcw style={{ width:10,height:10 }} /> Reset</button>
        <button onClick={onApply} style={{ fontSize:11,fontWeight:600,background:K.blue,color:'white',padding:'5px 18px',border:'none',borderRadius:4,cursor:'pointer',fontFamily:FONT }}>Apply</button>
      </div>
    </div>
  )
}

// ── Sidebar row card — refined ────────────────────────────────────────────────
function SidebarRow({ t, badge, badgeColor, navigate }) {
  const [hov, setHov] = useState(false)
  return (
    <div onClick={() => navigate(`/tenders/${t.id}`)}
      onMouseEnter={() => setHov(true)} onMouseLeave={() => setHov(false)}
      style={{ padding:'9px 14px', cursor:'pointer', borderBottom:`1px solid ${C.divider}`, background: hov ? C.pageBg : C.surface, transition:'background .12s', fontFamily:FONT }}>
      <div style={{ display:'flex', alignItems:'flex-start', justifyContent:'space-between', gap:8, marginBottom:4 }}>
        <div style={{ fontSize:11, color:C.text, fontWeight:500, lineHeight:1.35, display:'-webkit-box', WebkitLineClamp:2, WebkitBoxOrient:'vertical', overflow:'hidden' }}>
          {t.title_clean || 'Untitled'}
        </div>
        {badge && (
          <span style={{ fontSize:10, fontWeight:500, color:badgeColor||K.blue, background:`${badgeColor||K.blue}14`, border:`1px solid ${badgeColor||K.blue}30`, padding:'1px 6px', borderRadius:3, flexShrink:0, fontFamily:FONT, whiteSpace:'nowrap' }}>
            {badge}
          </span>
        )}
      </div>
      <div style={{ display:'flex', alignItems:'center', gap:4 }}>
        <div style={{ width:4, height:4, borderRadius:'50%', background:C.textFaint, flexShrink:0 }} />
        <span style={{ fontSize:10, color:C.textFaint, fontFamily:FONT }}>{t.funding_agency || '—'}</span>
      </div>
    </div>
  )
}

// ── Sidebar panel — light grey header matching filter panel style ──────────────
function SidebarSection({ icon: Icon, title, count, children }) {
  const [open, setOpen] = useState(false)
  return (
    <div style={{ border:`1px solid ${C.border}`, borderRadius:4, overflow:'hidden', marginBottom:10, boxShadow:'0 1px 3px rgba(0,0,0,.04)' }}>
      {/* Light header — matches FiltersPanel header style */}
      <button onClick={() => setOpen(o => !o)}
        style={{ width:'100%', display:'flex', alignItems:'center', justifyContent:'space-between', padding:'8px 12px', background:C.pageBg, border:'none', borderBottom:`1px solid ${C.border}`, cursor:'pointer', fontFamily:FONT }}>
        <div style={{ display:'flex', alignItems:'center', gap:6 }}>
          <Icon style={{ width:10, height:10, color:C.textFaint }} />
          <span style={{ fontSize:10, fontWeight:600, color:C.textMuted, textTransform:'uppercase', letterSpacing:'0.08em', fontFamily:FONT }}>{title}</span>
        </div>
        <div style={{ display:'flex', alignItems:'center', gap:6 }}>
          {count > 0 && (
            <span style={{ fontSize:10, fontWeight:600, color:K.blue, background:'#EEF2FF', border:'1px solid #BFDBFE', padding:'1px 6px', borderRadius:10, fontFamily:FONT }}>{count}</span>
          )}
          <ChevronDown style={{ width:10, height:10, color:C.textFaint, transform:open?'rotate(180deg)':'rotate(0deg)', transition:'transform .22s ease' }} />
        </div>
      </button>
      {/* Body */}
      <div style={{ maxHeight:open?500:0, overflow:'hidden', transition:'max-height .28s ease' }}>
        {children}
      </div>
    </div>
  )
}

// ── Sidebar ───────────────────────────────────────────────────────────────────
function Sidebar({ savedIds }) {
  const navigate = useNavigate()
  const [closingSoon, setClosingSoon] = useState([])
  const [savedItems,  setSavedItems]  = useState([])

  useEffect(() => {
    const now = new Date(), in7 = new Date(now.getTime() + 7*24*60*60*1000)

    // Fetch ALL open tenders ignoring active filters — sorted by deadline
    api.get('/tenders', { params: { status:'open', per_page:100, sort_by:'deadline' } })
      .then(r => {
        const closing = (r.data.items || []).filter(t => {
          if (!t.deadline_datetime) return false
          const d = new Date(t.deadline_datetime.replace(' ','T'))
          return d >= now && d <= in7
        }).slice(0, 7)
        setClosingSoon(closing)
      }).catch(() => {})
  }, [])

  useEffect(() => {
    if (!savedIds || savedIds.size === 0) {
      setSavedItems([])
      return
    }

    api.get('/tenders', { params: { status:'all', per_page:100 } })
      .then(r => {
        setSavedItems((r.data.items || []).filter(t => savedIds.has(t.id)).slice(0, 7))
      }).catch(() => {})
  }, [savedIds])

  const now = new Date()
  const Empty = ({ msg }) => (
    <div style={{ padding:'14px', fontSize:11, color:C.textFaint, fontStyle:'italic', textAlign:'center', fontFamily:FONT }}>{msg}</div>
  )

  return (
    <div style={{ width:215, flexShrink:0, background:C.surface, borderRight:`1px solid ${C.border}`, display:'flex', flexDirection:'column', padding:'14px 12px', fontFamily:FONT }}>
      <div style={{ fontSize:9, fontWeight:700, color:C.textFaint, textTransform:'uppercase', letterSpacing:'0.12em', marginBottom:12, paddingLeft:2, fontFamily:FONT }}>
        Intelligence Panel
      </div>

      <SidebarSection icon={Clock} title="Closing This Week" count={closingSoon.length}>
        {closingSoon.length === 0
          ? <Empty msg="None closing this week" />
          : closingSoon.map(t => {
              const d = new Date(t.deadline_datetime.replace(' ','T'))
              const days = Math.ceil((d - now) / (1000*60*60*24))
              return (
                <SidebarRow key={t.id} t={t} badge={`${days}d`}
                  badgeColor={days <= 2 ? K.mauve : days <= 5 ? K.violet : K.blue}
                  navigate={navigate} />
              )
            })}
      </SidebarSection>

      <SidebarSection icon={Bookmark} title="Saved" count={savedItems.length}>
        {savedItems.length === 0
          ? <Empty msg="No saved tenders" />
          : savedItems.map(t => <SidebarRow key={t.id} t={t} badgeColor={K.royal} navigate={navigate} />)}
      </SidebarSection>
    </div>
  )
}

// ── Main ──────────────────────────────────────────────────────────────────────
export default function Tenders() {
  const [searchParams,setSearchParams]=useSearchParams()
  const [filters,        setFilters]        = useState(()=>paramsToFilters(searchParams))
  const [pendingFilters, setPendingFilters] = useState(()=>paramsToFilters(searchParams))
  const [search,         setSearch]         = useState(()=>searchParams.get('q')||'')
  const [sortBy,         setSortBy]         = useState(()=>searchParams.get('sort')||'publication_datetime')
  const [page,           setPage]           = useState(()=>Number(searchParams.get('page'))||1)
  const [items,         setItems]         = useState([])
  const [total,         setTotal]         = useState(0)
  const [pages,         setPages]         = useState(1)
  const [loading,       setLoading]       = useState(true)
  const [showFilters,   setShowFilters]   = useState(false)
  const [showSidebar,   setShowSidebar]   = useState(true)
  const [savedIds,      setSavedIds]      = useState(new Set())
  const [filterOptions, setFilterOptions] = useState({countries:[],agencies:[],sectors:[],budget_min:0,budget_max:BUDGET_MAX})

  useEffect(()=>{
    api.get('/tenders/filters').then(r=>setFilterOptions(r.data)).catch(()=>{})
    api.get('/tenders/saved').then(r=>setSavedIds(new Set(r.data.saved_ids))).catch(()=>{})
  },[])

  useEffect(()=>{
    setSearchParams(filtersToParams(filters,search,sortBy,page),{replace:true})
  },[filters,search,sortBy,page])

  const activeFilterCount=[...filters.portals,...filters.procurements,...filters.languages,...filters.countries,...filters.agencies,...filters.sectors,filters.posted_from,filters.posted_till,filters.budget_min!=null?'1':'',filters.budget_max!=null?'1':'',filters.status!=='open'?filters.status:''].filter(Boolean).length

  const fetchTenders=useCallback(async(p=1)=>{
    setLoading(true)
    try {
      const hasOther=filters.languages.includes('other'),explicit=filters.languages.filter(l=>l!=='other')
      const langParam=filters.languages.length===0?undefined:hasOther&&explicit.length===0?'other':explicit.join(',')
      const params={page:p,per_page:20,sort_by:sortBy,status:filters.status||'open',...(search&&{search}),...(filters.portals.length&&{portal:filters.portals.join(',')}),...(filters.procurements.length&&{procurement:filters.procurements.join(',')}),...(filters.countries.length&&{country:filters.countries.join(',')}),...(filters.agencies.length&&{agency:filters.agencies.join(',')}),...(filters.sectors.length&&{sector:filters.sectors.join(',')}),...(langParam&&{language:langParam}),...(filters.posted_from&&{posted_from:filters.posted_from}),...(filters.posted_till&&{posted_till:filters.posted_till}),...(filters.budget_min!=null&&{budget_min:filters.budget_min}),...(filters.budget_max!=null&&{budget_max:filters.budget_max})}
      const{data}=await api.get('/tenders',{params})
      setItems(data.items);setTotal(data.total);setPages(data.pages);setPage(p)
    } catch(err){console.error(err)} finally{setLoading(false)}
  },[search,filters,sortBy])

  useEffect(()=>{fetchTenders(1)},[fetchTenders])

  async function handleSave(id,isSaved) {
    setSavedIds(prev=>{const n=new Set(prev);isSaved?n.delete(id):n.add(id);return n})
    try{ if(isSaved) await api.delete(`/tenders/${id}/save`); else await api.post(`/tenders/${id}/save`) }
    catch{ setSavedIds(prev=>{const n=new Set(prev);isSaved?n.add(id):n.delete(id);return n}) }
  }

  return (
    <div style={{ display:'flex',flexDirection:'column',minHeight:'100vh',fontFamily:FONT }}>

      {/* Intel strip */}
      <IntelStrip total={total} items={items} />

      {/* White shell */}
      <div style={{ background:'#FFFFFF',display:'flex',flexDirection:'column',flex:1 }}>

        {/* Hamburger toolbar */}
        <div style={{ borderBottom:'1px solid #E5E7EB',padding:'6px 16px',display:'flex',alignItems:'center' }}>
          <button onClick={()=>setShowSidebar(s=>!s)}
            style={{ width:26,height:26,borderRadius:'50%',background:K.blue,border:'none',cursor:'pointer',display:'flex',flexDirection:'column',alignItems:'center',justifyContent:'center',gap:3,flexShrink:0,transition:'background .15s' }}
            onMouseEnter={e=>e.currentTarget.style.background=K.royal} onMouseLeave={e=>e.currentTarget.style.background=K.blue}>
            {[0,1,2].map(i=><span key={i} style={{ display:'block',width:11,height:1.2,background:'#fff',borderRadius:2 }} />)}
          </button>
        </div>

        {/* Sidebar + content */}
        <div style={{ display:'flex',flex:1,alignItems:'flex-start' }}>
          {showSidebar && <Sidebar savedIds={savedIds} />}

          <div style={{ flex:1,minWidth:0,padding:'24px 32px',fontFamily:FONT }}>

            {/* ── CHANGED: Page header — professional, no giant h1 ── */}
            <div style={{ marginBottom:20 }}>
              <div style={{ fontSize:10, fontWeight:700, color:K.blue, letterSpacing:'0.10em', textTransform:'uppercase', marginBottom:4, fontFamily:FONT }}>
                Advisory Pipeline
              </div>
              <div style={{ display:'flex', alignItems:'baseline', justifyContent:'space-between' }}>
                <h1 style={{ fontSize:20, fontWeight:600, color:C.text, margin:0, letterSpacing:'-.02em', fontFamily:FONT }}>
                  Consulting Opportunities
                </h1>
                {!loading && (
                  <span style={{ fontSize:12, color:C.textFaint, fontFamily:FONT }}>
                    <span style={{ fontWeight:600, color:C.textMid }}>{total.toLocaleString()}</span> tenders
                    {activeFilterCount > 0 && <span> · {activeFilterCount} filter{activeFilterCount > 1 ? 's' : ''} active</span>}
                  </span>
                )}
              </div>
            </div>

            {/* Search + sort + filters */}
            <div style={{ display:'flex',gap:8,marginBottom:12 }}>
              <div style={{ position:'relative',flex:1 }}>
                <Search style={{ position:'absolute',left:10,top:'50%',transform:'translateY(-50%)',width:13,height:13,color:'#9CA3AF' }} />
                <input value={search} onChange={e=>setSearch(e.target.value)} onKeyDown={e=>e.key==='Enter'&&fetchTenders(1)} placeholder="Search title, country, agency…"
                  style={{ width:'100%',border:'1px solid #E5E7EB',borderRadius:6,paddingLeft:30,paddingRight:12,paddingTop:7,paddingBottom:7,fontSize:12,background:'white',outline:'none',boxSizing:'border-box',color:'#374151',fontFamily:FONT }} />
              </div>
              <select value={sortBy} onChange={e=>setSortBy(e.target.value)}
                style={{ border:'1px solid #E5E7EB',borderRadius:6,padding:'7px 11px',fontSize:12,background:'white',color:'#374151',outline:'none',cursor:'pointer',fontFamily:FONT }}>
                <option value="publication_datetime">Latest</option>
                <option value="deadline">Closing Soon</option>
                <option value="p_go">Top Ranked</option>
              </select>
              <button onClick={()=>{ setShowFilters(f=>!f); if(!showFilters) setPendingFilters(filters) }}
                style={{ display:'flex',alignItems:'center',gap:5,border:`1px solid ${activeFilterCount>0?'#BFDBFE':'#E5E7EB'}`,borderRadius:6,padding:'7px 13px',fontSize:12,cursor:'pointer',background:activeFilterCount>0?'#EEF2FF':'white',color:activeFilterCount>0?K.blue:'#374151',fontFamily:FONT }}>
                <SlidersHorizontal style={{ width:13,height:13 }} />Filters
                {activeFilterCount>0&&<span style={{ fontSize:10,fontWeight:700,background:K.blue,color:'white',borderRadius:'50%',width:16,height:16,display:'flex',alignItems:'center',justifyContent:'center' }}>{activeFilterCount}</span>}
              </button>
            </div>

            {showFilters&&(
              <div style={{ marginBottom:12 }}>
                <FiltersPanel filters={pendingFilters} onChange={setPendingFilters}
                  onApply={()=>{ setFilters(pendingFilters);setShowFilters(false) }}
                  onReset={()=>{ setPendingFilters(DEFAULT_FILTERS);setFilters(DEFAULT_FILTERS);setSearch('') }}
                  onClose={()=>setShowFilters(false)} filterOptions={filterOptions} />
              </div>
            )}

            {loading
              ?<div style={{ display:'flex',justifyContent:'center',padding:'60px 0' }}><Spinner size="lg" /></div>
              :items.length===0
                ?<EmptyState icon={Search} title="No tenders found" description="Try adjusting your search or filters." />
                :<div style={{ display:'flex',flexDirection:'column',gap:6 }}>{items.map(t=><TenderCard key={t.id} tender={t} onSave={handleSave} savedIds={savedIds} />)}</div>}

            {pages>1&&(
              <div style={{ display:'flex',alignItems:'center',justifyContent:'space-between',marginTop:24 }}>
                <span style={{ fontSize:12,color:'#9CA3AF' }}>Page {page} of {pages} · {total.toLocaleString()} tenders</span>
                <div style={{ display:'flex',gap:6 }}>
                  {[{l:'← Prev',d:page<=1,a:()=>fetchTenders(page-1)},{l:'Next →',d:page>=pages,a:()=>fetchTenders(page+1)}].map(b=>(
                    <button key={b.l} onClick={b.a} disabled={b.d} style={{ padding:'6px 13px',fontSize:12,border:'1px solid #E5E7EB',borderRadius:4,background:'white',cursor:b.d?'default':'pointer',color:b.d?'#D1D5DB':'#374151',opacity:b.d?.5:1,fontFamily:FONT }}>{b.l}</button>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* ── Platform notice ── */}
      <div style={{ background:K.navy, fontFamily:FONT }}>
        <div style={{ maxWidth:1100, margin:'0 auto', padding:'28px 40px', display:'grid', gridTemplateColumns:'220px 1fr', gap:48, alignItems:'start' }}>
          <div>
            <div style={{ fontSize:11, fontWeight:700, color:'#fff', letterSpacing:'-.01em', marginBottom:10 }}>KPMG Procurement Advisory</div>
            <p style={{ fontSize:11, color:'rgba(255,255,255,.45)', lineHeight:1.65, margin:'0 0 16px' }}>
              Scores opportunities daily across 4 portals using KPMG's Strategic Fit Model.
            </p>
            <div style={{ display:'flex', gap:20, paddingTop:14, borderTop:'1px solid rgba(255,255,255,.10)' }}>
              {[{v:'4',l:'Portals'},{v:'47',l:'Markets'},{v:'Daily',l:'Refresh'}].map(s=>(
                <div key={s.l}>
                  <div style={{ fontSize:14, fontWeight:700, color:'#fff', fontFamily:MONO, lineHeight:1 }}>{s.v}</div>
                  <div style={{ fontSize:9, color:'rgba(255,255,255,.35)', marginTop:3, textTransform:'uppercase', letterSpacing:'0.07em' }}>{s.l}</div>
                </div>
              ))}
            </div>
          </div>
          <div style={{ display:'grid', gridTemplateColumns:'repeat(4,1fr)', gap:24 }}>
            {[
              {n:'01',title:'Morning Review',      body:"Check priority signals and today's pipeline briefing. Opportunities ≥ 85% require immediate attention."},
              {n:'02',title:'Opportunity Analysis',body:'Open the Detail page for strategic analysis, eligibility, and AI scope summary before forming a position.'},
              {n:'03',title:'Partner Decision',    body:'Submit a Pursue or Decline decision with justification. Decisions are timestamped and logged automatically.'},
              {n:'04',title:'Team Oversight',      body:'Team Decisions tracks all assessments. Conflicts are flagged for Practice Lead review within 24 hours.'},
            ].map(step=>(
              <div key={step.n}>
                <div style={{ fontSize:10, fontWeight:700, color:'#0091DA', fontFamily:MONO, marginBottom:6, letterSpacing:'0.04em' }}>{step.n}</div>
                <div style={{ fontSize:11, fontWeight:600, color:'#fff', marginBottom:5 }}>{step.title}</div>
                <div style={{ fontSize:11, color:'rgba(255,255,255,.42)', lineHeight:1.65 }}>{step.body}</div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* ── Footer ── */}
      <footer style={{ background:'#fff', borderTop:`1px solid ${C.border}`, padding:'12px 40px', display:'flex', alignItems:'center', justifyContent:'space-between', fontFamily:FONT }}>
        <p style={{ fontSize:11, color:C.textFaint, margin:0 }}>© {new Date().getFullYear()} KPMG International — Confidential</p>
        <div style={{ display:'flex', gap:20 }}>
          {['Privacy','Terms','Contact'].map(l=>(
            <a key={l} href="#" style={{ fontSize:11, color:C.textFaint, textDecoration:'none', transition:'color .12s' }}
              onMouseEnter={e=>e.currentTarget.style.color=C.textMid}
              onMouseLeave={e=>e.currentTarget.style.color=C.textFaint}>{l}</a>
          ))}
        </div>
      </footer>

    </div>
  )
}