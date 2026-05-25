// src/components/layout/Navbar.jsx
import { useState, useRef, useEffect } from 'react'
import { NavLink, useNavigate } from 'react-router-dom'
import { useAuth } from '../../lib/auth'
import { LogOut, Clock, CheckCircle, XCircle, ChevronDown, Bell, Search, Settings } from 'lucide-react'
import { clsx } from 'clsx'
import api from '../../lib/api'

// Nav items — /today and /tenders already exist; others are placeholders until pages are created
const NAV_ITEMS = [
  { to: '/today',                  label: 'Overview'               },
  { to: '/tenders',                label: 'Opportunities'          },
  { to: '/decisions',              label: 'Decisions'              },
  { to: '/dashboard',              label: 'Dashboard'              },
]

export function getAvatarColor(email = '') {
  const colors = ['#3B82F6','#8B5CF6','#10B981','#F59E0B','#EC4899','#3fe6d3','#f68e8e','#6366F1']
  let h = 0
  for (let i = 0; i < email.length; i++) h = email.charCodeAt(i) + ((h << 5) - h)
  return colors[Math.abs(h) % colors.length]
}

export function Avatar({ email, name, size = 'md', className = '' }) {
  const letter = (name || email || '?')[0].toUpperCase()
  const color  = getAvatarColor(email || '')
  const sizes  = {
    xs: 'h-5 w-5 text-[9px]',
    sm: 'h-6 w-6 text-[10px]',
    md: 'h-7 w-7 text-xs',
    lg: 'h-9 w-9 text-sm',
  }
  return (
    <div className={clsx(sizes[size], className,
      'rounded-full flex items-center justify-center text-white font-semibold flex-shrink-0 select-none')}
      style={{ background: color }}>
      {letter}
    </div>
  )
}

// ── Profile dropdown ──────────────────────────────────────────────────────────
function ProfileDropdown({ onClose }) {
  const { user, logout } = useAuth()
  const navigate = useNavigate()
  const ref = useRef(null)
  const [decisions, setDecisions] = useState([])
  const [loading,   setLoading]   = useState(true)

  useEffect(() => {
    const handler = e => { if (ref.current && !ref.current.contains(e.target)) onClose() }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [onClose])

  useEffect(() => {
    api.get('/tenders', { params: { has_decision: true, per_page: 8, sort_by: 'enriched_at' } })
      .then(r => setDecisions(r.data.items.filter(t => t.partner_decision)))
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [])

  return (
    <div ref={ref}
      className="absolute right-0 top-[calc(100%+6px)] w-76 bg-white border border-gray-100
                 z-50 overflow-hidden"
      style={{
        width: '288px',
        boxShadow: '0 4px 24px rgba(0,0,0,0.08), 0 1px 4px rgba(0,0,0,0.04)',
        borderRadius: '6px',
      }}>

      {/* User header */}
      <div className="px-4 py-4 border-b border-gray-100">
        <div className="flex items-center gap-3">
          <Avatar email={user?.email} name={user?.full_name} size="lg" />
          <div className="flex-1 min-w-0">
            <div className="text-sm font-semibold text-gray-900 truncate leading-tight">
              {user?.full_name || 'User'}
            </div>
            <div className="text-xs text-gray-400 truncate mt-0.5">{user?.email}</div>
          </div>
          <span className="text-[10px] font-medium text-gray-400 bg-gray-100 px-2 py-0.5 capitalize flex-shrink-0"
                style={{borderRadius:'3px'}}>
            {user?.role}
          </span>
        </div>
      </div>

      {/* Recent decisions */}
      <div className="px-4 py-3">
        <p className="text-[10px] font-semibold text-gray-400 uppercase tracking-widest mb-2.5">
          Recent Decisions
        </p>
        {loading ? (
          <p className="text-xs text-gray-400 py-1">Loading…</p>
        ) : decisions.length === 0 ? (
          <p className="text-xs text-gray-400 py-1">No decisions yet.</p>
        ) : (
          <div className="space-y-0.5 max-h-48 overflow-y-auto">
            {decisions.map(t => (
              <button key={t.id}
                onClick={() => { navigate(`/tenders/${t.id}`); onClose() }}
                className="w-full flex items-center gap-2.5 px-2.5 py-2 hover:bg-gray-50
                           transition-colors text-left group"
                style={{borderRadius:'4px'}}>
                {t.partner_decision === 'GO'
                  ? <CheckCircle className="h-3.5 w-3.5 text-emerald-500 flex-shrink-0" />
                  : <XCircle    className="h-3.5 w-3.5 text-red-400    flex-shrink-0" />}
                <div className="flex-1 min-w-0">
                  <p className="text-xs font-medium text-gray-700 truncate group-hover:text-gray-900">
                    {t.title_clean || 'Untitled'}
                  </p>
                  <p className="text-[10px] text-gray-400 mt-0.5">
                    {t.partner_decision} · {t.country_name_normalized || '—'}
                  </p>
                </div>
              </button>
            ))}
          </div>
        )}
      </div>

      {/* Actions */}
      <div className="px-4 py-2 border-t border-gray-100 space-y-0.5">
        <button
          onClick={() => { logout(); navigate('/login') }}
          className="w-full flex items-center gap-2.5 px-2.5 py-2 text-xs font-medium
                     text-gray-500 hover:text-red-500 hover:bg-red-50 transition-colors"
          style={{borderRadius:'4px'}}>
          <LogOut className="h-3.5 w-3.5" />
          Sign out
        </button>
      </div>
    </div>
  )
}

// ── Navbar ────────────────────────────────────────────────────────────────────
export default function Navbar() {
  const { user } = useAuth()
  const [open, setOpen] = useState(false)

  return (
    <>
      {/* Load Geist font */}
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Geist:wght@400;500;600&display=swap');
        .navbar-root * { font-family: 'Geist', 'Inter', system-ui, sans-serif !important; }
      `}</style>

      <header
        className="navbar-root sticky top-0 z-40 bg-white border-b border-gray-100"
        style={{ height: '64px', boxShadow: '0 1px 0 rgba(0,0,0,0.05)' }}
      >
        <div className="h-full max-w-screen-xl mx-auto px-6 flex items-center gap-6">

          {/* Logo + wordmark */}
          <div className="flex items-center gap-3 flex-shrink-0">
            <img
              src="/kpmg-logo-blue.svg"
              alt="KPMG"
              className="h-5 object-contain mix-blend-multiply"
              onError={e => { e.target.style.display = 'none'; e.target.nextSibling.style.display = 'block' }}
            />
            {/* Fallback text logo */}
            <span style={{display:'none'}} className="text-base font-bold text-[#00338D] tracking-tight">
              KPMG
            </span>

            {/* Divider */}
            <div className="w-px h-8 bg-gray-200 mx-1" />

            {/* Platform name */}
            <div className="flex flex-col justify-center leading-none">
              <span className="text-[10px] font-medium text-gray-400 uppercase tracking-widest mt-0.5">
                Internal Platform · V2.4
              </span>
            </div>
          </div>

          {/* Nav items — flush to bar, underline active */}
          <nav className="flex items-center h-full flex-1">
            {NAV_ITEMS.map(({ to, label }) => (
              <NavLink
                key={to}
                to={to}
                className={({ isActive }) => clsx(
                  'relative flex items-center h-full px-4 text-sm font-medium tracking-tight whitespace-nowrap',
                  'transition-colors duration-150',
                  isActive ? 'text-[#00338D]' : 'text-gray-500 hover:text-gray-900'
                )}
              >
                {({ isActive }) => (
                  <>
                    {label}
                    {isActive && (
                      <span className="absolute bottom-0 left-0 right-0 h-[2px] bg-[#00338D]" />
                    )}
                  </>
                )}
              </NavLink>
            ))}
          </nav>

          {/* Right side */}
          <div className="flex items-center gap-1 flex-shrink-0">

            {/* Bell */}
            <button
              className="h-9 w-9 flex items-center justify-center text-gray-400
                         hover:text-gray-700 hover:bg-gray-50 transition-colors"
              style={{borderRadius:'4px'}}
              aria-label="Notifications"
            >
              <Bell className="h-4 w-4" />
            </button>

            {/* Divider */}
            <div className="w-px h-5 bg-gray-200 mx-2" />

            {/* Avatar */}
            <div className="relative">
              <button
                onClick={() => setOpen(!open)}
                className="flex items-center gap-2 px-2.5 py-1.5 hover:bg-gray-50
                           transition-colors"
                style={{borderRadius:'4px'}}
              >
                <Avatar email={user?.email} name={user?.full_name} size="sm" />
                <span className="hidden sm:block text-sm font-medium text-gray-700 max-w-[100px] truncate"
                      style={{letterSpacing:'-0.01em'}}>
                  {user?.full_name?.split(' ')[0] || 'Account'}
                </span>
                <ChevronDown className={clsx(
                  'h-3.5 w-3.5 text-gray-400 transition-transform duration-150',
                  open && 'rotate-180'
                )} />
              </button>
              {open && <ProfileDropdown onClose={() => setOpen(false)} />}
            </div>
          </div>
        </div>
      </header>
    </>
  )
}


