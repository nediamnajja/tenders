import { NavLink, useNavigate } from 'react-router-dom'
import { useAuth } from '../../lib/auth'
import { FileText, Star, LogOut, User } from 'lucide-react'
import { clsx } from 'clsx'

const nav = [
  { to: '/today',   label: "Today's GO",  icon: Star },
  { to: '/tenders', label: 'All Tenders', icon: FileText },
]

export default function Sidebar() {
  const { user, logout } = useAuth()
  const navigate = useNavigate()

  function handleLogout() {
    logout()
    navigate('/login')
  }

  return (
    <aside className="fixed inset-y-0 left-0 w-60 bg-kpmg-blue flex flex-col z-40">
      <div className="px-6 py-5 border-b border-blue-800">
        <div className="text-white font-bold text-xl tracking-tight">KPMG</div>
        <div className="text-blue-300 text-xs mt-0.5">Tender Intelligence</div>
      </div>

      <nav className="flex-1 px-3 py-4 space-y-0.5 overflow-y-auto">
        {nav.map(({ to, label, icon: Icon }) => (
          <NavLink
            key={to}
            to={to}
            className={({ isActive }) => clsx(
              'flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors',
              isActive
                ? 'bg-white/20 text-white'
                : 'text-blue-200 hover:bg-white/10 hover:text-white',
            )}
          >
            <Icon className="h-4 w-4 flex-shrink-0" />
            {label}
          </NavLink>
        ))}
      </nav>

      <div className="px-3 py-4 border-t border-blue-800">
        <div className="flex items-center gap-3 px-3 py-2 rounded-lg bg-white/10">
          <div className="h-7 w-7 rounded-full bg-kpmg-cobalt flex items-center justify-center flex-shrink-0">
            <User className="h-4 w-4 text-white" />
          </div>
          <div className="flex-1 min-w-0">
            <div className="text-white text-xs font-medium truncate">
              {user?.full_name || user?.email}
            </div>
            <div className="text-blue-300 text-xs capitalize">{user?.role}</div>
          </div>
          <button onClick={handleLogout} className="text-blue-300 hover:text-white transition-colors" title="Logout">
            <LogOut className="h-4 w-4" />
          </button>
        </div>
      </div>
    </aside>
  )
}