import { clsx } from 'clsx'

export function RecommendationBadge({ value }) {
  if (!value) return <span className="text-gray-400 text-xs">—</span>
  if (value === 'STRONG GO') return <span className="badge-strong-go">⬆ STRONG GO</span>
  if (value === 'GO')        return <span className="badge-go">✓ GO</span>
  return <span className="badge-no-go">✗ NO GO</span>
}

export function DecisionBadge({ value }) {
  if (!value) return <span className="text-gray-400 text-xs">Pending</span>
  if (value === 'GO')    return <span className="badge-go">GO</span>
  if (value === 'NO GO') return <span className="badge-no-go">NO GO</span>
  return null
}

export function PortalBadge({ portal }) {
  const colors = {
    afdb:      'bg-orange-100 text-orange-800',
    worldbank: 'bg-indigo-100 text-indigo-800',
    undp:      'bg-teal-100 text-teal-800',
    ungm:      'bg-purple-100 text-purple-800',
  }
  return (
    <span className={clsx(
      'inline-flex items-center px-2 py-0.5 rounded text-xs font-medium uppercase',
      colors[portal?.toLowerCase()] || 'bg-gray-100 text-gray-700'
    )}>
      {portal?.toUpperCase()}
    </span>
  )
}

export function ScoreBar({ value }) {
  if (value == null) return <span className="text-gray-400 text-xs">—</span>
  const pct   = Math.round(value * 100)
  const color = pct >= 80 ? 'bg-green-500' : pct >= 70 ? 'bg-blue-500' : 'bg-gray-300'
  return (
    <div className="flex items-center gap-2">
      <div className="w-16 bg-gray-200 rounded-full h-1.5">
        <div className={clsx('h-1.5 rounded-full', color)} style={{ width: `${pct}%` }} />
      </div>
      <span className={clsx(
        'text-sm font-semibold tabular-nums',
        pct >= 80 ? 'text-green-700' : pct >= 70 ? 'text-blue-700' : 'text-gray-500'
      )}>
        {pct}%
      </span>
    </div>
  )
}

export function Spinner({ size = 'md' }) {
  const s = size === 'sm' ? 'h-4 w-4' : size === 'lg' ? 'h-10 w-10' : 'h-6 w-6'
  return (
    <div className={clsx('animate-spin rounded-full border-2 border-gray-300 border-t-kpmg-blue', s)} />
  )
}

export function EmptyState({ icon: Icon, title, description }) {
  return (
    <div className="flex flex-col items-center justify-center py-16 text-center">
      {Icon && <Icon className="h-12 w-12 text-gray-300 mb-4" />}
      <p className="text-gray-500 font-medium">{title}</p>
      {description && <p className="text-gray-400 text-sm mt-1">{description}</p>}
    </div>
  )
}

export function StatCard({ label, value, sub, color = 'blue' }) {
  const colors = {
    green:  'text-green-700 bg-green-50 border-green-200',
    blue:   'text-blue-700  bg-blue-50  border-blue-200',
    orange: 'text-orange-700 bg-orange-50 border-orange-200',
    gray:   'text-gray-700  bg-gray-50  border-gray-200',
    red:    'text-red-700   bg-red-50   border-red-200',
  }
  return (
    <div className={clsx('card border p-5', colors[color])}>
      <div className="text-3xl font-bold tabular-nums">{value}</div>
      <div className="text-sm font-semibold mt-1">{label}</div>
      {sub && <div className="text-xs mt-0.5 opacity-70">{sub}</div>}
    </div>
  )
}

export function Input({ className, ...props }) {
  return (
    <input
      className={clsx(
        'block w-full rounded-lg border border-gray-300 px-3 py-2 text-sm',
        'placeholder-gray-400 focus:border-kpmg-blue focus:outline-none focus:ring-1 focus:ring-kpmg-blue',
        className,
      )}
      {...props}
    />
  )
}

export function Select({ className, children, ...props }) {
  return (
    <select
      className={clsx(
        'block rounded-lg border border-gray-300 px-3 py-2 text-sm bg-white',
        'focus:border-kpmg-blue focus:outline-none focus:ring-1 focus:ring-kpmg-blue',
        className,
      )}
      {...props}
    >
      {children}
    </select>
  )
}