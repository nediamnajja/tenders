import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Star, ExternalLink, Clock, TrendingUp } from 'lucide-react'
import api from '../lib/api'
import {
  RecommendationBadge, ScoreBar, PortalBadge,
  Spinner, EmptyState, StatCard,
} from '../components/ui'

function TenderCard({ tender, rank }) {
  const navigate = useNavigate()

  return (
    <div
      onClick={() => navigate(`/tenders/${tender.id}`)}
      className="card p-4 hover:shadow-md hover:border-kpmg-blue cursor-pointer transition-all duration-150 group"
    >
      <div className="flex items-start gap-3">
        <div className="flex-shrink-0 w-7 h-7 rounded-full bg-gray-100 flex items-center justify-center text-xs font-bold text-gray-500">
          {rank}
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-start justify-between gap-2">
            <h3 className="text-sm font-semibold text-gray-900 line-clamp-2 group-hover:text-kpmg-blue transition-colors">
              {tender.title_clean || 'Untitled'}
            </h3>
            <RecommendationBadge value={tender.recommendation} />
          </div>

          <div className="flex items-center gap-2 mt-1.5 flex-wrap">
            <PortalBadge portal={tender.source_portal} />
            <span className="text-xs text-gray-500">{tender.country_name_normalized || '—'}</span>
            {tender.funding_agency && (
              <span className="text-xs text-gray-500">· {tender.funding_agency}</span>
            )}
          </div>

          {tender.sector && (
            <div className="mt-1.5">
              <span className="text-xs text-gray-500 bg-gray-100 rounded px-2 py-0.5">
                {tender.sector.split(',')[0].trim()}
                {tender.sector.includes(',') ? ' +more' : ''}
              </span>
            </div>
          )}

          <div className="flex items-center justify-between mt-3">
            <ScoreBar value={tender.p_go} />
            <div className="flex items-center gap-3 text-xs text-gray-500">
              {tender.budget ? (
                <span className="font-medium text-gray-700">
                  {tender.budget.toLocaleString()} {tender.currency}
                </span>
              ) : null}
              <span className="flex items-center gap-1">
                <Clock className="h-3 w-3" />
                {tender.days_to_deadline}d left
              </span>
              <a
                href={tender.source_url}
                target="_blank"
                rel="noopener noreferrer"
                onClick={e => e.stopPropagation()}
                className="hover:text-kpmg-blue"
              >
                <ExternalLink className="h-3.5 w-3.5" />
              </a>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

export default function Today() {
  const [data,    setData]    = useState(null)
  const [loading, setLoading] = useState(true)
  const [error,   setError]   = useState(null)

  useEffect(() => {
    api.get('/tenders/today')
      .then(r => setData(r.data))
      .catch(() => setError('Failed to load recommendations'))
      .finally(() => setLoading(false))
  }, [])

  if (loading) return <div className="flex justify-center py-20"><Spinner size="lg" /></div>
  if (error)   return <div className="text-red-600 text-center py-20">{error}</div>

  const today = new Date().toLocaleDateString('en-GB', {
    weekday: 'long', year: 'numeric', month: 'long', day: 'numeric',
  })

  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Today's Recommendations</h1>
        <p className="text-gray-500 text-sm mt-1">{today}</p>
      </div>

      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-8">
        <StatCard label="Strong GO"        value={data.strong_go.length} sub="p_go ≥ 80%" color="green" />
        <StatCard label="GO"               value={data.go.length}        sub="p_go 70–79%" color="blue" />
        <StatCard label="Total Today"      value={data.total}            sub="scored tenders" color="gray" />
        <StatCard
          label="Pending Decision"
          value={[...data.strong_go, ...data.go].filter(t => !t.partner_decision).length}
          sub="awaiting your review"
          color="orange"
        />
      </div>

      {data.strong_go.length > 0 && (
        <section className="mb-8">
          <div className="flex items-center gap-2 mb-3">
            <TrendingUp className="h-5 w-5 text-green-600" />
            <h2 className="text-lg font-semibold text-gray-900">Strong GO</h2>
            <span className="bg-green-100 text-green-800 text-xs font-semibold px-2 py-0.5 rounded-full">
              {data.strong_go.length}
            </span>
          </div>
          <div className="space-y-3">
            {data.strong_go.map((t, i) => <TenderCard key={t.id} tender={t} rank={i + 1} />)}
          </div>
        </section>
      )}

      {data.go.length > 0 && (
        <section className="mb-8">
          <div className="flex items-center gap-2 mb-3">
            <Star className="h-5 w-5 text-blue-600" />
            <h2 className="text-lg font-semibold text-gray-900">GO</h2>
            <span className="bg-blue-100 text-blue-800 text-xs font-semibold px-2 py-0.5 rounded-full">
              {data.go.length}
            </span>
          </div>
          <div className="space-y-3">
            {data.go.map((t, i) => <TenderCard key={t.id} tender={t} rank={i + 1} />)}
          </div>
        </section>
      )}

      {data.total === 0 && (
        <EmptyState
          icon={Star}
          title="No GO tenders today"
          description="The pipeline ran successfully — no tenders met the threshold today."
        />
      )}
    </div>
  )
}