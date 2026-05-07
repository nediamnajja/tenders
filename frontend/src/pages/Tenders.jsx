import { useEffect, useState, useCallback } from 'react'
import { useNavigate } from 'react-router-dom'
import { Search, SlidersHorizontal, ChevronLeft, ChevronRight, ExternalLink } from 'lucide-react'
import api from '../lib/api'
import {
  RecommendationBadge, ScoreBar, PortalBadge,
  DecisionBadge, Spinner, EmptyState, Input, Select,
} from '../components/ui'

const PORTALS      = ['afdb', 'worldbank', 'undp', 'ungm']
const PROCUREMENTS = ['CONSULTING', 'WORKS', 'GOODS', 'NON-CONSULTING']
const SORT_OPTIONS = [
  { value: 'p_go',        label: 'Score' },
  { value: 'deadline',    label: 'Deadline' },
  { value: 'enriched_at', label: 'Newest' },
]

export default function Tenders() {
  const navigate = useNavigate()
  const [items,        setItems]        = useState([])
  const [total,        setTotal]        = useState(0)
  const [pages,        setPages]        = useState(1)
  const [page,         setPage]         = useState(1)
  const [loading,      setLoading]      = useState(true)
  const [search,       setSearch]       = useState('')
  const [portal,       setPortal]       = useState('')
  const [procurement,  setProcurement]  = useState('')
  const [rec,          setRec]          = useState('')
  const [sortBy,       setSortBy]       = useState('p_go')
  const [showFilters,  setShowFilters]  = useState(false)

  const fetchTenders = useCallback(async (p = 1) => {
    setLoading(true)
    try {
      const params = {
        page: p, per_page: 25,
        ...(search      && { search }),
        ...(portal      && { portal }),
        ...(procurement && { procurement }),
        ...(rec         && { recommendation: rec }),
        sort_by: sortBy,
      }
      const { data } = await api.get('/tenders', { params })
      setItems(data.items)
      setTotal(data.total)
      setPages(data.pages)
      setPage(p)
    } catch (err) {
      console.error(err)
    } finally {
      setLoading(false)
    }
  }, [search, portal, procurement, rec, sortBy])

  useEffect(() => { fetchTenders(1) }, [fetchTenders])

  return (
    <div>
      <div className="flex items-center justify-between mb-5">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">All Tenders</h1>
          <p className="text-gray-500 text-sm mt-0.5">{total.toLocaleString()} active tenders</p>
        </div>
      </div>

      <div className="card p-4 mb-4">
        <form onSubmit={e => { e.preventDefault(); fetchTenders(1) }} className="flex gap-2">
          <div className="relative flex-1">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-gray-400" />
            <Input
              value={search}
              onChange={e => setSearch(e.target.value)}
              placeholder="Search title, country, agency…"
              className="pl-9"
            />
          </div>
          <button type="submit" className="btn-primary px-5">Search</button>
          <button
            type="button"
            onClick={() => setShowFilters(!showFilters)}
            className="btn-secondary flex items-center gap-1.5"
          >
            <SlidersHorizontal className="h-4 w-4" /> Filters
          </button>
        </form>

        {showFilters && (
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mt-3 pt-3 border-t border-gray-100">
            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">Portal</label>
              <Select value={portal} onChange={e => setPortal(e.target.value)} className="w-full">
                <option value="">All portals</option>
                {PORTALS.map(p => <option key={p} value={p}>{p.toUpperCase()}</option>)}
              </Select>
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">Procurement</label>
              <Select value={procurement} onChange={e => setProcurement(e.target.value)} className="w-full">
                <option value="">All types</option>
                {PROCUREMENTS.map(p => <option key={p} value={p}>{p}</option>)}
              </Select>
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">Recommendation</label>
              <Select value={rec} onChange={e => setRec(e.target.value)} className="w-full">
                <option value="">All</option>
                <option value="STRONG GO">Strong GO</option>
                <option value="GO">GO</option>
                <option value="scored">Scored only</option>
              </Select>
            </div>
            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">Sort by</label>
              <Select value={sortBy} onChange={e => setSortBy(e.target.value)} className="w-full">
                {SORT_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
              </Select>
            </div>
          </div>
        )}
      </div>

      <div className="card overflow-hidden">
        {loading ? (
          <div className="flex justify-center py-16"><Spinner size="lg" /></div>
        ) : items.length === 0 ? (
          <EmptyState icon={Search} title="No tenders found" description="Try adjusting your filters." />
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-200 bg-gray-50">
                  <th className="text-left px-4 py-3 font-semibold text-gray-600 w-[38%]">Project</th>
                  <th className="text-left px-4 py-3 font-semibold text-gray-600">Sector</th>
                  <th className="text-left px-4 py-3 font-semibold text-gray-600">Budget</th>
                  <th className="text-left px-4 py-3 font-semibold text-gray-600">Deadline</th>
                  <th className="text-left px-4 py-3 font-semibold text-gray-600">Score</th>
                  <th className="text-left px-4 py-3 font-semibold text-gray-600">Decision</th>
                </tr>
              </thead>
              <tbody>
                {items.map(t => (
                  <tr
                    key={t.id}
                    onClick={() => navigate(`/tenders/${t.id}`)}
                    className="border-b border-gray-100 hover:bg-blue-50 cursor-pointer transition-colors group"
                  >
                    <td className="px-4 py-3">
                      <div className="flex items-start gap-2">
                        <div className="flex-1 min-w-0">
                          <div className="font-medium text-gray-900 line-clamp-2 group-hover:text-kpmg-blue transition-colors">
                            {t.title_clean || 'Untitled'}
                          </div>
                          <div className="flex items-center gap-1.5 mt-1 flex-wrap">
                            <PortalBadge portal={t.source_portal} />
                            <span className="text-xs text-gray-500">{t.country_name_normalized || '—'}</span>
                          </div>
                        </div>
                        <a
                          href={t.source_url}
                          target="_blank"
                          rel="noopener noreferrer"
                          onClick={e => e.stopPropagation()}
                          className="text-gray-400 hover:text-kpmg-blue mt-0.5 flex-shrink-0"
                        >
                          <ExternalLink className="h-3.5 w-3.5" />
                        </a>
                      </div>
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-xs text-gray-600 line-clamp-2">
                        {t.sector ? t.sector.split(',')[0].trim() : '—'}
                      </span>
                    </td>
                    <td className="px-4 py-3 whitespace-nowrap">
                      <span className="text-xs text-gray-700">
                        {t.budget ? `${(t.budget / 1_000_000).toFixed(1)}M ${t.currency || ''}` : '—'}
                      </span>
                    </td>
                    <td className="px-4 py-3 whitespace-nowrap">
                      <span className={`text-xs font-medium ${
                        t.days_to_deadline <= 5  ? 'text-red-600' :
                        t.days_to_deadline <= 14 ? 'text-orange-600' : 'text-gray-600'
                      }`}>
                        {t.days_to_deadline != null ? `${t.days_to_deadline}d` : '—'}
                      </span>
                    </td>
                    <td className="px-4 py-3"><ScoreBar value={t.p_go} /></td>
                    <td className="px-4 py-3"><DecisionBadge value={t.partner_decision} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {pages > 1 && (
        <div className="flex items-center justify-between mt-4">
          <span className="text-sm text-gray-500">Page {page} of {pages} — {total.toLocaleString()} tenders</span>
          <div className="flex gap-2">
            <button onClick={() => fetchTenders(page - 1)} disabled={page <= 1}
              className="btn-secondary px-3 py-1.5 flex items-center gap-1 text-sm disabled:opacity-40">
              <ChevronLeft className="h-4 w-4" /> Prev
            </button>
            <button onClick={() => fetchTenders(page + 1)} disabled={page >= pages}
              className="btn-secondary px-3 py-1.5 flex items-center gap-1 text-sm disabled:opacity-40">
              Next <ChevronRight className="h-4 w-4" />
            </button>
          </div>
        </div>
      )}
    </div>
  )
}