import { useEffect, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  ArrowLeft, ExternalLink, Clock, Globe, Building2,
  FileText, CheckCircle, AlertCircle,
} from 'lucide-react'
import api from '../lib/api'
import {
  RecommendationBadge, ScoreBar, PortalBadge,
  DecisionBadge, Spinner,
} from '../components/ui'

function Section({ title, icon: Icon, children }) {
  return (
    <div className="card p-5">
      <div className="flex items-center gap-2 mb-4 pb-3 border-b border-gray-100">
        {Icon && <Icon className="h-4 w-4 text-kpmg-blue" />}
        <h3 className="font-semibold text-gray-900 text-sm">{title}</h3>
      </div>
      {children}
    </div>
  )
}

function Field({ label, value }) {
  if (!value) return null
  return (
    <div className="mb-3">
      <div className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-1">{label}</div>
      <div className="text-sm text-gray-800">{value}</div>
    </div>
  )
}

function DecisionPanel({ tender, onDecisionMade }) {
  const [decision,      setDecision]      = useState('')
  const [justification, setJustification] = useState('')
  const [submitting,    setSubmitting]    = useState(false)
  const [error,         setError]         = useState('')
  const [success,       setSuccess]       = useState('')

  async function submit() {
    if (!decision) return
    setSubmitting(true)
    setError('')
    setSuccess('')
    try {
      await api.post(`/tenders/${tender.id}/decide`, { decision, justification })
      setSuccess(`Decision '${decision}' recorded.`)
      setDecision('')
      setJustification('')
      onDecisionMade()
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to submit')
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="card p-5">
      <div className="flex items-center gap-2 mb-4 pb-3 border-b border-gray-100">
        <CheckCircle className="h-4 w-4 text-kpmg-blue" />
        <h3 className="font-semibold text-gray-900 text-sm">Your Decision</h3>
      </div>

      {tender.partner_decision && (
        <div className="mb-4 p-3 bg-gray-50 rounded-lg">
          <div className="flex items-center gap-2 mb-1">
            <span className="text-xs text-gray-500">Current:</span>
            <DecisionBadge value={tender.partner_decision} />
          </div>
          {tender.partner_justification && (
            <p className="text-xs text-gray-600 mt-1">{tender.partner_justification}</p>
          )}
        </div>
      )}

      <div className="flex gap-2 mb-3">
        <button
          onClick={() => setDecision('GO')}
          className={`flex-1 py-2 rounded-lg text-sm font-semibold border-2 transition-colors
            ${decision === 'GO'
              ? 'bg-green-600 border-green-600 text-white'
              : 'border-green-300 text-green-700 hover:bg-green-50'}`}
        >
          ✓ GO
        </button>
        <button
          onClick={() => setDecision('NO GO')}
          className={`flex-1 py-2 rounded-lg text-sm font-semibold border-2 transition-colors
            ${decision === 'NO GO'
              ? 'bg-red-600 border-red-600 text-white'
              : 'border-red-300 text-red-700 hover:bg-red-50'}`}
        >
          ✗ NO GO
        </button>
      </div>

      <textarea
        value={justification}
        onChange={e => setJustification(e.target.value)}
        placeholder="Add justification (optional)…"
        rows={3}
        className="block w-full rounded-lg border border-gray-300 px-3 py-2 text-sm
                   focus:border-kpmg-blue focus:outline-none focus:ring-1 focus:ring-kpmg-blue
                   resize-none placeholder-gray-400"
      />

      {error   && <p className="text-red-600 text-xs mt-2">{error}</p>}
      {success && <p className="text-green-600 text-xs mt-2">{success}</p>}

      <button
        onClick={submit}
        disabled={!decision || submitting}
        className="w-full mt-3 btn-primary py-2 flex items-center justify-center gap-2
                   disabled:opacity-50 disabled:cursor-not-allowed"
      >
        {submitting ? 'Submitting…' : 'Submit Decision'}
      </button>

      {tender.decisions?.length > 0 && (
        <div className="mt-4 pt-4 border-t border-gray-100">
          <p className="text-xs font-medium text-gray-500 mb-2">All decisions</p>
          <div className="space-y-2">
            {tender.decisions.map((d, i) => (
              <div key={i} className="flex items-start gap-2 text-xs">
                <DecisionBadge value={d.decision} />
                <span className="text-gray-600">{d.user_full_name}</span>
                {d.justification && (
                  <span className="text-gray-400">— {d.justification.substring(0, 60)}</span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

export default function TenderDetail() {
  const { id }   = useParams()
  const navigate = useNavigate()
  const [tender,  setTender]  = useState(null)
  const [loading, setLoading] = useState(true)
  const [error,   setError]   = useState(null)

  function load() {
    setLoading(true)
    api.get(`/tenders/${id}`)
      .then(r => setTender(r.data))
      .catch(() => setError('Tender not found'))
      .finally(() => setLoading(false))
  }

  useEffect(() => { load() }, [id])

  if (loading) return <div className="flex justify-center py-20"><Spinner size="lg" /></div>
  if (error)   return <div className="text-red-600 text-center py-20">{error}</div>

  const t = tender
  const instrLabels   = { grant: 'Grant', loan: 'Loan', trust_fund: 'Trust Fund', own_funds: 'Own Funds' }
  const processLabels = { eoi_only: 'EOI Only', two_stage: 'Two-Stage (EOI → RFP)', two_envelope: 'Two Envelope', single_envelope: 'Single Envelope' }

  return (
    <div>
      <button
        onClick={() => navigate(-1)}
        className="flex items-center gap-1.5 text-sm text-gray-500 hover:text-gray-900 mb-5 transition-colors"
      >
        <ArrowLeft className="h-4 w-4" /> Back
      </button>

      <div className="card p-5 mb-5">
        <div className="flex items-start justify-between gap-4">
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2 flex-wrap mb-2">
              <PortalBadge portal={t.source_portal} />
              <RecommendationBadge value={t.recommendation} />
              {t.procurement_group && (
                <span className="text-xs bg-gray-100 text-gray-600 rounded px-2 py-0.5">
                  {t.procurement_group}
                </span>
              )}
            </div>
            <h1 className="text-xl font-bold text-gray-900 leading-tight">
              {t.title_clean || 'Untitled'}
            </h1>
            <div className="flex items-center gap-4 mt-2 text-sm text-gray-500 flex-wrap">
              <span className="flex items-center gap-1">
                <Globe className="h-3.5 w-3.5" />{t.country_name_normalized || '—'}
              </span>
              <span className="flex items-center gap-1">
                <Building2 className="h-3.5 w-3.5" />{t.funding_agency || '—'}
              </span>
              <span className="flex items-center gap-1">
                <Clock className="h-3.5 w-3.5" />
                {t.days_to_deadline != null ? `${t.days_to_deadline} days remaining` : 'Deadline unknown'}
              </span>
              {t.source_url && (
                <a href={t.source_url} target="_blank" rel="noopener noreferrer"
                  className="flex items-center gap-1 text-kpmg-blue hover:underline">
                  <ExternalLink className="h-3.5 w-3.5" /> Source
                </a>
              )}
            </div>
          </div>
          <div className="flex-shrink-0 text-right">
            <div className="text-3xl font-bold text-kpmg-blue">
              {t.p_go != null ? `${Math.round(t.p_go * 100)}%` : '—'}
            </div>
            <div className="text-xs text-gray-400 mt-0.5">P(GO)</div>
          </div>
        </div>

        <div className="flex flex-wrap gap-3 mt-4 pt-4 border-t border-gray-100">
          {t.budget && (
            <div className="text-sm">
              <span className="text-gray-500">Budget: </span>
              <span className="font-semibold text-gray-800">{t.budget.toLocaleString()} {t.currency}</span>
            </div>
          )}
          {t.sector && (
            <div className="text-sm">
              <span className="text-gray-500">Sector: </span>
              <span className="text-gray-800">{t.sector}</span>
            </div>
          )}
          {t.language && (
            <div className="text-sm">
              <span className="text-gray-500">Language: </span>
              <span className="text-gray-800">{t.language}</span>
            </div>
          )}
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-5">
        <div className="lg:col-span-2 space-y-5">
          {t.llm_scope_summary && (
            <Section title="Scope Summary" icon={FileText}>
              <p className="text-sm text-gray-700 leading-relaxed whitespace-pre-line">
                {t.llm_scope_summary}
              </p>
            </Section>
          )}
          {t.llm_eligibility_summary && (
            <Section title="Eligibility Criteria" icon={CheckCircle}>
              <p className="text-sm text-gray-700 whitespace-pre-line leading-relaxed">
                {t.llm_eligibility_summary}
              </p>
            </Section>
          )}
          {t.llm_submission_process && (
            <Section title="Submission Process" icon={AlertCircle}>
              <p className="text-sm text-gray-700 leading-relaxed">{t.llm_submission_process}</p>
            </Section>
          )}
          {!t.llm_scope_summary && t.description_clean && (
            <Section title="Description" icon={FileText}>
              <p className="text-sm text-gray-700 leading-relaxed whitespace-pre-line">
                {t.description_clean}
              </p>
            </Section>
          )}
          {t.justification && (
            <Section title="Score Justification" icon={FileText}>
              <pre className="text-xs text-gray-600 font-mono whitespace-pre-wrap leading-relaxed">
                {t.justification}
              </pre>
            </Section>
          )}
        </div>

        <div className="space-y-5">
          <Section title="Project Details" icon={Building2}>
            <Field label="Project / Program"  value={t.llm_project_program} />
            <Field label="Financing"          value={instrLabels[t.llm_financing_instrument] || t.llm_financing_instrument} />
            <Field label="Bid Process"        value={processLabels[t.llm_bid_process_type] || t.llm_bid_process_type} />
            <Field label="Contract Duration"  value={t.llm_contract_duration_months ? `${t.llm_contract_duration_months} months` : null} />
            <Field label="Organisation"       value={t.organisation_name} />
            <Field label="Procurement"        value={t.procurement_group} />
            {t.deadline_datetime && (
              <Field label="Deadline" value={new Date(t.deadline_datetime).toLocaleDateString('en-GB', {
                day: 'numeric', month: 'long', year: 'numeric',
              })} />
            )}
          </Section>

          <DecisionPanel tender={t} onDecisionMade={load} />
        </div>
      </div>
    </div>
  )
}