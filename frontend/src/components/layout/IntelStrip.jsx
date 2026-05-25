// src/components/layout/IntelStrip.jsx
// ─────────────────────────────────────────────────────────────────────────────
// Global navigation strip — solid navy, used on every page.
// Fetches its own data independently so values never depend on parent page state.
// All 4 stats are clickable, each navigating to a different page.
// ─────────────────────────────────────────────────────────────────────────────

import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { Globe, Zap, TrendingUp, Users } from 'lucide-react'
import api from '../../lib/api'

const FONT = "'Inter', 'Segoe UI', system-ui, sans-serif"
const MONO = "'DM Mono', 'JetBrains Mono', ui-monospace, monospace"
const NAVY = '#0D1F6B'

export default function IntelStrip() {
  const navigate = useNavigate()
  const [stats, setStats] = useState({ total: 0, hp: 0, nt: 0, decided: 0 })

  useEffect(() => {
    async function load() {
      try {
        const now = new Date()
         const [openRes, closedRes] = await Promise.all([
         api.get('/tenders', { params: { status: 'open',   per_page: 100, sort_by: 'publication_datetime' } }),
         api.get('/tenders', { params: { status: 'closed', per_page: 100, sort_by: 'publication_datetime' } }),
      ])
        const open   = openRes.data.items   || []
        const closed = closedRes.data.items || []
        const seen = new Set()
        const all = [...open, ...closed].filter(t => {
         if (seen.has(t.id)) return false
         seen.add(t.id)
         return true
         })
        const hp = open.filter(t => t.p_go >= 0.8).length
        const nt = open.filter(t => {
         if (!t.publication_datetime) return false
         return (now - new Date(t.publication_datetime.replace(' ', 'T'))) < 24 * 60 * 60 * 1000
         }).length
       const decided = all.filter(t => t.partner_decision).length

        setStats({ total: openRes.data.total, hp, nt, decided })
      } catch (e) {
        console.error('IntelStrip fetch error:', e)
      }
    }
    load()
  }, [])

  const [hovered, setHovered] = useState(null)

  const kpis = [
    {
      Icon: Globe,
      label: 'Active Opportunities',
      value: stats.total.toLocaleString(),
      onClick: () => navigate('/tenders'),
    },
    {
      Icon: Zap,
      label: 'High Priority',
      value: stats.hp,
      onClick: () => navigate('/today'),
    },
    {
      Icon: TrendingUp,
      label: 'New Today',
      value: stats.nt,
      onClick: () => navigate('/tenders?sort=publication_datetime'),
    },
    {
      Icon: Users,
      label: 'Team Decisions',
      value: stats.decided,
      onClick: () => navigate('/decisions'),
    },
  ]

  return (
    <div style={{
      display: 'flex',
      flexShrink: 0,
      fontFamily: FONT,
      background: NAVY,
      borderBottom: '2px solid rgba(0,0,0,.18)',
    }}>
      {kpis.map((k, i) => {
        const isH = hovered === i
        return (
          <div
            key={i}
            onMouseEnter={() => setHovered(i)}
            onMouseLeave={() => setHovered(null)}
            onClick={k.onClick}
            style={{
              flex: 1,
              display: 'flex',
              alignItems: 'center',
              gap: 10,
              padding: '11px 24px',
              position: 'relative',
              cursor: 'pointer',
              background: isH ? 'rgba(255,255,255,0.08)' : 'transparent',
              borderRight: i < kpis.length - 1 ? '1px solid rgba(255,255,255,0.12)' : 'none',
              transition: 'background 0.18s ease',
              userSelect: 'none',
            }}
          >
            {/* Bottom underline on hover */}
            <div style={{
              position: 'absolute', bottom: 0, left: 0, right: 0, height: 2,
              background: 'rgba(255,255,255,0.55)',
              opacity: isH ? 1 : 0,
              transition: 'opacity 0.18s ease',
            }} />

            <k.Icon style={{
              width: 13, height: 13,
              color: isH ? 'rgba(255,255,255,0.95)' : 'rgba(255,255,255,0.40)',
              flexShrink: 0,
              transition: 'color 0.18s ease',
            }} />

            <span style={{
              fontSize: 16,
              fontWeight: 600,
              color: '#fff',
              lineHeight: 1,
              fontFamily: MONO,
              fontVariantNumeric: 'tabular-nums',
              letterSpacing: '-0.01em',
            }}>{k.value}</span>

            <span style={{
              fontSize: 11,
              color: isH ? 'rgba(255,255,255,0.80)' : 'rgba(255,255,255,0.48)',
              fontFamily: FONT,
              transition: 'color 0.18s ease',
              whiteSpace: 'nowrap',
            }}>{k.label}</span>
          </div>
        )
      })}
    </div>
  )
}