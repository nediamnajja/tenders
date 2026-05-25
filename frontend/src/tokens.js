// src/styles/tokens.js
// ─────────────────────────────────────────────────────────────────────────────
// Single source of truth — import what you need in every page
// import { K, G, S, F, MONO, T } from '../styles/tokens'
// ─────────────────────────────────────────────────────────────────────────────

// ── KPMG colour palette ───────────────────────────────────────────────────────
export const K = {
  navy:    '#0D1F6B',   // sidebar background
  blue:    '#00338D',   // primary — buttons, active states, accents
  royal:   '#1B57C5',   // hover states
  mauve:   '#8C3075',   // status: closed / urgency
  violet:  '#5B4EA0',   // secondary accent
  teal:    '#00B2A9',   // status: open / positive
  accent:  '#0091DA',   // links, eyebrows, CTAs
}

// ── Grey text scale ───────────────────────────────────────────────────────────
// Each level has one job — do not mix
export const G = {
  950: '#0F172A',   // darkest headings
  900: '#1E293B',   // card titles, primary text
  700: '#374151',   // body text
  600: '#4B5563',   // secondary body
  500: '#6B7280',   // muted / metadata
  400: '#9CA3AF',   // placeholders, captions, disabled
}

// ── Surface & border scale ────────────────────────────────────────────────────
export const S = {
  white:   '#FFFFFF',   // cards, panels
  subtle:  '#F8FAFC',   // page background, hover rows
  muted:   '#F1F5F9',   // inset areas, table headers
  border:  '#E5E7EB',   // standard borders
}

// ── Typography ────────────────────────────────────────────────────────────────
export const F    = "'Inter','Segoe UI',system-ui,sans-serif"
export const MONO = "'DM Mono','JetBrains Mono',ui-monospace,monospace"

// ── Type scale ────────────────────────────────────────────────────────────────
// Use these constants — never hardcode font sizes
export const T = {
  label:    10,   // uppercase labels, tags, meta
  meta:     11,   // body/meta text, filter controls
  sm:       12,   // search input, sort select, result count
  base:     13,   // card titles, row text
  heading:  20,   // page heading
  kpi:      24,   // KPI numbers (live bar, stat tiles)
}

// ── Font weights ──────────────────────────────────────────────────────────────
export const W = {
  light:    300,   // KPI numbers (live bar)
  regular:  400,   // body text
  medium:   500,   // page titles
  semibold: 600,   // card titles, section labels, buttons
  bold:     700,   // badges, counts, scores
}

// ── Score state helper ────────────────────────────────────────────────────────
// Returns { color, label } for a p_go value (0–1)
export function scoreState(p) {
  if (p == null) return { color: G[400],  label: '—'        }
  if (p >= 0.80) return { color: K.blue,  label: 'Priority' }
  if (p >= 0.60) return { color: K.accent,label: 'GO'       }
  if (p >= 0.40) return { color: '#F59E0B',label: 'Marginal'}
  return               { color: G[400],  label: 'Low'      }
}

// ── Procurement type colours ──────────────────────────────────────────────────
export const PROC = {
  CONSULTING:       { bg: '#EEF2FF', color: '#3730A3', border: '#C7D2FE' },
  WORKS:            { bg: '#FFF7ED', color: '#C2410C', border: '#FED7AA' },
  GOODS:            { bg: '#F0FDF4', color: '#15803D', border: '#BBF7D0' },
  'NON-CONSULTING': { bg: '#FDF4FF', color: '#7E22CE', border: '#E9D5FF' },
}

// ── Status badge helper ───────────────────────────────────────────────────────
export function statusStyle(isOpen) {
  return isOpen
    ? { color: K.teal,  bg: '#F0FDFA', border: '#99F6E4', label: 'Open'   }
    : { color: K.mauve, bg: '#FDF4FF', border: '#E9D5FF', label: 'Closed' }
}

// ── Decision badge helper ─────────────────────────────────────────────────────
export function decisionStyle(decision) {
  if (!decision) return { color: '#B45309', bg: '#FFFBEB', border: '#FDE68A', label: 'Pending' }
  if (decision === 'GO') return { color: '#15803D', bg: '#F0FDF4', border: '#BBF7D0', label: '✓ GO' }
  return { color: '#B91C1C', bg: '#FFF1F2', border: '#FECDD3', label: '✗ NO GO' }
}

// ── Common box styles (use with spread) ───────────────────────────────────────
export const card = {
  background: '#FFFFFF',
  border: `1px solid #E5E7EB`,
  borderRadius: 4,
  fontFamily: "'Inter','Segoe UI',system-ui,sans-serif",
}

export const tableHeader = {
  padding: '10px 16px',
  fontSize: 10,
  fontWeight: 700,
  textTransform: 'uppercase',
  letterSpacing: '0.08em',
  color: '#9CA3AF',
  textAlign: 'left',
  borderBottom: '1px solid #E5E7EB',
  background: '#F8FAFC',
  whiteSpace: 'nowrap',
  fontFamily: "'Inter','Segoe UI',system-ui,sans-serif",
}

export const eyebrow = {
  fontSize: 11,
  fontWeight: 700,
  color: '#0091DA',
  letterSpacing: '0.10em',
  textTransform: 'uppercase',
  marginBottom: 6,
  fontFamily: "'Inter','Segoe UI',system-ui,sans-serif",
}

export const pageTitle = {
  fontSize: 28,
  fontWeight: 500,
  color: '#1E293B',
  margin: '0 0 6px',
  letterSpacing: '-0.02em',
  fontFamily: "'Inter','Segoe UI',system-ui,sans-serif",
}