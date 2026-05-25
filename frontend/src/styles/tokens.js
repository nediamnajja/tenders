// src/styles/tokens.js
// ─────────────────────────────────────────────────────────────────────────────
// Single source of truth for all inline-style values.
// Import what you need: import { K, F, MONO, R, S } from '../styles/tokens'
// ─────────────────────────────────────────────────────────────────────────────

// ── Fonts ─────────────────────────────────────────────────────────────────────
export const F    = "'Inter', 'Segoe UI', system-ui, -apple-system, sans-serif"
export const MONO = "'DM Mono', 'JetBrains Mono', 'Fira Code', ui-monospace, monospace"

// ── KPMG Blue palette ─────────────────────────────────────────────────────────
// Rule: blue is NEVER a background fill — only accent, border, text
export const K = {
  primary:  '#00338D',   // KPMG primary — used sparingly
  accent:   '#0091DA',   // Lighter accent — links, highlights
  navy:     '#0D1F6B',   // Deep navy — hero backgrounds only
  hover:    '#002878',   // Darker blue for hover states

  // Tint helpers (for icon backgrounds, subtle fills)
  tint10:   'rgba(0, 51, 141, 0.10)',
  tint06:   'rgba(0, 51, 141, 0.06)',
  tint04:   'rgba(0, 51, 141, 0.04)',

  // Chip surfaces
  chipBg:     '#EEF2FF',
  chipBorder: '#C7D7F5',
  chipText:   '#00338D',
}

// ── Grey text scale ───────────────────────────────────────────────────────────
// Each level has ONE job — do not mix
export const G = {
  900: '#111827',   // Page titles, primary headings
  700: '#374151',   // Body text, card content
  500: '#6B7280',   // Secondary text, metadata
  400: '#9CA3AF',   // Labels, placeholders, captions
  300: '#D1D5DB',   // Disabled text
}

// ── Surfaces & borders ────────────────────────────────────────────────────────
export const S = {
  page:    '#F8FAFC',   // Page background
  white:   '#FFFFFF',   // Cards, panels
  subtle:  '#F1F5F9',   // Hover states, inset areas
  muted:   '#E2E8F0',   // Dividers, secondary borders

  border:        '#E5E7EB',   // Standard card/input border
  borderStrong:  '#CBD5E1',   // Hover border, focused input
  borderFocus:   '#93B4E8',   // Input focus ring
}

// ── Border radius ─────────────────────────────────────────────────────────────
// KPMG uses very tight radii — this is a key brand signal
export const R = {
  sm:  2,   // px — chips, tags, tiny pills
  md:  3,   // px — buttons, inputs
  lg:  4,   // px — small cards
  xl:  6,   // px — dropdowns, modals
  '2xl': 8, // px — large cards
}

// ── Shadows ───────────────────────────────────────────────────────────────────
export const SH = {
  xs:   '0 1px 2px rgba(0,0,0,0.05)',
  sm:   '0 1px 4px rgba(0,0,0,0.06)',
  md:   '0 4px 12px rgba(0,0,0,0.08)',
  lg:   '0 8px 24px rgba(0,0,0,0.10)',
  xl:   '0 16px 40px rgba(0,0,0,0.12)',
  blue: '0 4px 16px rgba(0,51,141,0.09)',
}

// ── Spacing ───────────────────────────────────────────────────────────────────
export const SP = {
  1:  4,
  2:  8,
  3:  12,
  4:  16,
  5:  20,
  6:  24,
  8:  32,
  10: 40,
  12: 48,
}

// ── Score system ──────────────────────────────────────────────────────────────
// Four states — { fill, border, text, label }
// label = colour of the "Strong GO" / "GO" text on the dark header
export const SCORE = {
  strong: {
    fill:   '#00338D',
    border: '#001F5C',
    text:   '#FFFFFF',
    label:  '#5B8CD6',
    name:   'Strong GO',
    range:  'p_go ≥ 80',
  },
  go: {
    fill:   '#0091DA',
    border: '#006FAA',
    text:   '#FFFFFF',
    label:  '#7AC5EE',
    name:   'GO',
    range:  'p_go 60–79',
  },
  marginal: {
    fill:   '#F59E0B',
    border: '#D97706',
    text:   '#FFFFFF',
    label:  '#FDE68A',
    name:   'Marginal',
    range:  'p_go 40–59',
  },
  nogo: {
    fill:   '#6B7280',
    border: '#4B5563',
    text:   '#FFFFFF',
    label:  '#D1D5DB',
    name:   'No GO',
    range:  'p_go < 40',
  },
}

// Helper — returns the correct SCORE bucket for a p_go value (0–1)
export function scoreState(pGo) {
  if (pGo == null) return null
  if (pGo >= 0.80) return SCORE.strong
  if (pGo >= 0.60) return SCORE.go
  if (pGo >= 0.40) return SCORE.marginal
  return SCORE.nogo
}

// ── Semantic colours ──────────────────────────────────────────────────────────
export const C = {
  green:  { text: '#15803D', bg: '#F0FDF4', border: '#BBF7D0' },
  red:    { text: '#B91C1C', bg: '#FFF1F2', border: '#FECDD3' },
  amber:  { text: '#B45309', bg: '#FFFBEB', border: '#FDE68A' },
  teal:   { text: '#0F766E', bg: '#F0FDFA', border: '#99F6E4' },
  purple: { text: '#7E22CE', bg: '#FDF4FF', border: '#E9D5FF' },
  grey:   { text: '#374151', bg: '#F1F5F9', border: '#E5E7EB' },
}

// ── Procurement type colours ──────────────────────────────────────────────────
export const PROC_COLORS = {
  CONSULTING:       { bg: '#EEF2FF', color: '#3730A3', border: '#C7D2FE' },
  WORKS:            { bg: '#FFF7ED', color: '#C2410C', border: '#FED7AA' },
  GOODS:            { bg: '#F0FDF4', color: '#15803D', border: '#BBF7D0' },
  'NON-CONSULTING': { bg: '#FDF4FF', color: '#7E22CE', border: '#E9D5FF' },
}

// ── Layout constants ──────────────────────────────────────────────────────────
export const LAYOUT = {
  navbarHeight:      64,
  sidebarWidth:      220,
  sidebarCollapsed:  56,
}

// ── Transitions ───────────────────────────────────────────────────────────────
export const T = {
  fast: '0.12s ease',
  base: '0.18s ease',
  slow: '0.28s ease',
}