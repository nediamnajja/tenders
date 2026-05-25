// src/components/layout/PlatformFooter.jsx
const FONT = "'Inter', 'Segoe UI', system-ui, sans-serif"
const NAVY = '#0D1F6B'

export default function PlatformFooter() {
  return (
    <div style={{ background: NAVY, fontFamily: FONT, padding: '32px 40px' }}>
      <div style={{ maxWidth: 1100, margin: '0 auto' }}>

        {/* Logo */}
        <div style={{ marginBottom: 28 }}>
          <img
            src="/kpmg-logo-blue.svg"
            alt="KPMG"
            style={{ height: 24, filter: 'brightness(0) invert(1)', display: 'block' }}
            onError={e => {
              e.target.style.display = 'none'
              e.target.nextSibling.style.display = 'block'
            }}
          />
          {/* Fallback text if SVG fails */}
          <span style={{ display: 'none', fontSize: 16, fontWeight: 800, color: '#fff', letterSpacing: '0.08em' }}>
            KPMG
          </span>
        </div>

        {/* Nav links — full white */}
        <div style={{ display: 'flex', gap: 32, marginBottom: 28 }}>
          {['Contact Us', 'Privacy', 'Legal', 'Accessibility'].map(l => (
            <a key={l} href="#"
              style={{ fontSize: 13, fontWeight: 500, color: '#fff', textDecoration: 'none', transition: 'opacity .12s', opacity: 1 }}
              onMouseEnter={e => e.currentTarget.style.opacity = '0.65'}
              onMouseLeave={e => e.currentTarget.style.opacity = '1'}>
              {l}
            </a>
          ))}
        </div>

        {/* Divider + copyright */}
        <div style={{ borderTop: '1px solid rgba(255,255,255,.15)', paddingTop: 18 }}>
          <p style={{ fontSize: 11, color: 'rgba(255,255,255,.50)', margin: 0, lineHeight: 1.65 }}>
            © {new Date().getFullYear()} KPMG International — This platform is restricted to authorized KPMG Advisory personnel. Confidential and intended solely for internal use.
          </p>
        </div>

      </div>
    </div>
  )
}