import React, { useState, useEffect } from 'react'
import Reindexacao from './pages/Reindexacao'
import HotCold from './pages/HotCold'
import Aggregations from './pages/Aggregations'
import SchemaValidation from './pages/SchemaValidation'
import ChangeStreams from './pages/ChangeStreams'
import Transactions from './pages/Transactions'

const MODULES = [
  { key: 'reindex', icon: '⚡', title: 'Reindexação Online',        subtitle: 'Sem downtime, sem lock na coleção',            color: '#00A35C', component: Reindexacao },
  { key: 'hotcold', icon: '🌡️', title: 'Hot / Cold Tiering',        subtitle: 'Online Archive — dados históricos automáticos', color: '#016BF8', component: HotCold },
  { key: 'agg',     icon: '🔗', title: 'Aggregation Pipeline',      subtitle: '$lookup, $facet, $setWindowFields e mais',     color: '#7B61FF', component: Aggregations },
  { key: 'schema',  icon: '🛡️', title: 'Schema Validation',         subtitle: 'JSON Schema enforcement no banco',             color: '#E45825', component: SchemaValidation },
  { key: 'streams', icon: '📡', title: 'Change Streams',            subtitle: 'Eventos em tempo real — insert, update, delete', color: '#00684A', component: ChangeStreams },
  { key: 'tx',      icon: '🔒', title: 'Transações ACID',           subtitle: 'Multi-documento, multi-coleção, rollback total', color: '#944F01', component: Transactions },
]

// MongoDB leaf logo SVG (official mark)
function MongoDBLogo({ size = 32 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 256 549" fill="none" xmlns="http://www.w3.org/2000/svg">
      <path d="M175.622 61.108C152.612 33.807 132.797 5.315 128.69.239c-.5-.32-1.0-.239-1.0-.239s-.5-.081-1.0.239C122.583 5.315 102.768 33.807 79.758 61.108 24.914 128.23 0 188.949 0 245.85c0 68.687 31.064 130.1 79.875 171.037l1.872 1.253c1.522 16.09 4.254 51.884 3.551 75.43 0 0 4.596 3.112 9.94 3.928 5.343.816 11.435.816 11.435.816l-1.114-15.274c8.828 1.952 17.9 3.025 27.22 3.025 9.323 0 18.393-1.073 27.22-3.025l-1.114 15.274s6.093 0 11.435-.816c5.343-.816 9.94-3.928 9.94-3.928-.703-23.546 2.029-59.34 3.55-75.43l1.873-1.253C233.936 375.95 265 314.537 265 245.85c0-56.901-24.914-117.62-89.378-184.742z" fill="#00ED64"/>
      <path d="M134.03 468.678s0-175.178.816-175.255c4.474-.504 8.947-1.253 13.338-2.248-.041 0-14.154 177.503-14.154 177.503z" fill="#00684A"/>
      <path d="M128.69 493.092c-4.596-17.18-5.734-34.441-5.734-34.441s-4.148 2.818-4.148 6.745c0 3.928.816 27.696.816 27.696h9.066z" fill="#00684A"/>
    </svg>
  )
}

function MoonIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>
    </svg>
  )
}

function SunIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="5"/>
      <line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/>
      <line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/>
      <line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/>
      <line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/>
    </svg>
  )
}

export default function App() {
  const [active, setActive] = useState('reindex')
  const [dark, setDark] = useState(() => localStorage.getItem('poc-dark') === 'true')

  useEffect(() => {
    document.documentElement.classList.toggle('dark', dark)
    localStorage.setItem('poc-dark', dark)
  }, [dark])

  const mod = MODULES.find(m => m.key === active)
  const Component = mod.component

  return (
    <div style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column' }}>
      {/* ── Header ── */}
      <header style={{
        background: dark ? '#0A1520' : '#001E2B',
        padding: '0 28px',
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        height: 56, flexShrink: 0,
        borderBottom: '1px solid rgba(255,255,255,.07)',
      }}>
        {/* Logo + title */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <MongoDBLogo size={28} />
          <div style={{ borderLeft: '1px solid rgba(255,255,255,.15)', paddingLeft: 12 }}>
            <div style={{ color: '#fff', fontWeight: 700, fontSize: 14, lineHeight: 1.15 }}>MongoDB Atlas</div>
            <div style={{ color: '#00ED64', fontSize: 11, marginTop: 1 }}>Feature Showcase</div>
          </div>
        </div>

        {/* Right side */}
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <span className="badge" style={{ background: 'rgba(0,237,100,.12)', color: '#00ED64', borderColor: 'rgba(0,237,100,.25)' }}>
            Atlas M20
          </span>
          <span className="badge" style={{ background: 'rgba(255,255,255,.07)', color: '#C1C7C6', borderColor: 'rgba(255,255,255,.12)' }}>
            5M docs · Atlas cluster
          </span>
          {/* Dark mode toggle */}
          <button
            onClick={() => setDark(d => !d)}
            style={{
              marginLeft: 4, width: 34, height: 34, borderRadius: 6,
              background: 'rgba(255,255,255,.07)', border: '1px solid rgba(255,255,255,.12)',
              cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center',
              color: dark ? '#F9D96A' : '#C1C7C6', transition: 'all .15s',
            }}
            title={dark ? 'Light mode' : 'Dark mode'}
          >
            {dark ? <SunIcon /> : <MoonIcon />}
          </button>
        </div>
      </header>

      <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
        {/* ── Sidebar ── */}
        <aside style={{
          width: 248, background: 'var(--bg-card)',
          borderRight: '1px solid var(--border-color)',
          padding: '16px 0', flexShrink: 0, overflowY: 'auto',
        }}>
          <div style={{ padding: '0 16px 10px', borderBottom: '1px solid var(--border-color)', marginBottom: 4 }}>
            <span style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '.1em' }}>
              Módulos
            </span>
          </div>

          {MODULES.map(m => (
            <button key={m.key} onClick={() => setActive(m.key)} style={{
              width: '100%', padding: '10px 16px',
              background: active === m.key ? `${m.color}14` : 'transparent',
              border: 'none', borderLeft: `3px solid ${active === m.key ? m.color : 'transparent'}`,
              cursor: 'pointer', textAlign: 'left', display: 'flex', flexDirection: 'column', gap: 2,
              transition: 'background .1s',
            }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ fontSize: 14 }}>{m.icon}</span>
                <span style={{
                  fontWeight: active === m.key ? 600 : 400, fontSize: 13.5,
                  color: active === m.key ? m.color : 'var(--text-primary)',
                }}>
                  {m.title}
                </span>
              </div>
              <span style={{ fontSize: 11, color: 'var(--text-secondary)', paddingLeft: 22 }}>{m.subtitle}</span>
            </button>
          ))}

          {/* Dataset info */}
          <div style={{ margin: '20px 12px 0', padding: 14, background: 'var(--bg-subtle)', borderRadius: 8, border: '1px solid var(--border-color)' }}>
            <div style={{ fontSize: 10, fontWeight: 700, color: 'var(--text-secondary)', marginBottom: 8, textTransform: 'uppercase', letterSpacing: '.1em' }}>
              Dataset
            </div>
            <div style={{ fontSize: 12, display: 'flex', flexDirection: 'column', gap: 4, color: 'var(--text-primary)' }}>
              <div>📦 <strong>5M</strong> produtos</div>
              <div>⭐ <strong>1M</strong> avaliações</div>
              <div style={{ color: 'var(--text-secondary)', fontSize: 11, marginTop: 4 }}>MongoDB Atlas M20</div>
            </div>
          </div>
        </aside>

        {/* ── Main ── */}
        <main style={{ flex: 1, overflowY: 'auto', padding: '28px 32px', background: 'var(--bg-page)' }}>
          <div style={{ maxWidth: 980, margin: '0 auto' }}>
            {/* Page header */}
            <div style={{ marginBottom: 24 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 8 }}>
                <span style={{ fontSize: 24 }}>{mod.icon}</span>
                <h1 style={{ fontSize: 22, fontWeight: 700, color: 'var(--text-primary)' }}>{mod.title}</h1>
              </div>
            </div>
            <Component />
          </div>
        </main>
      </div>
    </div>
  )
}
