import React, { useCallback, useState, useEffect } from 'react'
import { useIntervaloVisivel } from './hooks/usePolling'
import Reindexacao from './pages/Reindexacao'
import HotCold from './pages/HotCold'
import Aggregations from './pages/Aggregations'
import SchemaValidation from './pages/SchemaValidation'
import ChangeStreams from './pages/ChangeStreams'
import Transactions from './pages/Transactions'
import Streaming from './pages/Streaming'
import Geo from './pages/Geo'

const MODULES = [
  { key: 'reindex', num: '01', title: 'Reindexação Online',   subtitle: 'Hybrid build sem bloqueio prolongado',            color: '#00ED64', component: Reindexacao },
  { key: 'hotcold', num: '02', title: 'Hot / Cold Tiering',   subtitle: 'Online Archive — dados históricos automáticos',  color: '#06b6d4', component: HotCold },
  { key: 'agg',     num: '03', title: 'Aggregation Pipeline', subtitle: '$lookup, $facet, $setWindowFields e mais',       color: '#a855f7', component: Aggregations },
  { key: 'schema',  num: '04', title: 'Schema Validation',    subtitle: 'JSON Schema enforcement no banco',               color: '#f97316', component: SchemaValidation },
  { key: 'streams', num: '05', title: 'Change Streams',       subtitle: 'Eventos em tempo real — insert, update, delete', color: '#14b8a6', component: ChangeStreams },
  { key: 'tx',      num: '06', title: 'Transações ACID',      subtitle: 'Multi-documento, multi-coleção, rollback total', color: '#eab308', component: Transactions },
  { key: 'streaming', num: '07', title: 'Streaming',            subtitle: 'Change Streams × Kafka Connector × Atlas Stream Processing', color: '#e11d48', component: Streaming },
  { key: 'geo',     num: '08', title: 'Risco geográfico',    subtitle: 'impossible travel, contexto do estabelecimento e geo no mesmo cluster', color: '#38bdf8', component: Geo },
]

// MongoDB leaf logo SVG (official mark)
function MongoDBLogo({ size = 32 }) {
  return (
    <svg aria-hidden="true" width={size} height={size} viewBox="0 0 256 549" fill="none" xmlns="http://www.w3.org/2000/svg">
      <path d="M175.622 61.108C152.612 33.807 132.797 5.315 128.69.239c-.5-.32-1.0-.239-1.0-.239s-.5-.081-1.0.239C122.583 5.315 102.768 33.807 79.758 61.108 24.914 128.23 0 188.949 0 245.85c0 68.687 31.064 130.1 79.875 171.037l1.872 1.253c1.522 16.09 4.254 51.884 3.551 75.43 0 0 4.596 3.112 9.94 3.928 5.343.816 11.435.816 11.435.816l-1.114-15.274c8.828 1.952 17.9 3.025 27.22 3.025 9.323 0 18.393-1.073 27.22-3.025l-1.114 15.274s6.093 0 11.435-.816c5.343-.816 9.94-3.928 9.94-3.928-.703-23.546 2.029-59.34 3.55-75.43l1.873-1.253C233.936 375.95 265 314.537 265 245.85c0-56.901-24.914-117.62-89.378-184.742z" fill="#00ED64"/>
      <path d="M134.03 468.678s0-175.178.816-175.255c4.474-.504 8.947-1.253 13.338-2.248-.041 0-14.154 177.503-14.154 177.503z" fill="#00684A"/>
      <path d="M128.69 493.092c-4.596-17.18-5.734-34.441-5.734-34.441s-4.148 2.818-4.148 6.745c0 3.928.816 27.696.816 27.696h9.066z" fill="#00684A"/>
    </svg>
  )
}

function ApiErrorToast() {
  const [toast, setToast] = useState(null)

  useEffect(() => {
    let timer
    let lastKey = ''
    let lastAt = 0
    const onError = (e) => {
      const key = `${e.detail?.path || ''}:${e.detail?.message || ''}`
      const now = Date.now()
      if (key === lastKey && now - lastAt < 8000) return
      lastKey = key
      lastAt = now
      setToast(e.detail)
      clearTimeout(timer)
      timer = setTimeout(() => setToast(null), 6000)
    }
    window.addEventListener('api-error', onError)
    return () => { window.removeEventListener('api-error', onError); clearTimeout(timer) }
  }, [])

  if (!toast) return null
  return (
    <div style={{
      position: 'fixed', bottom: 24, right: 24, zIndex: 1000, maxWidth: 420,
      display: 'flex', alignItems: 'flex-start', gap: 10, padding: '13px 16px',
      background: 'rgba(255,105,96,.12)', border: '1px solid rgba(255,105,96,.4)', borderRadius: 12,
      backdropFilter: 'blur(12px)',
      boxShadow: '0 8px 32px rgba(0,0,0,.45)', fontSize: 13, color: '#ff9b94',
    }}>
      <span style={{ flexShrink: 0 }}>⚠️</span>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontWeight: 700, marginBottom: 2, color: '#ffb3ae' }}>Erro na chamada à API</div>
        <div style={{ wordBreak: 'break-word' }}>
          <code style={{ background: 'transparent', border: 'none', padding: 0, fontSize: 12 }}>{toast.path}</code>
          {' — '}{toast.message}
        </div>
      </div>
      <button aria-label="Fechar aviso" onClick={() => setToast(null)} style={{
        background: 'none', border: 'none', cursor: 'pointer', color: '#ff9b94',
        fontSize: 16, lineHeight: 1, padding: 0, flexShrink: 0,
      }}>×</button>
    </div>
  )
}

// Formata contagens grandes: 5_000_000 → "5M", 100_000 → "100k"
function fmtCount(n) {
  if (n == null) return '…'
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(n % 1_000_000 >= 100_000 ? 1 : 0)}M`
  if (n >= 1_000) return `${Math.round(n / 1_000)}k`
  return String(n)
}

export default function App() {
  const [active, setActive] = useState(() => {
    const hash = window.location.hash.slice(1)
    return MODULES.some(m => m.key === hash) ? hash : 'reindex'
  })
  const [stats, setStats] = useState(null)
  const [preflight, setPreflight] = useState(null)
  // O cluster tem auto-scaling: anunciar um tier fixo faria a tela mentir.
  const [cluster, setCluster] = useState(null)
  // Barra fixa aberta por padrão; o alfinete solta e a escolha persiste.
  const [sidebarHover, setSidebarHover] = useState(false)
  const [sidebarFixa, setSidebarFixa] = useState(() => {
    try { return localStorage.getItem('sidebarFixa') !== 'false' } catch { return true }
  })
  const sidebarAberta = sidebarHover || sidebarFixa
  useEffect(() => {
    try { localStorage.setItem('sidebarFixa', String(sidebarFixa)) } catch { /* modo privado */ }
  }, [sidebarFixa])
  const refreshPreflight = useCallback(() => {
    fetch('/api/preflight').then(r => r.json()).then(setPreflight).catch(() => setPreflight({ ready: false }))
  }, [])

  useEffect(() => {
    window.history.replaceState(null, '', `#${active}`)
  }, [active])

  // Deep-link: reage a mudanças de hash com a app aberta
  useEffect(() => {
    const onHash = () => {
      const hash = window.location.hash.slice(1)
      if (MODULES.some(m => m.key === hash)) setActive(hash)
    }
    window.addEventListener('hashchange', onHash)
    return () => window.removeEventListener('hashchange', onHash)
  }, [])

  // Contagens reais do cluster (nada hard-coded na UI). Uma vez ao montar.
  useEffect(() => {
    fetch('/api/stats').then(r => r.ok ? r.json() : null).then(d => { if (d) setStats(d) }).catch(() => {})
    refreshPreflight()
    window.addEventListener('preflight-refresh', refreshPreflight)
    return () => window.removeEventListener('preflight-refresh', refreshPreflight)
  }, [refreshPreflight])

  // Tier do cluster: a Admin API é control plane, não consome CPU do banco, mas
  // uma aba esquecida não precisa perguntar de 30 em 30 segundos para sempre.
  // Só enquanto visível, e mais espaçado — tier muda em minutos, não segundos.
  useIntervaloVisivel(useCallback(() => {
    fetch('/api/streaming/cluster').then(r => r.ok ? r.json() : null)
      .then(d => { if (d) setCluster(d) }).catch(() => {})
  }, []), 120000)

  const mod = MODULES.find(m => m.key === active)
  const Component = mod.component

  return (
    <div style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column' }}>
      {/* ── Top nav (sticky, blurred) ── */}
      <header className="app-header" style={{
        position: 'sticky', top: 0, zIndex: 50,
        background: 'rgba(0,30,43,.92)', backdropFilter: 'blur(16px)',
        padding: '0 28px',
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        height: 58, flexShrink: 0,
        borderBottom: '1px solid var(--border-subtle)',
      }}>
        {/* Logo + title */}
        <div className="app-header-brand" style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <MongoDBLogo size={26} />
          <div style={{ borderLeft: '1px solid rgba(255,255,255,.12)', paddingLeft: 12 }}>
            <div style={{ color: '#fafafa', fontWeight: 700, fontSize: 14.5, lineHeight: 1.15, letterSpacing: '-.01em' }}>MongoDB Atlas</div>
            <div style={{
              color: 'var(--accent)', fontSize: 10, marginTop: 1,
              fontFamily: 'var(--font-mono)', fontWeight: 600,
              textTransform: 'uppercase', letterSpacing: '.12em',
            }}>Feature Showcase</div>
          </div>
        </div>

        {/* Right side */}
        <div className="app-header-status" style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          {/* M20 e M30 são estados válidos dentro do auto-scaling configurado.
              O tier real continua visível, sem transformar uma escala legítima
              em falha de pré-voo. */}
          <span className="badge badge-green"
            title={cluster?.autoscaling?.ativo
              ? `Auto-scaling ${cluster.autoscaling.min}→${cluster.autoscaling.max}`
              : 'Tier do cluster'}>
            Atlas {cluster?.tier || '…'}{cluster?.autoscaling?.ativo ? ' · auto' : ''}
          </span>
          <span className={`badge ${preflight?.ready ? 'badge-green' : 'badge-yellow'}`}
            title={preflight?.ready ? 'Pré-voo concluído' : 'Preparação incompleta — veja o diagnóstico no menu lateral'}>
            {preflight?.ready ? '● Pronto' : '● Pré-voo pendente'}
          </span>
          <span className="badge badge-gray app-header-docs">
            {stats ? `${fmtCount(stats.produtos + stats.avaliacoes)} docs · Atlas cluster` : 'Atlas cluster'}
          </span>
        </div>
      </header>

      <div className="app-shell-body" style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
        {/* ── Sidebar: fixa aberta por padrão (preferência em localStorage) ──
            O alfinete solta a barra para o modo retrátil: o módulo Streaming
            tem três colunas lado a lado e às vezes precisa dessa largura. */}
        <aside
          className={`app-sidebar${sidebarAberta ? ' aberta' : ''}`}
          aria-label="Módulos da demonstração"
          onMouseEnter={() => setSidebarHover(true)}
          onMouseLeave={() => setSidebarHover(false)}
        >
          <div className="sb-topo">
            <span className="kicker sb-so-aberta">Módulos</span>
            <button
              className="sb-pin"
              aria-pressed={sidebarFixa}
              aria-label={sidebarFixa ? 'Soltar barra lateral' : 'Fixar barra lateral aberta'}
              title={sidebarFixa ? 'Soltar a barra (volta a recolher)' : 'Fixar a barra aberta'}
              onClick={() => setSidebarFixa(v => !v)}
            >{sidebarFixa ? '📌' : '📍'}</button>
          </div>

          {MODULES.map(m => (
            <button key={m.key} aria-current={active === m.key ? 'page' : undefined}
              title={`${m.num} · ${m.title}`}
              onClick={() => setActive(m.key)}
              className="sb-item"
              style={{
                background: active === m.key ? `${m.color}10` : 'transparent',
                borderLeft: `3px solid ${active === m.key ? m.color : 'transparent'}`,
              }}>
              <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
                <span style={{
                  fontFamily: 'var(--font-mono)', fontSize: 10, fontWeight: 600,
                  letterSpacing: '.1em', color: active === m.key ? m.color : 'var(--text-disabled)',
                }}>{m.num}</span>
                <span className="sb-so-aberta" style={{
                  fontWeight: active === m.key ? 700 : 500, fontSize: 13.5,
                  color: active === m.key ? m.color : 'var(--text-primary)',
                  letterSpacing: '-.01em', whiteSpace: 'nowrap',
                }}>
                  {m.title}
                </span>
              </div>
              <span className="sb-so-aberta" style={{ fontSize: 11, color: 'var(--text-secondary)', paddingLeft: 26, lineHeight: 1.45 }}>{m.subtitle}</span>
            </button>
          ))}

          <div className="sb-so-aberta">

          {/* Dataset info */}
          <div style={{
            margin: '22px 14px 0', padding: 16,
            background: 'rgba(255,255,255,.02)', borderRadius: 14,
            border: '1px solid var(--border-subtle)',
          }}>
            <div className="kicker" style={{ marginBottom: 10, fontSize: 10 }}>Dataset</div>
            <div style={{ fontSize: 12.5, display: 'flex', flexDirection: 'column', gap: 5, color: 'var(--text-primary)' }}>
              <div>📦 <strong style={{ fontFamily: 'var(--font-mono)' }}>{fmtCount(stats?.produtos)}</strong> produtos</div>
              <div>⭐ <strong style={{ fontFamily: 'var(--font-mono)' }}>{fmtCount(stats?.avaliacoes)}</strong> avaliações</div>
              <div style={{ color: 'var(--text-secondary)', fontSize: 11, marginTop: 5, fontFamily: 'var(--font-mono)' }}>MongoDB Atlas {cluster?.tier || '…'}</div>
            </div>
          </div>

          <div style={{ margin: '12px 14px 0', padding: 12, borderRadius: 10, border: '1px solid var(--border-subtle)', fontSize: 11 }}>
            <div className="kicker" style={{ marginBottom: 7 }}>Pré-voo</div>
            <div style={{ color: preflight?.ready ? 'var(--accent)' : '#fbbf24', fontWeight: 700 }}>
              {preflight?.ready ? '✓ Ambiente pronto' : '⚠ Verificação necessária'}
            </div>
            {preflight?.checks && Object.entries(preflight.checks).filter(([, c]) => !c.ok).map(([key, check]) => (
              <div key={key} style={{ color: 'var(--text-secondary)', marginTop: 5 }}>{key}: {check.message}</div>
            ))}
          </div>
          </div>
        </aside>

        {/* ── Main ── */}
        <main className="app-main" style={{ flex: 1, overflowY: 'auto', padding: '32px 36px', background: 'var(--bg-primary)' }}>
          <div style={{ maxWidth: 980, margin: '0 auto' }} key={active} className="fade-in">
            {/* Page header */}
            <div style={{ marginBottom: 26 }}>
              <div className="kicker" style={{ color: mod.color, marginBottom: 10 }}>Módulo {mod.num}</div>
              <h1 style={{
                fontSize: 30, fontWeight: 800, color: 'var(--text-primary)',
                letterSpacing: '-.03em', lineHeight: 1.1,
              }}>{mod.title}</h1>
            </div>
            <Component />
          </div>
        </main>
      </div>
      <ApiErrorToast />
    </div>
  )
}
