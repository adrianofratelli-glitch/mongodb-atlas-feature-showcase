import React, { useState, useEffect } from 'react'
import { useApi } from '../hooks/useApi'

export default function HotCold() {
  const { call, loading } = useApi()
  const [distribution, setDistribution]   = useState(null)
  const [simulation, setSimulation]       = useState(null)
  const [transparent, setTransparent]     = useState(null)
  const [archives, setArchives]           = useState(null)
  const [archiveResult, setArchiveResult] = useState(null)
  const [active, setActive]               = useState(null)
  const [days, setDays]                   = useState(365)

  const fetchArchives = async () => {
    const d = await call('/hot-cold/online-archive/list')
    if (d) setArchives(d.archives)
  }

  useEffect(() => { fetchArchives() }, [])

  const fetchDistribution  = async () => { setActive('dist');   const d = await call('/hot-cold/distribution');           if (d) setDistribution(d) }
  const fetchSimulation    = async () => { setActive('sim');    const d = await call('/hot-cold/archive-simulation');      if (d) setSimulation(d) }
  const fetchTransparent   = async () => { setActive('transp'); const d = await call('/hot-cold/query-transparent');       if (d) setTransparent(d) }

  const createArchive = async () => {
    setActive('create')
    const d = await call(`/hot-cold/online-archive/create?expire_after_days=${days}`, { method: 'POST' })
    if (d) { setArchiveResult(d); await fetchArchives() }
  }

  const deleteArchive = async (id) => {
    await call(`/hot-cold/online-archive/${id}`, { method: 'DELETE' })
    await fetchArchives()
    setArchiveResult(null)
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
      <div className="banner banner-info">
        <span>ℹ️</span>
        <div>
          <strong>MongoDB Atlas Online Archive</strong> move dados históricos automaticamente para object storage de baixo custo, mantendo a <em>mesma connection string</em>. Queries são roteadas de forma transparente. O <strong>DocumentDB não possui equivalente</strong>.
        </div>
      </div>

      {/* Análise dos dados */}
      <div className="grid-3">
        {[
          { key: 'dist',   icon: '📊', title: 'Distribuição por Ano',  desc: 'Quando foram criados os documentos — base para a política de arquivo', fn: fetchDistribution },
          { key: 'sim',    icon: '🧮', title: 'Simulação de Arquivo',  desc: '60% dos documentos seriam arquivados com política de 1 ano', fn: fetchSimulation },
          { key: 'transp', icon: '🔗', title: 'Query Transparente',    desc: 'Com OA ativo, a mesma query retorna hot + cold sem mudar o código', fn: fetchTransparent },
        ].map(c => (
          <div key={c.key} className="card" style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
            <div style={{ fontSize: 22 }}>{c.icon}</div>
            <strong style={{ fontSize: 14 }}>{c.title}</strong>
            <p style={{ color: 'var(--mdb-gray-5)', fontSize: 12, flex: 1 }}>{c.desc}</p>
            <button className="btn btn-primary" style={{ width: '100%', justifyContent: 'center' }} onClick={c.fn} disabled={loading && active === c.key}>
              {loading && active === c.key ? <><span className="spinner" /> Executando...</> : 'Executar'}
            </button>
          </div>
        ))}
      </div>

      {distribution && (
        <div className="card">
          <strong style={{ fontSize: 15, display: 'block', marginBottom: 12 }}>
            Documentos por Ano de Criação
          </strong>
          <p style={{ color: 'var(--mdb-gray-5)', fontSize: 13, marginBottom: 12 }}>{distribution.note}</p>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {distribution.distribution.map(row => (
              <div key={row.year} className="result-row" style={{ justifyContent: 'space-between' }}>
                <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
                  <span className={`badge ${row.tier.includes('Hot') ? 'badge-red' : 'badge-blue'}`}>{row.tier}</span>
                  <strong>{row.year}</strong>
                  <span style={{ color: 'var(--mdb-gray-5)', fontSize: 13 }}>{row.count.toLocaleString()} docs</span>
                </div>
                <span style={{ color: 'var(--mdb-gray-5)', fontSize: 13 }}>preço médio R$ {row.avg_preco.toFixed(2)}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {simulation && (
        <div className="card">
          <strong style={{ fontSize: 15, display: 'block', marginBottom: 16 }}>Simulação — política: dados &gt; 1 ano → Cold tier</strong>
          <div className="row" style={{ marginBottom: 16 }}>
            <div className="stat-block col" style={{ borderColor: '#FF6A6A40' }}>
              <div className="stat-label">🔥 Hot — {simulation.hot.tier}</div>
              <div className="stat-value" style={{ color: '#C1271B' }}>{simulation.hot.pct}%</div>
              <div className="stat-sub">{simulation.hot.count.toLocaleString()} docs · {simulation.hot.latency}</div>
            </div>
            <div className="stat-block col" style={{ borderColor: '#016BF840' }}>
              <div className="stat-label">❄️ Cold — {simulation.cold.tier}</div>
              <div className="stat-value" style={{ color: '#016BF8' }}>{simulation.cold.pct}%</div>
              <div className="stat-sub">{simulation.cold.count.toLocaleString()} docs · {simulation.cold.latency}</div>
            </div>
          </div>
          <div style={{ padding: '12px 16px', background: '#E3FCF7', borderRadius: 8, border: '1px solid #71F6BA' }}>
            <div style={{ color: 'var(--mdb-green-dark-2)', fontWeight: 500 }}>💰 {simulation.savings_estimate}</div>
            <div style={{ color: 'var(--mdb-green-dark-2)', fontSize: 12, marginTop: 4 }}>🔗 {simulation.transparencia}</div>
          </div>
        </div>
      )}

      {transparent && (
        <div className="card">
          <strong style={{ fontSize: 15, display: 'block', marginBottom: 6 }}>Query Transparente — como funciona</strong>
          <div style={{ padding: '10px 14px', background: 'var(--mdb-gray-1)', borderRadius: 6, marginBottom: 12, fontSize: 13 }}>
            <code>{transparent.query_used}</code>
          </div>
          <div className="banner banner-success" style={{ marginBottom: 16 }}>
            <span>✅</span>
            <div>{transparent.explanation}</div>
          </div>
          <div className="grid-2">
            <div>
              <div style={{ marginBottom: 8 }}><span className="badge badge-red">🔥 Hot — Cluster Atlas (NVMe)</span></div>
              {transparent.hot_samples.map((d, i) => (
                <div key={i} style={{ padding: '8px 12px', background: '#FFF9F5', borderRadius: 6, marginBottom: 6, fontSize: 13 }}>
                  <div style={{ fontWeight: 500 }}>{d.nome}</div>
                  <div style={{ color: 'var(--mdb-gray-5)' }}>R$ {d.preco?.toFixed(2)} · {d.created_at?.slice(0, 10)}</div>
                </div>
              ))}
            </div>
            <div>
              <div style={{ marginBottom: 8 }}><span className="badge badge-blue">❄️ Cold — Online Archive</span></div>
              {transparent.cold_samples.map((d, i) => (
                <div key={i} style={{ padding: '8px 12px', background: '#F3F7FF', borderRadius: 6, marginBottom: 6, fontSize: 13 }}>
                  <div style={{ fontWeight: 500 }}>{d.nome}</div>
                  <div style={{ color: 'var(--mdb-gray-5)' }}>R$ {d.preco?.toFixed(2)} · {d.created_at?.slice(0, 10)}</div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Online Archive via Atlas API */}
      <div className="card" style={{ borderColor: '#016BF840', borderWidth: 2 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
          <div>
            <strong style={{ fontSize: 15 }}>🗄️ Criar Regra de Online Archive</strong>
            <p style={{ color: 'var(--mdb-gray-5)', fontSize: 13, marginTop: 4 }}>
              Cria a regra diretamente no cluster via Atlas API — visível no painel do Atlas após criar.
            </p>
          </div>
        </div>

        <div style={{ display: 'flex', gap: 12, alignItems: 'flex-end', marginBottom: 16, flexWrap: 'wrap' }}>
          <div>
            <label style={{ fontSize: 12, color: 'var(--mdb-gray-5)', display: 'block', marginBottom: 4 }}>
              Arquivar documentos com <code>created_at</code> mais antigo que:
            </label>
            <div style={{ display: 'flex', gap: 8 }}>
              {[180, 365, 730].map(d => (
                <button key={d} className={`tag ${days === d ? 'active' : ''}`} onClick={() => setDays(d)}
                  style={days === d ? { borderColor: '#016BF8', color: '#016BF8', background: '#EBF1FF' } : {}}>
                  {d === 180 ? '6 meses' : d === 365 ? '1 ano' : '2 anos'}
                </button>
              ))}
            </div>
          </div>
          <button className="btn btn-primary" onClick={createArchive} disabled={loading && active === 'create'}>
            {loading && active === 'create' ? <><span className="spinner" /> Criando...</> : '+ Criar Regra no Atlas'}
          </button>
        </div>

        {archiveResult && (
          <div style={{ padding: '12px 16px', background: '#E3FCF7', borderRadius: 8, border: '1px solid #71F6BA', marginBottom: 16, fontSize: 13 }}>
            <div style={{ fontWeight: 600, color: 'var(--mdb-green-dark-2)', marginBottom: 6 }}>✅ Regra criada com sucesso!</div>
            <div><strong>ID:</strong> {archiveResult.archive_id}</div>
            <div><strong>Status:</strong> {archiveResult.status}</div>
            <div style={{ marginTop: 4 }}>{archiveResult.message}</div>
            <div style={{ marginTop: 8, padding: '6px 10px', background: '#fff', borderRadius: 6, fontSize: 12, display: 'inline-block' }}>
              🔗 Confira no Atlas: <strong>Clusters → {'{'}cluster{'}'} → Online Archive</strong>
            </div>
          </div>
        )}

        <div>
          <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 8 }}>Regras Ativas no Cluster</div>
          {archives === null
            ? <div style={{ color: 'var(--mdb-gray-5)', fontSize: 13 }}>Carregando...</div>
            : archives.length === 0
              ? <div style={{ color: 'var(--mdb-gray-5)', fontSize: 13 }}>Nenhuma regra ativa. Crie uma acima.</div>
              : archives.map(a => (
                  <div key={a.id} className="result-row" style={{ justifyContent: 'space-between', marginBottom: 6 }}>
                    <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
                      <span className={`badge ${a.status === 'ACTIVE' || a.status === 'IDLE' ? 'badge-green' : 'badge-yellow'}`}>{a.status}</span>
                      <code style={{ fontSize: 12 }}>{a.collection}</code>
                      <span style={{ color: 'var(--mdb-gray-5)', fontSize: 12 }}>{a.date_field} &gt; {a.expire_after_days} dias</span>
                    </div>
                    <button className="btn btn-xs btn-danger" onClick={() => deleteArchive(a.id)}>Remover</button>
                  </div>
                ))
          }
        </div>
      </div>
    </div>
  )
}
