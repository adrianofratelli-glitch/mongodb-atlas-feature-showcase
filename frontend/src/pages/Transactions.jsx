import React, { useState } from 'react'
import { useApi } from '../hooks/useApi'

const STEP_ICONS = { 1: '🔍', 2: '📋', 3: '📦', 4: '💳', COMMIT: '✅', ROLLBACK: '🔴' }

function StepRow({ step, last }) {
  const ok        = step.ok
  const isCommit  = step.step === 'COMMIT'
  const isRollback= step.step === 'ROLLBACK'
  const icon      = STEP_ICONS[step.step] || (ok ? '✅' : '❌')

  return (
    <div style={{ display: 'flex', gap: 0, alignItems: 'stretch' }}>
      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', width: 36, flexShrink: 0 }}>
        <div style={{
          width: 30, height: 30, borderRadius: '50%', flexShrink: 0,
          display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 14,
          background: isRollback ? '#FEF3F0' : isCommit ? '#E3FCF7' : ok ? 'var(--bg-subtle)' : '#FEF3F0',
          border: `2px solid ${isRollback ? '#FCCBC5' : isCommit ? '#71F6BA' : ok ? 'var(--border-color)' : '#FCCBC5'}`,
        }}>
          {icon}
        </div>
        {!last && <div style={{ width: 2, flex: 1, minHeight: 10, background: 'var(--border-color)', margin: '2px 0' }} />}
      </div>

      <div style={{ flex: 1, paddingBottom: last ? 0 : 14, paddingLeft: 12 }}>
        <div style={{
          padding: '10px 14px', borderRadius: 6,
          background: isRollback ? '#FEF3F0' : isCommit ? '#E3FCF7' : 'var(--bg-subtle)',
          border: `1px solid ${isRollback ? '#FCCBC5' : isCommit ? '#71F6BA' : 'var(--border-color)'}`,
        }}>
          <div style={{ fontSize: 11, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '.05em', marginBottom: 4,
            color: isRollback ? '#C1271B' : isCommit ? '#00684A' : 'var(--text-secondary)' }}>
            {isCommit ? 'COMMIT' : isRollback ? 'ROLLBACK' : `Step ${step.step}`}
          </div>
          <div style={{ fontWeight: 600, fontSize: 13, color: 'var(--text-primary)', marginBottom: step.detalhe ? 4 : 0 }}>
            {step.descricao}
          </div>
          {step.detalhe && (
            <div style={{ fontSize: 12, color: 'var(--text-secondary)', fontFamily: 'var(--font-mono)', wordBreak: 'break-all' }}>
              {step.detalhe}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

function IDCard({ label, id, collection }) {
  const [copied, setCopied] = useState(false)
  const copy = () => {
    navigator.clipboard?.writeText(id)
    setCopied(true)
    setTimeout(() => setCopied(false), 1500)
  }
  return (
    <div style={{ padding: '14px 16px', background: '#E3FCF7', border: '1px solid #71F6BA', borderRadius: 8 }}>
      <div style={{ fontSize: 11, fontWeight: 700, color: '#00684A', textTransform: 'uppercase', letterSpacing: '.06em', marginBottom: 6 }}>
        {label}
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        <code style={{ fontSize: 12.5, background: 'rgba(0,104,74,.08)', border: '1px solid #71F6BA', color: '#001E2B', flex: 1, padding: '5px 10px', borderRadius: 4, wordBreak: 'break-all' }}>
          {id}
        </code>
        <button onClick={copy} className="btn btn-sm" style={{
          background: copied ? '#00684A' : 'white', color: copied ? 'white' : '#00684A',
          border: '1px solid #71F6BA', flexShrink: 0, transition: 'all .15s',
        }}>
          {copied ? '✓' : 'Copiar'}
        </button>
      </div>
      <div style={{ fontSize: 11, color: '#00684A', marginTop: 6 }}>
        → verifique em <code style={{ background: 'transparent', border: 'none', padding: 0, fontSize: 11 }}>{collection}</code> no Atlas Data Explorer
      </div>
    </div>
  )
}

export default function Transactions() {
  const { call, loading } = useApi()
  const [result,    setResult]    = useState(null)
  const [status,    setStatus]    = useState(null)
  const [resetting, setResetting] = useState(false)

  const loadStatus = async () => {
    const data = await call('/transactions/status')
    if (data) setStatus(data)
  }

  const executar = async (simularFalha) => {
    setResult(null)
    const data = await call(`/transactions/executar?simular_falha=${simularFalha}`, { method: 'POST' })
    if (data) { setResult(data); await loadStatus() }
  }

  const reset = async () => {
    setResetting(true)
    await call('/transactions/reset', { method: 'POST' })
    setResult(null)
    setStatus(null)
    setResetting(false)
  }

  React.useEffect(() => { loadStatus() }, [])

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

      <div className="banner banner-info">
        <span>ℹ️</span>
        <div>
          <strong>ACID Transactions</strong> garantem que múltiplas escritas em coleções distintas
          sejam confirmadas atomicamente — ou todas persistem, ou nenhuma. Essencial para
          operações financeiras onde qualquer inconsistência pode representar perda real.
        </div>
      </div>

      {/* Scenario */}
      <div className="card" style={{ padding: '16px 18px' }}>
        <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 10 }}>Cenário: Compra com PIX</div>
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginBottom: 10 }}>
          {[
            { icon: '🔍', label: 'Verificar produto', col: 'produtos'       },
            { icon: '📋', label: 'Criar pedido',      col: 'pedidos_demo'   },
            { icon: '📦', label: 'Reservar estoque',  col: 'estoque_demo'   },
            { icon: '💳', label: 'Registrar pagamento',col: 'pagamentos_demo'},
          ].map((s, i) => (
            <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '6px 12px', background: 'var(--bg-subtle)', borderRadius: 6, border: '1px solid var(--border-color)', fontSize: 12 }}>
              <span>{s.icon}</span>
              <div>
                <div style={{ fontWeight: 600 }}>{s.label}</div>
                <div style={{ color: 'var(--text-secondary)', fontFamily: 'var(--font-mono)', fontSize: 11 }}>{s.col}</div>
              </div>
              {i < 3 && <span style={{ color: 'var(--border-color)', marginLeft: 4 }}>→</span>}
            </div>
          ))}
        </div>
        <p style={{ fontSize: 13, color: 'var(--text-secondary)', lineHeight: 1.6 }}>
          Todos os 4 steps ocorrem dentro de uma única sessão. Se qualquer step falhar,
          todos os writes anteriores são revertidos automaticamente.
        </p>
      </div>

      {/* Buttons */}
      <div className="grid-2">
        <div className="card" style={{ display: 'flex', flexDirection: 'column', gap: 10, padding: '18px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{ fontSize: 20 }}>✅</span>
            <strong style={{ fontSize: 14 }}>Transação com sucesso</strong>
          </div>
          <p style={{ fontSize: 12.5, color: 'var(--text-secondary)', lineHeight: 1.6 }}>
            Todos os steps completam. COMMIT persiste os 3 documentos simultaneamente nas 3 coleções.
          </p>
          <button className="btn btn-primary" style={{ marginTop: 'auto' }}
            onClick={() => executar(false)} disabled={loading}>
            {loading ? <><span className="spinner" /> Executando…</> : '▶ Executar'}
          </button>
        </div>

        <div className="card" style={{ display: 'flex', flexDirection: 'column', gap: 10, padding: '18px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{ fontSize: 20 }}>🔴</span>
            <strong style={{ fontSize: 14 }}>Simular falha (ROLLBACK)</strong>
          </div>
          <p style={{ fontSize: 12.5, color: 'var(--text-secondary)', lineHeight: 1.6 }}>
            Força um erro no step 3. O MongoDB reverte tudo automaticamente — nenhuma coleção fica com dados parciais.
          </p>
          <button className="btn btn-default" style={{ marginTop: 'auto' }}
            onClick={() => executar(true)} disabled={loading}>
            {loading ? <><span className="spinner" /> Executando…</> : '💥 Simular falha'}
          </button>
        </div>
      </div>

      {/* Result */}
      {result && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

          {/* Step timeline */}
          <div className="card" style={{ padding: '18px 20px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 18 }}>
              <strong style={{ fontSize: 15 }}>Execução</strong>
              <span className={`badge ${result.success ? 'badge-green' : 'badge-red'}`}>
                {result.success ? '✅ COMMIT' : '🔴 ROLLBACK'}
              </span>
            </div>
            <div style={{ display: 'flex', flexDirection: 'column' }}>
              {result.steps.map((step, i) => (
                <StepRow key={i} step={step} last={i === result.steps.length - 1} />
              ))}
            </div>
          </div>

          {/* IDs — só exibe no sucesso */}
          {result.success && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
              <div style={{ fontWeight: 600, fontSize: 14, color: 'var(--text-primary)' }}>
                ✅ Documentos criados — verifique no Atlas Data Explorer
              </div>
              <IDCard
                label={`pedidos_demo — ${result.produto} — R$ ${result.valor?.toFixed(2)}`}
                id={result.pedido_id}
                collection="POC › pedidos_demo"
              />
              <IDCard
                label="pagamentos_demo — PIX aprovado"
                id={result.pagamento_id}
                collection="POC › pagamentos_demo"
              />
            </div>
          )}

          {/* Rollback explainer */}
          {!result.success && (
            <div className="banner banner-warning">
              <span>💡</span>
              <div>
                O pedido criado no step 2 foi revertido. As coleções <code>pedidos_demo</code>,{' '}
                <code>estoque_demo</code> e <code>pagamentos_demo</code> permanecem exatamente
                como estavam antes da transação iniciar.
              </div>
            </div>
          )}
        </div>
      )}

      {/* Collections status */}
      {status && (
        <div className="card" style={{ padding: '14px 18px' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
            <strong style={{ fontSize: 13 }}>Estado das coleções</strong>
            <div style={{ display: 'flex', gap: 8 }}>
              <button className="btn btn-default btn-xs" onClick={loadStatus}>↻ Atualizar</button>
              <button className="btn btn-danger btn-xs" onClick={reset} disabled={resetting}>
                {resetting ? <><span className="spinner" /> Limpando…</> : 'Limpar dados'}
              </button>
            </div>
          </div>
          <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
            {Object.entries(status).map(([col, count]) => (
              <div key={col} style={{ padding: '8px 14px', background: 'var(--bg-subtle)', borderRadius: 6, border: '1px solid var(--border-color)', fontSize: 12 }}>
                <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-secondary)' }}>{col}</span>
                <span style={{ fontWeight: 700, marginLeft: 8, color: count > 0 ? '#00A35C' : 'var(--text-secondary)' }}>
                  {count} doc{count !== 1 ? 's' : ''}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
