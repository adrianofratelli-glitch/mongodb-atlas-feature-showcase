import React, { useState } from 'react'
import { useApi } from '../hooks/useApi'
import Limites from '../components/Limites'

const fmtMs = (v) => (v == null ? '—' : v >= 1000
  ? `${(v / 1000).toFixed(2).replace('.', ',')} s`
  : `${v.toFixed(v < 10 ? 1 : 0).replace('.', ',')} ms`)

// Uma coluna do painel de medição. `nota` é o que impede o número de ser lido
// sozinho — em medição, o número sem a condição em que foi obtido é a parte
// fácil de citar errado.
function ColunaMedida({ titulo, dados, destaque, nota }) {
  return (
    <div className="card" style={{ padding: '14px 16px', borderColor: destaque ? 'rgba(0,237,100,.3)' : undefined }}>
      <div style={{ fontSize: 12, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '.05em',
        color: destaque ? '#00ED64' : 'var(--text-secondary)', marginBottom: 10 }}>{titulo}</div>
      <div style={{ display: 'flex', gap: 14, flexWrap: 'wrap' }}>
        {[['p50', dados.p50], ['p95', dados.p95], ['p99', dados.p99]].map(([r, v]) => (
          <div key={r}>
            <div style={{ fontFamily: 'var(--font-mono)', fontSize: 17, fontWeight: 700 }}>{fmtMs(v)}</div>
            <div style={{ fontSize: 10.5, color: 'var(--text-secondary)', textTransform: 'uppercase',
              letterSpacing: '.06em' }}>{r}</div>
          </div>
        ))}
      </div>
      {nota && <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginTop: 10, lineHeight: 1.55 }}>{nota}</div>}
    </div>
  )
}

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
          background: isRollback ? 'rgba(255,105,96,.08)' : isCommit ? 'rgba(0,237,100,.08)' : ok ? 'var(--bg-subtle)' : 'rgba(255,105,96,.08)',
          border: `2px solid ${isRollback ? 'rgba(255,105,96,.35)' : isCommit ? 'rgba(0,237,100,.3)' : ok ? 'var(--border-color)' : 'rgba(255,105,96,.35)'}`,
        }}>
          {icon}
        </div>
        {!last && <div style={{ width: 2, flex: 1, minHeight: 10, background: 'var(--border-color)', margin: '2px 0' }} />}
      </div>

      <div style={{ flex: 1, paddingBottom: last ? 0 : 14, paddingLeft: 12 }}>
        <div style={{
          padding: '10px 14px', borderRadius: 6,
          background: isRollback ? 'rgba(255,105,96,.08)' : isCommit ? 'rgba(0,237,100,.08)' : 'var(--bg-subtle)',
          border: `1px solid ${isRollback ? 'rgba(255,105,96,.35)' : isCommit ? 'rgba(0,237,100,.3)' : 'var(--border-color)'}`,
        }}>
          <div style={{ fontSize: 11, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '.05em', marginBottom: 4,
            color: isRollback ? '#ff6960' : isCommit ? '#00ED64' : 'var(--text-secondary)' }}>
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
  const copy = async () => {
    if (!navigator.clipboard) return
    try {
      await navigator.clipboard.writeText(id)
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    } catch {
      // Clipboard pode ser bloqueado fora de HTTPS; o ID continua selecionável.
    }
  }
  return (
    <div style={{ padding: '14px 16px', background: 'rgba(0,237,100,.08)', border: '1px solid rgba(0,237,100,.3)', borderRadius: 8 }}>
      <div style={{ fontSize: 11, fontWeight: 700, color: '#00ED64', textTransform: 'uppercase', letterSpacing: '.06em', marginBottom: 6 }}>
        {label}
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        <code style={{ fontSize: 12.5, background: 'rgba(0,104,74,.08)', border: '1px solid rgba(0,237,100,.3)', color: '#fafafa', flex: 1, padding: '5px 10px', borderRadius: 4, wordBreak: 'break-all' }}>
          {id}
        </code>
        <button onClick={copy} className="btn btn-sm" style={{
          background: copied ? '#00ED64' : 'transparent', color: copied ? '#001E2B' : '#00ED64',
          border: '1px solid rgba(0,237,100,.3)', flexShrink: 0, transition: 'all .15s',
        }}>
          {copied ? '✓' : 'Copiar'}
        </button>
      </div>
      <div style={{ fontSize: 11, color: '#00ED64', marginTop: 6 }}>
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
    const result = await call('/transactions/reset', { method: 'POST' })
    if (result) {
      setResult(null)
      setStatus(null)
    }
    setResetting(false)
  }

  React.useEffect(() => { loadStatus() }, [])

  // O benchmark tem `useApi` próprio: o `loading` do hook é uma flag única para
  // todas as chamadas daquela instância, e compartilhá-lo com os botões de
  // executar/reset desabilitaria a página inteira durante um minuto de medição.
  const bench = useApi()
  const [medida, setMedida] = useState(null)
  const [medindo, setMedindo] = useState(false)

  const medir = async () => {
    setMedindo(true)
    setMedida(null)
    // A medição faz dezenas de idas e voltas com `majority`; o teto padrão de
    // 30 s do hook aborta no meio e a página reportaria falha de rede onde
    // houve só uma medição honesta e demorada.
    const data = await bench.call('/transactions/benchmark?amostras=30&concorrencia=6',
      { method: 'POST', timeoutMs: 300_000 })
    if (data) setMedida(data)
    setMedindo(false)
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>

      <div className="banner banner-info">
        <span>ℹ️</span>
        <div>
          <strong>Transação ACID multi-documento:</strong> 3 coleções, um commit. Ou tudo persiste, ou nada.
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
          <code>session.with_transaction()</code> — retry de erro transiente e commit são do driver.
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
            COMMIT nas 3 coleções.
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
            Timeout no step 4. Pedido e reserva revertidos — nada parcial no banco.
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
                O pedido (step 2) e a reserva de estoque (step 3) foram revertidos. As coleções{' '}
                <code>pedidos_demo</code>, <code>estoque_demo</code> e <code>pagamentos_demo</code>{' '}
                permanecem exatamente como estavam antes da transação iniciar.
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
                <span style={{ fontWeight: 700, marginLeft: 8, color: count > 0 ? '#00ED64' : 'var(--text-secondary)' }}>
                  {count} doc{count !== 1 ? 's' : ''}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
      {/* ── Medição ──────────────────────────────────────────────────────────
          "O MongoDB reverte automaticamente" é uma frase que um time de dados
          já ouviu sobre o banco que opera hoje. A pergunta seguinte é sempre
          *quanto custa* — e sem número a demo perde para o incumbente. */}
      <div className="card" style={{ padding: '18px 20px' }}>
        <h2 style={{ fontSize: 17, marginBottom: 6 }}>Quanto custa esta garantia</h2>
        <p style={{ fontSize: 13, color: 'var(--text-secondary)', lineHeight: 1.6, marginBottom: 12 }}>
          Transação × a mesma escrita num documento só × sob contenção. Tudo com{' '}
          <code>writeConcern: majority</code>.
        </p>

        <button className="btn btn-primary" onClick={medir} disabled={medindo}>
          {medindo ? <><span className="spinner" /> Medindo… (~1 min)</> : '▶ Medir o custo real'}
        </button>

        {medida && (
          <div style={{ marginTop: 16, display: 'flex', flexDirection: 'column', gap: 12 }}>
            {medida.veredito === 'limitado_pela_rede' && (
              <div className="banner banner-warning">
                <span>⚠️</span>
                <div style={{ fontSize: 13, lineHeight: 1.6 }}>
                  <strong>Enlace dominando a medição</strong> — RTT puro {fmtMs(medida.rede.p50)}.
                  O absoluto aqui é da rede desta máquina; leve a razão ({medida.custo_da_transacao}×),
                  não o número. Verifique VPN/WARP.
                </div>
              </div>
            )}

            <div className="grid-2" style={{ gap: 12 }}>
              <ColunaMedida titulo="Transação multi-documento" dados={medida.multi_documento} destaque
                nota={<>3 coleções, uma transação, <code>majority</code>.{' '}
                  {medida.multi_documento.retries === 0
                    ? 'Nenhum retry de erro transiente'
                    : `${medida.multi_documento.retries} retries de erro transiente`}{' '}
                  em {medida.amostras} execuções.</>} />
              <ColunaMedida titulo="A mesma intenção, um documento só" dados={medida.documento_unico}
                nota="Pedido, pagamento e reserva no mesmo documento. Sem transação." />
            </div>

            <div className="card" style={{ padding: '14px 16px' }}>
              <div style={{ display: 'flex', gap: 26, flexWrap: 'wrap' }}>
                <div>
                  <div style={{ fontFamily: 'var(--font-mono)', fontSize: 22, fontWeight: 700, color: '#f59e0b' }}>
                    {medida.custo_da_transacao}×
                  </div>
                  <div style={{ fontSize: 11.5, color: 'var(--text-secondary)' }}>custo da transação sobre a escrita única</div>
                </div>
                <div>
                  <div style={{ fontFamily: 'var(--font-mono)', fontSize: 22, fontWeight: 700 }}>
                    {medida.viagens_de_rede_estimadas}
                  </div>
                  <div style={{ fontSize: 11.5, color: 'var(--text-secondary)' }}>idas e voltas por transação (p50 ÷ RTT)</div>
                </div>
                <div>
                  <div style={{ fontFamily: 'var(--font-mono)', fontSize: 22, fontWeight: 700 }}>
                    {medida.limite_transacao.segundos != null ? `${medida.limite_transacao.segundos} s` : 'n/d'}
                  </div>
                  <div style={{ fontSize: 11.5, color: 'var(--text-secondary)' }}>
                    teto de vida da transação · {medida.limite_transacao.fonte}
                  </div>
                </div>
                <div>
                  <div style={{ fontFamily: 'var(--font-mono)', fontSize: 22, fontWeight: 700 }}>
                    {fmtMs(medida.rede.p50)}
                  </div>
                  <div style={{ fontSize: 11.5, color: 'var(--text-secondary)' }}>RTT puro ao cluster</div>
                </div>
              </div>
              <p style={{ fontSize: 12.5, color: 'var(--text-secondary)', lineHeight: 1.6, marginTop: 12, marginBottom: 0 }}>
                O custo é <strong>viagem de rede, não motor</strong>. No MongoDB a escrita de um
                documento já é atômica — a transação existe para quando o domínio exige.
              </p>
            </div>

            <ColunaMedida titulo={`Sob contenção — ${medida.contencao.sessoes} sessões na mesma chave`}
              dados={medida.contencao}
              nota={<>
                <strong>{medida.contencao.retries} retries</strong> em {medida.contencao.operacoes} operações,{' '}
                {medida.contencao.abortos} aborto{medida.contencao.abortos !== 1 ? 's' : ''}. O callback
                precisa ser idempotente.
              </>} />
          </div>
        )}
        {bench.error && !medindo && (
          <div className="banner banner-error" style={{ marginTop: 12 }}>
            <span>❌</span><div style={{ fontSize: 13 }}>{String(bench.error)}</div>
          </div>
        )}
      </div>

      <Limites
        titulo="O que a transação cobra"
        itens={[
          <><strong>60 s por padrão</strong> (<code>transactionLifetimeLimitSeconds</code>). Transação longa aborta. Isto não é um lugar para trabalho em lote.</>,
          <>A transação segura um <strong>snapshot no WiredTiger</strong> enquanto vive. Quanto mais longa e maior o conjunto de escrita, mais pressão de cache — e a conta chega para o cluster inteiro, não só para quem abriu a transação.</>,
          <>Conflito de escrita <strong>aborta e exige retry</strong>. A callback API repete erros transientes automaticamente, mas o efeito colateral do seu código não é repetido por ninguém: o callback precisa ser idempotente.</>,
          <>Transação <strong>entre shards</strong> é consideravelmente mais cara: entra commit em duas fases e a latência deixa de ser comparável à do caso não distribuído.</>,
          <>A garantia depende de <code>readConcern</code> e <code>writeConcern</code>. É <code>majority</code> que torna o commit durável a uma eleição — o padrão do driver nem sempre é o que a plateia presume.</>,
          <><strong>Sem savepoint e sem transação aninhada.</strong> É tudo ou nada, num nível só.</>,
          <>O ponto mais desconfortável, e ele é de modelagem: no MongoDB a operação sobre <strong>um documento já é atômica</strong>. Precisar de transação multi-documento em toda escrita costuma indicar um modelo relacional transplantado, não um requisito do negócio. A transação existe para quando o domínio realmente exige — não para compensar modelagem.</>,
        ]}
      />

    </div>
  )
}
