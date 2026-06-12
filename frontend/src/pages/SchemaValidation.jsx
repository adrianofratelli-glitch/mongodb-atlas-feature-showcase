import React, { useState, useEffect } from 'react'
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter'
import js from 'react-syntax-highlighter/dist/esm/languages/hljs/javascript'
import { atomOneDark } from 'react-syntax-highlighter/dist/esm/styles/hljs'
import { useApi } from '../hooks/useApi'

SyntaxHighlighter.registerLanguage('javascript', js)

const SCHEMA_CODE = `db.runCommand({
  collMod: "schema_demo",
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["nome", "preco", "categoria", "em_estoque"],
      properties: {
        nome:       { bsonType: "string",  minLength: 2 },
        preco:      { bsonType: "number",  minimum: 0 },
        categoria:  { bsonType: "string",
          enum: ["Eletrônicos","Vestuário","Alimentos","Esportes","Casa"] },
        em_estoque: { bsonType: "bool" },
        sku:        { bsonType: "string",  pattern: "^[A-Z]{2}-[0-9]{4}$" }
      }
    }
  },
  validationLevel: "strict",
  validationAction: "error"  // rejeita na camada do banco
})`

const INVALID_SCENARIOS = [
  { key: 'preco_negativo',     label: 'Preço negativo',           desc: 'preco: -50 → viola minimum: 0' },
  { key: 'categoria_invalida', label: 'Categoria inválida',       desc: 'categoria: "Outro" → viola enum' },
  { key: 'campo_faltando',     label: 'Campo obrigatório ausente', desc: 'sem em_estoque → viola required' },
  { key: 'sku_formato_errado', label: 'SKU com formato errado',   desc: 'sku: "abc123" → viola pattern' },
]

const STEP_STATUS = {
  idle:      { color: 'var(--text-secondary)', bg: 'var(--bg-subtle)' },
  active:    { color: '#06b6d4',           bg: 'rgba(6,182,212,.08)' },
  done:      { color: 'var(--accent)', bg: 'rgba(0,237,100,.08)' },
}

function StepBadge({ n, status }) {
  const s = STEP_STATUS[status] || STEP_STATUS.idle
  return (
    <div style={{ width: 28, height: 28, borderRadius: '50%', background: s.bg, border: `2px solid ${s.color}`, display: 'flex', alignItems: 'center', justifyContent: 'center', fontWeight: 700, fontSize: 13, color: s.color, flexShrink: 0 }}>
      {status === 'done' ? '✓' : n}
    </div>
  )
}

export default function SchemaValidation() {
  const { call, loading } = useApi()
  const [status, setStatus]         = useState(null)
  const [step, setStep]             = useState(0)  // 0=not started, 1,2,3,4=done
  const [stepResults, setStepResults] = useState({})
  const [activeScenario, setActiveScenario] = useState(null)
  const [docs, setDocs]             = useState(null)

  const fetchStatus = async () => {
    const d = await call('/schema/status')
    if (d) setStatus(d)
    return d
  }

  useEffect(() => { fetchStatus() }, [])

  const fetchDocs = async () => {
    const d = await call('/schema/documents')
    if (d) setDocs(d)
  }

  const runStep = async (n, fn) => {
    const result = await fn()
    if (result) {
      setStepResults(r => ({ ...r, [n]: result }))
      setStep(n)
      await fetchStatus()
      await fetchDocs()
    }
  }

  const reset = async () => {
    await call('/schema/reset', { method: 'DELETE' })
    setStep(0); setStepResults({}); setDocs(null); setActiveScenario(null)
    await fetchStatus()
  }

  const STEPS = [
    {
      n: 1, title: 'Criar coleção sem schema',
      desc: 'Cria a coleção schema_demo sem nenhuma validação ativa — o comportamento padrão de qualquer coleção.',
      action: 'Criar Coleção',
      fn: () => call('/schema/step1-create-collection', { method: 'POST' }),
    },
    {
      n: 2, title: 'Inserir documentos inválidos (sem schema)',
      desc: 'Insere 5 documentos — incluindo preço negativo, categoria inválida, campo faltando e SKU errado. SEM schema, todos são aceitos.',
      action: 'Inserir Documentos Inválidos',
      fn: () => call('/schema/step2-insert-without-schema', { method: 'POST' }),
    },
    {
      n: 3, title: 'Ativar Schema Validation',
      desc: 'Aplica a regra $jsonSchema na coleção existente via collMod. Os documentos inválidos já inseridos permanecem, mas novas inserções inválidas serão bloqueadas.',
      action: 'Ativar Schema',
      fn: () => call('/schema/step3-activate-schema', { method: 'POST' }),
    },
  ]

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
      <div className="banner banner-info">
        <span>ℹ️</span>
        <div>
          MongoDB Atlas suporta <strong>JSON Schema validation completo</strong> — enum, regex, ranges, campos required — aplicado direto na camada do banco, sem mudar o código da aplicação. Siga os passos abaixo para ver ao vivo.
        </div>
      </div>

      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        {status && (
          <div style={{ display: 'flex', gap: 8 }}>
            <span className={`badge ${status.schema_active ? 'badge-green' : 'badge-gray'}`}>
              Schema: {status.schema_active ? 'ATIVO' : 'inativo'}
            </span>
            <span className="badge badge-blue">{status.document_count} documentos</span>
          </div>
        )}
        <button className="btn btn-sm btn-danger" onClick={reset}>↺ Resetar demo</button>
      </div>

      {/* Steps 1-3 */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
        {STEPS.map(s => {
          const stepStatus = step >= s.n ? 'done' : step === s.n - 1 ? 'active' : 'idle'
          const res = stepResults[s.n]
          return (
            <div key={s.n} className="card" style={{ borderColor: stepStatus === 'done' ? 'rgba(0,237,100,.3)' : stepStatus === 'active' ? '#06b6d4' : 'var(--border-subtle)' }}>
              <div style={{ display: 'flex', gap: 12, alignItems: 'flex-start' }}>
                <StepBadge n={s.n} status={stepStatus} />
                <div style={{ flex: 1 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
                    <strong style={{ fontSize: 14 }}>{s.title}</strong>
                    {step < s.n && (
                      <button className="btn btn-sm btn-primary" onClick={() => runStep(s.n, s.fn)} disabled={loading || step < s.n - 1}>
                        {loading ? <><span className="spinner" /></> : s.action}
                      </button>
                    )}
                  </div>
                  <p style={{ color: 'var(--text-secondary)', fontSize: 13 }}>{s.desc}</p>
                  {res && (
                    <div style={{ marginTop: 10, padding: '8px 12px', background: 'var(--bg-subtle)', borderRadius: 6, fontSize: 13 }}>
                      {s.n === 2 && <><strong>{res.inserted}</strong> documentos inseridos. <span style={{ color: 'var(--text-secondary)' }}>{res.message}</span></>}
                      {s.n === 3 && <span style={{ color: 'var(--accent)' }}>{res.message} <strong>{res.note}</strong></span>}
                      {s.n === 1 && <span style={{ color: 'var(--accent)' }}>{res.message}</span>}
                    </div>
                  )}
                </div>
              </div>
            </div>
          )
        })}
      </div>

      {/* Step 4 — só aparece após step 3 */}
      {step >= 3 && (
        <div className="card" style={{ borderColor: '#06b6d4', borderWidth: 2 }}>
          <div style={{ display: 'flex', gap: 12, alignItems: 'flex-start' }}>
            <StepBadge n={4} status={step >= 4 ? 'done' : 'active'} />
            <div style={{ flex: 1 }}>
              <strong style={{ fontSize: 14 }}>Tentar inserir o mesmo documento inválido (com schema ativo)</strong>
              <p style={{ color: 'var(--text-secondary)', fontSize: 13, margin: '6px 0 12px' }}>
                Agora com o schema ativo, os mesmos documentos são <strong>rejeitados pelo banco</strong> — sem nenhuma mudança no código da aplicação.
              </p>

              <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', marginBottom: 12 }}>
                {INVALID_SCENARIOS.map(sc => (
                  <button key={sc.key} disabled={loading}
                    className="tag"
                    style={activeScenario === sc.key ? { borderColor: '#ff6960', color: '#ff6960', background: 'rgba(255,105,96,.08)' } : {}}
                    onClick={async () => {
                      setActiveScenario(sc.key)
                      const d = await call(`/schema/step4-insert-invalid?scenario=${sc.key}`, { method: 'POST' })
                      if (d) { setStepResults(r => ({ ...r, 4: { ...d, scenario: sc } })); setStep(4) }
                    }}>
                    {sc.label}
                  </button>
                ))}
              </div>

              {stepResults[4] && (
                <div style={{ padding: '12px 14px', background: 'rgba(255,105,96,.08)', borderRadius: 8, border: '1px solid rgba(255,105,96,.35)', fontSize: 13 }}>
                  <div style={{ fontWeight: 700, color: '#ff6960', marginBottom: 6 }}>
                    ❌ Rejeitado pelo banco — {stepResults[4].scenario?.label}
                  </div>
                  <div style={{ marginBottom: 6 }}><strong>Documento tentado:</strong> <code>{JSON.stringify(stepResults[4].document_attempted)}</code></div>
                  <div style={{ color: 'var(--text-secondary)' }}><strong>Erro:</strong> {stepResults[4].error_message}</div>
                  <div style={{ marginTop: 8, color: 'var(--accent)', fontSize: 12 }}>✅ {stepResults[4].note}</div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Documentos na coleção */}
      {docs && (
        <div className="card">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
            <strong style={{ fontSize: 15 }}>
              Documentos na coleção
              <span className={`badge ${docs.schema_active ? 'badge-green' : 'badge-gray'}`} style={{ marginLeft: 10 }}>
                Schema {docs.schema_active ? 'ATIVO' : 'inativo'}
              </span>
            </strong>
            <button className="btn btn-sm btn-default" onClick={fetchDocs}>↻ Atualizar</button>
          </div>
          {docs.documents.length === 0
            ? <p style={{ color: 'var(--text-secondary)' }}>Nenhum documento. Execute o passo 2 para inserir.</p>
            : docs.documents.map((d, i) => (
                <div key={i} style={{ padding: '8px 12px', background: 'var(--bg-subtle)', borderRadius: 6, marginBottom: 6, fontSize: 12, fontFamily: 'monospace', wordBreak: 'break-all' }}>
                  {JSON.stringify(d)}
                </div>
              ))
          }
        </div>
      )}

      {/* Schema usado */}
      <div className="card">
        <strong style={{ fontSize: 14, display: 'block', marginBottom: 12 }}>Schema JSON aplicado no Passo 3</strong>
        <SyntaxHighlighter language="javascript" style={atomOneDark} customStyle={{ borderRadius: 8, fontSize: 12 }}>
          {SCHEMA_CODE}
        </SyntaxHighlighter>
        <div className="banner banner-info" style={{ marginTop: 12 }}>
          <span>💡</span>
          <div style={{ fontSize: 13 }}>
            O <code>collMod</code> aplica o validador em uma coleção <strong>já existente</strong>, sem downtime — as regras <code>enum</code>, <code>pattern</code> e <code>minimum</code> passam a valer imediatamente para novas escritas.
          </div>
        </div>
      </div>
    </div>
  )
}
