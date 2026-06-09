import React, { useState, useEffect } from 'react'
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter'
import js from 'react-syntax-highlighter/dist/esm/languages/hljs/javascript'
import { atomOneDark } from 'react-syntax-highlighter/dist/esm/styles/hljs'
import { useApi } from '../hooks/useApi'

SyntaxHighlighter.registerLanguage('javascript', js)

const SCENARIOS = [
  {
    key: 'simples', title: 'Índice Simples', fields: ['categoria'],
    description: 'Índice em campo único para filtros de categoria — base para a maioria das queries',
    query: `db.produtos.createIndex(\n  { categoria: 1 },\n  { background: true }  // não bloqueia leituras\n)`,
  },
  {
    key: 'composto', title: 'Índice Composto', fields: ['categoria', '-preco'],
    description: 'Cobre queries combinadas (categoria + preço desc) — elimina COLLSCAN',
    query: `db.produtos.createIndex(\n  { categoria: 1, preco: -1 },\n  { background: true }\n)`,
  },
  {
    key: 'parcial', title: 'Índice Parcial', fields: ['preco'],
    description: 'Apenas sobre produtos em estoque — menor footprint na RAM e disco',
    query: `db.produtos.createIndex(\n  { preco: 1 },\n  {\n    background: true,\n    partialFilterExpression: { em_estoque: true }\n  }\n)`,
    partial_filter: { em_estoque: true },
  },
]

export default function Reindexacao() {
  const { call, loading } = useApi()
  const [indexes, setIndexes] = useState([])
  const [result, setResult] = useState(null)
  const [selected, setSelected] = useState(null)

  const fetchIndexes = async () => {
    const data = await call('/reindexacao/indexes')
    if (data) setIndexes(data.indexes)
  }

  useEffect(() => { fetchIndexes() }, [])

  const handleCreate = async (scenario) => {
    setSelected(scenario.key)
    setResult(null)
    const params = new URLSearchParams()
    scenario.fields.forEach(f => params.append('fields', f))
    const data = await call(`/reindexacao/create?${params.toString()}`, { method: 'POST' })
    setResult(data)
    await fetchIndexes()
  }

  const handleDrop = async (name) => {
    await call(`/reindexacao/drop/${name}`, { method: 'DELETE' })
    await fetchIndexes()
    setResult(null)
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
      <div className="banner banner-info">
        <span>ℹ️</span>
        <div>
          <strong>Rolling Index Build</strong> — MongoDB Atlas cria e remove índices sem bloquear leituras ou escritas.
          O processo ocorre em background, nó a nó, mantendo o cluster totalmente operacional durante toda a operação.
        </div>
      </div>

      <div className="grid-auto">
        {SCENARIOS.map(s => (
          <div key={s.key} className="card" style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
              <strong style={{ fontSize: 15 }}>{s.title}</strong>
              <span className="badge badge-green">Online Build</span>
            </div>
            <p style={{ color: 'var(--text-secondary)', fontSize: 13 }}>{s.description}</p>
            <SyntaxHighlighter language="javascript" style={atomOneDark} customStyle={{ borderRadius: 6, fontSize: 11.5, margin: 0 }}>
              {s.query}
            </SyntaxHighlighter>
            <button className="btn btn-primary" style={{ width: '100%', justifyContent: 'center' }}
              onClick={() => handleCreate(s)} disabled={loading && selected === s.key}>
              {loading && selected === s.key ? <><span className="spinner" /> Criando...</> : 'Criar Índice'}
            </button>
          </div>
        ))}
      </div>

      {result && (
        <div className="card" style={{
          background: result.error ? '#FEF3F0' : '#E3FCF7',
          borderColor: result.error ? '#FCCBC5' : '#71F6BA',
        }}>
          {result.error
            ? <div>❌ {result.error}</div>
            : <>
                <div>✅ Índice criado: <strong>{result.index_name}</strong></div>
                <div style={{ marginTop: 4 }}>⏱ Tempo: <strong>{result.elapsed_seconds}s</strong></div>
                <div style={{ color: 'var(--text-secondary)', fontSize: 12, marginTop: 6 }}>{result.note}</div>
              </>
          }
        </div>
      )}

      <div className="card">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
          <strong style={{ fontSize: 15 }}>Índices Ativos — <code style={{ fontSize: 13 }}>produtos</code> (5M docs)</strong>
          <button className="btn btn-sm btn-default" onClick={fetchIndexes}>↻ Atualizar</button>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {indexes.map(idx => (
            <div key={idx.name} className="result-row" style={{ justifyContent: 'space-between' }}>
              <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                <span className={`badge ${idx.name === '_id_' ? 'badge-blue' : 'badge-green'}`}>
                  {idx.name === '_id_' ? 'Sistema' : 'Custom'}
                </span>
                <code style={{ fontSize: 12 }}>{idx.name}</code>
                <span style={{ color: 'var(--text-secondary)', fontSize: 12 }}>→ {JSON.stringify(idx.key)}</span>
              </div>
              {idx.name !== '_id_' && (
                <button className="btn btn-xs btn-danger" onClick={() => handleDrop(idx.name)}>Remover</button>
              )}
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
