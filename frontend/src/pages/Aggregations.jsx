import React, { useState } from 'react'
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter'
import js from 'react-syntax-highlighter/dist/esm/languages/hljs/javascript'
import { atomOneDark } from 'react-syntax-highlighter/dist/esm/styles/hljs'
import { useApi } from '../hooks/useApi'

SyntaxHighlighter.registerLanguage('javascript', js)

const TABS = [
  { key: 'lookup', icon: '🔗', label: '$lookup',             color: '#00ED64' },
  { key: 'facet',  icon: '📊', label: '$facet',              color: '#06b6d4' },
  { key: 'union',  icon: '🔀', label: '$unionWith',          color: '#a855f7' },
  { key: 'group',  icon: '📐', label: '$group + $addFields', color: '#f97316' },
  { key: 'window', icon: '📈', label: '$setWindowFields',    color: '#00ED64' },
  { key: 'bucket', icon: '🪣', label: '$bucketAuto',         color: '#f97316' },
]

const CODE = {
  lookup: `// Parte de avaliacoes (índice produto_id_1) para garantir
// que o join sempre retorna resultados populados
db.avaliacoes.aggregate([
  { $group: {
      _id: "$produto_id",
      total_reviews: { $sum: 1 },
      avg_nota: { $avg: "$nota" },
      top_reviews: { $push: { usuario: "$usuario", nota: "$nota" } }
  }},
  { $sort: { total_reviews: -1 } },
  { $limit: 5 },
  { $lookup: {
      from: "produtos",
      localField: "_id",
      foreignField: "produto_id",
      as: "produto",
      pipeline: [   // sub-pipeline: só os campos necessários
        { $project: { nome: 1, categoria: 1, preco: 1, marca: 1, _id: 0 } }
      ]
  }},
  { $unwind: "$produto" }
])`,

  facet: `// $match inicial usa índice em_estoque_1 para reduzir
// o working set antes de ramificar em múltiplas pipelines
db.produtos.aggregate([
  { $match: { em_estoque: true } },
  { $facet: {
      por_categoria: [
        { $group: { _id: "$categoria", count: { $sum: 1 }, avg_preco: { $avg: "$preco" } } },
        { $sort: { count: -1 } }, { $limit: 6 }
      ],
      por_faixa_preco: [
        { $bucket: { groupBy: "$preco",
            boundaries: [0, 100, 500, 1000, 5000, 999999] } }
      ],
      top_marcas: [
        { $group: { _id: "$marca", count: { $sum: 1 } } },
        { $sort: { count: -1 } }, { $limit: 5 }
      ]
  }}
])`,

  union: `// Cada lado usa sort + limit sobre um índice — sem $group caro
// Lado 1: reviews recentes (índice data_-1)
// Lado 2: produtos destaque (índice total_av_idx)
db.avaliacoes.aggregate([
  { $sort: { data: -1 } },
  { $match: { nota: { $gte: 4 } } },
  { $limit: 8 },
  { $project: { source: "avaliacoes", tipo: "Review recente",
      descricao: "$titulo", valor: "$nota", categoria: 1 } },
  { $unionWith: {
      coll: "produtos",
      pipeline: [
        { $match: { avaliacao_media: { $gte: 4.5 }, em_estoque: true } },
        { $sort: { total_avaliacoes: -1 } },
        { $limit: 8 },
        { $project: { source: "produtos", tipo: "Produto destaque",
            descricao: "$nome", valor: "$avaliacao_media", categoria: 1 } }
      ]
  }}
])`,

  group: `// $match em_estoque: true usa índice em_estoque_1
// reduz working set antes do $group em 5M docs
db.produtos.aggregate([
  { $match: { em_estoque: true } },
  { $group: {
      _id: "$categoria",
      total_produtos: { $sum: 1 },
      preco_medio:    { $avg: "$preco" },
      preco_max:      { $max: "$preco" },
      em_estoque:     { $sum: { $cond: ["$em_estoque", 1, 0] } }
  }},
  { $addFields: {
      pct_estoque: { $multiply: [
        { $divide: ["$em_estoque", "$total_produtos"] }, 100
      ]}
  }},
  { $sort: { total_produtos: -1 } }
])`,

  window: `// Working set limitado ANTES das janelas:
// match + sort + limit = 100 docs via índice categoria/total_avaliacoes
db.produtos.aggregate([
  { $match: { categoria: "Eletrônicos", em_estoque: true } },
  { $sort: { total_avaliacoes: -1 } },
  { $limit: 100 },
  { $setWindowFields: {
      partitionBy: "$marca",          // janela por marca
      sortBy: { total_avaliacoes: -1 },
      output: {
        rank_marca: { $rank: {} },
        acumulado_avaliacoes: {
          $sum: "$total_avaliacoes",
          window: { documents: ["unbounded", "current"] }
        },
        media_movel_preco: {
          $avg: "$preco",
          window: { documents: [-2, 2] }  // vizinhos ±2
        }
      }
  }}
])`,

  bucket: `// $match em_estoque: true usa índice — reduz working set
// $bucketAuto calcula os limites ideais automaticamente
db.produtos.aggregate([
  { $match: { em_estoque: true } },
  { $bucketAuto: {
      groupBy: "$preco",
      buckets: 6,           // MongoDB decide os limites
      output: {
        count:         { $sum: 1 },
        avg_preco:     { $avg: "$preco" },
        avg_avaliacao: { $avg: "$avaliacao_media" }
      }
  }}
])`,
}

const DESCRIPTIONS = {
  lookup: {
    title: '$lookup com sub-pipeline',
    what: 'Realiza joins entre coleções diretamente no banco. O sub-pipeline embutido projeta apenas os campos necessários do lado joined — sem trazer dados desnecessários para a aplicação.',
    why: 'Partindo de avaliacoes (com índice produto_id_1), o join sempre retorna resultados populados. O sub-pipeline evita trazer documentos completos — só os campos usados chegam ao resultado.',
    index: 'produto_id_1 (avaliacoes) + produto_id_1 (produtos)',
  },
  facet: {
    title: '$facet — múltiplas agregações em paralelo',
    what: 'Executa diversas pipelines de agregação independentes sobre o mesmo conjunto de dados em um único round-trip. Cada facet processa em paralelo e todos os resultados chegam juntos.',
    why: 'Um único comando retorna distribuição por categoria, faixas de preço e top marcas. O $match inicial em em_estoque usa índice para reduzir o working set antes de ramificar.',
    index: 'em_estoque_1',
  },
  union: {
    title: '$unionWith — unir resultados de múltiplas coleções',
    what: 'Concatena resultados de pipelines sobre coleções diferentes em um único retorno — como um UNION SQL, mas dentro de uma agregação e suportando schemas distintos.',
    why: 'Cada lado usa sort + limit sobre um índice (sem $group caro). O resultado unifica reviews recentes de avaliacoes com produtos destaque de produtos em uma única resposta.',
    index: 'data_-1 (avaliacoes) + total_av_idx (produtos)',
  },
  group: {
    title: '$group + $addFields — métricas e campos derivados',
    what: '$group agrupa documentos e calcula acumuladores (soma, média, min, max). $addFields injeta campos calculados sobre os grupos — percentuais, razões, labels condicionais.',
    why: 'O $match inicial em em_estoque usa índice para reduzir o working set antes do $group. Campos derivados como pct_estoque chegam prontos — sem cálculos na aplicação.',
    index: 'em_estoque_1',
  },
  window: {
    title: '$setWindowFields — Window Functions',
    what: 'Calcula rank dentro de partições, somas acumuladas e médias móveis — o equivalente ao OVER (PARTITION BY) do SQL — sem agrupar nem remover linhas do resultado.',
    why: 'Working set de 100 docs criado com match + sort + limit via índice ANTES das janelas. As janelas rodam sobre um conjunto mínimo, não sobre os 5M documentos completos.',
    index: 'categoria_1_avaliacao_media_-1_preco_1 → limit 100 docs',
  },
  bucket: {
    title: '$bucketAuto — faixas automáticas de distribuição',
    what: 'Distribui documentos em N grupos de tamanho aproximadamente igual, calculando automaticamente os limites de cada faixa a partir dos dados reais — sem definir boundaries manualmente.',
    why: 'Ideal para histogramas onde os limites ideais não são conhecidos. O $match inicial usa índice para reduzir o working set antes da distribuição.',
    index: 'em_estoque_1',
  },
}

/* ── Result renderers ─────────────────────────────────────────────────── */
function LookupResults({ data }) {
  const rows = data?.results || []
  if (!rows.length) return null
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      {rows.map((p, i) => (
        <div key={i} className="card" style={{ padding: '14px 16px' }}>
          <div style={{ display: 'flex', gap: 10, alignItems: 'center', marginBottom: 10 }}>
            <span className="badge badge-green">{p.categoria}</span>
            <span className="badge badge-gray">{p.marca}</span>
            <strong style={{ fontSize: 13 }}>{p.nome}</strong>
            <span style={{ marginLeft: 'auto', fontSize: 12, color: 'var(--text-secondary)' }}>
              R$ {p.preco?.toFixed(2)} · ⭐ {p.avg_nota} ({p.total_reviews?.toLocaleString()} reviews)
            </span>
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
            {(p.top_reviews || []).map((a, j) => (
              <div key={j} style={{ background: 'var(--bg-subtle)', border: '1px solid var(--border-color)', borderRadius: 6, padding: '6px 10px', fontSize: 12 }}>
                <span style={{ fontWeight: 600 }}>⭐ {a.nota}</span>
                <span style={{ color: 'var(--text-secondary)', marginLeft: 6 }}>{a.usuario}</span>
              </div>
            ))}
          </div>
        </div>
      ))}
    </div>
  )
}

function FacetResults({ data }) {
  if (!data?.data) return null
  const d = data.data
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <div className="grid-2">
        <div className="card" style={{ padding: '14px 16px' }}>
          <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 10 }}>Por Categoria</div>
          {(d.por_categoria || []).map((c, i) => (
            <div key={i} style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, padding: '4px 0', borderBottom: '1px solid var(--border-color)' }}>
              <span>{c._id}</span>
              <span><strong>{c.count?.toLocaleString()}</strong> · R$ {c.avg_preco?.toFixed(0)}</span>
            </div>
          ))}
        </div>
        <div className="card" style={{ padding: '14px 16px' }}>
          <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 10 }}>Top Marcas</div>
          {(d.top_marcas || []).map((m, i) => (
            <div key={i} style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, padding: '4px 0', borderBottom: '1px solid var(--border-color)' }}>
              <span>{m._id}</span>
              <span><strong>{m.count?.toLocaleString()}</strong></span>
            </div>
          ))}
        </div>
      </div>
      <div className="card" style={{ padding: '14px 16px' }}>
        <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 10 }}>Por Faixa de Preço</div>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
          {(d.por_faixa_preco || []).map((b, i) => (
            <div key={i} style={{ background: 'var(--bg-subtle)', border: '1px solid var(--border-color)', borderRadius: 6, padding: '8px 14px', fontSize: 12, textAlign: 'center' }}>
              <div style={{ fontWeight: 700, fontSize: 15 }}>{b.count?.toLocaleString()}</div>
              <div style={{ color: 'var(--text-secondary)', marginTop: 2 }}>
                R$ {(b._id === 'Outro' ? '5k' : b._id?.toLocaleString())}+
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

function UnionResults({ data }) {
  const rows = data?.results || []
  if (!rows.length) return null
  const avaliacoes = rows.filter(r => r.source === 'avaliacoes')
  const produtos   = rows.filter(r => r.source === 'produtos')
  return (
    <div className="grid-2">
      {[{ label: '📝 Reviews recentes', rows: avaliacoes, color: '#a855f7' },
        { label: '⭐ Produtos destaque', rows: produtos,   color: '#00ED64' }].map(({ label, rows: side, color }) => (
        <div key={label} className="card" style={{ padding: '14px 16px' }}>
          <div style={{ fontWeight: 600, fontSize: 13, marginBottom: 10, color }}>{label}</div>
          {side.map((r, i) => (
            <div key={i} style={{ padding: '6px 0', borderBottom: '1px solid var(--border-color)', fontSize: 12 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span style={{ maxWidth: 180, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{r.descricao}</span>
                <span className="badge badge-green" style={{ fontSize: 11 }}>{'⭐'.repeat(Math.min(Math.round(r.valor), 5))} {r.valor}</span>
              </div>
              <div style={{ color: 'var(--text-secondary)', marginTop: 2 }}>{r.categoria} · {r.usuario !== '—' ? r.usuario : ''}</div>
            </div>
          ))}
        </div>
      ))}
    </div>
  )
}

const TABLE_COLS = {
  group: [
    { key: '_id',             label: 'Categoria' },
    { key: 'total_produtos',  label: 'Produtos',    render: r => r.total_produtos?.toLocaleString() },
    { key: 'preco_medio',     label: 'Preço Médio', render: r => `R$ ${r.preco_medio?.toFixed(2)}` },
    { key: 'preco_max',       label: 'Preço Máx',   render: r => `R$ ${r.preco_max?.toFixed(0)}` },
    { key: 'pct_estoque',     label: '% Estoque',   render: r => `${r.pct_estoque?.toFixed(1)}%` },
    { key: 'avaliacao_media', label: 'Avg ⭐',      render: r => r.avaliacao_media?.toFixed(2) },
  ],
  window: [
    { key: 'nome',                label: 'Produto',    render: r => <span style={{ maxWidth: 160, display: 'block', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{r.nome}</span> },
    { key: 'marca',               label: 'Marca' },
    { key: 'rank_marca',          label: 'Rank',       render: r => <span className="badge badge-blue">#{r.rank_marca}</span> },
    { key: 'total_avaliacoes',    label: 'Reviews',    render: r => r.total_avaliacoes?.toLocaleString() },
    { key: 'media_movel_preco',   label: 'MM Preço',   render: r => `R$ ${r.media_movel_preco?.toFixed(0)}` },
    { key: 'acumulado_avaliacoes',label: 'Acum.',      render: r => r.acumulado_avaliacoes?.toLocaleString() },
  ],
  bucket: [
    { key: 'faixa',        label: 'Faixa de Preço' },
    { key: 'count',        label: 'Produtos',    render: r => r.count?.toLocaleString() },
    { key: 'avg_preco',    label: 'Preço Médio', render: r => `R$ ${r.avg_preco?.toFixed(2)}` },
    { key: 'avg_avaliacao',label: '⭐ Média',    render: r => r.avg_avaliacao?.toFixed(2) },
  ],
}

function TableResults({ data, cols }) {
  const rows = data?.results || []
  if (!rows.length) return null
  return (
    <div style={{ overflowX: 'auto' }}>
      <table className="lg-table">
        <thead><tr>{cols.map(c => <th key={c.key}>{c.label}</th>)}</tr></thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>
              {cols.map(c => <td key={c.key}>{c.render ? c.render(r) : (r[c.key] ?? '—')}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

/* ── Main ──────────────────────────────────────────────────────────────── */
export default function Aggregations() {
  const { call, loading } = useApi()
  const [tab,      setTab]      = useState('lookup')
  const [results,  setResults]  = useState({})
  const [showCode, setShowCode] = useState({})

  const ENDPOINTS = {
    lookup: '/aggregations/lookup',
    facet:  '/aggregations/facet',
    union:  '/aggregations/union-with',
    group:  '/aggregations/group-advanced',
    window: '/aggregations/window-functions',
    bucket: '/aggregations/bucket-auto',
  }

  const run = async () => {
    const data = await call(ENDPOINTS[tab])
    if (data) setResults(r => ({ ...r, [tab]: data }))
  }

  const desc     = DESCRIPTIONS[tab]
  const res      = results[tab]
  const activeTab = TABS.find(t => t.key === tab)

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* Tabs */}
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
        {TABS.map(t => (
          <button key={t.key} onClick={() => setTab(t.key)} className="tag" style={{
            borderColor: tab === t.key ? t.color : undefined,
            color:       tab === t.key ? t.color : undefined,
            fontWeight:  tab === t.key ? 700 : 400,
            background:  tab === t.key ? `${t.color}12` : undefined,
          }}>
            {t.icon} {t.label}
          </button>
        ))}
      </div>

      {/* Feature card */}
      <div className="card" style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
        <div>
          <h2 style={{ fontSize: 17, fontWeight: 700, color: 'var(--text-primary)', marginBottom: 6 }}>
            {desc.title}
          </h2>
          <p style={{ fontSize: 13.5, color: 'var(--text-secondary)', lineHeight: 1.65, marginBottom: 8 }}>
            {desc.what}
          </p>
          <div className="banner banner-success">
            <span>💡</span>
            <div><strong>Por que usar:</strong> {desc.why}</div>
          </div>
        </div>

        <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
          <button className="btn btn-primary" onClick={run} disabled={loading}>
            {loading ? <><span className="spinner" /> Executando...</> : '▶ Executar'}
          </button>
          <button className="btn btn-default btn-sm" onClick={() => setShowCode(s => ({ ...s, [tab]: !s[tab] }))}>
            {showCode[tab] ? '▲ Ocultar código' : '▼ Ver código'}
          </button>
          <span style={{ marginLeft: 'auto', fontSize: 12, color: 'var(--text-secondary)' }}>
            índice: <code>{desc.index}</code>
          </span>
        </div>

        {showCode[tab] && (
          <SyntaxHighlighter language="javascript" style={atomOneDark}
            customStyle={{ borderRadius: 6, fontSize: 12, margin: 0 }}>
            {CODE[tab]}
          </SyntaxHighlighter>
        )}
      </div>

      {/* Results */}
      {res && (
        <div>
          {tab === 'lookup' && <LookupResults data={res} />}
          {tab === 'facet'  && <FacetResults  data={res} />}
          {tab === 'union'  && <UnionResults  data={res} />}
          {(tab === 'group' || tab === 'window' || tab === 'bucket') && (
            <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
              <TableResults data={res} cols={TABLE_COLS[tab]} />
            </div>
          )}
        </div>
      )}
    </div>
  )
}
