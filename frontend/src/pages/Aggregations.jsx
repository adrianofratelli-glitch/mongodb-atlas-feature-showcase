import React, { useState } from 'react'
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter'
import js from 'react-syntax-highlighter/dist/esm/languages/hljs/javascript'
import { atomOneDark } from 'react-syntax-highlighter/dist/esm/styles/hljs'
import { useApi } from '../hooks/useApi'
import Limites from '../components/Limites'

SyntaxHighlighter.registerLanguage('javascript', js)

const TABS = [
  { key: 'lookup', icon: '🔗', label: 'Produtos + avaliações', operator: '$lookup',          source: 'avaliações', output: 'top produtos enriquecidos', color: '#00ED64' },
  { key: 'facet',  icon: '📊', label: 'Dashboard em uma query', operator: '$facet',           source: 'produtos em estoque', output: '3 recortes simultâneos', color: '#06b6d4' },
  { key: 'union',  icon: '🔀', label: 'Feed combinado',         operator: '$unionWith',       source: 'reviews + produtos', output: 'feed unificado', color: '#a855f7' },
  { key: 'group',  icon: '📐', label: 'Métricas por categoria', operator: '$group',           source: 'produtos em estoque', output: 'KPIs por categoria', color: '#f97316' },
  { key: 'window', icon: '📈', label: 'Ranking e média móvel',  operator: '$setWindowFields', source: 'top eletrônicos', output: 'ranking por marca', color: '#00ED64' },
  { key: 'bucket', icon: '🪣', label: 'Faixas de preço',        operator: '$bucketAuto',      source: 'produtos em estoque', output: 'distribuição automática', color: '#f97316' },
]

const CODE = {
  lookup: `// O $group varre avaliacoes; o índice produto_id_1 (produtos)
// é usado pelo $lookup para o join ser barato
db.avaliacoes.aggregate([
  { $group: {
      _id: "$produto_id",
      total_reviews: { $sum: 1 },
      avg_nota: { $avg: "$nota" },
      top_reviews: { $topN: {           // 5.2+: só as 3 melhores
        output: { usuario: "$usuario", nota: "$nota" },
        sortBy: { nota: -1 }, n: 3
      }}
  }},
  { $sort: { total_reviews: -1 } },
  { $limit: 5 },
  { $lookup: {
      from: "produtos",
      localField: "_id",
      foreignField: "produto_id",   // ← usa o índice produto_id_1
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
// Lado 1: reviews recentes (índice recent_nota_idx)
// Lado 2: produtos destaque (índice destaque_idx)
db.avaliacoes.aggregate([
  { $sort: { data: -1 } },
  { $match: { nota: { $gte: 4 } } },
  { $limit: 8 },
  { $lookup: { from: "produtos", localField: "produto_id",
      foreignField: "produto_id", as: "produto_ref",
      pipeline: [{ $project: { categoria: 1, _id: 0 } }] } },
  { $project: { source: "avaliacoes", tipo: "Review recente",
      descricao: "$titulo", valor: "$nota",
      categoria: { $ifNull: ["$categoria", { $arrayElemAt: ["$produto_ref.categoria", 0] }] } } },
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
// reduz working set antes do $group sobre a coleção inteira
db.produtos.aggregate([
  { $match: { em_estoque: true } },
  { $group: {
      _id: "$categoria",
      total_produtos: { $sum: 1 },
      preco_medio:    { $avg: "$preco" },
      preco_max:      { $max: "$preco" },
      preco_min:      { $min: "$preco" }
  }},
  { $addFields: {   // campo derivado, calculado no banco
      amplitude_preco: { $subtract: ["$preco_max", "$preco_min"] }
  }},
  { $sort: { total_produtos: -1 } }
])`,

  window: `// Working set limitado ANTES das janelas:
// match + sort + limit = 100 docs via índice cat_total_av_idx
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
    linha: 'Join entre coleções dentro do banco — sem o N+1 na aplicação, sem ETL para juntar depois.',
    index: 'produto_id_1 (produtos) — usado pelo $lookup',
  },
  facet: {
    title: '$facet — múltiplas agregações em um único round-trip',
    linha: 'Um dashboard inteiro em um comando: N recortes numa passada, sobre o mesmo instante do dado.',
    index: 'em_estoque_1',
  },
  union: {
    title: '$unionWith — unir resultados de múltiplas coleções',
    linha: 'Coleções com formatos diferentes num feed só, ordenado e paginado pelo banco.',
    index: 'data_-1 (avaliacoes) + total_av_idx (produtos)',
  },
  group: {
    title: '$group + $addFields — métricas e campos derivados',
    linha: 'Métrica calculada sobre o dado corrente — no lugar da tabela de resumo e do job que a mantém.',
    index: 'em_estoque_1',
  },
  window: {
    title: '$setWindowFields — Window Functions',
    linha: 'Rank, soma acumulada e média móvel sem sair do banco operacional — o OVER (PARTITION BY) do SQL.',
    index: 'cat_total_av_idx (categoria + total_avaliacoes) → limit 100 docs',
  },
  bucket: {
    title: '$bucketAuto — faixas automáticas de distribuição',
    linha: 'Faixas calculadas a partir da distribuição real, não fixadas no código.',
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
    { key: 'amplitude_preco', label: 'Amplitude',   render: r => `R$ ${r.amplitude_preco?.toFixed(0)}` },
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
          <button key={t.key} onClick={() => setTab(t.key)} className="tag"
            aria-pressed={tab === t.key} style={{
            borderColor: tab === t.key ? t.color : undefined,
            color:       tab === t.key ? t.color : undefined,
            fontWeight:  tab === t.key ? 700 : 400,
            background:  tab === t.key ? `${t.color}12` : undefined,
          }}>
            <span>{t.icon} {t.label}</span>
            <code className="agg-tab-op">{t.operator}</code>
          </button>
        ))}
      </div>

      <div className="agg-flow" aria-label="Fluxo da agregação selecionada">
        <div><span className="agg-step">1</span><strong>Fonte</strong><small>{activeTab.source}</small></div>
        <span className="agg-arrow" aria-hidden="true">→</span>
        <div><span className="agg-step">2</span><strong>Pipeline</strong><code>{activeTab.operator}</code></div>
        <span className="agg-arrow" aria-hidden="true">→</span>
        <div><span className="agg-step">3</span><strong>Resultado</strong><small>{activeTab.output}</small></div>
      </div>

      {/* Uma linha, o código executável e o botão. A explicação é narrada na
          call; a tela carrega o que a narração não carrega — a query e o
          resultado. O código deixou de ficar atrás de "Ver código": mostrar
          quão pouco se escreve É o argumento. */}
      <div className="card" style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
        <div>
          <h2 style={{ fontSize: 17, fontWeight: 700, marginBottom: 4 }}>{desc.title}</h2>
          <p style={{ fontSize: 13.5, color: 'var(--text-secondary)', margin: 0 }}>{desc.linha}</p>
        </div>

        <SyntaxHighlighter language="javascript" style={atomOneDark}
          customStyle={{ borderRadius: 6, fontSize: 12, margin: 0 }}>
          {CODE[tab]}
        </SyntaxHighlighter>

        <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
          <button className="btn btn-primary" onClick={run} disabled={loading}>
            {loading ? <><span className="spinner" /> Executando...</> : '▶ Executar'}
          </button>
          <span style={{ marginLeft: 'auto', fontSize: 12, color: 'var(--text-secondary)' }}>
            índice: <code>{desc.index}</code>
          </span>
        </div>
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
      {!res && (
        <div className="agg-empty-state">
          <span>▷</span>
          <div><strong>Execute para ver o resultado do cluster</strong></div>
        </div>
      )}
      <Limites
        titulo="Onde perde para SQL maduro"
        itens={[
          <><strong>100 MB por stage.</strong> Acima disso é preciso <code>allowDiskUse</code>, e aí a stage passa a fazer IO. O limite é por stage, então um <code>$group</code> de alta cardinalidade estoura antes do que a intuição sugere.</>,
          <><strong>16 MB por documento</strong>, na entrada, no meio e na saída do pipeline. Um <code>$group</code> que acumula array cresce até bater nisso.</>,
          <><code>$lookup</code> <strong>não é um join com otimizador</strong>: não há escolha entre hash join e merge join. Sem índice adequado na coleção estrangeira, ele degrada muito além do que o mesmo join degradaria num RDBMS.</>,
          <>O planner é <strong>heurístico e baseado em plan cache</strong>, sem as estatísticas de cardinalidade que um otimizador de custo maduro usa. Menos previsível em consulta ad-hoc complexa.</>,
          <>Sem CTE recursiva. Hierarquia é <code>$graphLookup</code>, com o mesmo teto de memória e sem o poder de expressão de uma recursiva.</>,
          <>Em cluster sharded, o merge das partições acontece num nó só — pipeline pesado concentra trabalho no shard primário ou no mongos.</>,
          <><strong>Não é SQL.</strong> O conhecimento, as queries e as ferramentas de BI do time não portam direto. Atlas SQL e o BI Connector existem, mas são outra superfície, com limites próprios — e não são o que esta página demonstra.</>,
        ]}
      />

    </div>
  )
}
