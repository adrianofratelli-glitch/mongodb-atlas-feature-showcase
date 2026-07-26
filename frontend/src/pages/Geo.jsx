import React, { useEffect, useMemo, useState } from 'react'
import { useApi } from '../hooks/useApi'
import QueryBlock from '../components/QueryBlock'

// Bounding box do Brasil. A projeção é linear de propósito: a apresentação é em
// auditório com rede ruim, então o mapa é SVG local, sem tiles e sem
// dependência nova no frontend. Feio e offline vence bonito e travado.
const BBOX = { oeste: -74.2, leste: -33.8, norte: 5.6, sul: -34.2 }
const CATEGORIAS = ['alimentação', 'combustível', 'farmácia', 'vestuário', 'serviços']

function projetar([lng, lat]) {
  const x = ((lng - BBOX.oeste) / (BBOX.leste - BBOX.oeste)) * 100
  const y = ((BBOX.norte - lat) / (BBOX.norte - BBOX.sul)) * 100
  return [x, y]
}

function MiniMapa({ pontos = [], linha = null, altura = 260 }) {
  const [hover, setHover] = useState(null)
  return (
    <div className="geo-mapa" style={{ height: altura }}>
      <svg viewBox="0 0 100 100" preserveAspectRatio="none" role="img"
        aria-label="Mapa esquemático do Brasil com os pontos da consulta">
        {/* Grade de referência — o mapa não tem contorno de país por decisão:
            nenhum GeoJSON externo, nenhum download em runtime. */}
        {[20, 40, 60, 80].map(v => (
          <g key={v}>
            <line x1={v} y1="0" x2={v} y2="100" stroke="rgba(255,255,255,.05)" strokeWidth=".3" />
            <line x1="0" y1={v} x2="100" y2={v} stroke="rgba(255,255,255,.05)" strokeWidth=".3" />
          </g>
        ))}
        {linha && (() => {
          const [x1, y1] = projetar(linha.de)
          const [x2, y2] = projetar(linha.para)
          return <line x1={x1} y1={y1} x2={x2} y2={y2} stroke="#ff6960" strokeWidth=".6" strokeDasharray="2 1.5" />
        })()}
        {pontos.map((p, i) => {
          const [x, y] = projetar(p.coord)
          return (
            <circle key={i} cx={x} cy={y} r={p.destaque ? 1.6 : 0.9}
              fill={p.cor || 'var(--accent)'} opacity={p.destaque ? 1 : 0.65}
              onMouseEnter={() => setHover(p)} onMouseLeave={() => setHover(null)} />
          )
        })}
      </svg>
      {hover && <div className="geo-mapa-tip">{hover.rotulo}</div>}
      {pontos.length === 0 && !linha && (
        <div className="geo-mapa-vazio">Execute uma consulta para plotar os pontos</div>
      )}
    </div>
  )
}

function LinhaPlano({ plano, referencia }) {
  const melhor = (campo) => referencia != null && plano[campo] != null && plano[campo] <= referencia
  return (
    <div className="card" style={{ flex: 1, minWidth: 260 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 10 }}>
        <strong style={{ fontSize: 13.5 }}>{plano.rotulo}</strong>
        <code style={{ fontSize: 11 }}>{plano.hint}</code>
      </div>
      <div className="geo-metricas">
        {[
          ['totalKeysExamined', 'chaves examinadas'],
          ['totalDocsExamined', 'documentos examinados'],
          ['nReturned', 'documentos retornados'],
          ['executionTimeMillis', 'tempo (ms)'],
        ].map(([campo, rotulo]) => (
          <div key={campo} className={`geo-metrica${melhor(campo) && campo !== 'nReturned' ? ' vencedora' : ''}`}>
            <div className="stat-label">{rotulo}</div>
            <div className="stat-value" style={{ fontSize: 20 }}>{plano[campo] ?? '—'}</div>
          </div>
        ))}
      </div>
      <div style={{ marginTop: 10, fontSize: 11.5, color: 'var(--text-secondary)' }}>
        Plano: {plano.estagios?.join(' → ') || '—'}
        {plano.indice_usado && <> · índice <code style={{ fontSize: 11 }}>{plano.indice_usado}</code></>}
      </div>
    </div>
  )
}

export default function Geo() {
  const { call, loading } = useApi()
  const [status, setStatus] = useState(null)
  const [municipios, setMunicipios] = useState([])

  // Demo A
  const [clienteId, setClienteId] = useState('CLI00000')
  const [statusTx, setStatusTx] = useState('APROVADA')
  const [raioExplain, setRaioExplain] = useState(50)
  const [centroIdx, setCentroIdx] = useState(0)
  const [explain, setExplain] = useState(null)

  // Demo B
  const [limiteKmh, setLimiteKmh] = useState(900)
  const [viagens, setViagens] = useState(null)
  const [viagemSel, setViagemSel] = useState(null)

  // Demo C
  const [termo, setTermo] = useState('padaria')
  const [raioBusca, setRaioBusca] = useState(25)
  const [categorias, setCategorias] = useState([])
  const [busca, setBusca] = useState(null)

  useEffect(() => {
    call('/geo/status').then(d => d && setStatus(d))
    call('/geo/municipios').then(d => d && setMunicipios(d.municipios || []))
  }, [])

  const centro = municipios[centroIdx]?.centro || null

  const rodarExplain = async () => {
    if (!centro) return
    const d = await call('/geo/explain-compare', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ clienteId, status: statusTx, raioKm: Number(raioExplain), centro }),
    })
    if (d) setExplain(d)
  }

  const rodarViagens = async () => {
    const d = await call(`/geo/impossible-travel?limiteKmh=${Number(limiteKmh)}`)
    if (d) { setViagens(d); setViagemSel(null) }
  }

  const rodarBusca = async () => {
    if (!centro) return
    const d = await call('/geo/search', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ termo, centro, raioKm: Number(raioBusca), categorias }),
    })
    if (d) setBusca(d)
  }

  const pontosBusca = useMemo(() => (busca?.resultados || []).map(r => ({
    coord: r.local.coordinates,
    rotulo: `${r.estabelecimento.nome} — ${r.municipio}/${r.uf} · ${r.km_do_centro} km`,
    cor: '#06b6d4',
  })), [busca])

  const semDados = status && status.transacoes === 0

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
      <div className="banner banner-info">
        <span>🗺️</span>
        <div>
          Geo no Atlas não é um subsistema à parte: o <code>2dsphere</code> entra no mesmo índice
          composto dos campos de negócio, a análise de janela roda sobre o dado operacional e a busca
          textual filtra por geografia numa única stage — <strong>no mesmo cluster onde a transação vive</strong>.
        </div>
      </div>

      {status && (
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center' }}>
          <span className={`badge ${status.transacoes > 0 ? 'badge-green' : 'badge-yellow'}`}>
            {status.transacoes.toLocaleString('pt-BR')} transações · {status.db}.{status.colecao}
          </span>
          <span className="badge badge-gray">{status.indices.length} índices</span>
          <span className={`badge ${status.search.disponivel ? 'badge-green' : 'badge-yellow'}`}>
            Atlas Search: {status.search.disponivel ? status.search.index : 'não configurado'}
          </span>
          {status.fraudes_plantadas?.clientes > 0 && (
            <span className="badge badge-purple">
              {status.fraudes_plantadas.clientes} pares de fraude plantados
            </span>
          )}
        </div>
      )}

      {semDados && (
        <div className="banner banner-warning">
          <span>⚠️</span>
          <div>Dataset vazio. Rode <code>python scripts/seed_geo.py</code> antes da demonstração.</div>
        </div>
      )}

      {/* ── Demo A ─────────────────────────────────────────────────────── */}
      <section className="card">
        <div className="kicker" style={{ marginBottom: 8, color: 'var(--accent)' }}>Demo A</div>
        <h2 style={{ fontSize: 18, marginBottom: 6 }}>Índice composto com 2dsphere</h2>
        <p style={{ color: 'var(--text-secondary)', fontSize: 13, marginBottom: 14 }}>
          A mesma query <code>$geoWithin</code> com dois <code>hint</code> diferentes. Campos de
          igualdade primeiro, geo por último — o campo geo <strong>não precisa ser prefixo</strong> do
          índice para <code>$geoWithin</code>/<code>$geoIntersects</code>. Os números abaixo são o
          <code>executionStats</code> medido agora; se contradisserem a nota, vale o medido.
        </p>

        <div className="geo-controles">
          <label>clienteId
            <input value={clienteId} onChange={e => setClienteId(e.target.value)} />
          </label>
          <label>status
            <select value={statusTx} onChange={e => setStatusTx(e.target.value)}>
              <option>APROVADA</option><option>NEGADA</option><option>PENDENTE</option>
            </select>
          </label>
          <label>centro
            <select value={centroIdx} onChange={e => setCentroIdx(Number(e.target.value))}>
              {municipios.map((m, i) => <option key={i} value={i}>{m.municipio}/{m.uf}</option>)}
            </select>
          </label>
          <label>raio (km)
            <input type="number" min="1" max="5000" value={raioExplain}
              onChange={e => setRaioExplain(e.target.value)} />
          </label>
          <button className="btn btn-sm btn-primary" onClick={rodarExplain} disabled={loading || !centro}>
            {loading ? <><span className="spinner" /> Executando…</> : 'Comparar planos'}
          </button>
        </div>

        {explain && (
          <>
            <div className="row" style={{ marginTop: 16 }}>
              {explain.planos.map(p => (
                <LinhaPlano key={p.hint} plano={p}
                  referencia={Math.min(...explain.planos.map(x => x.totalKeysExamined ?? Infinity))} />
              ))}
            </div>
            <div style={{ marginTop: 12 }}>
              <QueryBlock query={explain.query} label="Ver query e hint executados" />
            </div>
          </>
        )}
      </section>

      {/* ── Demo B ─────────────────────────────────────────────────────── */}
      <section className="card">
        <div className="kicker" style={{ marginBottom: 8, color: '#ff6960' }}>Demo B</div>
        <h2 style={{ fontSize: 18, marginBottom: 6 }}>Impossible travel</h2>
        <p style={{ color: 'var(--text-secondary)', fontSize: 13, marginBottom: 14 }}>
          <code>$setWindowFields</code> particiona por cliente, <code>$shift</code> traz a transação
          anterior e a distância sai de haversine em operadores MQL nativos — sem{' '}
          <code>$function</code> e sem trazer um único documento para a aplicação.
        </p>

        <div className="geo-controles">
          <label>limite (km/h)
            <input type="number" min="1" value={limiteKmh} onChange={e => setLimiteKmh(e.target.value)} />
          </label>
          <button className="btn btn-sm btn-primary" onClick={rodarViagens} disabled={loading}>
            {loading ? <><span className="spinner" /> Calculando…</> : 'Detectar pares'}
          </button>
          {viagens && (
            <span className="badge badge-red">
              {viagens.encontrados} pares acima de {viagens.limite_kmh} km/h
              {viagens.truncado && ' (truncado)'}
            </span>
          )}
        </div>

        {viagens && (
          <div className="row" style={{ marginTop: 16, alignItems: 'flex-start' }}>
            <div className="col" style={{ minWidth: 320 }}>
              <div className="geo-tabela-wrap">
                <table className="geo-tabela">
                  <thead>
                    <tr><th>cliente</th><th>km</th><th>min</th><th>km/h</th><th>trajeto</th></tr>
                  </thead>
                  <tbody>
                    {viagens.resultados.map(v => (
                      <tr key={v.endToEndId}
                        className={viagemSel?.endToEndId === v.endToEndId ? 'sel' : ''}
                        onClick={() => setViagemSel(v)}>
                        <td><code>{v.clienteId}</code></td>
                        <td>{v.km}</td>
                        <td>{v.minutos}</td>
                        <td style={{ color: '#ff6960', fontWeight: 700 }}>{v.kmh}</td>
                        <td>{v.de.municipio} → {v.para.municipio}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {viagens.resultados.length === 0 && (
                <p style={{ color: 'var(--text-secondary)', fontSize: 13 }}>
                  Nenhum par acima do limite. Baixe o valor para ver o comportamento.
                </p>
              )}
            </div>
            <div className="col" style={{ minWidth: 260 }}>
              <MiniMapa
                pontos={viagemSel ? [
                  { coord: viagemSel.de.coordinates, rotulo: `origem — ${viagemSel.de.municipio}`, cor: '#06b6d4', destaque: true },
                  { coord: viagemSel.para.coordinates, rotulo: `destino — ${viagemSel.para.municipio}`, cor: '#ff6960', destaque: true },
                ] : []}
                linha={viagemSel ? { de: viagemSel.de.coordinates, para: viagemSel.para.coordinates } : null}
              />
              {viagemSel && (
                <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginTop: 8 }}>
                  <code>{viagemSel.clienteId}</code> · {viagemSel.km} km em {viagemSel.minutos} min
                  <button className="btn btn-xs btn-ghost" style={{ marginLeft: 8 }}
                    onClick={() => setClienteId(viagemSel.clienteId)}>usar na Demo A</button>
                </div>
              )}
            </div>
          </div>
        )}

        {viagens && (
          <div style={{ marginTop: 12 }}>
            <QueryBlock label="Ver pipeline completo"
              query={JSON.stringify(viagens.pipeline, null, 2)} />
          </div>
        )}
      </section>

      {/* ── Demo C ─────────────────────────────────────────────────────── */}
      <section className="card">
        <div className="kicker" style={{ marginBottom: 8, color: '#a855f7' }}>Demo C</div>
        <h2 style={{ fontSize: 18, marginBottom: 6 }}>Geo + Atlas Search numa stage</h2>
        <p style={{ color: 'var(--text-secondary)', fontSize: 13, marginBottom: 14 }}>
          Um único <code>$search</code>: <code>must</code> de texto com <code>fuzzy</code>,{' '}
          <code>filter</code> de <code>geoWithin</code> e de categoria, mais <code>$searchMeta</code>{' '}
          para as facetas. A alternativa usual exige um motor de busca ao lado, sincronizado por CDC.
        </p>

        <div className="geo-controles">
          <label>termo
            <input value={termo} onChange={e => setTermo(e.target.value)} />
          </label>
          <label>centro
            <select value={centroIdx} onChange={e => setCentroIdx(Number(e.target.value))}>
              {municipios.map((m, i) => <option key={i} value={i}>{m.municipio}/{m.uf}</option>)}
            </select>
          </label>
          <label>raio: {raioBusca} km
            <input type="range" min="1" max="300" value={raioBusca}
              onChange={e => setRaioBusca(e.target.value)} />
          </label>
          <button className="btn btn-sm btn-primary" onClick={rodarBusca} disabled={loading || !centro}>
            {loading ? <><span className="spinner" /> Buscando…</> : 'Buscar'}
          </button>
        </div>

        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', margin: '10px 0' }}>
          {CATEGORIAS.map(c => (
            <button key={c} className={`tag${categorias.includes(c) ? ' active' : ''}`}
              style={categorias.includes(c) ? { borderColor: 'var(--accent)', color: 'var(--accent)' } : {}}
              onClick={() => setCategorias(v => v.includes(c) ? v.filter(x => x !== c) : [...v, c])}>
              {c}
            </button>
          ))}
        </div>

        {busca?.estado === 'nao_configurado' && (
          <div className="banner banner-warning">
            <span>⚙️</span>
            <div>
              Search index <code>{busca.index}</code> ausente — {busca.mensagem}. Nenhum resultado é
              inventado enquanto o índice não existir.
            </div>
          </div>
        )}

        {busca?.estado === 'ok' && (
          <div className="row" style={{ alignItems: 'flex-start' }}>
            <div className="col" style={{ minWidth: 320 }}>
              {busca.meta?.facet && (
                <div style={{ display: 'flex', gap: 14, flexWrap: 'wrap', marginBottom: 10, fontSize: 12 }}>
                  {Object.entries(busca.meta.facet).map(([nome, dados]) => (
                    <div key={nome}>
                      <div className="kicker" style={{ fontSize: 10, marginBottom: 4 }}>{nome}</div>
                      {(dados.buckets || []).slice(0, 6).map(b => (
                        <div key={b._id} style={{ color: 'var(--text-secondary)' }}>
                          {b._id} <strong style={{ fontFamily: 'var(--font-mono)' }}>{b.count}</strong>
                        </div>
                      ))}
                    </div>
                  ))}
                  <div>
                    <div className="kicker" style={{ fontSize: 10, marginBottom: 4 }}>total</div>
                    <div style={{ fontFamily: 'var(--font-mono)' }}>
                      {busca.meta.count?.lowerBound ?? busca.meta.count?.total ?? '—'}
                    </div>
                  </div>
                </div>
              )}
              {busca.resultados.map(r => (
                <div key={r.endToEndId} className="result-row">
                  <strong style={{ fontSize: 13 }}>{r.estabelecimento.nome}</strong>
                  <span className="badge badge-gray" style={{ marginLeft: 8 }}>{r.estabelecimento.categoria}</span>
                  <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginTop: 3 }}>
                    {r.municipio}/{r.uf} · {r.km_do_centro} km do centro · score {r.score}
                  </div>
                </div>
              ))}
              {busca.resultados.length === 0 && (
                <p style={{ color: 'var(--text-secondary)', fontSize: 13 }}>
                  Nenhum estabelecimento dentro do raio para esse termo.
                </p>
              )}
            </div>
            <div className="col" style={{ minWidth: 260 }}>
              <MiniMapa pontos={pontosBusca} />
            </div>
          </div>
        )}

        {busca?.pipeline && (
          <div style={{ marginTop: 12, display: 'flex', flexDirection: 'column', gap: 8 }}>
            <QueryBlock label="Ver $search executado" query={JSON.stringify(busca.pipeline, null, 2)} />
            <QueryBlock label="Ver $searchMeta das facetas" query={JSON.stringify(busca.pipeline_meta, null, 2)} />
          </div>
        )}
      </section>

      {/* Limite declarado: um DBA que ouve a limitação acredita no resto. */}
      <details className="card">
        <summary style={{ cursor: 'pointer', fontSize: 13.5, fontWeight: 600 }}>
          Onde o geo do MongoDB não vai — dito antes da pergunta
        </summary>
        <ul style={{ marginTop: 10, paddingLeft: 18, fontSize: 13, color: 'var(--text-secondary)', lineHeight: 1.7 }}>
          <li>Não há álgebra de geometria: sem <code>buffer</code>, <code>union</code>,{' '}
            <code>intersection</code> ou cálculo de área. O MongoDB responde predicados
            (dentro? cruza? perto?), não constrói geometria nova.</li>
          <li>Só WGS84 — sem SRID, sem reprojeção, sem sistema de coordenadas local.</li>
          <li>Sem raster, sem topologia e sem roteamento/isócrona.</li>
          <li><code>$geoNear</code> só como primeira stage do pipeline; filtros anteriores vão
            dentro do próprio operador.</li>
          <li>O <code>filter</code> do <code>$vectorSearch</code> não aceita operadores
            geoespaciais — o caminho é <code>$search</code> ou uma stage separada.</li>
          <li>Polígono inválido é erro, não conserto automático.</li>
        </ul>
      </details>
    </div>
  )
}
