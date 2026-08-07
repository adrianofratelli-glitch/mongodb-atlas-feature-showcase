import React, { useCallback, useEffect, useMemo, useState } from 'react'
import { useApi } from '../hooks/useApi'
import { useIntervaloVisivel } from '../hooks/usePolling'
import QueryBlock from '../components/QueryBlock'

// Bounding box do Brasil. A projeção é linear de propósito: a apresentação é em
// auditório com rede ruim, então o mapa é SVG local, sem tiles e sem dependência
// nova no frontend — silhueta, arco e halos são todos desenhados aqui.
const BBOX = { oeste: -74.2, leste: -33.8, norte: 5.6, sul: -34.2 }
const CATEGORIAS = ['alimentação', 'combustível', 'farmácia', 'vestuário', 'serviços']

function projetar([lng, lat]) {
  const x = ((lng - BBOX.oeste) / (BBOX.leste - BBOX.oeste)) * 100
  const y = ((BBOX.norte - lat) / (BBOX.norte - BBOX.sul)) * 100
  return [x, y]
}

// Silhueta ESQUEMÁTICA do Brasil, ~60 vértices em [lon, lat], embutida no
// bundle. Não é cartografia: é referência visual para o olho localizar o ponto.
// Continua sem Leaflet, sem tiles e sem nenhuma requisição em runtime — o mapa
// tem de renderizar igual com a rede do auditório fora do ar.
const CONTORNO_BR = [
  [-51.8, 4.3], [-50.0, 1.8], [-48.5, -0.8], [-44.3, -2.4], [-41.8, -2.9],
  [-38.5, -3.7], [-35.2, -5.8], [-34.8, -7.1], [-35.5, -9.5], [-37.0, -11.0],
  [-38.5, -12.9], [-39.0, -15.0], [-38.9, -17.3], [-39.7, -19.6], [-40.9, -21.5],
  [-42.0, -22.9], [-44.5, -23.3], [-46.6, -24.0], [-48.5, -25.5], [-48.6, -27.0],
  [-50.0, -29.0], [-51.2, -31.0], [-52.3, -33.0], [-53.4, -33.7], [-53.5, -32.5],
  [-55.6, -30.9], [-57.6, -30.2], [-56.0, -28.5], [-54.5, -27.5], [-54.6, -25.6],
  [-54.3, -24.0], [-55.0, -22.3], [-57.6, -22.1], [-57.8, -20.0], [-58.2, -19.8],
  [-58.4, -17.2], [-60.2, -16.3], [-60.5, -15.1], [-62.0, -13.0], [-63.9, -12.5],
  [-65.3, -11.0], [-66.8, -9.8], [-68.7, -11.0], [-70.6, -11.0], [-70.6, -9.5],
  [-72.2, -9.8], [-73.2, -7.3], [-74.0, -7.5], [-73.0, -6.0], [-70.0, -4.3],
  [-69.4, -1.1], [-69.9, 0.6], [-67.9, 1.7], [-67.1, 2.8], [-64.5, 4.1],
  [-63.4, 3.9], [-62.1, 4.1], [-60.7, 5.2], [-60.0, 4.5], [-59.0, 4.5],
  [-57.5, 3.4], [-56.0, 2.0], [-54.5, 2.3],
]

const PATH_BR = CONTORNO_BR
  .map((c, i) => `${i ? 'L' : 'M'}${projetar(c).map(v => v.toFixed(2)).join(' ')}`)
  .join(' ') + ' Z'

function MiniMapa({ pontos = [], linha = null, altura = 260, rotuloLinha = null }) {
  const [hover, setHover] = useState(null)
  // Identificador único por instância: dois mapas na mesma página compartilhariam
  // os <defs> e o segundo herdaria o gradiente do primeiro.
  const uid = React.useId().replace(/:/g, '')
  return (
    <div className="geo-mapa" style={{ height: altura }}>
      <svg viewBox="0 0 100 100" preserveAspectRatio="none" role="img"
        aria-label="Mapa esquemático do Brasil com os pontos da consulta">
        <defs>
          <linearGradient id={`br-${uid}`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#00ED64" stopOpacity=".10" />
            <stop offset="100%" stopColor="#00684A" stopOpacity=".16" />
          </linearGradient>
          <radialGradient id={`halo-${uid}`}>
            <stop offset="0%" stopColor="#fff" stopOpacity=".55" />
            <stop offset="100%" stopColor="#fff" stopOpacity="0" />
          </radialGradient>
        </defs>

        {/* Paralelos e meridianos como referência, atrás do país. */}
        {[20, 40, 60, 80].map(v => (
          <g key={v}>
            <line x1={v} y1="0" x2={v} y2="100" stroke="rgba(255,255,255,.04)" strokeWidth=".3" />
            <line x1="0" y1={v} x2="100" y2={v} stroke="rgba(255,255,255,.04)" strokeWidth=".3" />
          </g>
        ))}

        <path d={PATH_BR} fill={`url(#br-${uid})`} stroke="rgba(0,237,100,.45)"
          strokeWidth=".45" strokeLinejoin="round" />

        {linha && (() => {
          const [x1, y1] = projetar(linha.de)
          const [x2, y2] = projetar(linha.para)
          // Arco em vez de reta: duas cidades ligadas por uma linha reta somem
          // dentro da silhueta; a curva sai do corpo do país e se lê de longe.
          const [mx, my] = [(x1 + x2) / 2, (y1 + y2) / 2]
          const [dx, dy] = [x2 - x1, y2 - y1]
          const arco = `M${x1} ${y1} Q${mx - dy * 0.22} ${my + dx * 0.22} ${x2} ${y2}`
          return (
            <g>
              <path d={arco} fill="none" stroke="#ff6960" strokeWidth=".55"
                strokeDasharray="2.5 1.8" strokeLinecap="round">
                <animate attributeName="stroke-dashoffset" from="8.6" to="0"
                  dur="1.1s" repeatCount="indefinite" />
              </path>
              {rotuloLinha && (
                <text x={mx - dy * 0.13} y={my + dx * 0.13} fill="#ff6960" fontSize="3.4"
                  fontWeight="700" textAnchor="middle" style={{ paintOrder: 'stroke' }}
                  stroke="rgba(0,30,43,.85)" strokeWidth="1.1">{rotuloLinha}</text>
              )}
            </g>
          )
        })()}

        {pontos.map((p, i) => {
          const [x, y] = projetar(p.coord)
          return (
            <g key={i} onMouseEnter={() => setHover(p)} onMouseLeave={() => setHover(null)}>
              {p.destaque && (
                <>
                  <circle cx={x} cy={y} r="4.5" fill={`url(#halo-${uid})`} />
                  <circle cx={x} cy={y} r="1.6" fill="none" stroke={p.cor || 'var(--accent)'} strokeWidth=".35">
                    <animate attributeName="r" values="1.6;4.2;1.6" dur="2s" repeatCount="indefinite" />
                    <animate attributeName="opacity" values=".9;0;.9" dur="2s" repeatCount="indefinite" />
                  </circle>
                </>
              )}
              <circle cx={x} cy={y} r={p.destaque ? 1.5 : 0.85}
                fill={p.cor || 'var(--accent)'} opacity={p.destaque ? 1 : 0.7}
                stroke={p.destaque ? 'rgba(0,30,43,.9)' : 'none'} strokeWidth=".3" />
            </g>
          )
        })}
      </svg>
      {hover && <div className="geo-mapa-tip">{hover.rotulo}</div>}
      {pontos.length === 0 && !linha && (
        <div className="geo-mapa-vazio">Execute uma consulta para plotar os pontos</div>
      )}
      <span className="geo-mapa-selo">contorno esquemático · WGS84 · sem tiles</span>
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

  // Detalhe técnico: comparação de planos de execução
  const [clienteId, setClienteId] = useState('CLI00000')
  const [statusTx, setStatusTx] = useState('APROVADA')
  const [raioExplain, setRaioExplain] = useState(50)
  const [centroIdx, setCentroIdx] = useState(0)
  const [explain, setExplain] = useState(null)

  // 01 — sinal de risco (impossible travel)
  const [limiteKmh, setLimiteKmh] = useState(900)
  const [viagens, setViagens] = useState(null)
  const [viagemSel, setViagemSel] = useState(null)

  // 02 — contexto para investigação (geo + Atlas Search)
  const [termo, setTermo] = useState('padaria')
  const [raioBusca, setRaioBusca] = useState(25)
  const [categorias, setCategorias] = useState([])
  const [busca, setBusca] = useState(null)

  // 00 — sinal em event time, materializado pelo processor do módulo 07
  const [aoVivo, setAoVivo] = useState(null)
  const [sinalSel, setSinalSel] = useState(null)

  useEffect(() => {
    call('/geo/status').then(d => d && setStatus(d))
    call('/geo/municipios').then(d => d && setMunicipios(d.municipios || []))
  }, [])

  // O painel ao vivo lê uma coleção que só muda quando o gerador da aba 07 está
  // rodando; 4 s é rápido o bastante para o sinal aparecer durante a fala e
  // lento o bastante para não competir com as três colunas do módulo anterior.
  useIntervaloVisivel(useCallback(async () => {
    const d = await call('/geo/sinais-ao-vivo')
    if (d) setAoVivo(d)
  }, [call]), 4000, true)

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
          No módulo anterior a transação foi <strong>gravada e distribuída</strong>. Aqui ela é{' '}
          <strong>avaliada</strong> — no mesmo cluster, sem copiar dado para um motor geoespacial à parte.
          A frente muda de propósito: PIX é transferência online e <strong>não carrega coordenada</strong>;
          esta aba trabalha sobre <strong>compra presencial com cartão</strong>, onde a localização é a do{' '}
          <strong>terminal do adquirente</strong> — posição cadastral que não depende do GPS do cliente.
          O cadastro ainda exige qualidade e atualização. O que sai daqui é <strong>sinal de risco</strong> para compor política, jamais uma
          decisão automática.
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
              {status.fraudes_plantadas.clientes} cenários de risco plantados
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

      {/* ── 00 · O sinal saindo do stream, não de uma varredura ─────────────
          Esta é a junção com o módulo 07: o mesmo commit que alimenta as três
          colunas alimenta este cálculo, dentro da janela, sem segundo motor. */}
      <section className="card geo-live">
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, flexWrap: 'wrap', alignItems: 'baseline' }}>
          <div>
            <div className="kicker" style={{ marginBottom: 8, color: '#00ED64' }}>00 · Em event time</div>
            <h2 style={{ fontSize: 18, marginBottom: 6 }}>O sinal sai na passagem, não numa varredura</h2>
          </div>
          {aoVivo?.total > 0 && (
            <span className="badge badge-green">{aoVivo.total} detectados nesta execução</span>
          )}
        </div>
        <p style={{ color: 'var(--text-secondary)', fontSize: 13, marginBottom: 14 }}>
          O gerador do <strong>módulo 07</strong> emite dois canais no mesmo stream: PIX, que{' '}
          <strong>não carrega coordenada</strong>, e compra presencial com cartão, que carrega a do terminal.
          Um segundo processor do <strong>Atlas Stream Processing</strong> lê o mesmo change stream, agrupa por
          cartão numa janela deslizante de 30 s e calcula haversine ali dentro — o resultado cai em{' '}
          <code>{aoVivo?.colecao || 'geo.sinais_ao_vivo'}</code> antes de qualquer analista perguntar.
          Os painéis abaixo continuam valendo: eles respondem a investigação retrospectiva, que é outra pergunta.
        </p>

        {aoVivo?.total > 0 && (
          <div className="geo-live-contagem">
            <div>
              <span>{aoVivo.plantados}</span>
              <small>plantados pelo gerador — sinal garantido para a demo</small>
            </div>
            <div>
              <span style={{ color: aoVivo.emergentes ? '#00ED64' : 'var(--text-secondary)' }}>{aoVivo.emergentes}</span>
              <small>emergentes — ninguém armou; o pipeline achou no tráfego</small>
            </div>
          </div>
        )}

        {!aoVivo?.total && (
          <div className="banner banner-info" style={{ marginBottom: 0 }}>
            <span>▶</span>
            <div>
              Nenhum sinal ainda. Rode o fluxo no <strong>módulo 07 · Streaming</strong> e volte: os primeiros
              pares aparecem cerca de 30 s depois do início, quando a primeira janela fecha.
            </div>
          </div>
        )}

        {aoVivo?.sinais?.length > 0 && (
          <div className="row" style={{ marginTop: 12, alignItems: 'flex-start' }}>
            <div className="col" style={{ minWidth: 340 }}>
              <div className="geo-tabela-wrap">
                <table className="geo-tabela">
                  <thead>
                    <tr><th>cartão</th><th>km</th><th>min</th><th>km/h</th><th>trajeto</th><th>origem</th></tr>
                  </thead>
                  <tbody>
                    {aoVivo.sinais.map(s => (
                      <tr key={s._id} className={sinalSel?._id === s._id ? 'sel' : ''}
                        onClick={() => setSinalSel(s)}>
                        <td><code>{s.clienteId}</code></td>
                        <td>{s.km}</td>
                        <td>{s.minutos}</td>
                        <td style={{ color: '#ff6960', fontWeight: 700 }}>{s.kmh}</td>
                        <td>{s.de?.municipio} → {s.para?.municipio}</td>
                        <td>
                          <span className={`badge ${s.origem === 'emergente' ? 'badge-green' : 'badge-gray'}`}>
                            {s.origem}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
            <div className="col" style={{ minWidth: 260 }}>
              <MiniMapa
                pontos={sinalSel ? [
                  { coord: sinalSel.de.coordinates, rotulo: `origem — ${sinalSel.de.municipio}`, cor: '#06b6d4', destaque: true },
                  { coord: sinalSel.para.coordinates, rotulo: `destino — ${sinalSel.para.municipio}`, cor: '#ff6960', destaque: true },
                ] : []}
                linha={sinalSel ? { de: sinalSel.de.coordinates, para: sinalSel.para.coordinates } : null}
                rotuloLinha={sinalSel ? `${sinalSel.km} km · ${sinalSel.minutos} min` : null}
              />
              {sinalSel && (
                <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginTop: 8 }}>
                  terminais <code>{sinalSel.de.terminal}</code> → <code>{sinalSel.para.terminal}</code>
                  <br />detectado pela janela às {new Date(sinalSel.detectadoEm).toLocaleTimeString('pt-BR')}
                </div>
              )}
              {!sinalSel && (
                <div style={{ fontSize: 12, color: 'var(--text-disabled)', marginTop: 8 }}>
                  Clique num sinal para traçar o percurso.
                </div>
              )}
            </div>
          </div>
        )}

        <div className="banner banner-warning" style={{ marginTop: 12, marginBottom: 0 }}>
          <span>⚠️</span>
          <div>
            A janela agrupa por <strong>tempo de chegada</strong>; a velocidade usa o instante de{' '}
            <strong>captura de cada compra</strong>, que chega atrasada do adquirente. Pares abaixo de 200 km ou
            de 1 minuto são descartados: ali &quot;velocidade&quot; é ruído de captura simultânea, não deslocamento.
            Continua sendo sinal para compor política, nunca decisão automática.
          </div>
        </div>
      </section>

      {/* ── 01 · Sinal de risco ─────────────────────────────────────────── */}
      <section className="card">
        <div className="kicker" style={{ marginBottom: 8, color: '#ff6960' }}>01 · Investigação retrospectiva</div>
        <h2 style={{ fontSize: 18, marginBottom: 6 }}>O mesmo cálculo sobre 90 dias de histórico</h2>
        <p style={{ color: 'var(--text-secondary)', fontSize: 13, marginBottom: 14 }}>
          Duas compras presenciais do mesmo cliente, distantes demais para o tempo entre elas — a assinatura
          clássica de cartão clonado. Como <strong>cada ponto é um terminal físico</strong>, a contradição é
          geográfica, não uma suspeita sobre o aparelho de quem paga. O cálculo roda{' '}
          <strong>inteiro no banco</strong>: <code>$setWindowFields</code> particiona por cliente,{' '}
          <code>$shift</code> traz a compra anterior e a distância sai de haversine em operadores MQL
          nativos — sem <code>$function</code> e <strong>sem transportar o histórico para fazer o cálculo</strong>.
          Isso evita manter uma cópia especializada e sua sincronização apenas para esta análise.
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
                rotuloLinha={viagemSel ? `${viagemSel.km} km · ${viagemSel.minutos} min` : null}
              />
              {viagemSel && (
                <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginTop: 8 }}>
                  <code>{viagemSel.clienteId}</code> · {viagemSel.km} km em {viagemSel.minutos} min
                  {/* Proveniência à vista: o terminal é o que separa este sinal
                      de um palpite sobre o GPS do cliente. */}
                  <div style={{ marginTop: 5 }}>
                    origem <code>{viagemSel.de.dispositivo?.id || '—'}</code> → destino{' '}
                    <code>{viagemSel.para.dispositivo?.id || '—'}</code>
                    <br />captura por {viagemSel.para.localizacaoMeta?.origem === 'TERMINAL_ADQUIRENTE'
                      ? 'terminal do adquirente (posição fixa)'
                      : (viagemSel.para.localizacaoMeta?.origem || 'origem desconhecida')}
                  </div>
                  <button className="btn btn-xs btn-ghost" style={{ marginLeft: 8 }}
                    onClick={() => setClienteId(viagemSel.clienteId)}>investigar este cliente</button>
                </div>
              )}
            </div>
          </div>
        )}

        {viagens && (
          <div style={{ marginTop: 12 }}>
            <div className="banner banner-warning" style={{ marginBottom: 10 }}>
              <span>⚠️</span>
              <div>
                Resultado retrospectivo, para investigação. O terminal é uma fonte muito mais confiável que
                GPS de aparelho, mas o sinal ainda não decide sozinho: cartão adicional, compra por terceiro
                autorizado e atraso na captura produzem falso positivo. Produção combina isto com
                autenticação, comportamento e política de risco.
              </div>
            </div>
            <QueryBlock label="Ver pipeline completo"
              query={JSON.stringify(viagens.pipeline, null, 2)} />
          </div>
        )}
      </section>

      <section className="card" aria-labelledby="geo-bank-impact-title">
        <div className="kicker" style={{ color: '#00ED64', marginBottom: 8 }}>Impacto para risco e contestação</div>
        <h2 id="geo-bank-impact-title" style={{ fontSize: 18, marginBottom: 14 }}>O dado operacional já nasce investigável</h2>
        <div className="row" style={{ alignItems: 'stretch' }}>
          <div className="col card" style={{ minWidth: 230 }}>
            <strong>Menos cópias</strong>
            <p style={{ color: 'var(--text-secondary)', fontSize: 13, marginTop: 6 }}>Documento, GeoJSON, campos de negócio e proveniência permanecem juntos; não há ETL só para calcular o sinal.</p>
          </div>
          <div className="col card" style={{ minWidth: 230 }}>
            <strong>Menor defasagem</strong>
            <p style={{ color: 'var(--text-secondary)', fontSize: 13, marginTop: 6 }}>Índice geoespacial e Atlas Search consultam a mesma base operacional, evitando lag entre banco, GIS e busca na investigação.</p>
          </div>
          <div className="col card" style={{ minWidth: 230 }}>
            <strong>Uma linguagem</strong>
            <p style={{ color: 'var(--text-secondary)', fontSize: 13, marginTop: 6 }}>Filtro geo, janela temporal, texto, fuzzy e facetas compõem pipelines MQL auditáveis pela mesma equipe.</p>
          </div>
        </div>
        <div className="banner banner-warning" style={{ marginTop: 12, marginBottom: 0 }}>
          <span>✓</span><div><strong>O diferencial demonstrado é convergência de capacidades.</strong> O ganho financeiro e o desenho online dependem do inventário real de motores, CDCs, volume e SLO do banco.</div>
        </div>
      </section>

      {/* ── 02 · Contexto para investigação ──────────────────────────────── */}
      <section className="card">
        <div className="kicker" style={{ marginBottom: 8, color: '#a855f7' }}>02 · Contexto para investigação</div>
        <h2 style={{ fontSize: 18, marginBottom: 6 }}>Texto, geografia e categoria numa consulta só</h2>
        <p style={{ color: 'var(--text-secondary)', fontSize: 13, marginBottom: 14 }}>
          Quando o analista abre uma disputa ou um alerta, a pergunta é <em>&quot;o que existe em volta
          disso?&quot;</em>. Um único <code>$search</code> responde: <code>must</code> de texto com{' '}
          <code>fuzzy</code> (nome digitado errado ainda acha), <code>filter</code> de{' '}
          <code>geoWithin</code> e de categoria, mais <code>$searchMeta</code> para as facetas.
          <strong> A alternativa usual é um motor de busca ao lado</strong>, sincronizado por CDC, com
          contrato e operação próprios. Aqui é o mesmo cluster, no mesmo índice.
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
                <div key={r.terminalId || r.endToEndId} className="result-row">
                  <strong style={{ fontSize: 13 }}>{r.estabelecimento.nome}</strong>
                  <span className="badge badge-gray" style={{ marginLeft: 8 }}>{r.estabelecimento.categoria}</span>
                  <div style={{ fontSize: 12, color: 'var(--text-secondary)', marginTop: 3 }}>
                    {r.municipio}/{r.uf} · {r.km_do_centro} km do centro · terminal <code>{r.terminalId}</code> · score {r.score}
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

      <details className="card">
        <summary style={{ cursor: 'pointer', fontSize: 13.5, fontWeight: 600 }}>
          Controles antes de levar isto para produção
        </summary>
        <ul style={{ marginTop: 10, paddingLeft: 18, fontSize: 13, color: 'var(--text-secondary)', lineHeight: 1.7 }}>
          <li>Guardar proveniência, instante de captura e identificador do terminal junto ao ponto — sem isso o sinal vira palpite.</li>
          <li>Separar cadastro de <strong>terminal</strong> (posição independente do cliente, mas sujeita a erro ou desatualização) de <strong>GPS de aparelho</strong> (controlável pelo cliente e sujeito a spoofing). Nenhuma fonte sustenta bloqueio isoladamente.</li>
          <li>Tratar a diferença entre <strong>data da compra e data da captura</strong>: atraso de liquidação do adquirente produz falso positivo de velocidade.</li>
          <li>Definir base legal, minimização, retenção, acesso e auditoria para dados pessoais sob LGPD.</li>
          <li>Calibrar limiar por canal e contexto; velocidade fixa de 900 km/h é didática, não política antifraude.</li>
          <li>Esta demo calcula histórico sob demanda. Decisão quase em tempo real exige integrar o sinal ao fluxo e isolar a carga analítica conforme o SLO.</li>
        </ul>
      </details>

      {/* Conversa de DBA, não de palco: responde "o índice está certo?",
          não "que problema isso resolve?". Fica acessível, fora do fluxo. */}
      <details className="card">
        <summary style={{ cursor: 'pointer', fontSize: 13.5, fontWeight: 600 }}>
          Como o índice sustenta isso <span style={{ color: 'var(--text-secondary)', fontWeight: 400, fontSize: 12 }}>
            — plano de execução medido, para a conversa técnica</span>
        </summary>
        <p style={{ color: 'var(--text-secondary)', fontSize: 13, margin: '10px 0 0' }}>
          A mesma consulta sob dois índices. Campos de igualdade primeiro, geo por último — o campo
          geo <strong>não precisa ser prefixo</strong> do índice para <code>$geoWithin</code>. Os números
          são o <code>executionStats</code> medido agora; se contradisserem a nota, vale o medido.
        </p>
      <div>
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
      </div>

      </details>

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
