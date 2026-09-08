import React, { useEffect, useMemo, useState } from 'react'
import { useApi } from '../hooks/useApi'
import QueryBlock from '../components/QueryBlock'
import MiniMapa from '../components/MapaBrasil'

// Nenhum campo desta aba é texto livre. Um clienteId digitado errado devolve
// tela vazia, e no palco isso é lido como "a demo não encontrou nada" — não
// como erro de digitação. Toda opção abaixo existe no dataset gerado por
// scripts/seed_geo.py.
const LIMITES_KMH = [300, 600, 900, 1200, 2000]
const RAIOS_OPERADORES_KM = [10, 25, 50, 100, 200]

// "2239.8 ms" obriga a plateia a contar casas; acima de 1 s a unidade muda.
const fmtDuracao = (ms) => (ms == null
  ? '—'
  : ms >= 1000 ? `${(ms / 1000).toFixed(1).replace('.', ',')} s` : `${Math.round(ms)} ms`)

export default function Geo() {
  const { call } = useApi()
  // Cada ação controla o próprio estado: rodar a detecção não pode desabilitar
  // outra ação da mesma tela.
  const [ocupado, setOcupado] = useState({})
  const comOcupado = async (chave, fn) => {
    setOcupado((o) => ({ ...o, [chave]: true }))
    try { return await fn() } finally { setOcupado((o) => ({ ...o, [chave]: false })) }
  }
  const [status, setStatus] = useState(null)
  const [municipios, setMunicipios] = useState([])

  // 01 — sinal de risco (impossible travel)
  const [limiteKmh, setLimiteKmh] = useState(900)
  const [viagens, setViagens] = useState(null)
  const [viagemSel, setViagemSel] = useState(null)
  const [clienteFiltro, setClienteFiltro] = useState('')

  // 02 — operadores de consulta geoespacial
  const [centroIdx, setCentroIdx] = useState(0)
  const [raioOperadores, setRaioOperadores] = useState(50)
  const [operadores, setOperadores] = useState(null)

  useEffect(() => {
    call('/geo/status').then(d => d && setStatus(d))
    call('/geo/municipios').then(d => d && setMunicipios(d.municipios || []))
  }, [])

  const centro = municipios[centroIdx]?.centro || null

  const rodarOperadores = () => comOcupado('operadores', async () => {
    if (!centro) return
    const d = await call('/geo/operadores', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ centro, raioKm: Number(raioOperadores), limite: 5 }),
    })
    if (d) setOperadores(d)
  })

  // O recorte é a resposta à pergunta de escala: filtrar antes da janela reduz
  // o universo varrido.
  const rodarViagens = (filtrarCliente = false) => comOcupado('viagens', async () => {
    const alvo = filtrarCliente ? clienteFiltro.trim() : ''
    const query = `limiteKmh=${Number(limiteKmh)}${alvo ? `&clienteId=${encodeURIComponent(alvo)}` : ''}`
    const d = await call(`/geo/impossible-travel?${query}`)
    if (d) { setViagens(d); setViagemSel(null) }
  })

  // Os clientes ofertados são os que o seed plantou (backend lê fraud_seeds.json):
  // a opção existe no dataset, então nenhuma seleção devolve tela vazia por engano.
  // O valor selecionado entra na lista mesmo quando não foi plantado (um caso
  // emergente vindo do próprio resultado), senão o <select> ficaria exibindo uma
  // opção que não existe.
  const clientesPlantados = useMemo(() => {
    const base = status?.fraudes_plantadas?.lista || []
    const extras = [clienteFiltro].filter(c => c && !base.includes(c))
    return [...base, ...extras].sort()
  }, [status, clienteFiltro])

  const semDados = status && status.transacoes === 0

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
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
              {status.fraudes_plantadas.clientes} cenários de risco disponíveis
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

      {/* ── 01 · Sinal de risco ─────────────────────────────────────────── */}
      <section className="card">
        <div className="kicker" style={{ marginBottom: 8, color: '#ff6960' }}>01 · Investigação retrospectiva</div>
        {/* O título prometia detecção ("o mesmo cálculo sobre 90 dias") e a
            evidência entrega investigação. Quem opera antifraude percebe a
            diferença na hora, e a promessa maior é a que derruba a menor. */}
        <h2 style={{ fontSize: 18, marginBottom: 6 }}>
          Investigar 90 dias sem tirar o histórico do banco
        </h2>
        <p style={{ color: 'var(--text-secondary)', fontSize: 13, marginBottom: 14 }}>
          Duas compras presenciais do mesmo cliente, distantes demais para o tempo entre elas.{' '}
          <code>$setWindowFields</code> particiona por cliente, <code>$shift</code> traz a compra anterior e
          a distância sai de haversine em MQL nativo. <strong>O histórico não sai do banco</strong> — nenhuma
          cópia especializada para manter.
        </p>
        {/* A pergunta que a tela deixava sem resposta: "isso está rodando ONDE?".
            Sem a origem explícita, o painel parece cálculo local do frontend. */}
        {status && (
          <div className="geo-origem">
            <div>
              <small>onde roda</small>
              <span>agregação no cluster Atlas</span>
            </div>
            <div>
              <small>coleção</small>
              <span><code>{status.db}.{status.colecao}</code></span>
            </div>
            <div>
              <small>documentos varridos</small>
              <span>{status.transacoes.toLocaleString('pt-BR')}</span>
            </div>
            <div>
              <small>índice do recorte</small>
              <span><code>cliente_ts_idx</code></span>
            </div>
          </div>
        )}
        <div className="geo-controles">
          <label>limite (km/h)
            <select value={limiteKmh} onChange={e => setLimiteKmh(Number(e.target.value))}>
              {LIMITES_KMH.map(v => <option key={v} value={v}>{v} km/h</option>)}
            </select>
          </label>
          <button className="btn btn-sm btn-primary" onClick={() => rodarViagens(false)} disabled={ocupado.viagens}>
            {ocupado.viagens ? <><span className="spinner" /> Calculando…</> : 'Varrer a coleção inteira'}
          </button>
          <label>recorte por cliente
            <select value={clienteFiltro} onChange={e => setClienteFiltro(e.target.value)}>
              <option value="">— escolha um cliente —</option>
              {clientesPlantados.map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </label>
          <button className="btn btn-sm" onClick={() => rodarViagens(true)}
            disabled={ocupado.viagens || !clienteFiltro.trim()}
            title="O mesmo pipeline sobre um cliente só: é o recorte que mantém isto barato em produção">
            Só este cliente
          </button>
          {viagens && (
            <>
              <span className="badge badge-red">
                {viagens.encontrados} sinalizadas acima de {viagens.limite_kmh} km/h
                {viagens.truncado && ' (truncado)'}
              </span>
              <span className="badge badge-green">
                {viagens.encontrados_aprovados} aprovadas dentro do limite
              </span>
            </>
          )}
          {/* O custo medido fica ao lado do resultado, não numa nota de rodapé:
              é a primeira pergunta de quem tem 90 dias reais de histórico. */}
          {viagens?.custo && (
            <span className="badge badge-gray" title={viagens.custo.complexidade}>
              {fmtDuracao(viagens.custo.ms)} sobre {viagens.custo.escopo}
            </span>
          )}
        </div>

        {viagens && (
          <div className="row" style={{ marginTop: 16, alignItems: 'flex-start' }}>
            <div className="col" style={{ minWidth: 320 }}>
              <div className="geo-tabela-wrap">
                <table className="geo-tabela">
                  <thead>
                    <tr><th>cliente</th><th>km</th><th>min</th><th>km/h</th><th>trajeto</th><th>decisão</th></tr>
                  </thead>
                  <tbody>
                    {viagens.resultados.map(v => (
                      <tr key={v.endToEndId}
                        className={viagemSel?.endToEndId === v.endToEndId ? 'sel' : ''}
                        onClick={() => setViagemSel(v)}>
                        <td><code>{v.clienteId}</code></td>
                        <td>{v.km}</td>
                        <td>{v.minutos}</td>
                        <td style={{ color: v.classificacao === 'sinalizada' ? '#ff6960' : 'inherit', fontWeight: 700 }}>
                          {v.kmh}
                        </td>
                        <td>{v.de.municipio} → {v.para.municipio}</td>
                        {/* Duas faces da mesma decisão: sinalizada acima do
                            limite, aprovada dentro dele. */}
                        <td>
                          <span className={`badge ${v.classificacao === 'sinalizada' ? 'badge-red' : 'badge-green'}`}>
                            {v.classificacao === 'sinalizada' ? 'sinalizada' : 'aprovada'}
                          </span>
                        </td>
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
                    onClick={() => setClienteFiltro(viagemSel.clienteId)}>
                    investigar este cliente
                  </button>
                </div>
              )}
            </div>
          </div>
        )}

        {viagens && (
          <div style={{ marginTop: 12 }}>
            {/* A pergunta de risco vem antes da de engenharia: quantos casos
                isto joga na fila? Sem denominador, "40 pares" não diz se a
                regra é seletiva ou se inunda a operação. */}
            {viagens.seletividade && (
              <div className="geo-seletividade">
                <div>
                  <span>{(viagens.seletividade.pares_avaliados ?? 0).toLocaleString('pt-BR')}</span>
                  <small>pares consecutivos avaliados</small>
                </div>
                <div>
                  <span style={{ color: '#ff6960' }}>{viagens.seletividade.sinalizados}</span>
                  <small>sinalizados acima de {viagens.limite_kmh} km/h</small>
                </div>
                <div>
                  <span>{viagens.seletividade.taxa_pct != null
                    ? `${viagens.seletividade.taxa_pct.toLocaleString('pt-BR', { maximumFractionDigits: 4 })}%`
                    : '—'}</span>
                  <small>taxa de sinalização</small>
                </div>
                {viagens.seletividade.alertas_por_dia != null && (
                  <div>
                    <span>{viagens.seletividade.alertas_por_dia.toLocaleString('pt-BR')}</span>
                    <small>alertas por dia em {viagens.seletividade.janela_dias} dias de histórico</small>
                  </div>
                )}
              </div>
            )}
            {viagens.seletividade?.nota && (
              <p className="geo-nota-seletividade">
                {viagens.seletividade.nota}
                {/* A taxa é consequência do que o seed plantou. Deixar isso
                    implícito convida o analista a comparar com o número dele. */}
                {viagens.seletividade.aviso && (
                  <> <strong>{viagens.seletividade.aviso}</strong></>
                )}
              </p>
            )}

            <QueryBlock label="Ver pipeline completo"
              query={JSON.stringify(viagens.pipeline, null, 2)} />
          </div>
        )}
      </section>

      {/* ── 02 · Operadores de consulta ─────────────────────────────────── */}
      <section className="card">
        <div className="kicker" style={{ marginBottom: 8, color: '#00ED64' }}>02 · Operadores de consulta geoespacial</div>
        <h2 style={{ fontSize: 18, marginBottom: 6 }}>
          Os cinco operadores geo do MongoDB, sobre a mesma área
        </h2>
        <p style={{ color: 'var(--text-secondary)', fontSize: 13, marginBottom: 14 }}>
          Um centro e um raio geram uma geometria; os cinco rodam sobre ela ao mesmo tempo —{' '}
          <code>$geoWithin</code>, <code>$geoIntersects</code>, <code>$near</code>,{' '}
          <code>$nearSphere</code> (todos com <code>$geometry</code>) e <code>$geoNear</code>{' '}
          (o estágio de agregação, que combina com outros estágios e devolve distância).
        </p>

        <div className="geo-controles">
          <label>centro
            <select value={centroIdx} onChange={e => setCentroIdx(Number(e.target.value))}>
              {municipios.map((m, i) => <option key={i} value={i}>{m.municipio}/{m.uf}</option>)}
            </select>
          </label>
          <label>raio (km)
            <select value={raioOperadores} onChange={e => setRaioOperadores(Number(e.target.value))}>
              {RAIOS_OPERADORES_KM.map(v => <option key={v} value={v}>{v} km</option>)}
            </select>
          </label>
          <button className="btn btn-sm btn-primary" onClick={rodarOperadores} disabled={ocupado.operadores || !centro}>
            {ocupado.operadores ? <><span className="spinner" /> Rodando…</> : 'Rodar os 5 operadores'}
          </button>
        </div>

        {operadores && (
          <div className="row" style={{ marginTop: 16, alignItems: 'stretch', flexWrap: 'wrap' }}>
            {operadores.resultados.map(r => (
              <div key={r.operador} className="col card" style={{ minWidth: 260, flex: '1 1 260px' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
                  <code style={{ fontSize: 14, fontWeight: 700 }}>{r.operador}</code>
                  <span className="badge badge-gray">
                    {r.contagem == null ? 'sem contagem' : `${r.contagem} documentos`}
                  </span>
                </div>
                <p style={{ color: 'var(--text-secondary)', fontSize: 12, marginTop: 6, marginBottom: 8 }}>
                  {r.descricao}
                </p>
                {r.amostra.length > 0 ? (
                  <>
                    {/* endToEndId à vista: é o identificador real do documento no
                        cluster, não um rótulo inventado pra tela — prova que a
                        amostra veio da consulta, não de um mock. */}
                    <p style={{ color: 'var(--text-disabled)', fontSize: 11, margin: '0 0 4px' }}>
                      amostra real de {r.contagem != null ? r.contagem : 'N'} documentos:
                    </p>
                    <ul style={{ fontSize: 12, paddingLeft: 16, margin: 0, lineHeight: 1.7 }}>
                      {r.amostra.map(a => (
                        <li key={a.endToEndId}>
                          <code style={{ fontSize: 11 }}>{a.endToEndId}</code> · {a.municipio}/{a.uf}
                          {a.distanciaMetros != null && ` · ${(a.distanciaMetros / 1000).toFixed(1)} km`}
                        </li>
                      ))}
                    </ul>
                  </>
                ) : (
                  <p style={{ color: 'var(--text-secondary)', fontSize: 12 }}>Nenhum documento nesta área.</p>
                )}
                <div style={{ marginTop: 8 }}>
                  <QueryBlock label="Ver query" query={JSON.stringify(r.query, null, 2)} />
                </div>
              </div>
            ))}
          </div>
        )}
      </section>
    </div>
  )
}
