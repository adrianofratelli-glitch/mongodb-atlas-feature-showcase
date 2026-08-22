import React, { useCallback, useEffect, useMemo, useState } from 'react'
import { useApi } from '../hooks/useApi'
import { useIntervaloVisivel } from '../hooks/usePolling'
import QueryBlock from '../components/QueryBlock'
import MiniMapa from '../components/MapaBrasil'

const CATEGORIAS = ['alimentação', 'combustível', 'farmácia', 'vestuário', 'serviços']

// "2239.8 ms" obriga a plateia a contar casas; acima de 1 s a unidade muda.
const fmtDuracao = (ms) => (ms == null
  ? '—'
  : ms >= 1000 ? `${(ms / 1000).toFixed(1).replace('.', ',')} s` : `${Math.round(ms)} ms`)

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
  const { call } = useApi()
  // Instância separada para o poll do painel ao vivo. `loading` do useApi é um
  // único sinal para TODAS as chamadas do componente: com o poll de 4 s na mesma
  // instância, os três botões da aba entravam em "carregando" e ficavam
  // DESABILITADOS por ~1 s a cada ciclo, sem ninguém ter clicado. No palco isso
  // é um clique que não faz nada.
  const { call: callPoll } = useApi()
  // Cada ação controla o próprio estado: rodar a detecção não pode desabilitar
  // a busca do painel ao lado.
  const [ocupado, setOcupado] = useState({})
  const comOcupado = async (chave, fn) => {
    setOcupado((o) => ({ ...o, [chave]: true }))
    try { return await fn() } finally { setOcupado((o) => ({ ...o, [chave]: false })) }
  }
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
  const [clienteFiltro, setClienteFiltro] = useState('')
  // Guarda o custo dos dois escopos para a comparação ficar visível mesmo depois
  // de trocar de consulta — é o número que responde "e sobre 90 dias reais?".
  const [custoPorEscopo, setCustoPorEscopo] = useState({})

  // 02 — contexto para investigação (geo + Atlas Search)
  // Sem termo por padrão: a pergunta é o entorno do terminal, e o nome é um
  // refinamento em cima dela.
  const [termo, setTermo] = useState('')
  const [ancoraId, setAncoraId] = useState('')
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
    const d = await callPoll('/geo/sinais-ao-vivo')
    if (d) setAoVivo(d)
  }, [callPoll]), 4000, true)

  const centro = municipios[centroIdx]?.centro || null

  const rodarExplain = () => comOcupado('explain', async () => {
    if (!centro) return
    const d = await call('/geo/explain-compare', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ clienteId, status: statusTx, raioKm: Number(raioExplain), centro }),
    })
    if (d) setExplain(d)
  })

  // O recorte é a resposta à pergunta de escala: filtrar antes da janela reduz
  // o universo varrido, e o custo medido de cada modo fica lado a lado na tela.
  const rodarViagens = (filtrarCliente = false) => comOcupado('viagens', async () => {
    const alvo = filtrarCliente ? clienteFiltro.trim() : ''
    const query = `limiteKmh=${Number(limiteKmh)}${alvo ? `&clienteId=${encodeURIComponent(alvo)}` : ''}`
    const d = await call(`/geo/impossible-travel?${query}`)
    if (d) {
      setViagens(d); setViagemSel(null)
      setCustoPorEscopo((prev) => ({ ...prev, [alvo ? 'cliente' : 'colecao']: d.custo }))
    }
  })

  const rodarBusca = () => comOcupado('busca', async () => {
    if (!ancoraId.trim() && !centro) return
    const d = await call('/geo/search', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        termo,
        // Com uma compra escolhida o backend deriva o centro do terminal dela.
        ...(ancoraId.trim() ? { endToEndId: ancoraId.trim() } : { centro }),
        raioKm: Number(raioBusca),
        categorias,
      }),
    })
    if (d) setBusca(d)
  })

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
          Um segundo processor do <strong>Atlas Stream Processing</strong> lê o mesmo change stream do
          módulo 07, agrupa por cartão em janela de 30 s e calcula haversine ali dentro. O sinal cai em{' '}
          <code>{aoVivo?.colecao || 'geo.sinais_ao_vivo'}</code> em tempo de evento.
        </p>

        {aoVivo?.total > 0 && (
          <div className="geo-live-contagem">
            <div>
              <span>{aoVivo.plantados}</span>
              <small>plantados pelo gerador — sinal garantido para a demo</small>
            </div>
            <div>
              <span style={{ color: aoVivo.emergentes ? '#00ED64' : 'var(--text-secondary)' }}>{aoVivo.emergentes}</span>
              <small>
                emergentes — ninguém armou; o pipeline achou no tráfego.{' '}
                <strong>Zero é o resultado esperado</strong> com dado sintético.
              </small>
            </div>
          </div>
        )}

        {/* Um contador que quase sempre marca zero precisa dizer por quê ANTES
            de alguém perguntar. Caso contrário "0 emergentes" é lido como "só
            acha o que vocês plantaram" — e a leitura correta é o contrário:
            tráfego aleatório não fabrica coincidência, então um emergente,
            quando aparece, é achado de verdade. */}
        {aoVivo?.total > 0 && (
          <div className="banner banner-info" style={{ marginTop: 12, marginBottom: 0 }}>
            <span>ℹ️</span>
            <div>
              Um sinal <strong>emergente</strong> exige duas compras do <em>mesmo cartão</em>, longe uma da
              outra, dentro da mesma janela de 30 s. O gerador sorteia cada compra entre milhares de cartões
              independentes, então essa coincidência praticamente não ocorre: <strong>o normal é zero</strong>,
              e é assim que se sabe que o número plantado não está sendo inflado. Com tráfego real de um
              emissor — mesmo cartão comprando várias vezes por dia — é este contador que se move, sem trocar
              uma linha do processor.
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
        {/* Posicionamento explícito. Sem esta frase a aba soa como se disputasse
            com o motor antifraude do cliente — uma disputa que ela perde e que
            não precisa travar: o argumento é a cópia de dados que some. */}
        <div className="banner banner-info" style={{ marginBottom: 14 }}>
          <span>🧭</span>
          <div>
            <strong>Isto não é um motor antifraude e não substitui o que já existe.</strong>{' '}
            Um emissor tem regras, escore comportamental e modelos treinados com fraude confirmada —
            nada disso está aqui. O que muda é <strong>onde a conta acontece</strong>: o sinal
            geográfico deixa de exigir uma cópia do histórico num motor à parte, com CDC, contrato e
            operação próprios, e passa a sair da mesma base que já registra a transação.
          </div>
        </div>

        <div className="geo-controles">
          <label>limite (km/h)
            <input type="number" min="1" value={limiteKmh} onChange={e => setLimiteKmh(e.target.value)} />
          </label>
          <button className="btn btn-sm btn-primary" onClick={() => rodarViagens(false)} disabled={ocupado.viagens}>
            {ocupado.viagens ? <><span className="spinner" /> Calculando…</> : 'Varrer a coleção inteira'}
          </button>
          <label>recorte por cliente
            <input value={clienteFiltro} placeholder="CLI00007"
              onChange={e => setClienteFiltro(e.target.value)} />
          </label>
          <button className="btn btn-sm" onClick={() => rodarViagens(true)}
            disabled={ocupado.viagens || !clienteFiltro.trim()}
            title="O mesmo pipeline sobre um cliente só: é o recorte que mantém isto barato em produção">
            Só este cliente
          </button>
          {viagens && (
            <span className="badge badge-red">
              {viagens.encontrados} pares acima de {viagens.limite_kmh} km/h
              {viagens.truncado && ' (truncado)'}
            </span>
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
                    <tr><th>cliente</th><th>km</th><th>min</th><th>km/h</th><th>trajeto</th><th>origem</th></tr>
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
                        {/* Mesma honestidade do painel em event time: o que o
                            seed plantou aparece marcado como plantado. */}
                        <td>
                          <span className={`badge ${v.origem === 'emergente' ? 'badge-green' : 'badge-gray'}`}>
                            {v.origem}
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
                  {/* Leva o cliente para os dois lugares onde ele é útil: o
                      recorte barato deste painel e a comparação de planos. */}
                  <button className="btn btn-xs btn-ghost" style={{ marginLeft: 8 }}
                    onClick={() => { setClienteId(viagemSel.clienteId); setClienteFiltro(viagemSel.clienteId) }}>
                    investigar este cliente
                  </button>
                  {/* O caminho que o analista percorre: do sinal para o entorno
                      do terminal onde a compra aconteceu. */}
                  <button className="btn btn-xs btn-ghost" style={{ marginLeft: 6 }}
                    onClick={() => { setAncoraId(viagemSel.endToEndId); setTermo('') }}
                    title="Leva esta compra para o painel 02 como a transação contestada">
                    ver o entorno desta compra
                  </button>
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
                {viagens.origem && (
                  <div>
                    <span style={{ color: viagens.origem.emergentes ? '#00ED64' : 'var(--text-secondary)' }}>
                      {viagens.origem.plantados}/{viagens.origem.emergentes}
                    </span>
                    <small>plantados / emergentes</small>
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

            {/* Escala é a objeção real deste painel, e ela vem antes de qualquer
                elogio ao pipeline. Melhor responder de frente do que deixar o
                cliente calcular sozinho e desistir em silêncio. */}
            {viagens.custo && (
              <div className="banner banner-info" style={{ marginBottom: 10 }}>
                <span>⏱️</span>
                <div>
                  <strong>Custo desta execução: {fmtDuracao(viagens.custo.ms)} sobre {viagens.custo.escopo}.</strong>{' '}
                  {viagens.custo.complexidade} {viagens.custo.memoria} {viagens.custo.leitura}
                  {custoPorEscopo.colecao && custoPorEscopo.cliente && (
                    <div style={{ marginTop: 6 }}>
                      <strong>Medido nos dois escopos:</strong>{' '}
                      coleção inteira <strong>{fmtDuracao(custoPorEscopo.colecao.ms)}</strong> ·
                      {' '}um cliente <strong>{fmtDuracao(custoPorEscopo.cliente.ms)}</strong>
                      {custoPorEscopo.cliente.ms > 0 && (
                        <> — <strong>{(custoPorEscopo.colecao.ms / custoPorEscopo.cliente.ms)
                          .toFixed(1).replace('.', ',')}× </strong>
                        mais barato. É esse recorte, e não hardware, que mantém o pipeline viável
                        sobre um histórico real.</>
                      )}
                    </div>
                  )}
                </div>
              </div>
            )}
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
        <div className="kicker" style={{ marginBottom: 8, color: '#a855f7' }}>02 · Contestação</div>
        {/* A pergunta era "ache uma padaria", que é demo de catálogo. Quem abre
            uma disputa parte da COMPRA CONTESTADA e pergunta o que existe em
            volta daquele terminal. A prova técnica é a mesma — uma stage de
            $search com texto, geoWithin e facetas —, mas agora responde à
            pergunta que um analista faz de verdade. */}
        <h2 style={{ fontSize: 18, marginBottom: 6 }}>
          O portador contesta esta compra. O que existe em volta do terminal?
        </h2>
        <p style={{ color: 'var(--text-secondary)', fontSize: 13, marginBottom: 14 }}>
          Escolha um caso sinalizado acima e o centro passa a ser a coordenada{' '}
          <strong>daquele terminal</strong>. Um <code>$search</code> responde o entorno:{' '}
          <code>geoWithin</code> + categoria em <code>filter</code>, facetas em <code>$searchMeta</code>,{' '}
          <code>fuzzy</code> de nome para estabelecimento clonado.{' '}
          <strong>Sem um motor de busca ao lado, sincronizado por CDC.</strong>
        </p>

        <div className="geo-controles">
          <label>compra contestada (endToEndId)
            <input value={ancoraId} placeholder="cole ou escolha um caso acima"
              onChange={e => setAncoraId(e.target.value)} style={{ minWidth: 230 }} />
          </label>
          <label>raio: {raioBusca} km
            <input type="range" min="1" max="300" value={raioBusca}
              onChange={e => setRaioBusca(e.target.value)} />
          </label>
          <label>refinar por nome (opcional)
            <input value={termo} placeholder="ex.: nome parecido com o do recibo"
              onChange={e => setTermo(e.target.value)} />
          </label>
          <button className="btn btn-sm btn-primary" onClick={rodarBusca}
            disabled={ocupado.busca || (!ancoraId.trim() && !centro)}>
            {ocupado.busca ? <><span className="spinner" /> Buscando…</> : 'Ver o entorno'}
          </button>
          {!ancoraId.trim() && (
            <span style={{ fontSize: 11.5, color: 'var(--text-disabled)' }}>
              sem uma compra escolhida, o centro cai no município selecionado no painel de planos
            </span>
          )}
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

        {busca?.ancora && (
          <div className="geo-ancora">
            <div className="kicker" style={{ color: '#a855f7', marginBottom: 6 }}>compra contestada</div>
            <div className="geo-ancora-linha">
              <strong>{busca.ancora.estabelecimento?.nome}</strong>
              <span className="badge badge-gray">{busca.ancora.estabelecimento?.categoria}</span>
              <span>R$ {busca.ancora.valor}</span>
              <span>{busca.ancora.municipio}/{busca.ancora.uf}</span>
              <code>{busca.ancora.dispositivo?.id}</code>
              <span>{busca.ancora.status}</span>
            </div>
            <div className="geo-ancora-nota">
              Centro do raio: a coordenada cadastral <strong>deste terminal</strong>
              {busca.ancora.localizacaoMeta?.origem
                ? <> (<code>{busca.ancora.localizacaoMeta.origem}</code>)</> : null}
              {' '}· resultados ordenados por {busca.ordenacao}.
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
                <div key={r.terminalId || r.endToEndId}
                  className={`result-row${r.e_a_ancora ? ' geo-e-ancora' : ''}`}>
                  <strong style={{ fontSize: 13 }}>{r.estabelecimento.nome}</strong>
                  <span className="badge badge-gray" style={{ marginLeft: 8 }}>{r.estabelecimento.categoria}</span>
                  {r.e_a_ancora && (
                    <span className="badge badge-purple" style={{ marginLeft: 6 }}>
                      terminal da compra contestada
                    </span>
                  )}
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
              <MiniMapa pontos={pontosBusca} ajustar
                circulo={busca?.centro ? { centro: busca.centro, raioKm: Number(raioBusca) } : null} />
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
          <button className="btn btn-sm btn-primary" onClick={rodarExplain} disabled={ocupado.explain || !centro}>
            {ocupado.explain ? <><span className="spinner" /> Executando…</> : 'Comparar planos'}
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
          Onde o geo do MongoDB não vai
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
