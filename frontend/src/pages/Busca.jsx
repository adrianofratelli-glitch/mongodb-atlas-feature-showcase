import React, { useCallback, useEffect, useState } from 'react'
import { useApi } from '../hooks/useApi'
import { useIntervaloVisivel } from '../hooks/usePolling'
import Limites from '../components/Limites'
import QueryBlock from '../components/QueryBlock'
import { fmtMs, fmtReais, num } from '../lib/formato'

// Módulo 08 · Busca (MongoDB Search, antes "Atlas Search").
// O índice é próprio (showcase_busca): no banco da demo, `produtos` também tem
// o índice de outra PoV, que esta tela nunca usa nem remove.
// $search com índice inexistente devolve vazio sem erro; por isso a busca só
// roda com o índice pronto, e a tela diz o estado em vez de mostrar "0 resultados".

const ROTULO_ESTADO = {
  pronto: { classe: 'badge-green', texto: 'pronto' },
  construindo: { classe: 'badge-yellow', texto: 'em construção' },
  ausente: { classe: 'badge-gray', texto: 'ausente' },
  falhou: { classe: 'badge-red', texto: 'falhou' },
  falhou_na_consulta: { classe: 'badge-red', texto: 'consulta recusada' },
  indisponivel: { classe: 'badge-red', texto: 'indisponível neste cluster' },
}

const SUGESTOES = ['notebok', 'camizeta', 'fone smart', 'cadeiras']

export default function Busca() {
  const poll = useApi()
  const acao = useApi()
  const [indice, setIndice] = useState(null)
  const [comando, setComando] = useState(null)
  const [q, setQ] = useState('notebok')
  const [fuzzy, setFuzzy] = useState(true)
  const [soEstoque, setSoEstoque] = useState(false)
  const [resp, setResp] = useState(null)

  const carregarIndice = useCallback(async () => {
    const d = await poll.call('/busca/indice')
    if (d) setIndice(d)
  }, [poll.call])

  useEffect(() => { carregarIndice() }, [carregarIndice])
  // Só consulta o estado enquanto o índice estiver construindo.
  useIntervaloVisivel(carregarIndice, 5000, indice?.estado === 'construindo')

  const criar = async () => {
    const d = await acao.call('/busca/indice', { method: 'POST' })
    if (d) { setComando(d.query || null); setIndice(prev => ({ ...prev, ...d })) }
  }

  const buscar = async (termo = q) => {
    const params = new URLSearchParams({ q: termo, fuzzy: String(fuzzy), so_em_estoque: String(soEstoque) })
    const d = await acao.call(`/busca?${params}`)
    if (d) { setResp(d); if (d.indice) setIndice(prev => ({ ...prev, ...d.indice })) }
  }

  const estado = indice?.estado
  const rotulo = ROTULO_ESTADO[estado] || { classe: 'badge-gray', texto: '…' }
  const pronto = estado === 'pronto'

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 24 }}>
      <div className="card" style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
          <strong style={{ fontSize: 14 }}>Índice de busca <code>{indice?.nome || 'showcase_busca'}</code> em <code>produtos</code></strong>
          <span className={`badge ${rotulo.classe}`}>{rotulo.texto}</span>
          {estado === 'ausente' && (
            <button className="btn btn-primary" onClick={criar} disabled={acao.loading}>
              {acao.loading ? <><span className="spinner" /> Criando…</> : 'Criar índice'}
            </button>
          )}
        </div>
        {estado === 'construindo' && (
          <p style={{ fontSize: 13, color: 'var(--text-secondary)' }}>
            O Atlas está construindo o índice a partir da coleção. A coleção segue atendendo leituras e escritas.
          </p>
        )}
        {estado === 'indisponivel' && (
          <p style={{ fontSize: 13, color: 'var(--text-secondary)' }}>
            Este servidor não respondeu a <code>$listSearchIndexes</code>. O módulo exige um cluster Atlas.
          </p>
        )}
        {indice?.definicao && (
          <QueryBlock label="Ver definição do índice" query={JSON.stringify(comando || indice.definicao, null, 2)} />
        )}
      </div>

      <div className="card" style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
        <form onSubmit={(e) => { e.preventDefault(); buscar() }} style={{ display: 'flex', gap: 10, flexWrap: 'wrap', alignItems: 'center' }}>
          <label htmlFor="busca-q" style={{ fontSize: 13 }}>Buscar no nome</label>
          <input id="busca-q" value={q} maxLength={80} onChange={(e) => setQ(e.target.value)}
            style={{ flex: '1 1 220px', padding: '8px 10px', borderRadius: 8, border: '1px solid var(--border-subtle)', background: 'var(--bg-secondary, transparent)', color: 'var(--text-primary)' }} />
          <label style={{ fontSize: 13, display: 'flex', gap: 6, alignItems: 'center' }}>
            <input type="checkbox" checked={fuzzy} onChange={(e) => setFuzzy(e.target.checked)} /> tolerar 1 erro de digitação
          </label>
          <label style={{ fontSize: 13, display: 'flex', gap: 6, alignItems: 'center' }}>
            <input type="checkbox" checked={soEstoque} onChange={(e) => setSoEstoque(e.target.checked)} /> só em estoque
          </label>
          <button className="btn btn-primary" type="submit" disabled={acao.loading || !q.trim()}>
            {acao.loading ? <><span className="spinner" /> Buscando…</> : 'Buscar'}
          </button>
        </form>
        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', fontSize: 12 }}>
          {SUGESTOES.map(s => (
            <button key={s} className="btn" type="button" onClick={() => { setQ(s); buscar(s) }} disabled={acao.loading}>{s}</button>
          ))}
        </div>

        {resp && !resp.executado && (
          <div className="banner banner-warning" role="status">
            <span aria-hidden="true">⚠</span>
            <div>{resp.aviso}</div>
          </div>
        )}

        {resp?.executado && (
          <>
            <div style={{ fontSize: 13, color: 'var(--text-secondary)' }}>
              <strong style={{ color: 'var(--text-primary)' }}>{num(resp.total)}</strong> produtos para “{resp.q}”
              {resp.fuzzy ? ' (fuzzy, maxEdits 1)' : ' (termo exato, com stemming do analisador português)'}
              {' · '}resultados {fmtMs(resp.tempos_ms.resultados)} · facetas {fmtMs(resp.tempos_ms.facetas)}
            </div>
            {resp.resultados.length > 0 && (
              <table className="lg-table">
                <thead><tr><th>Nome</th><th>Categoria</th><th>Marca</th><th>Preço</th><th>Estoque</th><th>Score</th></tr></thead>
                <tbody>
                  {resp.resultados.map((r, i) => (
                    <tr key={i}>
                      <td>{r.nome}</td><td>{r.categoria}</td><td>{r.marca}</td>
                      <td>{fmtReais(r.preco)}</td><td>{r.em_estoque ? 'sim' : 'não'}</td><td>{r.score == null ? '—' : Number(r.score).toLocaleString('pt-BR', { minimumFractionDigits: 3, maximumFractionDigits: 3 })}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
            {resp.facetas && (
              <div className="grid-2" style={{ fontSize: 13 }}>
                {['categoria', 'marca'].map(nome => (
                  <div key={nome}>
                    <strong style={{ textTransform: 'capitalize' }}>{nome}</strong>
                    <ul style={{ margin: '6px 0 0', paddingLeft: 18 }}>
                      {(resp.facetas[nome] || []).map(b => <li key={b.valor}>{b.valor}: {num(b.n)}</li>)}
                    </ul>
                  </div>
                ))}
              </div>
            )}
          </>
        )}
        {resp && (
          <QueryBlock
            label={resp.executado ? 'Ver pipelines executados ($search + $searchMeta)' : 'Ver pipelines (não executados)'}
            query={JSON.stringify(resp.executado ? resp.pipeline : resp.pipeline_nao_executado, null, 2)}
          />
        )}
        {!pronto && !resp && (
          <p style={{ fontSize: 13, color: 'var(--text-secondary)' }}>A busca roda quando o índice estiver pronto.</p>
        )}
      </div>

      <Limites titulo="Limites da busca" itens={[
        <>O índice é <strong>eventualmente consistente</strong>: o MongoDB Search lê os change streams da coleção e indexa de forma assíncrona. Um documento recém-gravado pode ainda não aparecer no <code>$search</code>.</>,
        <><code>$search</code> com índice inexistente (ou campo não indexado) devolve <strong>vazio, sem erro</strong>. Confira o índice com <code>$listSearchIndexes</code> antes de concluir que não há resultado.</>,
        <>Criar ou reconstruir o índice consome recursos do cluster; com muitos índices de busca, cresce a latência de indexação.</>,
        <><code>$search</code> precisa ser o primeiro estágio do pipeline e não roda dentro de <code>$facet</code>; facetas vêm de <code>$searchMeta</code> ou do operador <code>facet</code>.</>,
        <>O fuzzy aceita no máximo 2 edições por termo; aqui usamos 1. Busca semântica (Vector Search) não faz parte deste módulo.</>,
      ]} />
    </div>
  )
}
