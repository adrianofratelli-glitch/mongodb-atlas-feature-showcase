import React, { useState } from 'react'
import { useApi } from '../hooks/useApi'

// Página de abertura, curta de propósito. O apresentador narra; a tela só
// precisa fixar o enquadramento para que a demo não seja lida como catálogo de
// features — e declarar os não-objetivos, que é o que dá crédito ao resto.
//
// "Medir agora" executa uma operação real de cada capacidade pelo mesmo
// MongoClient, no mesmo cluster (POST /tese/medir). Nenhum número é fixo aqui:
// sem medição, a tabela não aparece; com falha, a linha diz o que falhou.

export default function Tese() {
  const { call } = useApi()
  const [medicao, setMedicao] = useState(null)
  const [estado, setEstado] = useState('ocioso') // ocioso | medindo | erro
  const [erro, setErro] = useState('')

  const medir = async () => {
    if (estado === 'medindo') return
    setEstado('medindo')
    setErro('')
    const d = await call('/tese/medir', { method: 'POST', timeoutMs: 60_000 })
    if (d) {
      setMedicao(d)
      setEstado('ocioso')
    } else {
      setEstado('erro')
      setErro('A medição não voltou. Confira o pré-voo (backend na porta 8002 e cluster alcançável) e tente de novo.')
    }
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <div className="card" style={{ padding: '28px', borderColor: 'rgba(0,237,100,.28)' }}>
        <p style={{ fontSize: 22, lineHeight: 1.45, color: 'var(--text-primary)', margin: 0, maxWidth: 760 }}>
          O argumento é <strong style={{ color: 'var(--accent)' }}>convergência</strong>: sete capacidades,
          uma plataforma e consultas na mesma linguagem.
        </p>
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginTop: 22 }}>
          <span className="badge badge-green">cluster real</span>
          <span className="badge badge-gray">dados de demonstração</span>
          <span className="badge badge-gray">não é benchmark</span>
        </div>
        <p style={{ fontSize: 13, color: 'var(--text-secondary)', marginTop: 16, marginBottom: 0 }}>
          Prova funcional: não estima economia, não substitui o warehouse e não define capacidade de produção.
        </p>
      </div>

      <section className="card" aria-labelledby="tese-medir-titulo">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
          <div>
            <h2 id="tese-medir-titulo" style={{ fontSize: 15, margin: 0 }}>Uma conexão, várias capacidades — medido agora</h2>
            <p style={{ fontSize: 12.5, color: 'var(--text-secondary)', margin: '4px 0 0' }}>
              Uma operação real de cada capacidade, pelo mesmo <code>MongoClient</code>, no mesmo cluster. 5 repetições, p50 e máximo.
            </p>
          </div>
          <button className="btn btn-primary" onClick={medir} disabled={estado === 'medindo'} aria-busy={estado === 'medindo'}>
            {estado === 'medindo' ? 'Medindo…' : medicao ? 'Medir de novo' : 'Medir agora'}
          </button>
        </div>

        <div aria-live="polite">
          {estado === 'erro' && (
            <div className="banner banner-error" role="alert" style={{ marginTop: 12 }}>
              <span aria-hidden="true">⚠</span><div><strong>Falhou:</strong> {erro}</div>
            </div>
          )}
          {!medicao && estado === 'ocioso' && (
            <p style={{ fontSize: 12.5, color: 'var(--text-secondary)', marginTop: 12, marginBottom: 0 }}>
              Nenhuma medição ainda. Clique em “Medir agora”: leva poucos segundos e grava só em coleções <code>tese_probe*</code>, removidas ao final.
            </p>
          )}
          {medicao && (
            <div style={{ marginTop: 14 }}>
              <div className="tese-tabela-wrap">
                <table className="tese-tabela">
                  <caption className="sr-only">Latência medida por capacidade</caption>
                  <thead>
                    <tr><th scope="col">Capacidade</th><th scope="col">p50</th><th scope="col">máx</th><th scope="col">Evidência</th></tr>
                  </thead>
                  <tbody>
                    {medicao.resultados.map(r => (
                      <tr key={r.chave}>
                        <td>{r.capacidade}</td>
                        {r.ok ? (
                          <>
                            <td><code>{r.p50_ms} ms</code></td>
                            <td><code>{r.max_ms} ms</code></td>
                            <td>{r.evidencia}</td>
                          </>
                        ) : (
                          <td colSpan={3}><span className="badge badge-red">falhou</span> {r.erro}</td>
                        )}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <p style={{ fontSize: 12, color: 'var(--text-secondary)', marginTop: 8 }}>
                {medicao.conexoes.clientes_mongo} cliente MongoDB · {medicao.conexoes.nos_do_replica_set} nós do replica set ·
                banco <code>{medicao.banco}</code> · {new Date(medicao.medido_em).toLocaleTimeString('pt-BR')}. {medicao.aviso}
              </p>
              <p style={{ fontSize: 12, color: 'var(--text-secondary)', marginTop: 4 }}>
                Medido nos próprios módulos, não aqui: {medicao.nao_medido_aqui.map(n => `${n.capacidade} (${n.onde})`).join('; ')}.
              </p>
            </div>
          )}
        </div>

        <details style={{ marginTop: 12 }}>
          <summary style={{ cursor: 'pointer', fontSize: 13, fontWeight: 600 }}>
            O que cada capacidade exige fora do Atlas (desenho de arquitetura, não medição)
          </summary>
          <div className="tese-tabela-wrap" style={{ marginTop: 10 }}>
            <table className="tese-tabela">
              <thead><tr><th scope="col">Capacidade</th><th scope="col">No Atlas</th><th scope="col">Stack montada por peças</th></tr></thead>
              <tbody>
                {(medicao?.componentes || COMPONENTES_FALLBACK).map(c => (
                  <tr key={c.capacidade}><td>{c.capacidade}</td><td>{c.atlas}</td><td>{c.stack}</td></tr>
                ))}
              </tbody>
            </table>
          </div>
        </details>
      </section>
    </div>
  )
}

// Mesmo conteúdo de GET /tese/componentes; serve antes da primeira medição.
const COMPONENTES_FALLBACK = [
  { capacidade: 'Consulta indexada + agregação', atlas: 'cluster', stack: 'banco operacional' },
  { capacidade: 'Validação de schema no banco', atlas: 'cluster', stack: 'validação na aplicação ou banco relacional' },
  { capacidade: 'Change Streams (CDC ordenado)', atlas: 'cluster', stack: 'conector CDC + broker' },
  { capacidade: 'Transação multi-documento', atlas: 'cluster', stack: 'banco transacional' },
  { capacidade: 'Hot/cold tiering consultável', atlas: 'Online Archive (mesmo namespace)', stack: 'object storage + motor de consulta federada' },
  { capacidade: 'Janelas sobre o fluxo', atlas: 'Atlas Stream Processing', stack: 'processador de stream dedicado' },
]
