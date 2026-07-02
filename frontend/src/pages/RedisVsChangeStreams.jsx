import React from 'react'
import LiveBattle from '../components/LiveBattle'
import ArchComplexity from '../components/ArchComplexity'
import SourceOfTruthDemo from '../components/SourceOfTruthDemo'
import { Tooltip } from '../components/DemoFlow'

// Placar da Etapa 3 — acumula a partir das ops JÁ reveladas (feed ao vivo).
const count = (ops, pred) => ops.filter(pred).length

const ETAPA3 = {
  redis: (ops) => [
    { label: 'consistentes', value: count(ops, o => o.redis.tag === 'consistente'), tone: 'idle' },
    { label: 'inconsistentes', value: count(ops, o => o.redis.tag === 'INCONSISTENTE'), tone: count(ops, o => o.redis.tag === 'INCONSISTENTE') ? 'red' : 'idle' },
  ],
  mongo: (ops) => [
    { label: 'consistentes', value: ops.length, tone: 'green' },
    { label: 'inconsistentes', value: 0, tone: ops.length ? 'green' : 'idle' },
  ],
}

// Placar da Etapa 4 — queda do consumidor: perdidas × recuperadas via resumeToken.
const ETAPA4 = {
  redis: (ops) => [
    { label: 'avisadas', value: count(ops, o => o.redis.tag === 'avisado'), tone: 'idle' },
    { label: 'perdidas p/ sempre', value: count(ops, o => o.redis.tag === 'PERDIDO'), tone: count(ops, o => o.redis.tag === 'PERDIDO') ? 'red' : 'idle' },
  ],
  mongo: (ops) => [
    { label: 'avisadas', value: ops.length, tone: 'green' },
    { label: 'recuperadas (resumeToken)', value: count(ops, o => o.mongo.tag === 'recuperado'), tone: count(ops, o => o.mongo.tag === 'recuperado') ? 'green' : 'idle' },
  ],
}

export default function RedisVsChangeStreams() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* Cenário — enredo para vendas/cliente */}
      <div className="card" style={{ padding: '18px 20px' }}>
        <div style={{ fontWeight: 700, fontSize: 15, marginBottom: 10 }}>O cenário: reagir a uma mudança de dado, com garantia</div>
        <div style={{ fontSize: 13, color: 'var(--text-primary)', lineHeight: 1.65, marginBottom: 12 }}>
          Uma transação é gravada → o sistema precisa <strong>reagir a isso</strong> (avisar o dispositivo, disparar antifraude, atualizar
          outro serviço). O dado <strong>precisa ser durável</strong> e o aviso <strong>não pode se perder</strong>. A pergunta não é
          “quem é mais rápido” — é <em>“o aviso é uma propriedade do dado, ou um segundo sistema que você mantém em sincronia?”</em>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, fontSize: 13 }}>
          <div style={{ padding: '12px 14px', background: 'rgba(255,105,96,.05)', borderRadius: 6, border: '1px solid rgba(255,105,96,.28)' }}>
            <div style={{ fontWeight: 600, marginBottom: 6, color: '#ff6960', fontSize: 11, textTransform: 'uppercase', letterSpacing: '.05em' }}>🟥 Redis (excelente cache/broker)</div>
            <div style={{ lineHeight: 1.6, color: 'var(--text-primary)' }}>
              Rápido de verdade. Mas o aviso é um <strong>2º sistema</strong> ao lado da sua fonte de verdade — então o mesmo evento
              vira <strong>duas escritas</strong> que você precisa manter em sincronia (dual-write). Vale para Pub/Sub <em>e</em> Streams.
            </div>
          </div>
          <div style={{ padding: '12px 14px', background: 'rgba(0,237,100,.05)', borderRadius: 6, border: '2px solid #00ED64' }}>
            <div style={{ fontWeight: 600, marginBottom: 6, color: '#00ED64', fontSize: 11, textTransform: 'uppercase', letterSpacing: '.05em' }}>🍃 MongoDB Change Streams</div>
            <div style={{ lineHeight: 1.6, color: 'var(--text-primary)' }}>
              O aviso <strong>deriva do próprio commit</strong> do dado (o{' '}
              <Tooltip text="Fluxo de eventos que o MongoDB emite a partir de cada gravação já confirmada — o aviso nasce do mesmo dado, sem segunda escrita.">change stream</Tooltip> lê o oplog).
              <strong> 1 sistema, 1 fonte de verdade</strong>, entrega recuperável e em ordem — sem dual-write.
            </div>
          </div>
        </div>
      </div>

      <ArchComplexity
        num="ETAPA 01" title="Quantos sistemas você tem que operar?"
        subtitle="O Redis não substitui seu banco — ele é somado por cima. Conte os sistemas: o MongoDB entrega tudo em 1 (o aviso já vem no banco); o Redis exige banco + Redis."
        seeing={<>O ponto não é o Redis ser rápido — é que ele é <b>somado</b> à sua stack, não subtraído. Você opera <b>2 sistemas</b> (banco + Redis) onde o MongoDB opera <b>1</b>: menos para provisionar, pagar e manter, pelo mesmo resultado.</>}
      />

      <SourceOfTruthDemo
        num="ETAPA 02" title="Você vai precisar de um processo de CDC — ou ele já vem no banco?"
        subtitle="Com Redis, o dado e o aviso vivem em dois sistemas; para não divergirem, você opera um processo de CDC/sync. No MongoDB, o change stream É esse CDC — nativo, da fonte de verdade."
        seeing={<>Veja o dado e o aviso se <b>separarem</b> no Redis, exigindo um <b>processo de CDC/sync que você constrói e opera</b>. No MongoDB, o aviso <b>É o CDC</b> — deriva do commit, sem 2º banco nem sync. É essa separação que, sob falha, vira o problema da Etapa 3.</>}
      />

      <LiveBattle
        num="ETAPA 03" title="A prova: o gap do dual-write sob falha"
        subtitle="20 transações reais, com um crash aleatório no meio de algumas delas."
        seeing={<>Aquela separação da Etapa 2 agora <b>quebra de verdade</b>: veja quantas transações do Redis terminam <b>INCONSISTENTES</b> (durável sem aviso, ou avisado sem persistir). No MongoDB é sempre <b>0</b> — persistência e aviso são o mesmo commit, então não há gap para cair.</>}
        cta="▶ Rodar 20 transações"
        endpoint="/redis-changestream/demo/lote-consistencia?n=20"
        scoreRedis={ETAPA3.redis} scoreMongo={ETAPA3.mongo}
      />

      <LiveBattle
        num="ETAPA 04" title="Queda do consumidor: o que se perde — e o que se recupera"
        subtitle="20 transações; o consumidor cai numa janela no meio da rotina e religa em seguida."
        seeing={<>No Pub/Sub, aviso publicado sem ouvinte é <b>perdido para sempre</b> (fire-and-forget). No change stream, o consumidor religa com{' '}
          <Tooltip text="Marcador durável do último evento processado; ao religar, resume_after(resumeToken) retoma exatamente de onde parou.">resume_after(resumeToken)</Tooltip>{' '}
          e <b>recupera todas</b> as notificações da janela — dentro da retenção do oplog.</>}
        cta="▶ Derrubar o consumidor no meio"
        endpoint="/redis-changestream/demo/lote-resiliencia?n=20"
        scoreRedis={ETAPA4.redis} scoreMongo={ETAPA4.mongo}
      />

      {/* Positivo — o que o change stream entrega de fábrica */}
      <div className="card" style={{ padding: '18px 20px', border: '1px solid rgba(0,237,100,.35)' }}>
        <div style={{ fontWeight: 700, fontSize: 14, marginBottom: 4, color: '#00ED64' }}>🍃 O change stream já te entrega a consistência — de fábrica</div>
        <div style={{ fontSize: 12.5, color: 'var(--text-secondary)', lineHeight: 1.6, marginBottom: 14 }}>
          A garantia que esse cenário pede vem embutida no MongoDB, da sua fonte de verdade — sem construir nem operar nada a mais:
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', gap: 10 }}>
          {[
            { i: '🎯', t: 'Fonte única de verdade', d: 'O aviso deriva do commit: existe se, e só se, o dado foi gravado. Dado e evento nunca divergem — a inconsistência é estruturalmente impossível.' },
            { i: '📦', t: 'Entrega garantida e em ordem', d: 'At-least-once e sequencial — nada de fire-and-forget. Recuperável dentro da janela de retenção do oplog (dimensionável no Atlas).' },
            { i: '🔁', t: 'Recuperável (resumeToken)', d: 'O consumidor cai e religa retomando exatamente de onde parou, dentro da retenção do oplog. Nenhuma notificação perdida.' },
            { i: '🧰', t: 'Zero infra extra', d: 'Sem 2º banco, sem broker, sem processo de sync para provisionar e manter — é uma capacidade do banco que você já tem.' },
            { i: '🔍', t: 'Auditoria e histórico nativos', d: 'O próprio doc + o stream já são a trilha — sem escrita extra.' },
            { i: '⚡', t: 'Dentro do SLA', d: 'Co-localizado, a notificação chega em poucos ms — derivada de um write já durável.' },
          ].map(c => (
            <div key={c.t} style={{ padding: '12px 14px', borderRadius: 8, background: 'rgba(0,237,100,.05)', border: '1px solid rgba(0,237,100,.22)' }}>
              <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 4 }}>{c.i} {c.t}</div>
              <div style={{ fontSize: 12, color: 'var(--text-secondary)', lineHeight: 1.55 }}>{c.d}</div>
            </div>
          ))}
        </div>
        <div style={{ marginTop: 14, fontSize: 12.5, color: 'var(--text-primary)', lineHeight: 1.6 }}>
          É por isso que o change stream é a <strong>ferramenta certa para reagir a mudanças de dado com garantia</strong>: tudo isso pronto,
          testado e mantido pela MongoDB — enquanto o Redis segue excelente no que ele faz de melhor (cache e mensageria rápida).
        </div>
      </div>

      {/* Fechamento p/ o SA */}
      <div className="card" style={{ padding: '16px 20px' }}>
        <div style={{ fontWeight: 700, fontSize: 14, marginBottom: 8 }}>Por que o change stream é a ferramenta certa aqui</div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8, fontSize: 12.5, color: 'var(--text-primary)', lineHeight: 1.55 }}>
          <div>🧩 <strong>Um sistema só:</strong> o aviso já vem no banco. Você opera <strong>1</strong>, não 2 — menos para provisionar, monitorar e pagar.</div>
          <div>🎯 <strong>Fonte única de verdade:</strong> o aviso <strong>deriva do commit</strong>, então dado e evento nunca divergem — a consistência é uma propriedade do próprio dado.</div>
          <div>🛡️ <strong>Garantia embutida:</strong> entrega <strong>at-least-once, em ordem e recuperável</strong> via <Tooltip text="Marcador do último evento processado; ao religar, o change stream retoma exatamente de onde parou.">resumeToken</Tooltip> — a consistência que o cenário pede, pronta e mantida pela MongoDB.</div>
          <div style={{ color: 'var(--text-secondary)', marginTop: 2 }}>➕ O Redis continua excelente como cache e mensageria. Para <strong>reagir a mudanças de dado com garantia</strong>, o change stream entrega isso de fábrica.</div>
        </div>
      </div>

      {/* Transparência metodológica */}
      <div style={{ fontSize: 11.5, color: 'var(--text-secondary)', lineHeight: 1.6, padding: '0 4px' }}>
        🔎 <strong>Transparência:</strong> o Redis destas demos é <em>simulado em memória, in-process</em> — o melhor caso
        possível de latência para o Redis (sem rede, sem serialização). As operações MongoDB são reais, contra o Atlas.
        O argumento é <em>estrutural</em> (dual-write vs commit único), não um benchmark de velocidade.
      </div>
    </div>
  )
}
