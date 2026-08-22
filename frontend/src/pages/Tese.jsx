import React from 'react'

// Página de abertura, curta de propósito. O apresentador narra; a tela só
// precisa fixar o enquadramento para que a demo não seja lida como catálogo de
// features — e declarar os não-objetivos, que é o que dá crédito ao resto.

export default function Tese() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

      <div className="card" style={{ padding: '22px 24px', borderColor: 'rgba(0,237,100,.28)' }}>
        <p style={{ fontSize: 17, lineHeight: 1.6, color: 'var(--text-primary)', margin: 0 }}>
          Nenhuma das oito capacidades é exclusiva do MongoDB.{' '}
          <strong>O argumento não é a capacidade — é a convergência:</strong> todas sobre o mesmo
          dado, no mesmo cluster, na mesma linguagem. Cada motor especializado a menos é um CDC,
          um schema duplicado e uma janela de defasagem que saem do desenho.
        </p>
      </div>

      <div className="grid-2" style={{ gap: 16 }}>
        <div className="card" style={{ padding: '18px 20px' }}>
          <h2 style={{ fontSize: 15, marginBottom: 10 }}>O que esta demo prova</h2>
          <ul style={{ paddingLeft: 18, margin: 0, fontSize: 13.5, color: 'var(--text-secondary)', lineHeight: 1.8 }}>
            <li>Cluster Atlas real. Peça não configurada aparece como tal, nunca como número inventado.</li>
            <li>O que pode falhar é quebrado ao vivo — connector, evento inválido, failover do primário.</li>
            <li>Todo número vem com a condição em que foi medido.</li>
          </ul>
        </div>
        <div className="card" style={{ padding: '18px 20px' }}>
          <h2 style={{ fontSize: 15, marginBottom: 10 }}>O que ela não prova</h2>
          <ul style={{ paddingLeft: 18, margin: 0, fontSize: 13.5, color: 'var(--text-secondary)', lineHeight: 1.8 }}>
            <li>Não é benchmark competitivo.</li>
            <li>Não estima economia — isso depende do seu inventário e dos seus SLOs.</li>
            <li>Não substitui o data warehouse.</li>
            <li>Não é desenho de produção: cluster descartável, Kafka de nó único.</li>
          </ul>
        </div>
      </div>

      <div className="card" style={{ padding: '16px 20px' }}>
        <span style={{ fontSize: 13.5, color: 'var(--text-secondary)' }}>
          <strong style={{ color: 'var(--text-primary)' }}>Com 15 minutos:</strong> módulo 07, e quebre
          alguma coisa. Depois o 08. Os módulos 01 a 06 abrem sob demanda. Medições todas em{' '}
          <strong>M20</strong> — tier pequeno de propósito.
        </span>
      </div>

    </div>
  )
}
