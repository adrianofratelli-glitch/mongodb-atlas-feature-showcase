import React from 'react'

// Página de abertura, curta de propósito. O apresentador narra; a tela só
// precisa fixar o enquadramento para que a demo não seja lida como catálogo de
// features — e declarar os não-objetivos, que é o que dá crédito ao resto.

export default function Tese() {
  return (
    <div className="card" style={{ padding: '28px', borderColor: 'rgba(0,237,100,.28)' }}>
      <p style={{ fontSize: 22, lineHeight: 1.45, color: 'var(--text-primary)', margin: 0, maxWidth: 760 }}>
        O argumento é <strong style={{ color: 'var(--accent)' }}>convergência</strong>: oito capacidades,
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
  )
}
