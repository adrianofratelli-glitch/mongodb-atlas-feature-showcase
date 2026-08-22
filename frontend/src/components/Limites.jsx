import React from 'react'

// Limite declarado, no padrão que o módulo 08 já usava e que agora vale para os
// oito. Existe por um motivo comercial, não estético: um time de dados que ouve
// a limitação antes de perguntar por ela acredita no resto da demo. Demo que só
// mostra o que funciona convida à resposta "já temos isso" — e, pior, deixa a
// objeção para a reunião seguinte, onde ninguém do nosso lado está presente.
//
// Regra ao editar: cada item precisa ser verificável na documentação do produto
// ou medido nesta PoV. Limite inventado é pior que limite omitido.
export default function Limites({ titulo, itens }) {
  return (
    <details className="card">
      <summary style={{ cursor: 'pointer', fontSize: 13.5, fontWeight: 600 }}>
        {titulo}
      </summary>
      <ul style={{ marginTop: 10, paddingLeft: 18, fontSize: 13, color: 'var(--text-secondary)', lineHeight: 1.7 }}>
        {itens.map((item, i) => <li key={i}>{item}</li>)}
      </ul>
    </details>
  )
}
