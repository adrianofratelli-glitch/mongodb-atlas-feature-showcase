// Formatadores puros da UI (pt-BR). Ficam fora dos componentes para terem teste
// em `frontend/tests/formato.test.mjs` sem precisar de React nem de navegador.

// Acima de 10 s, milissegundos com 5 dígitos obrigam a plateia a fazer conta.
export const fmtMs = (v) => {
  if (v == null) return '—'
  return v >= 10000 ? `${(v / 1000).toFixed(1).replace('.', ',')} s` : `${Math.round(v)} ms`
}

// Variante do módulo 06: latências de transação pedem uma casa a mais.
export const fmtMsPreciso = (v) => (v == null ? '—' : v >= 1000
  ? `${(v / 1000).toFixed(2).replace('.', ',')} s`
  : `${v.toFixed(v < 10 ? 1 : 0).replace('.', ',')} ms`)

export const num = (v) => (v == null ? '—' : Number(v).toLocaleString('pt-BR'))

export const fmtBRL = (v) => (v == null ? '—' : Number(v).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 }))

// Moeda completa: 17429.6 → "R$ 17.429,60"; casas = 0 para preço médio/faixas.
export const fmtReais = (v, casas = 2) => (v == null || Number.isNaN(Number(v)) ? '—'
  : `R$ ${Number(v).toLocaleString('pt-BR', { minimumFractionDigits: casas, maximumFractionDigits: casas })}`)

// 1.234.567.890 → "1,2 bi" / "30 mi" — números de escala PIX não cabem por extenso.
export function fmtEscala(v) {
  if (v == null) return '—'
  if (v >= 1e9) return `${(v / 1e9).toFixed(1).replace('.', ',')} bi`
  if (v >= 1e6) return `${(v / 1e6).toFixed(v >= 1e7 ? 0 : 1).replace('.', ',')} mi`
  if (v >= 1e3) return `${Math.round(v / 1e3)} mil`
  return String(v)
}

export function fmtDataGravacao(v) {
  if (!v) return 'data não informada'
  const d = new Date(v)
  return Number.isNaN(d.getTime()) ? String(v) : d.toLocaleString('pt-BR', {
    dateStyle: 'short', timeStyle: 'short', timeZone: 'America/Sao_Paulo',
  })
}

// Formata contagens grandes: 5_000_000 → "5M", 100_000 → "100k"
export function fmtCount(n) {
  if (n == null) return '…'
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(n % 1_000_000 >= 100_000 ? 1 : 0)}M`
  if (n >= 1_000) return `${Math.round(n / 1_000)}k`
  return String(n)
}
