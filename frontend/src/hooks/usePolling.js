import { useEffect, useRef, useState } from 'react'

/**
 * Aba visível? Um dashboard esquecido aberto numa aba de fundo não deve custar
 * nada — nem conexão segurada no navegador, nem trabalho no backend.
 */
export function useVisivel() {
  const [visivel, setVisivel] = useState(
    typeof document === 'undefined' || document.visibilityState === 'visible',
  )
  useEffect(() => {
    const onChange = () => setVisivel(document.visibilityState === 'visible')
    document.addEventListener('visibilitychange', onChange)
    return () => document.removeEventListener('visibilitychange', onChange)
  }, [])
  return visivel
}

/**
 * setInterval que só existe enquanto a aba está visível E `ativo` é verdadeiro.
 *
 * Dispara uma vez imediatamente ao (re)ativar, para a tela não ficar com dado
 * velho pelo resto do intervalo quando o operador volta para a aba.
 *
 * `fn` é guardada numa ref: assim a identidade dela não recria o timer a cada
 * render, que é o erro clássico que transforma um poll de 5 s em uma rajada.
 */
export function useIntervaloVisivel(fn, ms, ativo = true) {
  const visivel = useVisivel()
  const ref = useRef(fn)
  ref.current = fn

  useEffect(() => {
    if (!ativo || !visivel) return undefined
    let vivo = true
    const tick = () => { if (vivo) ref.current() }
    tick()
    const t = setInterval(tick, ms)
    return () => { vivo = false; clearInterval(t) }
  }, [ms, ativo, visivel])

  return visivel
}
