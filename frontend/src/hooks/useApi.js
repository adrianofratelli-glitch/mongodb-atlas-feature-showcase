import { useState, useCallback, useEffect, useRef } from 'react'

const REQUEST_TIMEOUT_MS = 30_000

export function useApi() {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const pending = useRef(0)
  const mounted = useRef(true)
  const controllers = useRef(new Set())

  useEffect(() => {
    // StrictMode executa setup/cleanup/setup no desenvolvimento.
    mounted.current = true
    return () => {
      mounted.current = false
      controllers.current.forEach(controller => controller.abort())
      controllers.current.clear()
    }
  }, [])

  const call = useCallback(async (path, options = {}) => {
    if (!mounted.current) return null
    const { timeoutMs: requestedTimeout = REQUEST_TIMEOUT_MS, ...fetchOptions } = options
    const timeoutMs = Number.isFinite(requestedTimeout)
      ? Math.max(1_000, Math.min(requestedTimeout, 300_000))
      : REQUEST_TIMEOUT_MS
    pending.current += 1
    setLoading(true)
    setError(null)
    const controller = new AbortController()
    const upstreamSignal = fetchOptions.signal
    const abortFromUpstream = () => controller.abort()
    if (upstreamSignal) upstreamSignal.addEventListener('abort', abortFromUpstream, { once: true })
    controllers.current.add(controller)
    let timedOut = false
    const timeout = setTimeout(() => {
      timedOut = true
      controller.abort()
    }, timeoutMs)
    try {
      const headers = new Headers(fetchOptions.headers || {})
      const demoToken = import.meta.env.VITE_DEMO_API_TOKEN
      if (demoToken) headers.set('X-Demo-Token', demoToken)
      const res = await fetch(`/api${path}`, { ...fetchOptions, headers, signal: controller.signal })
      if (!res.ok) {
        let detail = `HTTP ${res.status}`
        try {
          const body = await res.json()
          if (body.detail) detail = `${detail} — ${typeof body.detail === 'string' ? body.detail : JSON.stringify(body.detail)}`
        } catch { /* corpo não-JSON, mantém só o status */ }
        throw new Error(detail)
      }
      return await res.json()
    } catch (e) {
      // Navegar entre módulos desmonta a tela e cancela suas requisições.
      // Esse cancelamento é esperado e não deve virar um falso erro global.
      if ((controller.signal.aborted && !timedOut) || !mounted.current || upstreamSignal?.aborted) return null
      const rawMessage = e instanceof Error ? e.message : String(e)
      const message = timedOut
        ? `Tempo limite de ${Math.round(timeoutMs / 1000)} s excedido — verifique o backend e a operação no Atlas`
        : rawMessage === 'Failed to fetch'
        ? 'API indisponível — verifique se o backend está rodando na porta 8002'
        : rawMessage
      setError(message)
      window.dispatchEvent(new CustomEvent('api-error', { detail: { path, message } }))
      return null
    } finally {
      clearTimeout(timeout)
      controllers.current.delete(controller)
      if (upstreamSignal) upstreamSignal.removeEventListener('abort', abortFromUpstream)
      pending.current = Math.max(0, pending.current - 1)
      if (mounted.current) setLoading(pending.current > 0)
    }
  }, [])

  return { call, loading, error }
}
