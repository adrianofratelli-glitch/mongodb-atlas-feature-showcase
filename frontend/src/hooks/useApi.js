import { useState, useCallback, useRef } from 'react'

export function useApi() {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const pending = useRef(0)

  const call = useCallback(async (path, options = {}) => {
    pending.current += 1
    setLoading(true)
    setError(null)
    try {
      const headers = new Headers(options.headers || {})
      const demoToken = import.meta.env.VITE_DEMO_API_TOKEN
      if (demoToken) headers.set('X-Demo-Token', demoToken)
      const res = await fetch(`/api${path}`, { ...options, headers })
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
      const message = e.message === 'Failed to fetch'
        ? 'API indisponível — verifique se o backend está rodando na porta 8002'
        : e.message
      setError(message)
      window.dispatchEvent(new CustomEvent('api-error', { detail: { path, message } }))
      return null
    } finally {
      pending.current = Math.max(0, pending.current - 1)
      setLoading(pending.current > 0)
    }
  }, [])

  return { call, loading, error }
}
