import { useState, useCallback } from 'react'

export function useApi() {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const call = useCallback(async (path, options = {}) => {
    setLoading(true)
    setError(null)
    try {
      const res = await fetch(`/api${path}`, options)
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
      setLoading(false)
    }
  }, [])

  return { call, loading, error }
}
