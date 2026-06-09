import React from 'react'

export default function DiffBadge({ supported }) {
  return supported
    ? <span className="badge badge-green">✓ MongoDB Atlas</span>
    : <span className="badge badge-red">✗ DocumentDB</span>
}
