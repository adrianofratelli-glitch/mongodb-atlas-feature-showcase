import React, { useState } from 'react'
import { Light as SyntaxHighlighter } from 'react-syntax-highlighter'
import js from 'react-syntax-highlighter/dist/esm/languages/hljs/javascript'
import { atomOneDark } from 'react-syntax-highlighter/dist/esm/styles/hljs'

SyntaxHighlighter.registerLanguage('javascript', js)

export default function QueryBlock({ query, label = 'Ver query executada' }) {
  const [open, setOpen] = useState(false)
  return (
    <div>
      <button className="code-toggle-btn" onClick={() => setOpen(v => !v)}>
        <span>{open ? '▼' : '▶'}</span> {label}
      </button>
      {open && (
        <div style={{ marginTop: 8 }}>
          <SyntaxHighlighter language="javascript" style={atomOneDark} customStyle={{ borderRadius: 8, fontSize: 12.5 }}>
            {query}
          </SyntaxHighlighter>
        </div>
      )}
    </div>
  )
}
