import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { UFS } from '../data/brasil-uf'

// ── Projeção ────────────────────────────────────────────────────────────────
// Equiretangular com correção de latitude. Um grau de longitude vale
// cos(lat) grau de latitude em distância real; sem esse fator o Sul do país sai
// ~18% mais largo que o Norte, e num módulo que vende precisão geoespacial um
// mapa deformado desmonta a alegação antes da primeira consulta.
// LAT0 é o paralelo padrão: -15 é o centro aproximado da massa do Brasil.
const LAT0 = -15
const K = Math.cos((LAT0 * Math.PI) / 180)

// Envelope da malha do IBGE, com uma folga pequena para a linha do contorno não
// encostar na borda do viewBox.
const BBOX = { oeste: -74.4, leste: -33.9, norte: 5.7, sul: -34.4 }

const ALTURA_VB = 100
const ESCALA = ALTURA_VB / (BBOX.norte - BBOX.sul)
const LARGURA_VB = (BBOX.leste - BBOX.oeste) * K * ESCALA

export function projetar([lng, lat]) {
  return [(lng - BBOX.oeste) * K * ESCALA, (BBOX.norte - lat) * ESCALA]
}

const paraPath = (anel) => anel
  .map((c, i) => `${i ? 'L' : 'M'}${projetar(c).map(v => v.toFixed(2)).join(' ')}`)
  .join(' ') + 'Z'

// Pré-computado uma vez no import: 27 UFs × N anéis viram strings de path e
// nunca mais são recalculados, independente de quantas vezes o mapa remonta.
const PATHS_UF = Object.entries(UFS).map(([sigla, aneis]) => ({
  sigla,
  d: aneis.map(paraPath).join(' '),
}))

// Âncoras de leitura. A plateia reconhece capital, não silhueta: sem elas o
// olho leva alguns segundos para achar onde o ponto caiu. Ficam discretas de
// propósito — são régua, não conteúdo.
const REFERENCIAS = [
  { nome: 'Manaus', coord: [-60.0217, -3.1019], anchor: 'start' },
  { nome: 'Belém', coord: [-48.5044, -1.4558], anchor: 'start' },
  { nome: 'Fortaleza', coord: [-38.5434, -3.7319], anchor: 'end' },
  { nome: 'Recife', coord: [-34.8811, -8.0539], anchor: 'end' },
  { nome: 'Salvador', coord: [-38.5014, -12.9777], anchor: 'end' },
  { nome: 'Brasília', coord: [-47.8825, -15.7942], anchor: 'start' },
  { nome: 'São Paulo', coord: [-46.6333, -23.5505], anchor: 'end' },
  { nome: 'Porto Alegre', coord: [-51.2177, -30.0346], anchor: 'end' },
]

// ── Mapa SVG local ──────────────────────────────────────────────────────────
// Sem Leaflet, sem tiles, sem requisição em runtime: a malha estadual está no
// bundle. É o modo padrão porque tem de renderizar igual com a rede do auditório
// fora do ar.
// Janela de visualização. Sem `ajustar`, o mapa é o país inteiro. Com ele, a
// janela fecha em volta dos pontos: um raio de 25 km sobre um mapa nacional é um
// pixel só, e o painel de entorno do terminal deixa de mostrar o que promete.
// MIN_SPAN_GRAUS impede o zoom de passar do ponto em que a malha estadual ainda
// dá contexto — abaixo disso sobrariam pontos flutuando num fundo vazio, e aí o
// mapa certo é um com ruas, não este.
// Com um círculo de consulta desenhado, o piso cai: a moldura passa a ser o
// próprio raio pedido, e o que a plateia lê é a dispersão dos resultados dentro
// dele — que é exatamente o que o `$geoWithin` decidiu. Sem círculo, o piso alto
// evita zoom num fundo vazio.
const MIN_SPAN_GRAUS = 4
const MIN_SPAN_COM_CIRCULO = 0.2

function janela(pontos, linha, ajustar, circulo) {
  const coords = pontos.map(p => p.coord)
  if (linha) coords.push(linha.de, linha.para)
  if (circulo) {
    // Bounding box do raio, para a moldura nunca cortar o círculo.
    const dLat = circulo.raioKm / 111.32
    const dLng = dLat / Math.max(0.2, Math.cos((circulo.centro[1] * Math.PI) / 180))
    coords.push(
      [circulo.centro[0] - dLng, circulo.centro[1] - dLat],
      [circulo.centro[0] + dLng, circulo.centro[1] + dLat],
    )
  }
  if (!ajustar || coords.length === 0) {
    return { x: 0, y: 0, w: LARGURA_VB, h: ALTURA_VB, k: 1 }
  }
  const lngs = coords.map(c => c[0]); const lats = coords.map(c => c[1])
  const cx = (Math.min(...lngs) + Math.max(...lngs)) / 2
  const cy = (Math.min(...lats) + Math.max(...lats)) / 2
  // Altura em grau de latitude, com 25% de folga; a largura sai da mesma
  // proporção do viewBox cheio, então a projeção não distorce ao dar zoom.
  const alturaG = Math.max(
    circulo ? MIN_SPAN_COM_CIRCULO : MIN_SPAN_GRAUS,
    (Math.max(...lats) - Math.min(...lats)) * 1.25,
    ((Math.max(...lngs) - Math.min(...lngs)) * K * 1.25) / (LARGURA_VB / ALTURA_VB),
  )
  const h = Math.min(ALTURA_VB, alturaG * ESCALA)
  const w = h * (LARGURA_VB / ALTURA_VB)
  const [px, py] = projetar([cx, cy])
  return {
    x: Math.max(0, Math.min(LARGURA_VB - w, px - w / 2)),
    y: Math.max(0, Math.min(ALTURA_VB - h, py - h / 2)),
    w, h,
    // Fator de escala do conteúdo: fonte e raio estão em unidade de viewBox, e
    // sem essa correção um rótulo de 2,9 vira um outdoor quando a janela fecha.
    k: h / ALTURA_VB,
  }
}

function MapaSvg({ pontos, linha, rotuloLinha, onHover, hover, ajustar, circulo }) {
  // Identificador único por instância: dois mapas na mesma página
  // compartilhariam os <defs> e o segundo herdaria o gradiente do primeiro.
  const uid = React.useId().replace(/:/g, '')
  const destaques = pontos.filter(p => p.destaque)
  const vb = useMemo(() => janela(pontos, linha, ajustar, circulo), [pontos, linha, ajustar, circulo])
  const k = vb.k

  return (
    <svg viewBox={`${vb.x.toFixed(2)} ${vb.y.toFixed(2)} ${vb.w.toFixed(2)} ${vb.h.toFixed(2)}`}
      preserveAspectRatio="xMidYMid meet" role="img"
      aria-label="Mapa do Brasil com divisas estaduais e os pontos da consulta">
      <defs>
        <linearGradient id={`br-${uid}`} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="#00ED64" stopOpacity=".13" />
          <stop offset="100%" stopColor="#00684A" stopOpacity=".20" />
        </linearGradient>
        <radialGradient id={`halo-${uid}`}>
          <stop offset="0%" stopColor="#fff" stopOpacity=".55" />
          <stop offset="100%" stopColor="#fff" stopOpacity="0" />
        </radialGradient>
      </defs>

      {/* Paralelos e meridianos a cada 10°, atrás do país. */}
      <g stroke="rgba(255,255,255,.045)" strokeWidth={0.25 * k}>
        {[-70, -60, -50, -40].map(lng => (
          <line key={`m${lng}`} x1={projetar([lng, 0])[0]} y1="0"
            x2={projetar([lng, 0])[0]} y2={ALTURA_VB} />
        ))}
        {[0, -10, -20, -30].map(lat => (
          <line key={`p${lat}`} x1="0" y1={projetar([0, lat])[1]}
            x2={LARGURA_VB} y2={projetar([0, lat])[1]} />
        ))}
      </g>

      {/* Duas passagens sobre a mesma malha: a primeira preenche e produz a
          silhueta do país como massa única, a segunda desenha só as divisas.
          Um contorno nacional separado exigiria a união das 27 geometrias. */}
      <g fill={`url(#br-${uid})`} stroke="none"
        style={{ filter: `drop-shadow(0 0 ${1.4 * k}px rgba(0,237,100,.30))` }}>
        {PATHS_UF.map(uf => <path key={uf.sigla} d={uf.d} />)}
      </g>
      <g fill="none" stroke="rgba(0,237,100,.32)" strokeWidth={0.22 * k} strokeLinejoin="round">
        {PATHS_UF.map(uf => <path key={uf.sigla} d={uf.d} />)}
      </g>

      <g pointerEvents="none">
        {REFERENCIAS.filter(r => {
          // Um destaque a menos de 3 unidades do viewBox já nomeia essa cidade;
          // manter a referência cinza embaixo do rótulo colorido vira borrão.
          const [rx, ry] = projetar(r.coord)
          return !destaques.some(p => {
            const [px, py] = projetar(p.coord)
            return Math.hypot(px - rx, py - ry) < 3
          })
        }).map(r => {
          const [x, y] = projetar(r.coord)
          const dx = (r.anchor === 'end' ? -1.4 : 1.4) * k
          return (
            <g key={r.nome}>
              <circle cx={x} cy={y} r={0.45 * k} fill="rgba(255,255,255,.34)" />
              <text x={x + dx} y={y + 0.9 * k} fill="rgba(255,255,255,.34)" fontSize={2.3 * k}
                textAnchor={r.anchor} fontFamily="var(--font-mono, monospace)">{r.nome}</text>
            </g>
          )
        })}
      </g>

      {linha && (() => {
        const [x1, y1] = projetar(linha.de)
        const [x2, y2] = projetar(linha.para)
        // Arco em vez de reta: duas cidades ligadas por uma linha reta somem
        // dentro da silhueta; a curva sai do corpo do país e se lê de longe.
        const [mx, my] = [(x1 + x2) / 2, (y1 + y2) / 2]
        const [dx, dy] = [x2 - x1, y2 - y1]
        const arco = `M${x1} ${y1} Q${mx - dy * 0.22} ${my + dx * 0.22} ${x2} ${y2}`
        return (
          <g pointerEvents="none">
            <path d={arco} fill="none" stroke="#ff6960" strokeWidth={0.55 * k}
              strokeDasharray={`${2.5 * k} ${1.8 * k}`} strokeLinecap="round">
              <animate attributeName="stroke-dashoffset" from={8.6 * k} to="0"
                dur="1.1s" repeatCount="indefinite" />
            </path>
            {rotuloLinha && (
              <text x={mx - dy * 0.13} y={my + dx * 0.13} fill="#ff6960" fontSize={3.4 * k}
                fontWeight="700" textAnchor="middle" style={{ paintOrder: 'stroke' }}
                stroke="rgba(0,30,43,.85)" strokeWidth={1.1 * k}>{rotuloLinha}</text>
            )}
          </g>
        )
      })()}

      {/* O raio que o `$geoWithin` usou. Elipse, não círculo: a projeção corrige a
          longitude por cos(-15°) fixo, então num centro fora desse paralelo um
          raio geodésico constante não é um círculo na tela. Desenhar um círculo
          aqui mentiria sobre qual ponto está dentro do filtro. */}
      {circulo && (() => {
        const [cx, cy] = projetar(circulo.centro)
        const ry = (circulo.raioKm / 111.32) * ESCALA
        const rx = ry * (K / Math.max(0.2, Math.cos((circulo.centro[1] * Math.PI) / 180)))
        return (
          <g pointerEvents="none">
            <ellipse cx={cx} cy={cy} rx={rx} ry={ry} fill="rgba(6,182,212,.06)"
              stroke="rgba(6,182,212,.55)" strokeWidth={0.35 * k} strokeDasharray={`${1.6 * k} ${1.2 * k}`} />
            <text x={cx} y={cy - ry - 1.2 * k} fontSize={2.2 * k} fill="rgba(6,182,212,.8)"
              textAnchor="middle" fontFamily="var(--font-mono, monospace)">
              raio {circulo.raioKm} km
            </text>
          </g>
        )
      })()}

      {pontos.map((p, i) => {
        const [x, y] = projetar(p.coord)
        return (
          <g key={i} onMouseEnter={() => onHover(p)} onMouseLeave={() => onHover(null)}>
            {p.destaque && (
              <>
                <circle cx={x} cy={y} r={4.5 * k} fill={`url(#halo-${uid})`} />
                <circle cx={x} cy={y} r={1.6 * k} fill="none" stroke={p.cor || 'var(--accent)'} strokeWidth={0.35 * k}>
                  <animate attributeName="r" values={`${1.6 * k};${4.2 * k};${1.6 * k}`} dur="2s" repeatCount="indefinite" />
                  <animate attributeName="opacity" values=".9;0;.9" dur="2s" repeatCount="indefinite" />
                </circle>
              </>
            )}
            <circle cx={x} cy={y} r={(p.destaque ? 1.5 : 0.85) * k}
              fill={p.cor || 'var(--accent)'} opacity={p.destaque ? 1 : 0.7}
              stroke={p.destaque ? 'rgba(0,30,43,.9)' : 'none'} strokeWidth={0.3 * k} />
          </g>
        )
      })}

      {/* Só os pontos em destaque ganham rótulo fixo: o par origem/destino é o
          que a plateia precisa nomear sem passar o mouse. Os demais continuam
          no tooltip — 50 rótulos sobrepostos não se leem. */}
      <g pointerEvents="none">
        {destaques.map((p, i) => {
          const [x, y] = projetar(p.coord)
          const acima = y > vb.y + 10 * k
          const nome = (p.rotulo || '').split('—').pop().trim()
          return (
            <text key={i} x={x} y={acima ? y - 2.6 * k : y + 4.4 * k} fontSize={2.9 * k} fontWeight="600"
              textAnchor="middle" fill={p.cor || 'var(--accent)'}
              style={{ paintOrder: 'stroke' }} stroke="rgba(0,30,43,.85)" strokeWidth={1 * k}>
              {nome}
            </text>
          )
        })}
      </g>
      {/* Barra de escala. Com a janela fechando em volta dos pontos, "perto" e
          "longe" deixam de ser legíveis pelo tamanho do país — a barra devolve
          a referência métrica sem depender de tile de mapa. */}
      {(() => {
        const alvo = vb.w * 0.22
        const kmPorUnidade = 111.32 / ESCALA
        const bruto = alvo * kmPorUnidade
        const passo = [10, 25, 50, 100, 250, 500, 1000, 2000]
          .reduce((a, b) => (Math.abs(b - bruto) < Math.abs(a - bruto) ? b : a))
        const larg = passo / kmPorUnidade
        const bx = vb.x + vb.w * 0.045
        const by = vb.y + vb.h * 0.945
        return (
          <g pointerEvents="none" stroke="rgba(255,255,255,.45)" strokeWidth={0.3 * k}>
            <line x1={bx} y1={by} x2={bx + larg} y2={by} />
            <line x1={bx} y1={by - 1 * k} x2={bx} y2={by + 1 * k} />
            <line x1={bx + larg} y1={by - 1 * k} x2={bx + larg} y2={by + 1 * k} />
            <text x={bx + larg / 2} y={by - 1.8 * k} fontSize={2.2 * k} stroke="none"
              fill="rgba(255,255,255,.55)" textAnchor="middle"
              fontFamily="var(--font-mono, monospace)">{passo} km</text>
          </g>
        )
      })()}
      {hover && null}
    </svg>
  )
}

// ── Mapa Google (opcional) ──────────────────────────────────────────────────
// Só existe se `VITE_GOOGLE_MAPS_KEY` estiver definida no ambiente de quem roda.
// A chave NUNCA entra no repositório: ela vive em `frontend/.env`, que está no
// gitignore. Mesmo assim, uma chave de Maps JS é servida ao navegador e é
// pública por construção — restrinja por referenciador HTTP no console do
// Google, senão qualquer um que abra a demo pode reusá-la na sua fatura.
const CHAVE_GOOGLE = import.meta.env.VITE_GOOGLE_MAPS_KEY || ''

let promessaSdk = null
function carregarSdk() {
  if (window.google?.maps) return Promise.resolve(window.google.maps)
  if (promessaSdk) return promessaSdk
  promessaSdk = new Promise((resolve, reject) => {
    const s = document.createElement('script')
    s.src = `https://maps.googleapis.com/maps/api/js?key=${encodeURIComponent(CHAVE_GOOGLE)}&v=weekly&language=pt-BR&region=BR`
    s.async = true
    s.onload = () => (window.google?.maps ? resolve(window.google.maps) : reject(new Error('SDK carregou sem google.maps')))
    s.onerror = () => reject(new Error('falha de rede ao carregar o SDK'))
    document.head.appendChild(s)
  }).catch(err => { promessaSdk = null; throw err })
  return promessaSdk
}

const ESTILO_ESCURO = [
  { elementType: 'geometry', stylers: [{ color: '#0b2a33' }] },
  { elementType: 'labels.text.fill', stylers: [{ color: '#7b95a0' }] },
  { elementType: 'labels.text.stroke', stylers: [{ color: '#001E2B' }] },
  { featureType: 'administrative', elementType: 'geometry.stroke', stylers: [{ color: '#12603f' }] },
  { featureType: 'poi', stylers: [{ visibility: 'off' }] },
  { featureType: 'road', stylers: [{ visibility: 'off' }] },
  { featureType: 'transit', stylers: [{ visibility: 'off' }] },
  { featureType: 'water', elementType: 'geometry', stylers: [{ color: '#001E2B' }] },
]

function MapaGoogle({ pontos, linha, onErro }) {
  const div = useRef(null)
  const mapa = useRef(null)
  const desenhado = useRef([])

  useEffect(() => {
    let vivo = true
    carregarSdk().then(maps => {
      if (!vivo || !div.current) return
      mapa.current = mapa.current || new maps.Map(div.current, {
        center: { lat: -15, lng: -54 }, zoom: 3.6, styles: ESTILO_ESCURO,
        disableDefaultUI: true, zoomControl: true, gestureHandling: 'cooperative',
        backgroundColor: '#001E2B',
      })
      desenhado.current.forEach(o => o.setMap(null))
      desenhado.current = []
      const limites = new maps.LatLngBounds()

      pontos.forEach(p => {
        const pos = { lat: p.coord[1], lng: p.coord[0] }
        limites.extend(pos)
        desenhado.current.push(new maps.Circle({
          map: mapa.current, center: pos,
          radius: p.destaque ? 26000 : 14000,
          strokeColor: p.cor || '#00ED64', strokeOpacity: 0.95, strokeWeight: p.destaque ? 2 : 1,
          fillColor: p.cor || '#00ED64', fillOpacity: p.destaque ? 0.55 : 0.3,
        }))
      })

      if (linha) {
        ;[linha.de, linha.para].forEach(c => limites.extend({ lat: c[1], lng: c[0] }))
        desenhado.current.push(new maps.Polyline({
          map: mapa.current, geodesic: true,
          path: [{ lat: linha.de[1], lng: linha.de[0] }, { lat: linha.para[1], lng: linha.para[0] }],
          strokeColor: '#ff6960', strokeOpacity: 0.9, strokeWeight: 2,
        }))
      }

      if (!limites.isEmpty()) mapa.current.fitBounds(limites, 56)
    }).catch(err => { if (vivo) onErro(err.message) })
    return () => { vivo = false }
  }, [pontos, linha, onErro])

  return <div ref={div} style={{ position: 'absolute', inset: 0 }} />
}

// ── Casca ───────────────────────────────────────────────────────────────────
export default function MiniMapa({ pontos = [], linha = null, altura = 260, rotuloLinha = null, ajustar = false, circulo = null }) {
  const [hover, setHover] = useState(null)
  const [modo, setModo] = useState('local')
  const [erroGoogle, setErroGoogle] = useState(null)
  const aoErrar = useCallback((msg) => { setErroGoogle(msg); setModo('local') }, [])

  const temGoogle = Boolean(CHAVE_GOOGLE) && !erroGoogle
  const usandoGoogle = temGoogle && modo === 'google'

  // Identidade estável: sem isso o array literal remontaria as camadas do
  // Google a cada render do pai (a página faz polling de 4 em 4 segundos).
  const pontosMemo = useMemo(() => pontos, [JSON.stringify(pontos)])
  const linhaMemo = useMemo(() => linha, [JSON.stringify(linha)])
  const circuloMemo = useMemo(() => circulo, [JSON.stringify(circulo)])

  return (
    <div className="geo-mapa" style={{ minHeight: altura }}>
      {usandoGoogle
        ? <MapaGoogle pontos={pontosMemo} linha={linhaMemo} onErro={aoErrar} />
        : <MapaSvg pontos={pontosMemo} linha={linhaMemo} rotuloLinha={rotuloLinha}
            hover={hover} onHover={setHover} ajustar={ajustar} circulo={circuloMemo} />}

      {hover && !usandoGoogle && <div className="geo-mapa-tip">{hover.rotulo}</div>}
      {pontos.length === 0 && !linha && (
        <div className="geo-mapa-vazio">Execute uma consulta para plotar os pontos</div>
      )}

      {temGoogle && (
        <div className="geo-mapa-modo">
          <button type="button" className={modo === 'local' ? 'on' : ''}
            onClick={() => setModo('local')}>malha local</button>
          <button type="button" className={modo === 'google' ? 'on' : ''}
            onClick={() => setModo('google')}>Google Maps</button>
        </div>
      )}
      {erroGoogle && (
        <div className="geo-mapa-modo geo-mapa-modo-erro" title={erroGoogle}>
          Google Maps indisponível · malha local
        </div>
      )}

      <span className="geo-mapa-selo">
        {usandoGoogle ? 'Google Maps · WGS84' : 'malha IBGE · WGS84 · sem tiles'}
      </span>
    </div>
  )
}
