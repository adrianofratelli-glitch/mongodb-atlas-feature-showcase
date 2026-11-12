import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

// Carrega useApi.js trocando React e import.meta.env por stubs: o hook vira uma
// função comum e `call` pode ser exercitado contra um fetch falso.
async function carregar({ token } = {}) {
  const efeitos = [];
  globalThis.__apiHooks = {
    useState: v => [v, () => {}],
    useRef: v => ({ current: v }),
    useCallback: fn => fn,
    useEffect: fn => efeitos.push(fn),
  };
  globalThis.__apiEnv = { VITE_DEMO_API_TOKEN: token };
  const fonte = (await readFile(new URL('../src/hooks/useApi.js', import.meta.url), 'utf8'))
    .replace("import { useState, useCallback, useEffect, useRef } from 'react'",
      'const { useState, useCallback, useEffect, useRef } = globalThis.__apiHooks')
    .replace('import.meta.env', 'globalThis.__apiEnv');
  const mod = await import('data:text/javascript;base64,' + Buffer.from(fonte + `\n// ${Math.random()}`).toString('base64'));
  const api = mod.useApi();
  const desmontar = efeitos[0]();
  return { api, desmontar };
}

function capturarEventos() {
  const eventos = [];
  globalThis.window = { dispatchEvent: e => eventos.push(e) };
  globalThis.CustomEvent = class { constructor(type, init) { this.type = type; this.detail = init?.detail; } };
  return eventos;
}

const resposta = (status, corpo) => ({
  ok: status >= 200 && status < 300, status, json: async () => corpo,
});

test('envia o token de demo e publica a query executada', async () => {
  const eventos = capturarEventos();
  let pedido;
  globalThis.fetch = async (url, opts) => { pedido = { url, opts }; return resposta(200, { pipeline: [{ $match: {} }] }); };
  const { api } = await carregar({ token: 'segredo' });
  const corpo = await api.call('/aggregations/facet');
  assert.deepEqual(corpo, { pipeline: [{ $match: {} }] });
  assert.equal(pedido.url, '/api/aggregations/facet');
  assert.equal(pedido.opts.headers.get('X-Demo-Token'), 'segredo');
  assert.equal(eventos[0].type, 'api-query-executed');
  assert.deepEqual(eventos[0].detail.technical, [{ $match: {} }]);
});

test('sem token não manda o cabeçalho', async () => {
  capturarEventos();
  let cabecalhos;
  globalThis.fetch = async (_url, opts) => { cabecalhos = opts.headers; return resposta(200, {}); };
  const { api } = await carregar();
  await api.call('/stats');
  assert.equal(cabecalhos.has('X-Demo-Token'), false);
});

test('erro HTTP vira mensagem com status e detail do backend', async () => {
  const eventos = capturarEventos();
  globalThis.fetch = async () => resposta(409, { detail: 'Ative o schema strict primeiro.' });
  const { api } = await carregar();
  assert.equal(await api.call('/schema/step4-try-constraint', { method: 'POST' }), null);
  const erro = eventos.find(e => e.type === 'api-error');
  assert.equal(erro.detail.message, 'HTTP 409 — Ative o schema strict primeiro.');
});

test('backend fora do ar vira mensagem acionável', async () => {
  const eventos = capturarEventos();
  globalThis.fetch = async () => { throw new TypeError('Failed to fetch'); };
  const { api } = await carregar();
  await api.call('/preflight');
  assert.match(eventos.find(e => e.type === 'api-error').detail.message, /API indisponível/);
});

test('timeout aborta e explica; desmontar a tela não gera falso erro', async () => {
  const eventos = capturarEventos();
  globalThis.fetch = (_url, opts) => new Promise((_r, rejeitar) => {
    opts.signal.addEventListener('abort', () => rejeitar(new Error('AbortError')));
  });
  const { api, desmontar } = await carregar();
  const lento = api.call('/tese/medir', { timeoutMs: 1000 });
  assert.equal(await lento, null);
  assert.match(eventos.find(e => e.type === 'api-error').detail.message, /Tempo limite de 1 s/);

  const antes = eventos.length;
  const pendente = api.call('/aggregations/lookup');
  desmontar();
  assert.equal(await pendente, null);
  assert.equal(eventos.length, antes, 'cancelamento por navegação não é erro');
});
