import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

test('slow polling does not overlap and cleanup prevents another tick', async () => {
  const effects = [];
  globalThis.__pollHooks = {
    useRef: value => ({ current: value }),
    useState: value => [value, () => {}],
    useEffect: effect => effects.push(effect),
  };
  const source = (await readFile(new URL('../src/hooks/usePolling.js', import.meta.url), 'utf8'))
    .replace("import { useEffect, useRef, useState } from 'react'", 'const { useEffect, useRef, useState } = globalThis.__pollHooks');
  const { useIntervaloVisivel } = await import('data:text/javascript;base64,' + Buffer.from(source).toString('base64'));
  const originalSet = globalThis.setInterval, originalClear = globalThis.clearInterval;
  let tick, clearCount = 0, calls = 0, resolve;
  globalThis.setInterval = callback => { tick = callback; return 1; };
  globalThis.clearInterval = () => { clearCount++; };
  try {
    useIntervaloVisivel(() => { calls++; return new Promise(r => { resolve = r; }); }, 1000);
    const cleanup = effects[1]();
    await tick(); await tick();
    assert.equal(calls, 1);
    resolve(); await Promise.resolve();
    const second = tick();
    assert.equal(calls, 2);
    cleanup(); resolve(); await second; await tick();
    assert.equal(calls, 2); assert.equal(clearCount, 1);
  } finally {
    globalThis.setInterval = originalSet; globalThis.clearInterval = originalClear;
    delete globalThis.__pollHooks;
  }
});
