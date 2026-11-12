import test from 'node:test';
import assert from 'node:assert/strict';
import {
  fmtBRL, fmtCount, fmtDataGravacao, fmtEscala, fmtMs, fmtMsPreciso, fmtReais, num,
} from '../src/lib/formato.js';

test('fmtMs troca para segundos só a partir de 10 s', () => {
  assert.equal(fmtMs(null), '—');
  assert.equal(fmtMs(306.4), '306 ms');
  assert.equal(fmtMs(9999), '9999 ms');
  assert.equal(fmtMs(12345), '12,3 s');
});

test('fmtMsPreciso (06) usa vírgula e casa decimal abaixo de 10 ms', () => {
  assert.equal(fmtMsPreciso(undefined), '—');
  assert.equal(fmtMsPreciso(7.44), '7,4 ms');
  assert.equal(fmtMsPreciso(17.7), '18 ms');
  assert.equal(fmtMsPreciso(1534), '1,53 s');
});

test('num e fmtBRL seguem o pt-BR', () => {
  assert.equal(num(null), '—');
  assert.equal(num(332568), '332.568');
  assert.equal(fmtBRL(104486759.65), '104.486.759,65');
  assert.equal(fmtBRL(0), '0,00');
  assert.equal(fmtBRL(null), '—');
});

test('fmtEscala resume ordens de grandeza do PIX', () => {
  assert.equal(fmtEscala(null), '—');
  assert.equal(fmtEscala(999), '999');
  assert.equal(fmtEscala(1500), '2 mil');
  assert.equal(fmtEscala(2_500_000), '2,5 mi');
  assert.equal(fmtEscala(30_000_000), '30 mi');
  assert.equal(fmtEscala(1_234_567_890), '1,2 bi');
});

test('fmtDataGravacao usa o fuso de São Paulo e não quebra com lixo', () => {
  assert.equal(fmtDataGravacao(''), 'data não informada');
  assert.equal(fmtDataGravacao('não é data'), 'não é data');
  assert.match(fmtDataGravacao('2026-07-28T12:03:30Z'), /28\/07\/2026.*09:03/);
});

test('fmtCount abrevia contagens do cabeçalho', () => {
  assert.equal(fmtCount(null), '…');
  assert.equal(fmtCount(950), '950');
  assert.equal(fmtCount(120_000), '120k');
  assert.equal(fmtCount(5_000_000), '5M');
  assert.equal(fmtCount(6_000_000 + 200_000), '6.2M');
});

test('fmtReais formata moeda completa em pt-BR', () => {
  assert.equal(fmtReais(17429.6), 'R$ 17.429,60');
  assert.equal(fmtReais(1669.76, 0), 'R$ 1.670');
  assert.equal(fmtReais(null), '—');
  assert.equal(fmtReais('x'), '—');
});
