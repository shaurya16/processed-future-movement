import { describe, expect, it } from 'vitest';
import { barGeometry, changedKeys, expiryBadge } from './cell-view';

describe('expiryBadge', () => {
  it('flags an expiry before the last trade date as expired, worded relative to trade date', () => {
    const badge = expiryBadge('2010-08-15', '2010-08-20');

    expect(badge.status).toBe('expired');
    // Must not read as live status — the data is historical.
    expect(badge.label).toBe('expired as of trade date');
  });

  it('flags an expiry within seven days of the last trade date as near', () => {
    const badge = expiryBadge('2010-08-25', '2010-08-20');

    expect(badge.status).toBe('near');
    expect(badge.label).toBe('5 days from trade date');
  });

  it('uses the singular for one day', () => {
    expect(expiryBadge('2010-08-21', '2010-08-20').label).toBe('1 day from trade date');
  });

  it('treats same-day expiry as near', () => {
    const badge = expiryBadge('2010-08-20', '2010-08-20');

    expect(badge.status).toBe('near');
    expect(badge.label).toBe('expires on trade date');
  });

  it('does not badge an expiry comfortably beyond the trade date', () => {
    // The real sample case: 2010-08-20 trade, 2010-09-10 expiry = 21 days.
    const badge = expiryBadge('2010-09-10', '2010-08-20');

    expect(badge.status).toBe('normal');
    expect(badge.days).toBe(21);
  });

  it('cannot measure without a trade date, so does not badge', () => {
    const badge = expiryBadge('2010-09-10', null);

    expect(badge.status).toBe('normal');
    expect(badge.days).toBeNull();
  });
});

describe('barGeometry', () => {
  it('extends right for a positive net', () => {
    expect(barGeometry(100, 200)).toEqual({ side: 'long', percent: 50 });
  });

  it('extends left for a negative net', () => {
    expect(barGeometry(-200, 200)).toEqual({ side: 'short', percent: 100 });
  });

  it('is flat at zero', () => {
    expect(barGeometry(0, 200)).toEqual({ side: 'flat', percent: 0 });
  });

  it('does not divide by zero when every row is flat', () => {
    expect(barGeometry(0, 0)).toEqual({ side: 'flat', percent: 0 });
  });

  it('scales to the largest absolute value in view', () => {
    expect(barGeometry(50, 100).percent).toBe(50);
    expect(barGeometry(50, 500).percent).toBe(10);
  });
});

describe('changedKeys', () => {
  const key = (r: { c: string; p: string }) => r.c + '|' + r.p;

  it('reports nothing on the first snapshot', () => {
    // Everything is "new" initially; flashing every row on load would be noise.
    const rows = [{ k: 'a', updated: 't1' }];

    expect(changedKeys(null, rows, (r) => r.k, (r) => r.updated).size).toBe(0);
  });

  it('reports a row whose timestamp advanced', () => {
    const before = new Map([['a', 't1'], ['b', 't1']]);
    const rows = [{ k: 'a', updated: 't2' }, { k: 'b', updated: 't1' }];

    const changed = changedKeys(before, rows, (r) => r.k, (r) => r.updated);

    expect([...changed]).toEqual(['a']);
  });

  it('reports a newly appeared row', () => {
    const before = new Map([['a', 't1']]);
    const rows = [{ k: 'a', updated: 't1' }, { k: 'b', updated: 't1' }];

    expect([...changedKeys(before, rows, (r) => r.k, (r) => r.updated)]).toEqual(['b']);
  });

  it('reports nothing when a poll returns identical data', () => {
    const before = new Map([['a', 't1']]);
    const rows = [{ k: 'a', updated: 't1' }];

    expect(changedKeys(before, rows, (r) => r.k, (r) => r.updated).size).toBe(0);
  });
});
