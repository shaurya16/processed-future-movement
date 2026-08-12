import { beforeEach, describe, expect, it, vi } from 'vitest';
import { readPreference, writePreference } from './local-preference';

describe('local-preference', () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it('returns the fallback when nothing is stored', () => {
    expect(readPreference('pfm.missing', 'default')).toBe('default');
  });

  it('round-trips a value', () => {
    writePreference('pfm.key', { a: 1 });
    expect(readPreference('pfm.key', null)).toEqual({ a: 1 });
  });

  it('returns the fallback when the stored value is not valid JSON', () => {
    localStorage.setItem('pfm.broken', '{not json');
    expect(readPreference('pfm.broken', 'fallback')).toBe('fallback');
  });

  it('does not throw when storage is unavailable', () => {
    // Private browsing can make setItem throw; a preference is never worth a crash.
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new Error('QuotaExceededError');
    });
    expect(() => writePreference('pfm.key', 'v')).not.toThrow();
    vi.restoreAllMocks();
  });
});
