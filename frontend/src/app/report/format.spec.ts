import { describe, expect, it } from 'vitest';
import { formatBytes, formatDate, formatDateTime, formatRelative } from './format';

describe('formatBytes', () => {
  it('formats bytes, KB and MB', () => {
    expect(formatBytes(512)).toBe('512 B');
    expect(formatBytes(127624)).toBe('124.6 KB');
    expect(formatBytes(5 * 1024 * 1024)).toBe('5.0 MB');
  });

  it('renders an em dash for null', () => {
    expect(formatBytes(null)).toBe('—');
  });

  it('handles zero without dividing', () => {
    expect(formatBytes(0)).toBe('0 B');
  });
});

describe('formatDateTime', () => {
  it('renders an em dash for null', () => {
    expect(formatDateTime(null)).toBe('—');
  });

  it('includes the date and time', () => {
    const formatted = formatDateTime('2026-08-12T14:31:52Z');
    expect(formatted).toContain('2026');
  });

  it('renders an em dash for an unparseable string', () => {
    expect(formatDateTime('garbage')).toBe('—');
  });
});

describe('formatDate', () => {
  it('renders a date-only LocalDate string without a time component', () => {
    expect(formatDate('2010-09-10')).toBe('10 Sep 2010');
  });

  it('renders an em dash for null', () => {
    expect(formatDate(null)).toBe('—');
  });

  it('renders an em dash for an unparseable string', () => {
    expect(formatDate('garbage')).toBe('—');
  });

  it('does not shift the day the way formatDateTime would in a negative-offset zone', () => {
    // Regression for: new Date('2010-09-10') is UTC midnight, which a
    // negative-offset timezone renders as the previous day. Asserting on the
    // returned string's day number (rather than depending on the test
    // runner's own timezone, which may be UTC where the bug is invisible)
    // catches the regression regardless of where the suite runs.
    const formatted = formatDate('2010-09-10');
    expect(formatted).toContain('10');
    expect(formatted).not.toContain('09 Sep');
    expect(formatted).not.toContain(' pm');
    expect(formatted).not.toContain(' am');
  });
});

describe('formatRelative', () => {
  const now = new Date('2026-08-12T14:35:00Z');

  it('reports seconds under a minute', () => {
    expect(formatRelative('2026-08-12T14:34:30Z', now)).toBe('30s ago');
  });

  it('reports whole minutes', () => {
    expect(formatRelative('2026-08-12T14:32:00Z', now)).toBe('3m ago');
  });

  it('reports hours', () => {
    expect(formatRelative('2026-08-12T11:35:00Z', now)).toBe('3h ago');
  });

  it('says just now for the current instant', () => {
    expect(formatRelative('2026-08-12T14:35:00Z', now)).toBe('just now');
  });

  it('says just now for a non-positive delta', () => {
    expect(formatRelative('2026-08-12T14:36:00Z', now)).toBe('just now');
  });

  it('renders an em dash for null', () => {
    expect(formatRelative(null, now)).toBe('—');
  });

  it('renders an em dash for an unparseable string', () => {
    expect(formatRelative('garbage', now)).toBe('—');
  });
});
