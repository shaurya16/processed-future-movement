const EM_DASH = '—';

const MONTHS = [
  'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec',
];

export function formatBytes(bytes: number | null): string {
  if (bytes === null) return EM_DASH;
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

export function formatDateTime(iso: string | null): string {
  if (!iso) return EM_DASH;
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return EM_DASH;
  return date.toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

/**
 * Date-only ISO strings (LocalDate, e.g. "2010-09-10") must NOT go through
 * formatDateTime: `new Date('2010-09-10')` is UTC midnight, so a negative-offset
 * timezone renders the previous day plus a meaningless time.
 */
export function formatDate(iso: string | null): string {
  if (!iso) return EM_DASH;
  const match = /^(\d{4})-(\d{2})-(\d{2})/.exec(iso);
  if (!match) return EM_DASH;
  const [, year, month, day] = match;
  const monthName = MONTHS[Number(month) - 1];
  return monthName ? `${day} ${monthName} ${year}` : EM_DASH;
}

/** @param now injected so tests are deterministic. */
export function formatRelative(iso: string | null, now: Date = new Date()): string {
  if (!iso) return EM_DASH;
  const then = new Date(iso);
  if (Number.isNaN(then.getTime())) return EM_DASH;

  const seconds = Math.floor((now.getTime() - then.getTime()) / 1000);
  if (seconds <= 0) return 'just now';
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}
