export type ExpiryStatus = 'expired' | 'near' | 'normal';

export interface ExpiryBadge {
  status: ExpiryStatus;
  label: string;
  /** Days from the last trade date to expiry; null when it cannot be measured. */
  days: number | null;
}

const NEAR_EXPIRY_DAYS = 7;
const MS_PER_DAY = 86_400_000;

/**
 * Days-to-expiry measured against the row's LAST TRADE DATE, deliberately not
 * against today. The sample data expires in 2010, so a wall-clock comparison
 * would badge every row "expired" — true but informationally empty. Relative to
 * the trade date the number describes the data, which is what the reader wants.
 *
 * Labels name the reference point ("as of trade date") so a historical report is
 * never mistaken for live contract status.
 */
export function expiryBadge(
  expirationDate: string,
  lastTransactionDate: string | null,
): ExpiryBadge {
  if (!lastTransactionDate) {
    return { status: 'normal', label: '', days: null };
  }

  const expiry = Date.parse(expirationDate);
  const traded = Date.parse(lastTransactionDate);
  if (Number.isNaN(expiry) || Number.isNaN(traded)) {
    return { status: 'normal', label: '', days: null };
  }

  const days = Math.round((expiry - traded) / MS_PER_DAY);

  if (days < 0) {
    return { status: 'expired', label: 'expired as of trade date', days };
  }
  if (days === 0) {
    return { status: 'near', label: 'expires on trade date', days };
  }
  if (days <= NEAR_EXPIRY_DAYS) {
    return { status: 'near', label: `${days} day${days === 1 ? '' : 's'} from trade date`, days };
  }
  return { status: 'normal', label: '', days };
}

export interface BarGeometry {
  side: 'long' | 'short' | 'flat';
  /** 0–100, relative to the largest absolute value currently in view. */
  percent: number;
}

export function barGeometry(value: number, maxAbsolute: number): BarGeometry {
  if (value === 0 || maxAbsolute === 0) {
    return { side: 'flat', percent: 0 };
  }
  return {
    side: value > 0 ? 'long' : 'short',
    percent: Math.round((Math.abs(value) / maxAbsolute) * 100),
  };
}

/**
 * Keys whose timestamp advanced since the previous snapshot, plus keys that are
 * new. Returns empty for a null previous snapshot: on first load every row would
 * qualify, and flashing the whole table conveys nothing.
 */
export function changedKeys<T>(
  previous: ReadonlyMap<string, string | null> | null,
  rows: readonly T[],
  keyOf: (row: T) => string,
  updatedAtOf: (row: T) => string | null,
): Set<string> {
  if (previous === null) {
    return new Set();
  }
  const changed = new Set<string>();
  for (const row of rows) {
    const key = keyOf(row);
    if (!previous.has(key) || previous.get(key) !== updatedAtOf(row)) {
      changed.add(key);
    }
  }
  return changed;
}
