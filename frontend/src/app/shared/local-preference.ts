/**
 * localStorage access that never throws. A stored preference is a convenience;
 * private-browsing quota errors or hand-corrupted values must degrade to the
 * caller's fallback rather than break the page.
 */
export function readPreference<T>(key: string, fallback: T): T {
  try {
    const raw = localStorage.getItem(key);
    return raw === null ? fallback : (JSON.parse(raw) as T);
  } catch {
    return fallback;
  }
}

export function writePreference<T>(key: string, value: T): void {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch {
    // Preferences are best-effort.
  }
}
