import { Injectable, computed, signal } from '@angular/core';
import { readPreference, writePreference } from '../shared/local-preference';
import { ColumnDef, DEFAULT_VISIBLE_COLUMN_IDS, REPORT_COLUMNS } from './report-columns';

// Bumped from 'pfm.visibleColumns' when the default set changed: a stored value
// always wins over the defaults, so without a new key nobody would ever see the
// new columns. The old key is left to expire rather than migrated.
const STORAGE_KEY = 'pfm.visibleColumns.v2';

@Injectable({ providedIn: 'root' })
export class ColumnPreferences {
  private readonly _visibleIds = signal<readonly string[]>(restore());

  readonly visibleIds = this._visibleIds.asReadonly();

  /**
   * Always in declaration order, never in the order the user toggled things —
   * otherwise columns would jump around as they are switched on.
   */
  readonly visibleColumns = computed<readonly ColumnDef[]>(() => {
    const visible = new Set(this._visibleIds());
    return REPORT_COLUMNS.filter((column) => visible.has(column.id));
  });

  isVisible(id: string): boolean {
    return this._visibleIds().includes(id);
  }

  toggle(id: string): void {
    const next = this.isVisible(id)
      ? this._visibleIds().filter((visibleId) => visibleId !== id)
      : [...this._visibleIds(), id];
    this._visibleIds.set(next);
    writePreference(STORAGE_KEY, next);
  }

  reset(): void {
    this._visibleIds.set([...DEFAULT_VISIBLE_COLUMN_IDS]);
    writePreference(STORAGE_KEY, DEFAULT_VISIBLE_COLUMN_IDS);
  }
}

/**
 * Stored ids are filtered against the current definitions, so renaming or removing
 * a column in a future release cannot strand a user with an empty table.
 */
function restore(): readonly string[] {
  const stored = readPreference<unknown>(STORAGE_KEY, null);
  if (!Array.isArray(stored)) {
    return [...DEFAULT_VISIBLE_COLUMN_IDS];
  }
  const known = new Set(REPORT_COLUMNS.map((column) => column.id));
  const filtered = stored.filter((id): id is string => typeof id === 'string' && known.has(id));
  return filtered.length > 0 ? filtered : [...DEFAULT_VISIBLE_COLUMN_IDS];
}
