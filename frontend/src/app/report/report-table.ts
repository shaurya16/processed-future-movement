import { Component, computed, effect, inject, signal } from '@angular/core';
import { ColumnPreferences } from './column-preferences';
import { ReportFilters } from './report-filters';
import { ColumnDef } from './report-columns';
import { ReportEntry } from './report-entry';
import { BarGeometry, ExpiryBadge, barGeometry, changedKeys, expiryBadge } from './cell-view';
import { formatDate, formatDateTime } from './format';

@Component({
  selector: 'app-report-table',
  templateUrl: './report-table.html',
  styles: [
    `
      @keyframes row-flash {
        from {
          background-color: color-mix(in oklab, var(--net-long) 18%, transparent);
        }
        to {
          background-color: transparent;
        }
      }
      .row-changed {
        animation: row-flash 1.2s ease-out;
      }
    `,
  ],
})
export class ReportTable {
  protected readonly filters = inject(ReportFilters);
  protected readonly columnPreferences = inject(ColumnPreferences);

  /** The bar scales to the largest absolute net currently in view, not overall. */
  protected readonly maxAbsoluteNet = computed(() =>
    this.filters.rows().reduce((max, row) => Math.max(max, Math.abs(row.Total_Transaction_Amount)), 0),
  );

  private previousSnapshot: Map<string, string | null> | null = null;
  private readonly _changed = signal<ReadonlySet<string>>(new Set());

  protected readonly changed = this._changed.asReadonly();

  constructor() {
    // Recompute whenever a poll replaces the rows.
    effect(() => {
      const rows = this.filters.rows();
      this._changed.set(
        changedKeys(this.previousSnapshot, rows, (row) => this.rowKey(row), (row) => row.lastUpdatedAt),
      );
      this.previousSnapshot = new Map(rows.map((row) => [this.rowKey(row), row.lastUpdatedAt]));
    });
  }

  protected cellText(column: ColumnDef, entry: ReportEntry): string {
    const value = column.sortValue(entry);
    const stringValue = typeof value === 'string' && value !== '' ? value : null;
    if (column.render === 'date') {
      return formatDate(stringValue);
    }
    if (column.render === 'dateTime') {
      return formatDateTime(stringValue);
    }
    return String(value);
  }

  protected bar(entry: ReportEntry): BarGeometry {
    return barGeometry(entry.Total_Transaction_Amount, this.maxAbsoluteNet());
  }

  /**
   * The expiry cell has a bespoke template (value + badge) so it bypasses cellText().
   * Exposed so that cell formats the date the same way every other date column does,
   * and the same way the Expiry filter dropdown does.
   */
  protected readonly formatDate = formatDate;

  protected expiry(entry: ReportEntry): ExpiryBadge {
    return expiryBadge(entry.expirationDate, entry.lastTransactionDate);
  }

  protected sortIndicator(column: ColumnDef): string {
    if (this.filters.sortColumnId() !== column.id) return '';
    return this.filters.sortDirection() === 'asc' ? '▲' : '▼';
  }

  protected rowKey(entry: ReportEntry): string {
    // Built from the eight discrete fields, not the concatenations: those are
    // trimmed and variable-width, so distinct keys can collide (CL|4321 and
    // CL4|321 both concatenate to CL4321...).
    return [
      entry.clientType, entry.clientNumber, entry.accountNumber, entry.subaccountNumber,
      entry.exchangeCode, entry.productGroupCode, entry.symbol, entry.expirationDate,
    ].join('|');
  }
}
