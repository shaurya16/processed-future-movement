import { Injectable, computed, inject, signal } from '@angular/core';
import { ReportEntry } from './report-entry';
import { REPORT_COLUMNS } from './report-columns';
import { ReportService } from './report.service';

export type SortDirection = 'asc' | 'desc';

export interface FilterCriteria {
  /** '' means "all" for each dimension. */
  client: string;
  account: string;
  product: string;
  search: string;
  sortColumnId: string | null;
  sortDirection: SortDirection;
}

/**
 * Pure so it can be tested without Angular. Filters compose with AND; an empty
 * dimension value means "all". A null sortColumnId preserves the server's order,
 * which is already sorted by client then product.
 */
export function filterAndSort(
  entries: readonly ReportEntry[],
  criteria: FilterCriteria,
): ReportEntry[] {
  const search = criteria.search.trim().toLowerCase();

  const filtered = entries.filter((entry) => {
    if (criteria.client && entry.Client_Information !== criteria.client) return false;
    if (criteria.account && entry.accountNumber !== criteria.account) return false;
    if (criteria.product && entry.Product_Information !== criteria.product) return false;
    if (!search) return true;
    return REPORT_COLUMNS.some((column) =>
      String(column.sortValue(entry)).toLowerCase().includes(search),
    );
  });

  const column = REPORT_COLUMNS.find((candidate) => candidate.id === criteria.sortColumnId);
  if (!column) {
    return filtered;
  }

  const direction = criteria.sortDirection === 'desc' ? -1 : 1;
  // Copy first: sort() mutates, and the input is the service's signal value.
  return [...filtered].sort((left, right) => {
    const a = column.sortValue(left);
    const b = column.sortValue(right);
    if (typeof a === 'number' && typeof b === 'number') {
      return (a - b) * direction;
    }
    return String(a).localeCompare(String(b)) * direction;
  });
}

@Injectable({ providedIn: 'root' })
export class ReportFilters {
  private readonly reportService = inject(ReportService);

  readonly client = signal('');
  readonly account = signal('');
  readonly product = signal('');
  readonly search = signal('');
  readonly sortColumnId = signal<string | null>(null);
  readonly sortDirection = signal<SortDirection>('asc');

  /** Options come from the data, so cardinality is whatever the response contains. */
  readonly clientOptions = computed(() =>
    distinct(this.reportService.entries(), (e) => e.Client_Information),
  );
  readonly accountOptions = computed(() =>
    distinct(this.reportService.entries(), (e) => e.accountNumber),
  );
  readonly productOptions = computed(() =>
    distinct(this.reportService.entries(), (e) => e.Product_Information),
  );

  readonly rows = computed(() =>
    filterAndSort(this.reportService.entries(), {
      client: this.client(),
      account: this.account(),
      product: this.product(),
      search: this.search(),
      sortColumnId: this.sortColumnId(),
      sortDirection: this.sortDirection(),
    }),
  );

  readonly totalCount = computed(() => this.reportService.entries().length);

  readonly activeFilterCount = computed(
    () =>
      [this.client(), this.account(), this.product(), this.search().trim()].filter(
        (value) => value !== '',
      ).length,
  );

  setClient(value: string): void {
    this.client.set(value);
  }

  setAccount(value: string): void {
    this.account.set(value);
  }

  setProduct(value: string): void {
    this.product.set(value);
  }

  setSearch(value: string): void {
    this.search.set(value);
  }

  /** First click sorts ascending; clicking the active column flips direction. */
  toggleSort(columnId: string): void {
    if (this.sortColumnId() === columnId) {
      this.sortDirection.update((direction) => (direction === 'asc' ? 'desc' : 'asc'));
      return;
    }
    this.sortColumnId.set(columnId);
    this.sortDirection.set('asc');
  }

  clearAll(): void {
    this.client.set('');
    this.account.set('');
    this.product.set('');
    this.search.set('');
  }
}

function distinct(
  entries: readonly ReportEntry[],
  select: (entry: ReportEntry) => string,
): string[] {
  return [...new Set(entries.map(select))].sort((a, b) => a.localeCompare(b));
}
