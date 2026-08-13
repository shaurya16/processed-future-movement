import { Injectable, computed, inject, signal } from '@angular/core';
import { ReportEntry } from './report-entry';
import { REPORT_COLUMNS } from './report-columns';
import {
  FILTER_DIMENSIONS,
  FilterDimensionId,
  FilterSelection,
  NO_SELECTION,
} from './report-filter-dimensions';
import { ReportService } from './report.service';

export type SortDirection = 'asc' | 'desc';

export interface FilterCriteria {
  /** '' means "all" for each dimension. */
  selection: FilterSelection;
  search: string;
  sortColumnId: string | null;
  sortDirection: SortDirection;
}

/**
 * Pure so it can be tested without Angular. Dimensions compose with AND; an empty
 * value means "all". A null sortColumnId preserves the server's order, which is
 * already sorted by client then product.
 */
export function filterAndSort(
  entries: readonly ReportEntry[],
  criteria: FilterCriteria,
): ReportEntry[] {
  const search = criteria.search.trim().toLowerCase();

  const filtered = entries.filter((entry) => {
    for (const dimension of FILTER_DIMENSIONS) {
      const selected = criteria.selection[dimension.id];
      if (selected && entry[dimension.id] !== selected) return false;
    }
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

  readonly selection = signal<FilterSelection>({ ...NO_SELECTION });
  readonly search = signal('');
  readonly sortColumnId = signal<string | null>(null);
  readonly sortDirection = signal<SortDirection>('asc');

  /**
   * Options come from the data, so cardinality is whatever the response contains.
   * A dimension that is single-valued in the loaded file shows one option; that is
   * the file, not a broken control.
   */
  readonly options = computed<Record<FilterDimensionId, string[]>>(() => {
    const entries = this.reportService.entries();
    const result = {} as Record<FilterDimensionId, string[]>;
    for (const dimension of FILTER_DIMENSIONS) {
      result[dimension.id] = [...new Set(entries.map((entry) => entry[dimension.id]))].sort(
        (a, b) => a.localeCompare(b),
      );
    }
    return result;
  });

  readonly rows = computed(() =>
    filterAndSort(this.reportService.entries(), {
      selection: this.selection(),
      search: this.search(),
      sortColumnId: this.sortColumnId(),
      sortDirection: this.sortDirection(),
    }),
  );

  readonly totalCount = computed(() => this.reportService.entries().length);

  readonly activeFilterCount = computed(() => {
    const selection = this.selection();
    const dimensions = FILTER_DIMENSIONS.filter((d) => selection[d.id] !== '').length;
    return dimensions + (this.search().trim() === '' ? 0 : 1);
  });

  setDimension(dimension: FilterDimensionId, value: string): void {
    this.selection.update((current) => ({ ...current, [dimension]: value }));
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
    this.selection.set({ ...NO_SELECTION });
    this.search.set('');
  }
}
