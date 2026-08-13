import { Component, inject } from '@angular/core';
import { ReportFilters } from './report-filters';
import { FILTER_DIMENSIONS, FilterDimensionDef } from './report-filter-dimensions';
import { formatDate } from './format';

@Component({
  selector: 'app-filter-bar',
  template: `
    <div class="flex flex-wrap items-end gap-3">
      @for (dimension of dimensions; track dimension.id) {
        <label class="flex flex-col gap-1 text-xs text-ink-muted">
          {{ dimension.label }}
          <select
            [attr.data-testid]="'filter-' + dimension.id"
            class="rounded border border-rule bg-surface-1 px-2 py-1 text-sm text-ink-primary"
            [value]="filters.selection()[dimension.id]"
            (change)="filters.setDimension(dimension.id, $any($event.target).value)"
          >
            <option value="">All ({{ filters.options()[dimension.id].length }})</option>
            @for (option of filters.options()[dimension.id]; track option) {
              <!-- value stays raw; only the label is formatted -->
              <option [value]="option">{{ optionLabel(dimension, option) }}</option>
            }
          </select>
        </label>
      }

      <label class="flex flex-col gap-1 text-xs text-ink-muted">
        Search
        <input
          type="search"
          data-testid="filter-search"
          placeholder="any field"
          class="rounded border border-rule bg-surface-1 px-2 py-1 text-sm text-ink-primary"
          [value]="filters.search()"
          (input)="filters.setSearch($any($event.target).value)"
        />
      </label>

      <p class="ml-auto text-sm text-ink-secondary" aria-live="polite">
        {{ filters.rows().length }} of {{ filters.totalCount() }} rows
        @if (filters.activeFilterCount() > 0) {
          <button
            type="button"
            data-testid="clear-filters"
            class="ml-2 underline hover:text-ink-primary"
            (click)="filters.clearAll()"
          >
            Clear filters
          </button>
        }
      </p>
    </div>
  `,
})
export class FilterBar {
  protected readonly filters = inject(ReportFilters);
  protected readonly dimensions = FILTER_DIMENSIONS;

  /** Dates are shown formatted; every other dimension is already a short code. */
  protected optionLabel(dimension: FilterDimensionDef, option: string): string {
    return dimension.isDate ? formatDate(option) : option;
  }
}
