import { Component, inject } from '@angular/core';
import { ReportFilters } from './report-filters';

@Component({
  selector: 'app-filter-bar',
  template: `
    <div class="flex flex-wrap items-end gap-3">
      <label class="flex flex-col gap-1 text-xs text-ink-muted">
        Client
        <select
          data-testid="filter-client"
          class="rounded border border-rule bg-surface-1 px-2 py-1 text-sm text-ink-primary"
          [value]="filters.client()"
          (change)="filters.setClient($any($event.target).value)"
        >
          <option value="">All ({{ filters.clientOptions().length }})</option>
          @for (option of filters.clientOptions(); track option) {
            <option [value]="option">{{ option }}</option>
          }
        </select>
      </label>

      <label class="flex flex-col gap-1 text-xs text-ink-muted">
        Account
        <select
          data-testid="filter-account"
          class="rounded border border-rule bg-surface-1 px-2 py-1 text-sm text-ink-primary"
          [value]="filters.account()"
          (change)="filters.setAccount($any($event.target).value)"
        >
          <option value="">All ({{ filters.accountOptions().length }})</option>
          @for (option of filters.accountOptions(); track option) {
            <option [value]="option">{{ option }}</option>
          }
        </select>
      </label>

      <label class="flex flex-col gap-1 text-xs text-ink-muted">
        Product
        <select
          data-testid="filter-product"
          class="rounded border border-rule bg-surface-1 px-2 py-1 text-sm text-ink-primary"
          [value]="filters.product()"
          (change)="filters.setProduct($any($event.target).value)"
        >
          <option value="">All ({{ filters.productOptions().length }})</option>
          @for (option of filters.productOptions(); track option) {
            <option [value]="option">{{ option }}</option>
          }
        </select>
      </label>

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
}
