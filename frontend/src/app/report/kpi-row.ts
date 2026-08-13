import { Component, computed, inject } from '@angular/core';
import { ReportFilters } from './report-filters';
import { IngestionStatusService } from './ingestion-status.service';

@Component({
  selector: 'app-kpi-row',
  template: `
    <dl class="grid grid-cols-2 gap-3 sm:grid-cols-4">
      <div class="rounded-lg border border-rule bg-surface-1 p-3" data-testid="kpi-transactions">
        <dt class="text-xs uppercase tracking-wide text-ink-muted">Transactions</dt>
        <dd class="mt-1 text-2xl text-ink-primary">{{ transactions() }}</dd>
        @if (reconciliationMismatch()) {
          <p
            data-testid="reconcile-warning"
            class="mt-1 text-xs text-status-critical"
          >
            ⚠ {{ published() }} published — {{ transactions() }} aggregated
          </p>
        }
      </div>

      <div class="rounded-lg border border-rule bg-surface-1 p-3" data-testid="kpi-pairs">
        <dt class="text-xs uppercase tracking-wide text-ink-muted">Client/product pairs</dt>
        <dd class="mt-1 text-2xl text-ink-primary">{{ filters.rows().length }}</dd>
      </div>

      <div class="rounded-lg border border-rule bg-surface-1 p-3" data-testid="kpi-clients">
        <dt class="text-xs uppercase tracking-wide text-ink-muted">Distinct clients</dt>
        <dd class="mt-1 text-2xl text-ink-primary">{{ distinctClients() }}</dd>
      </div>

      <div class="rounded-lg border border-rule bg-surface-1 p-3" data-testid="kpi-products">
        <dt class="text-xs uppercase tracking-wide text-ink-muted">Distinct products</dt>
        <dd class="mt-1 text-2xl text-ink-primary">{{ distinctProducts() }}</dd>
      </div>
    </dl>
  `,
})
export class KpiRow {
  protected readonly filters = inject(ReportFilters);
  private readonly statusService = inject(IngestionStatusService);

  protected readonly transactions = computed(() =>
    this.filters.rows().reduce((total, row) => total + row.tradeCount, 0),
  );

  protected readonly distinctClients = computed(
    () => new Set(this.filters.rows().map((row) => row.Client_Information)).size,
  );

  protected readonly distinctProducts = computed(
    () => new Set(this.filters.rows().map((row) => row.Product_Information)).size,
  );

  protected readonly published = computed(() => this.statusService.status()?.published ?? null);

  /**
   * A mismatch means records were lost between ingestion and aggregation. Only
   * meaningful with no filters applied and a known published count, so it is
   * suppressed otherwise rather than crying wolf.
   */
  protected readonly reconciliationMismatch = computed(() => {
    // Comparing a filtered subtotal against the whole file's published count would
    // always "mismatch"; the check is only meaningful over the unfiltered set.
    if (this.filters.activeFilterCount() > 0) return false;
    const published = this.published();
    if (published === null) return false;
    return published !== this.transactions();
  });
}
