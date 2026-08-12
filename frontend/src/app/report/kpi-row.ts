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

      <div class="rounded-lg border border-rule bg-surface-1 p-3" data-testid="kpi-fees">
        <dt class="text-xs uppercase tracking-wide text-ink-muted">Fees</dt>
        <dd class="mt-1 text-sm text-ink-primary">
          @if (feeEntries().length === 0) {
            <span class="text-2xl">—</span>
          } @else {
            <!-- One figure per currency: two currencies are never added together. -->
            @for (fee of feeEntries(); track fee.currency) {
              <span class="tabular mr-3 whitespace-nowrap">
                {{ fee.currency }} {{ fee.amount }}
              </span>
            }
          }
        </dd>
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

  protected readonly published = computed(() => this.statusService.status()?.published ?? null);

  /**
   * A mismatch means records were lost between ingestion and aggregation. Only
   * meaningful with no filters applied and a known published count, so it is
   * suppressed otherwise rather than crying wolf.
   */
  protected readonly reconciliationMismatch = computed(() => {
    const published = this.published();
    if (published === null) return false;
    return published !== this.transactions();
  });

  /** Totals per currency, in a stable order. Negative is expected: D = debit. */
  protected readonly feeEntries = computed(() => {
    const totals = new Map<string, number>();
    for (const row of this.filters.rows()) {
      for (const [currency, amount] of Object.entries(row.feesByCurrency)) {
        totals.set(currency, (totals.get(currency) ?? 0) + amount);
      }
    }
    return [...totals.entries()]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([currency, amount]) => ({
        currency,
        // toFixed(2) then strip a trailing ".00" so JPY reads -120, USD reads -1.05.
        amount: amount.toFixed(2).replace(/\.00$/, ''),
      }));
  });
}
