import { Component, inject } from '@angular/core';
import { ReportService } from './report.service';
import { formatRelative } from './format';

@Component({
  selector: 'app-refresh-control',
  template: `
    <div class="flex flex-wrap items-center gap-3 text-sm">
      <label class="flex items-center gap-2 text-ink-secondary">
        <input
          type="checkbox"
          data-testid="auto-refresh"
          [checked]="reportService.autoRefresh()"
          (change)="reportService.setAutoRefresh($any($event.target).checked)"
        />
        Auto-refresh
      </label>

      <!-- Manual refresh is only meaningful when polling is off. -->
      <button
        type="button"
        data-testid="refresh"
        class="rounded border border-rule px-2 py-1 text-ink-secondary hover:text-ink-primary disabled:opacity-40"
        [disabled]="reportService.autoRefresh()"
        (click)="reportService.refresh()"
      >
        Refresh
      </button>

      <span class="text-ink-muted" data-testid="last-updated">
        updated {{ relative(reportService.lastLoadedAt()) }}
      </span>

      @if (reportService.stale()) {
        <span
          data-testid="stale-badge"
          class="rounded bg-status-warning/20 px-1.5 py-0.5 text-xs text-ink-primary"
        >
          ⚠ stale — {{ reportService.errorMessage() }}
        </span>
      }
    </div>
  `,
})
export class RefreshControl {
  protected readonly reportService = inject(ReportService);

  protected relative(date: Date | null): string {
    return formatRelative(date === null ? null : date.toISOString());
  }
}
