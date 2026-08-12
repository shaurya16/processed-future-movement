import { Component, inject } from '@angular/core';
import { IngestionStatusService } from './ingestion-status.service';
import { formatBytes, formatDateTime, formatRelative } from './format';

@Component({
  selector: 'app-source-file-panel',
  template: `
    <section
      class="rounded-lg border border-rule bg-surface-1 p-4"
      aria-labelledby="source-file-heading"
    >
      <h2 id="source-file-heading" class="text-xs font-semibold uppercase tracking-wide text-ink-muted">
        Source file
      </h2>

      @if (statusService.status(); as status) {
        <p class="mt-2 font-mono text-sm text-ink-primary" data-testid="configured-path">
          {{ status.configuredPath }}
        </p>

        @if (status.fileExists) {
          <p class="mt-1 text-sm text-ink-secondary">
            {{ bytes(status.fileSizeBytes) }} · modified {{ dateTime(status.fileLastModified) }}
          </p>
        } @else {
          <p class="mt-1 text-sm text-status-critical" data-testid="file-missing">
            ⚠ File not found at this path
          </p>
        }

        @if (status.lastIngestAt) {
          <dl class="mt-3 grid grid-cols-2 gap-x-4 gap-y-1 text-sm sm:grid-cols-4">
            <div>
              <dt class="text-ink-muted">Ingested</dt>
              <dd class="text-ink-primary" data-testid="ingested-at">
                {{ dateTime(status.lastIngestAt) }} ({{ relative(status.lastIngestAt) }})
              </dd>
            </div>
            <div>
              <dt class="text-ink-muted">Published</dt>
              <dd class="tabular text-ink-primary">{{ status.published }}</dd>
            </div>
            <div>
              <dt class="text-ink-muted">Skipped</dt>
              <dd class="tabular text-ink-primary">{{ status.skipped }}</dd>
            </div>
            <div>
              <dt class="text-ink-muted">Failed</dt>
              <dd
                class="tabular"
                [class.text-status-critical]="(status.errorCount ?? 0) > 0"
                [class.text-ink-primary]="(status.errorCount ?? 0) === 0"
              >
                {{ status.errorCount }}
              </dd>
            </div>
          </dl>
        } @else {
          <p class="mt-3 text-sm text-ink-secondary" data-testid="not-ingested">
            Not yet ingested.
          </p>
        }
      } @else {
        <p class="mt-2 text-sm text-ink-secondary" data-testid="status-unavailable">
          File details unavailable.
        </p>
      }
    </section>
  `,
})
export class SourceFilePanel {
  protected readonly statusService = inject(IngestionStatusService);

  protected bytes = formatBytes;
  protected dateTime = formatDateTime;
  protected relative = (iso: string | null) => formatRelative(iso);
}
