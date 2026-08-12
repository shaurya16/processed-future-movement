import { Injectable, computed, inject, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { IngestionStatus } from './report-entry';

@Injectable({ providedIn: 'root' })
export class IngestionStatusService {
  private readonly http = inject(HttpClient);
  private readonly _status = signal<IngestionStatus | null>(null);

  readonly status = this._status.asReadonly();
  readonly available = computed(() => this._status() !== null);

  load(): void {
    this.http.get<IngestionStatus>('/api/v1/ingest/status').subscribe({
      next: (status) => this._status.set(status),
      // Provenance is supplementary; losing it must never break the report view.
      error: () => this._status.set(null),
    });
  }
}
