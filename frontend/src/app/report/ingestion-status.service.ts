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
      // load() now runs on every report poll (every 5s), not just once at init.
      // A transient failure must not blank a status that's already on screen:
      // keep the last-known value rather than nulling it out. If nothing has
      // loaded successfully yet, this is a no-op and status stays null, so the
      // "unavailable" empty state is still reachable on first load.
      error: () => {},
    });
  }
}
