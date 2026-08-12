import { DestroyRef, Injectable, inject, signal } from '@angular/core';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { timer } from 'rxjs';
import { ReportEntry } from './report-entry';
import { readPreference, writePreference } from '../shared/local-preference';

const RETRY_DELAY_MS = 3000;
const POLL_INTERVAL_MS = 5000;
const AUTO_REFRESH_KEY = 'pfm.autoRefresh';

export type ReportStatus = 'loading' | 'ready' | 'error';

@Injectable({ providedIn: 'root' })
export class ReportService {
  private readonly http = inject(HttpClient);

  private readonly _status = signal<ReportStatus>('loading');
  private readonly _entries = signal<ReportEntry[]>([]);
  private readonly _errorMessage = signal<string | null>(null);
  private readonly _retryCount = signal<number>(0);
  private readonly _stale = signal<boolean>(false);
  private readonly _lastLoadedAt = signal<Date | null>(null);
  private readonly _autoRefresh = signal<boolean>(readPreference(AUTO_REFRESH_KEY, true));

  readonly status = this._status.asReadonly();
  readonly entries = this._entries.asReadonly();
  readonly errorMessage = this._errorMessage.asReadonly();
  readonly retryCount = this._retryCount.asReadonly();
  readonly stale = this._stale.asReadonly();
  readonly lastLoadedAt = this._lastLoadedAt.asReadonly();
  readonly autoRefresh = this._autoRefresh.asReadonly();

  private pollTimer: ReturnType<typeof setInterval> | null = null;

  constructor() {
    const onVisibilityChange = () => this.syncPolling(true);
    document.addEventListener('visibilitychange', onVisibilityChange);

    // Without this, every TestBed-created instance leaves a listener on the shared
    // jsdom document and a possibly-live interval behind.
    inject(DestroyRef).onDestroy(() => {
      document.removeEventListener('visibilitychange', onVisibilityChange);
      if (this.pollTimer !== null) {
        clearInterval(this.pollTimer);
        this.pollTimer = null;
      }
    });
  }

  /** Initial load: a failure here is fatal to the view and shows the error screen. */
  load(): void {
    this._status.set('loading');
    this._errorMessage.set(null);
    this._retryCount.set(0);
    this._stale.set(false);
    this.fetch(true);
  }

  /** Poll or manual refresh: a failure here must preserve whatever is on screen. */
  refresh(): void {
    this.fetch(false);
  }

  startPolling(): void {
    this.syncPolling(false);
  }

  setAutoRefresh(on: boolean): void {
    this._autoRefresh.set(on);
    writePreference(AUTO_REFRESH_KEY, on);
    this.syncPolling(false);
  }

  /**
   * @param fetchOnResume refetch immediately when polling (re)starts. True for a
   *        tab becoming visible again — its data may be up to an interval stale —
   *        and false when the caller has just loaded, to avoid a double request.
   */
  private syncPolling(fetchOnResume: boolean): void {
    const shouldPoll = this._autoRefresh() && document.visibilityState !== 'hidden';

    if (shouldPoll && this.pollTimer === null) {
      this.pollTimer = setInterval(() => this.refresh(), POLL_INTERVAL_MS);
      if (fetchOnResume) {
        this.refresh();
      }
      return;
    }
    if (!shouldPoll && this.pollTimer !== null) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
  }

  private fetch(initial: boolean): void {
    this.http.get<ReportEntry[]>('/api/v1/report').subscribe({
      next: (entries) => {
        this._entries.set(entries);
        this._status.set('ready');
        this._stale.set(false);
        this._errorMessage.set(null);
        this._lastLoadedAt.set(new Date());
      },
      error: (err: HttpErrorResponse) => {
        // 503 means "Kafka Streams still starting". Worth waiting out on first
        // load; on a refresh we already have data, so just mark it stale.
        if (err.status === 503 && initial) {
          timer(RETRY_DELAY_MS).subscribe(() => {
            this._retryCount.update((count) => count + 1);
            this.fetch(true);
          });
          return;
        }

        const message =
          err.error?.error ?? err.message ?? 'Unable to reach processing-service.';

        if (initial || this._status() !== 'ready') {
          this._status.set('error');
          this._errorMessage.set(message);
          return;
        }

        // Data is already on screen: keep it and flag it rather than blanking the view.
        this._stale.set(true);
        this._errorMessage.set(message);
      },
    });
  }
}
