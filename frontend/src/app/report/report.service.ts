import { Injectable, signal } from '@angular/core';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { timer } from 'rxjs';
import { ReportEntry } from './report-entry';

const RETRY_DELAY_MS = 3000;

@Injectable({ providedIn: 'root' })
export class ReportService {
  private readonly _status = signal<'loading' | 'ready' | 'error'>('loading');
  private readonly _entries = signal<ReportEntry[]>([]);
  private readonly _errorMessage = signal<string | null>(null);
  private readonly _retryCount = signal<number>(0);

  readonly status = this._status.asReadonly();
  readonly entries = this._entries.asReadonly();
  readonly errorMessage = this._errorMessage.asReadonly();
  readonly retryCount = this._retryCount.asReadonly();

  constructor(private readonly http: HttpClient) {}

  load(): void {
    this._status.set('loading');
    this._errorMessage.set(null);
    this._retryCount.set(0);
    this.fetch();
  }

  private fetch(): void {
    this.http.get<ReportEntry[]>('/api/report').subscribe({
      next: (entries) => {
        this._entries.set(entries);
        this._status.set('ready');
      },
      error: (err: HttpErrorResponse) => {
        if (err.status === 503) {
          timer(RETRY_DELAY_MS).subscribe(() => {
            this._retryCount.update((count) => count + 1);
            this.fetch();
          });
          return;
        }
        this._status.set('error');
        this._errorMessage.set(
          err.error?.error ?? err.message ?? 'Unable to reach processing-service.',
        );
      },
    });
  }
}
