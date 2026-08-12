import { TestBed } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ReportService } from './report.service';
import { ReportEntry } from './report-entry';

function row(overrides: Partial<ReportEntry> = {}): ReportEntry {
  return {
    Client_Information: 'CL432100020001',
    Product_Information: 'SGXFUNK20100910',
    Total_Transaction_Amount: 46,
    clientType: 'CL',
    clientNumber: '4321',
    accountNumber: '0002',
    subaccountNumber: '0001',
    exchangeCode: 'SGX',
    productGroupCode: 'FU',
    symbol: 'NK',
    expirationDate: '2010-09-10',
    grossLong: 46,
    grossShort: 0,
    tradeCount: 3,
    firstTransactionDate: '2010-08-19',
    lastTransactionDate: '2010-08-20',
    lastUpdatedAt: '2026-08-12T14:31:52Z',
    feesByCurrency: { USD: -0.9 },
    ...overrides,
  };
}

describe('ReportService', () => {
  let service: ReportService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    localStorage.clear();
    vi.useFakeTimers();
    TestBed.configureTestingModule({
      providers: [provideHttpClient(), provideHttpClientTesting()],
    });
    service = TestBed.inject(ReportService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
    vi.useRealTimers();
  });

  it('starts in loading state before load() is called', () => {
    expect(service.status()).toBe('loading');
    expect(service.entries()).toEqual([]);
    expect(service.errorMessage()).toBeNull();
    expect(service.retryCount()).toBe(0);
  });

  it('retries on 503 and ends ready with data once the store becomes available', async () => {
    const sample: ReportEntry[] = [
      row({ Client_Information: 'C1', Product_Information: 'P1', Total_Transaction_Amount: 42 }),
    ];

    service.load();
    httpMock.expectOne('/api/v1/report').flush(
      { error: 'not ready' },
      { status: 503, statusText: 'Service Unavailable' },
    );
    expect(service.status()).toBe('loading');
    expect(service.retryCount()).toBe(0);

    await vi.advanceTimersByTimeAsync(3000);
    expect(service.retryCount()).toBe(1);
    httpMock.expectOne('/api/v1/report').flush(
      { error: 'not ready' },
      { status: 503, statusText: 'Service Unavailable' },
    );
    expect(service.status()).toBe('loading');

    await vi.advanceTimersByTimeAsync(3000);
    expect(service.retryCount()).toBe(2);
    httpMock.expectOne('/api/v1/report').flush(sample);

    expect(service.status()).toBe('ready');
    expect(service.entries()).toEqual(sample);
    expect(service.retryCount()).toBe(2);
  });

  it('keeps incrementing retryCount past the 10-attempt threshold without capping retries', async () => {
    service.load();

    for (let i = 1; i <= 15; i++) {
      httpMock.expectOne('/api/v1/report').flush(
        { error: 'not ready' },
        { status: 503, statusText: 'Service Unavailable' },
      );
      expect(service.status()).toBe('loading');
      await vi.advanceTimersByTimeAsync(3000);
      expect(service.retryCount()).toBe(i);
    }

    // Still polling indefinitely — no cap introduced.
    httpMock.expectOne('/api/v1/report').flush([]);
    expect(service.status()).toBe('ready');
    expect(service.retryCount()).toBe(15);
  });

  it('resets retryCount to 0 when load() is called fresh after retries', async () => {
    service.load();
    httpMock.expectOne('/api/v1/report').flush(
      { error: 'not ready' },
      { status: 503, statusText: 'Service Unavailable' },
    );
    await vi.advanceTimersByTimeAsync(3000);
    expect(service.retryCount()).toBe(1);
    httpMock.expectOne('/api/v1/report').flush(
      { error: 'not ready' },
      { status: 503, statusText: 'Service Unavailable' },
    );
    await vi.advanceTimersByTimeAsync(3000);
    expect(service.retryCount()).toBe(2);
    httpMock.expectOne('/api/v1/report').flush(
      { error: 'not ready' },
      { status: 503, statusText: 'Service Unavailable' },
    );

    // A fresh load() call (e.g. triggered by Retry/Refresh) resets the count,
    // even though the previous polling sequence's retry timer is still pending.
    service.load();
    expect(service.retryCount()).toBe(0);
    httpMock.expectOne('/api/v1/report').flush([]);
    expect(service.status()).toBe('ready');
    expect(service.retryCount()).toBe(0);
  });

  it('resets retryCount to 0 when a fresh load() follows a non-retryable error', () => {
    service.load();
    httpMock.expectOne('/api/v1/report').flush(
      { error: 'not ready' },
      { status: 503, statusText: 'Service Unavailable' },
    );

    service.load();
    expect(service.retryCount()).toBe(0);
    httpMock.expectOne('/api/v1/report').flush(
      { error: 'boom' },
      { status: 500, statusText: 'Internal Server Error' },
    );
    expect(service.status()).toBe('error');
    expect(service.retryCount()).toBe(0);
  });

  it('treats a 200 with an empty array as ready, not loading', () => {
    service.load();
    httpMock.expectOne('/api/v1/report').flush([]);

    expect(service.status()).toBe('ready');
    expect(service.entries()).toEqual([]);
  });

  it('treats a non-503 error as an error state and stops retrying', async () => {
    service.load();
    httpMock.expectOne('/api/v1/report').flush(
      { error: 'boom' },
      { status: 500, statusText: 'Internal Server Error' },
    );

    expect(service.status()).toBe('error');
    expect(service.errorMessage()).toBeTruthy();

    await vi.advanceTimersByTimeAsync(10000);
    httpMock.expectNone('/api/v1/report');
  });

  it('load() called again from an error state retries the request', () => {
    service.load();
    httpMock.expectOne('/api/v1/report').flush(null, { status: 500, statusText: 'Internal Server Error' });
    expect(service.status()).toBe('error');

    service.load();
    expect(service.status()).toBe('loading');
    httpMock.expectOne('/api/v1/report').flush([]);
    expect(service.status()).toBe('ready');
  });
});

describe('ReportService refresh semantics', () => {
  let service: ReportService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    localStorage.clear();
    vi.useFakeTimers();
    TestBed.configureTestingModule({
      providers: [provideHttpClient(), provideHttpClientTesting()],
    });
    service = TestBed.inject(ReportService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
    vi.useRealTimers();
  });

  it('keeps the previously loaded rows when a refresh fails', () => {
    // The regression this exists to prevent: with a 5s poll, one failed request
    // must not blank a table the user is reading.
    service.load();
    httpMock.expectOne('/api/v1/report').flush([row()]);
    expect(service.status()).toBe('ready');

    service.refresh();
    httpMock.expectOne('/api/v1/report').flush(
      { error: 'boom' },
      { status: 500, statusText: 'Server Error' },
    );

    expect(service.status()).toBe('ready');
    expect(service.entries().length).toBe(1);
    expect(service.stale()).toBe(true);
    expect(service.errorMessage()).toBe('boom');
  });

  it('clears stale once a later refresh succeeds', () => {
    service.load();
    httpMock.expectOne('/api/v1/report').flush([row()]);
    service.refresh();
    httpMock.expectOne('/api/v1/report').flush(
      { error: 'boom' },
      { status: 500, statusText: 'Server Error' },
    );
    expect(service.stale()).toBe(true);

    service.refresh();
    httpMock.expectOne('/api/v1/report').flush([row(), row({ symbol: 'N1' })]);

    expect(service.stale()).toBe(false);
    expect(service.errorMessage()).toBeNull();
    expect(service.entries().length).toBe(2);
  });

  it('shows the error screen when the INITIAL load fails', () => {
    service.load();
    httpMock.expectOne('/api/v1/report').flush(
      { error: 'boom' },
      { status: 500, statusText: 'Server Error' },
    );

    expect(service.status()).toBe('error');
    expect(service.entries()).toEqual([]);
  });

  it('treats a 503 during refresh as stale rather than restarting the retry loop', () => {
    service.load();
    httpMock.expectOne('/api/v1/report').flush([row()]);

    service.refresh();
    httpMock.expectOne('/api/v1/report').flush(
      { error: 'not ready' },
      { status: 503, statusText: 'Service Unavailable' },
    );

    expect(service.status()).toBe('ready');
    expect(service.stale()).toBe(true);
    // No pending retry timer should have been scheduled.
    httpMock.verify();
  });

  it('records lastLoadedAt on success', () => {
    expect(service.lastLoadedAt()).toBeNull();
    service.load();
    httpMock.expectOne('/api/v1/report').flush([row()]);
    expect(service.lastLoadedAt()).not.toBeNull();
  });
});

describe('ReportService polling', () => {
  let service: ReportService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    localStorage.clear();
    vi.useFakeTimers();
    TestBed.configureTestingModule({
      providers: [provideHttpClient(), provideHttpClientTesting()],
    });
    service = TestBed.inject(ReportService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
    vi.useRealTimers();
  });

  it('polls every 5s once started', async () => {
    service.load();
    httpMock.expectOne('/api/v1/report').flush([row()]);

    service.startPolling();
    await vi.advanceTimersByTimeAsync(5000);
    httpMock.expectOne('/api/v1/report').flush([row()]);
    await vi.advanceTimersByTimeAsync(5000);
    httpMock.expectOne('/api/v1/report').flush([row()]);
  });

  it('does not poll while auto-refresh is off', async () => {
    service.load();
    httpMock.expectOne('/api/v1/report').flush([row()]);

    service.setAutoRefresh(false);
    service.startPolling();
    await vi.advanceTimersByTimeAsync(15000);

    // No outstanding requests: httpMock.verify() in afterEach would fail otherwise.
    httpMock.verify();
  });

  it('stops polling while the tab is hidden and refetches immediately on return', async () => {
    service.load();
    httpMock.expectOne('/api/v1/report').flush([row()]);
    service.startPolling();

    vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('hidden');
    document.dispatchEvent(new Event('visibilitychange'));
    await vi.advanceTimersByTimeAsync(15000);
    httpMock.verify(); // nothing fired while hidden

    vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('visible');
    document.dispatchEvent(new Event('visibilitychange'));
    // Resuming refetches at once rather than waiting out the interval.
    httpMock.expectOne('/api/v1/report').flush([row()]);

    vi.restoreAllMocks();
  });

  it('persists the auto-refresh choice', () => {
    service.setAutoRefresh(false);
    expect(localStorage.getItem('pfm.autoRefresh')).toBe('false');
  });
});
