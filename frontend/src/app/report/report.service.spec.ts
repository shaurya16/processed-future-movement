import { TestBed } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ReportService } from './report.service';
import { ReportEntry } from './report-entry';

describe('ReportService', () => {
  let service: ReportService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
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
      { Client_Information: 'C1', Product_Information: 'P1', Total_Transaction_Amount: 42 },
    ];

    service.load();
    httpMock.expectOne('/api/report').flush(
      { error: 'not ready' },
      { status: 503, statusText: 'Service Unavailable' },
    );
    expect(service.status()).toBe('loading');
    expect(service.retryCount()).toBe(0);

    await vi.advanceTimersByTimeAsync(3000);
    expect(service.retryCount()).toBe(1);
    httpMock.expectOne('/api/report').flush(
      { error: 'not ready' },
      { status: 503, statusText: 'Service Unavailable' },
    );
    expect(service.status()).toBe('loading');

    await vi.advanceTimersByTimeAsync(3000);
    expect(service.retryCount()).toBe(2);
    httpMock.expectOne('/api/report').flush(sample);

    expect(service.status()).toBe('ready');
    expect(service.entries()).toEqual(sample);
    expect(service.retryCount()).toBe(2);
  });

  it('keeps incrementing retryCount past the 10-attempt threshold without capping retries', async () => {
    service.load();

    for (let i = 1; i <= 15; i++) {
      httpMock.expectOne('/api/report').flush(
        { error: 'not ready' },
        { status: 503, statusText: 'Service Unavailable' },
      );
      expect(service.status()).toBe('loading');
      await vi.advanceTimersByTimeAsync(3000);
      expect(service.retryCount()).toBe(i);
    }

    // Still polling indefinitely — no cap introduced.
    httpMock.expectOne('/api/report').flush([]);
    expect(service.status()).toBe('ready');
    expect(service.retryCount()).toBe(15);
  });

  it('resets retryCount to 0 when load() is called fresh after retries', async () => {
    service.load();
    httpMock.expectOne('/api/report').flush(
      { error: 'not ready' },
      { status: 503, statusText: 'Service Unavailable' },
    );
    await vi.advanceTimersByTimeAsync(3000);
    expect(service.retryCount()).toBe(1);
    httpMock.expectOne('/api/report').flush(
      { error: 'not ready' },
      { status: 503, statusText: 'Service Unavailable' },
    );
    await vi.advanceTimersByTimeAsync(3000);
    expect(service.retryCount()).toBe(2);
    httpMock.expectOne('/api/report').flush(
      { error: 'not ready' },
      { status: 503, statusText: 'Service Unavailable' },
    );

    // A fresh load() call (e.g. triggered by Retry/Refresh) resets the count,
    // even though the previous polling sequence's retry timer is still pending.
    service.load();
    expect(service.retryCount()).toBe(0);
    httpMock.expectOne('/api/report').flush([]);
    expect(service.status()).toBe('ready');
    expect(service.retryCount()).toBe(0);
  });

  it('resets retryCount to 0 when a fresh load() follows a non-retryable error', () => {
    service.load();
    httpMock.expectOne('/api/report').flush(
      { error: 'not ready' },
      { status: 503, statusText: 'Service Unavailable' },
    );

    service.load();
    expect(service.retryCount()).toBe(0);
    httpMock.expectOne('/api/report').flush(
      { error: 'boom' },
      { status: 500, statusText: 'Internal Server Error' },
    );
    expect(service.status()).toBe('error');
    expect(service.retryCount()).toBe(0);
  });

  it('treats a 200 with an empty array as ready, not loading', () => {
    service.load();
    httpMock.expectOne('/api/report').flush([]);

    expect(service.status()).toBe('ready');
    expect(service.entries()).toEqual([]);
  });

  it('treats a non-503 error as an error state and stops retrying', async () => {
    service.load();
    httpMock.expectOne('/api/report').flush(
      { error: 'boom' },
      { status: 500, statusText: 'Internal Server Error' },
    );

    expect(service.status()).toBe('error');
    expect(service.errorMessage()).toBeTruthy();

    await vi.advanceTimersByTimeAsync(10000);
    httpMock.expectNone('/api/report');
  });

  it('load() called again from an error state retries the request', () => {
    service.load();
    httpMock.expectOne('/api/report').flush(null, { status: 500, statusText: 'Internal Server Error' });
    expect(service.status()).toBe('error');

    service.load();
    expect(service.status()).toBe('loading');
    httpMock.expectOne('/api/report').flush([]);
    expect(service.status()).toBe('ready');
  });
});
