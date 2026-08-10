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

    await vi.advanceTimersByTimeAsync(3000);
    httpMock.expectOne('/api/report').flush(
      { error: 'not ready' },
      { status: 503, statusText: 'Service Unavailable' },
    );
    expect(service.status()).toBe('loading');

    await vi.advanceTimersByTimeAsync(3000);
    httpMock.expectOne('/api/report').flush(sample);

    expect(service.status()).toBe('ready');
    expect(service.entries()).toEqual(sample);
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
