import { TestBed } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Report } from './report';
import { ReportEntry } from './report-entry';

describe('Report + ReportService integration', () => {
  let httpMock: HttpTestingController;

  beforeEach(() => {
    localStorage.clear();
    vi.useFakeTimers();
    TestBed.configureTestingModule({
      imports: [Report],
      providers: [provideHttpClient(), provideHttpClientTesting()],
    });
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
    vi.useRealTimers();
  });

  it('shows the loading banner on 503, then the table and CSV link once the store becomes ready', async () => {
    const sample: ReportEntry[] = [
      {
        Client_Information: 'C1',
        Product_Information: 'P1',
        Total_Transaction_Amount: 42,
        clientType: 'CL',
        clientNumber: '4321',
        accountNumber: '0002',
        subaccountNumber: '0001',
        exchangeCode: 'SGX',
        productGroupCode: 'FU',
        symbol: 'NK',
        expirationDate: '2010-09-10',
        grossLong: 42,
        grossShort: 0,
        tradeCount: 3,
        firstTransactionDate: '2010-08-19',
        lastTransactionDate: '2010-08-20',
        lastUpdatedAt: '2026-08-12T14:31:52Z',
        feesByCurrency: { USD: -0.9 },
      },
      {
        Client_Information: 'C2',
        Product_Information: 'P2',
        Total_Transaction_Amount: -7,
        clientType: 'CL',
        clientNumber: '4322',
        accountNumber: '0003',
        subaccountNumber: '0001',
        exchangeCode: 'SGX',
        productGroupCode: 'FU',
        symbol: 'N1',
        expirationDate: '2010-09-10',
        grossLong: 0,
        grossShort: 7,
        tradeCount: 1,
        firstTransactionDate: '2010-08-19',
        lastTransactionDate: '2010-08-20',
        lastUpdatedAt: '2026-08-12T14:31:52Z',
        feesByCurrency: { USD: -0.2 },
      },
    ];

    const fixture = TestBed.createComponent(Report);
    fixture.detectChanges();
    await fixture.whenStable();

    // Ingestion has not run yet at page-load time -- the normal case, since the
    // POST that triggers it happens after the UI is open.
    httpMock.expectOne('/api/v1/ingest/status').flush({
      configuredPath: 'sample-data/Input.txt',
      fileExists: true,
      fileSizeBytes: 127624,
      fileLastModified: '2026-08-12T09:14:00Z',
      lastIngestAt: null,
      fingerprint: null,
      totalLines: null,
      published: null,
      skipped: null,
      errorCount: null,
    });

    httpMock.expectOne('/api/v1/report').flush(
      { error: 'not ready' },
      { status: 503, statusText: 'Service Unavailable' },
    );
    fixture.detectChanges();

    let text = fixture.nativeElement.textContent;
    expect(text).toContain('still being generated');
    expect(fixture.nativeElement.querySelector('table')).toBeNull();

    await vi.advanceTimersByTimeAsync(3000);
    httpMock.expectOne('/api/v1/report').flush(sample);
    fixture.detectChanges();
    await fixture.whenStable();

    // The report arriving must refresh provenance too, or the panel stays stuck
    // on "Not yet ingested." while the table fills in.
    httpMock.expectOne('/api/v1/ingest/status').flush({
      configuredPath: 'sample-data/Input.txt',
      fileExists: true,
      fileSizeBytes: 127624,
      fileLastModified: '2026-08-12T09:14:00Z',
      lastIngestAt: '2026-08-12T14:31:52Z',
      fingerprint: 'fp-1',
      totalLines: 717,
      published: 717,
      skipped: 0,
      errorCount: 0,
    });
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.querySelector('[data-testid="not-ingested"]')).toBeNull();
    expect(
      fixture.nativeElement.querySelector('[data-testid="ingested-at"]').textContent,
    ).toContain('2026');

    text = fixture.nativeElement.textContent;
    expect(text).not.toContain('still being generated');

    const rows = fixture.nativeElement.querySelectorAll('tbody tr');
    expect(rows.length).toBe(2);
    // Client_Information/Product_Information are Legacy columns, hidden by default;
    // clientNumber is one of the columns actually shown, so assert on that instead.
    expect(rows[0].textContent).toContain('4321');

    const csvLink: HTMLAnchorElement = fixture.nativeElement.querySelector(
      'a[data-testid="csv-download"]',
    );
    expect(csvLink).not.toBeNull();
    expect(csvLink.getAttribute('href')).toBe('/api/v1/report/csv');
  });
});
