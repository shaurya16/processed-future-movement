import { TestBed } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { Report } from './report';
import { ReportEntry } from './report-entry';

describe('Report + ReportService integration', () => {
  let httpMock: HttpTestingController;

  beforeEach(() => {
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
      { Client_Information: 'C1', Product_Information: 'P1', Total_Transaction_Amount: 42 },
      { Client_Information: 'C2', Product_Information: 'P2', Total_Transaction_Amount: -7 },
    ];

    const fixture = TestBed.createComponent(Report);
    fixture.detectChanges();
    await fixture.whenStable();

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

    text = fixture.nativeElement.textContent;
    expect(text).not.toContain('still being generated');

    const rows = fixture.nativeElement.querySelectorAll('tbody tr');
    expect(rows.length).toBe(2);
    expect(rows[0].textContent).toContain('C1');

    const csvLink: HTMLAnchorElement = fixture.nativeElement.querySelector(
      'a[data-testid="csv-download"]',
    );
    expect(csvLink).not.toBeNull();
    expect(csvLink.getAttribute('href')).toBe('/api/v1/report/csv');
  });
});
