import { Component, signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { describe, expect, it, vi } from 'vitest';
import { Report } from './report';
import { ReportService } from './report.service';
import { ReportEntry } from './report-entry';

function makeStubService(overrides: {
  status: 'loading' | 'ready' | 'error';
  entries?: ReportEntry[];
  errorMessage?: string | null;
  retryCount?: number;
}) {
  return {
    status: signal(overrides.status),
    entries: signal(overrides.entries ?? []),
    errorMessage: signal(overrides.errorMessage ?? null),
    retryCount: signal(overrides.retryCount ?? 0),
    load: vi.fn(),
  };
}

describe('Report', () => {
  it('renders a loading banner while status is loading', async () => {
    const stub = makeStubService({ status: 'loading' });
    TestBed.configureTestingModule({
      imports: [Report],
      providers: [{ provide: ReportService, useValue: stub }],
    });
    const fixture = TestBed.createComponent(Report);
    fixture.detectChanges();
    await fixture.whenStable();

    const text = fixture.nativeElement.textContent;
    expect(text).toContain('still being generated');
    expect(fixture.nativeElement.querySelector('table')).toBeNull();
  });

  it('renders the error banner and wires Retry to load()', async () => {
    const stub = makeStubService({ status: 'error', errorMessage: 'network down' });
    TestBed.configureTestingModule({
      imports: [Report],
      providers: [{ provide: ReportService, useValue: stub }],
    });
    const fixture = TestBed.createComponent(Report);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.textContent).toContain('network down');
    const retryButton: HTMLButtonElement = fixture.nativeElement.querySelector(
      'button[data-testid="retry"]',
    );
    expect(retryButton).not.toBeNull();
    retryButton.click();
    expect(stub.load).toHaveBeenCalled();
  });

  it('renders an empty-state message (not the loading banner) when ready with zero rows', async () => {
    const stub = makeStubService({ status: 'ready', entries: [] });
    TestBed.configureTestingModule({
      imports: [Report],
      providers: [{ provide: ReportService, useValue: stub }],
    });
    const fixture = TestBed.createComponent(Report);
    fixture.detectChanges();
    await fixture.whenStable();

    const text = fixture.nativeElement.textContent;
    expect(text).toContain('No transactions recorded yet');
    expect(text).not.toContain('still being generated');
    expect(fixture.nativeElement.querySelector('table')).toBeNull();
  });

  it('renders the table and CSV link when ready with data', async () => {
    const sample: ReportEntry[] = [
      { Client_Information: 'C1', Product_Information: 'P1', Total_Transaction_Amount: 42 },
      { Client_Information: 'C2', Product_Information: 'P2', Total_Transaction_Amount: -7 },
    ];
    const stub = makeStubService({ status: 'ready', entries: sample });
    TestBed.configureTestingModule({
      imports: [Report],
      providers: [{ provide: ReportService, useValue: stub }],
    });
    const fixture = TestBed.createComponent(Report);
    fixture.detectChanges();
    await fixture.whenStable();

    const rows = fixture.nativeElement.querySelectorAll('tbody tr');
    expect(rows.length).toBe(2);
    expect(rows[0].textContent).toContain('C1');
    expect(rows[0].textContent).toContain('42');

    const csvLink: HTMLAnchorElement = fixture.nativeElement.querySelector(
      'a[data-testid="csv-download"]',
    );
    expect(csvLink).not.toBeNull();
    expect(csvLink.getAttribute('href')).toBe('/api/report/csv');

    const refreshButton: HTMLButtonElement = fixture.nativeElement.querySelector(
      'button[data-testid="refresh"]',
    );
    refreshButton.click();
    expect(stub.load).toHaveBeenCalled();
  });

  it('does not show the stuck-loading notice while retryCount is at or below the threshold', async () => {
    const stub = makeStubService({ status: 'loading', retryCount: 10 });
    TestBed.configureTestingModule({
      imports: [Report],
      providers: [{ provide: ReportService, useValue: stub }],
    });
    const fixture = TestBed.createComponent(Report);
    fixture.detectChanges();
    await fixture.whenStable();

    const text = fixture.nativeElement.textContent;
    expect(text).toContain('still being generated');
    expect(text).not.toContain('Still waiting after 30s');
    expect(fixture.nativeElement.querySelector('[data-testid="stuck-notice"]')).toBeNull();
  });

  it('shows the stuck-loading notice once retryCount exceeds the threshold', async () => {
    const stub = makeStubService({ status: 'loading', retryCount: 11 });
    TestBed.configureTestingModule({
      imports: [Report],
      providers: [{ provide: ReportService, useValue: stub }],
    });
    const fixture = TestBed.createComponent(Report);
    fixture.detectChanges();
    await fixture.whenStable();

    const text = fixture.nativeElement.textContent;
    expect(text).toContain('still being generated');
    expect(text).toContain('Still waiting after 30s — processing-service may not be healthy.');
    expect(fixture.nativeElement.querySelector('[data-testid="stuck-notice"]')).not.toBeNull();
  });

  it('calls load() once on init', () => {
    const stub = makeStubService({ status: 'loading' });
    TestBed.configureTestingModule({
      imports: [Report],
      providers: [{ provide: ReportService, useValue: stub }],
    });
    const fixture = TestBed.createComponent(Report);
    fixture.detectChanges();
    expect(stub.load).toHaveBeenCalledTimes(1);
  });
});
