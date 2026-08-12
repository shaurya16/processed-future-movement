import { signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { Report } from './report';
import { ReportService } from './report.service';
import { ReportFilters } from './report-filters';
import { ColumnPreferences } from './column-preferences';
import { IngestionStatusService } from './ingestion-status.service';
import { REPORT_COLUMNS } from './report-columns';

function setup(overrides: {
  status: 'loading' | 'ready' | 'error';
  errorMessage?: string | null;
  retryCount?: number;
}) {
  const reportService = {
    status: signal(overrides.status),
    entries: signal([]),
    errorMessage: signal(overrides.errorMessage ?? null),
    retryCount: signal(overrides.retryCount ?? 0),
    stale: signal(false),
    lastLoadedAt: signal<Date | null>(null),
    autoRefresh: signal(true),
    load: vi.fn(),
    refresh: vi.fn(),
    startPolling: vi.fn(),
    setAutoRefresh: vi.fn(),
  };
  const statusService = { status: signal(null), available: signal(false), load: vi.fn() };
  const filters = {
    client: signal(''),
    account: signal(''),
    product: signal(''),
    search: signal(''),
    clientOptions: signal<string[]>([]),
    accountOptions: signal<string[]>([]),
    productOptions: signal<string[]>([]),
    rows: signal([]),
    totalCount: signal(0),
    activeFilterCount: signal(0),
    sortColumnId: signal<string | null>(null),
    sortDirection: signal<'asc' | 'desc'>('asc'),
    setClient: vi.fn(),
    setAccount: vi.fn(),
    setProduct: vi.fn(),
    setSearch: vi.fn(),
    clearAll: vi.fn(),
    toggleSort: vi.fn(),
  };
  const prefs = {
    visibleIds: signal(['clientNumber']),
    visibleColumns: signal(REPORT_COLUMNS.filter((c) => c.id === 'clientNumber')),
    isVisible: () => true,
    toggle: vi.fn(),
    reset: vi.fn(),
  };

  TestBed.configureTestingModule({
    imports: [Report],
    providers: [
      { provide: ReportService, useValue: reportService },
      { provide: IngestionStatusService, useValue: statusService },
      { provide: ReportFilters, useValue: filters },
      { provide: ColumnPreferences, useValue: prefs },
    ],
  });
  return { reportService, statusService };
}

describe('Report shell', () => {
  beforeEach(() => {
    localStorage.clear();
    TestBed.resetTestingModule();
  });

  it('loads the report, starts polling and loads provenance on init', () => {
    const { reportService, statusService } = setup({ status: 'loading' });

    TestBed.createComponent(Report).detectChanges();

    expect(reportService.load).toHaveBeenCalledTimes(1);
    expect(reportService.startPolling).toHaveBeenCalledTimes(1);
    expect(statusService.load).toHaveBeenCalledTimes(1);
  });

  it('shows the loading banner and no table while loading', async () => {
    setup({ status: 'loading' });
    const fixture = TestBed.createComponent(Report);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.textContent).toContain('still being generated');
    expect(fixture.nativeElement.querySelector('table')).toBeNull();
  });

  it('hides the stuck notice at or below the retry threshold', async () => {
    setup({ status: 'loading', retryCount: 10 });
    const fixture = TestBed.createComponent(Report);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.querySelector('[data-testid="stuck-notice"]')).toBeNull();
  });

  it('shows the stuck notice past the retry threshold', async () => {
    setup({ status: 'loading', retryCount: 11 });
    const fixture = TestBed.createComponent(Report);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.querySelector('[data-testid="stuck-notice"]')).not.toBeNull();
    expect(fixture.nativeElement.textContent).toContain('Still waiting after 30s');
  });

  it('shows the error banner and wires Retry to load()', async () => {
    const { reportService } = setup({ status: 'error', errorMessage: 'network down' });
    const fixture = TestBed.createComponent(Report);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.textContent).toContain('network down');
    reportService.load.mockClear();
    fixture.nativeElement.querySelector('button[data-testid="retry"]').click();
    expect(reportService.load).toHaveBeenCalledTimes(1);
  });

  it('renders the table region and CSV link when ready', async () => {
    setup({ status: 'ready' });
    const fixture = TestBed.createComponent(Report);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.querySelector('table')).not.toBeNull();
    const csvLink: HTMLAnchorElement = fixture.nativeElement.querySelector(
      'a[data-testid="csv-download"]',
    );
    expect(csvLink.getAttribute('href')).toBe('/api/v1/report/csv');
  });
});
