import { signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ReportTable } from './report-table';
import { ReportFilters } from './report-filters';
import { ColumnPreferences } from './column-preferences';
import { REPORT_COLUMNS } from './report-columns';
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

function setup(rows: ReportEntry[], visibleIds = ['clientNumber', 'accountNumber', 'netQuantity']) {
  const filters = {
    rows: signal(rows),
    totalCount: signal(rows.length),
    activeFilterCount: signal(0),
    sortColumnId: signal<string | null>(null),
    sortDirection: signal<'asc' | 'desc'>('asc'),
    toggleSort: vi.fn(),
  };
  const prefs = {
    visibleColumns: signal(REPORT_COLUMNS.filter((c) => visibleIds.includes(c.id))),
  };
  TestBed.configureTestingModule({
    imports: [ReportTable],
    providers: [
      { provide: ReportFilters, useValue: filters },
      { provide: ColumnPreferences, useValue: prefs },
    ],
  });
  return { filters, prefs };
}

describe('ReportTable', () => {
  beforeEach(() => TestBed.resetTestingModule());

  it('renders one header cell per visible column, in declaration order', async () => {
    setup([row()]);
    const fixture = TestBed.createComponent(ReportTable);
    fixture.detectChanges();
    await fixture.whenStable();

    const headers = [...fixture.nativeElement.querySelectorAll('thead th')].map((th: HTMLElement) =>
      th.textContent?.trim(),
    );
    expect(headers.length).toBe(3);
    expect(headers[0]).toContain('Client');
    expect(headers[1]).toContain('Account');
    expect(headers[2]).toContain('Net');
  });

  it('renders exactly as many body cells per row as there are headers', async () => {
    // The invariant a column-definition-driven table exists to guarantee.
    setup([row(), row({ clientNumber: '1234' })], [
      'clientNumber',
      'accountNumber',
      'symbol',
      'netQuantity',
      'tradeCount',
    ]);
    const fixture = TestBed.createComponent(ReportTable);
    fixture.detectChanges();
    await fixture.whenStable();

    const headerCount = fixture.nativeElement.querySelectorAll('thead th').length;
    const bodyRows = fixture.nativeElement.querySelectorAll('tbody tr');
    expect(bodyRows.length).toBe(2);
    for (const bodyRow of bodyRows) {
      expect(bodyRow.querySelectorAll('td').length).toBe(headerCount);
    }
  });

  it('shows the signed net value as text, never colour alone', async () => {
    setup([row({ Total_Transaction_Amount: -215 })]);
    const fixture = TestBed.createComponent(ReportTable);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.querySelector('tbody').textContent).toContain('-215');
  });

  it('labels a flat row', async () => {
    setup([row({ Total_Transaction_Amount: 0 })]);
    const fixture = TestBed.createComponent(ReportTable);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.textContent).toContain('flat');
  });

  it('wires a header click to toggleSort', async () => {
    const { filters } = setup([row()]);
    const fixture = TestBed.createComponent(ReportTable);
    fixture.detectChanges();
    await fixture.whenStable();

    fixture.nativeElement.querySelector('thead th button').click();

    expect(filters.toggleSort).toHaveBeenCalledWith('clientNumber');
  });

  it('shows a filtered-empty message distinct from a genuinely empty report', async () => {
    const { filters } = setup([]);
    filters.totalCount.set(5);
    filters.activeFilterCount.set(1);
    const fixture = TestBed.createComponent(ReportTable);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.textContent).toContain('No rows match');
  });

  it('shows the expiry badge relative to trade date', async () => {
    setup([row({ expirationDate: '2010-08-22', lastTransactionDate: '2010-08-20' })], [
      'expirationDate',
    ]);
    const fixture = TestBed.createComponent(ReportTable);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.textContent).toContain('2 days from trade date');
  });
});
