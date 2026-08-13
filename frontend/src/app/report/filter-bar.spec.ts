import { signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { FilterBar } from './filter-bar';
import { ReportFilters } from './report-filters';
import { FILTER_DIMENSIONS, NO_SELECTION } from './report-filter-dimensions';

function setup() {
  const filters = {
    selection: signal({ ...NO_SELECTION }),
    search: signal(''),
    options: signal({
      clientType: ['CL', 'IN'],
      clientNumber: ['1234', '4321'],
      exchangeCode: ['CME', 'SGX'],
      productGroupCode: ['FU'],
      symbol: ['N1', 'NK', 'NK.'],
      expirationDate: ['2010-09-10', '2010-12-10'],
    }),
    rows: signal([]),
    totalCount: signal(5),
    activeFilterCount: signal(0),
    setDimension: vi.fn(),
    setSearch: vi.fn(),
    clearAll: vi.fn(),
  };
  TestBed.configureTestingModule({
    imports: [FilterBar],
    providers: [{ provide: ReportFilters, useValue: filters }],
  });
  return filters;
}

describe('FilterBar', () => {
  beforeEach(() => TestBed.resetTestingModule());

  it('renders one select per dimension and no account filter', async () => {
    setup();
    const fixture = TestBed.createComponent(FilterBar);
    fixture.detectChanges();
    await fixture.whenStable();

    for (const dimension of FILTER_DIMENSIONS) {
      expect(
        fixture.nativeElement.querySelector(`select[data-testid="filter-${dimension.id}"]`),
        `missing select for ${dimension.id}`,
      ).not.toBeNull();
    }
    // Account and subaccount are deliberately not filterable.
    expect(fixture.nativeElement.querySelector('[data-testid="filter-accountNumber"]')).toBeNull();
  });

  it('populates each select from the data, plus an All option', async () => {
    setup();
    const fixture = TestBed.createComponent(FilterBar);
    fixture.detectChanges();
    await fixture.whenStable();

    const symbols: HTMLSelectElement = fixture.nativeElement.querySelector(
      'select[data-testid="filter-symbol"]',
    );
    // 3 values + "All"
    expect(symbols.options.length).toBe(4);
    expect(symbols.options[0].textContent).toContain('All');
  });

  it('displays expiry formatted but keeps the raw ISO string as the value', async () => {
    setup();
    const fixture = TestBed.createComponent(FilterBar);
    fixture.detectChanges();
    await fixture.whenStable();

    const expiry: HTMLSelectElement = fixture.nativeElement.querySelector(
      'select[data-testid="filter-expirationDate"]',
    );
    expect(expiry.options[1].textContent).toContain('10 Sep 2010');
    expect(expiry.options[1].value).toBe('2010-09-10');
  });

  it('forwards a dimension selection', async () => {
    const filters = setup();
    const fixture = TestBed.createComponent(FilterBar);
    fixture.detectChanges();
    await fixture.whenStable();

    const select: HTMLSelectElement = fixture.nativeElement.querySelector(
      'select[data-testid="filter-clientNumber"]',
    );
    select.value = '4321';
    select.dispatchEvent(new Event('change'));

    expect(filters.setDimension).toHaveBeenCalledWith('clientNumber', '4321');
  });

  it('reports the visible-of-total count', async () => {
    const filters = setup();
    filters.rows.set([{} as never, {} as never]);
    const fixture = TestBed.createComponent(FilterBar);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.textContent).toContain('2 of 5');
  });

  it('shows a clear-all control only while a filter is active', async () => {
    const filters = setup();
    const fixture = TestBed.createComponent(FilterBar);
    fixture.detectChanges();
    await fixture.whenStable();
    expect(fixture.nativeElement.querySelector('[data-testid="clear-filters"]')).toBeNull();

    filters.activeFilterCount.set(1);
    fixture.detectChanges();
    await fixture.whenStable();
    expect(fixture.nativeElement.querySelector('[data-testid="clear-filters"]')).not.toBeNull();
  });

  it('forwards a search entry', async () => {
    const filters = setup();
    const fixture = TestBed.createComponent(FilterBar);
    fixture.detectChanges();
    await fixture.whenStable();

    const input: HTMLInputElement = fixture.nativeElement.querySelector(
      'input[data-testid="filter-search"]',
    );
    input.value = 'NK';
    input.dispatchEvent(new Event('input'));

    expect(filters.setSearch).toHaveBeenCalledWith('NK');
  });
});
