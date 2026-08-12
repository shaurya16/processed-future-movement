import { signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { FilterBar } from './filter-bar';
import { ReportFilters } from './report-filters';

function setup() {
  const filters = {
    client: signal(''),
    account: signal(''),
    product: signal(''),
    search: signal(''),
    clientOptions: signal(['CL123400020001', 'CL432100020001']),
    accountOptions: signal(['0002', '0003']),
    productOptions: signal(['CMEFUNK.20100910', 'SGXFUNK20100910']),
    rows: signal([]),
    totalCount: signal(5),
    activeFilterCount: signal(0),
    setClient: vi.fn(),
    setAccount: vi.fn(),
    setProduct: vi.fn(),
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

  it('populates each dimension select from the data, plus an All option', async () => {
    setup();
    const fixture = TestBed.createComponent(FilterBar);
    fixture.detectChanges();
    await fixture.whenStable();

    const clientSelect: HTMLSelectElement = fixture.nativeElement.querySelector(
      'select[data-testid="filter-client"]',
    );
    // 2 values + "All"
    expect(clientSelect.options.length).toBe(3);
    expect(clientSelect.options[0].textContent).toContain('All');
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
