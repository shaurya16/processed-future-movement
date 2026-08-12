import { signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { beforeEach, describe, expect, it } from 'vitest';
import { KpiRow } from './kpi-row';
import { ReportFilters } from './report-filters';
import { IngestionStatusService } from './ingestion-status.service';
import { IngestionStatus, ReportEntry } from './report-entry';

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

function setup(rows: ReportEntry[], status: IngestionStatus | null) {
  TestBed.configureTestingModule({
    imports: [KpiRow],
    providers: [
      { provide: ReportFilters, useValue: { rows: signal(rows) } },
      { provide: IngestionStatusService, useValue: { status: signal(status) } },
    ],
  });
}

describe('KpiRow', () => {
  beforeEach(() => TestBed.resetTestingModule());

  it('sums trade counts and counts pairs and distinct clients', async () => {
    setup(
      [
        row({ tradeCount: 3 }),
        row({ tradeCount: 4, Client_Information: 'CL123400030001' }),
        row({ tradeCount: 5, Client_Information: 'CL123400030001', symbol: 'N1' }),
      ],
      null,
    );
    const fixture = TestBed.createComponent(KpiRow);
    fixture.detectChanges();
    await fixture.whenStable();

    const text = fixture.nativeElement.textContent;
    expect(fixture.nativeElement.querySelector('[data-testid="kpi-transactions"]').textContent)
      .toContain('12');
    expect(fixture.nativeElement.querySelector('[data-testid="kpi-pairs"]').textContent)
      .toContain('3');
    expect(fixture.nativeElement.querySelector('[data-testid="kpi-clients"]').textContent)
      .toContain('2');
    expect(text).toBeTruthy();
  });

  it('renders one figure per currency and never blends them', async () => {
    setup(
      [
        row({ feesByCurrency: { USD: -0.9, JPY: -120 } }),
        row({ feesByCurrency: { USD: -0.15 } }),
      ],
      null,
    );
    const fixture = TestBed.createComponent(KpiRow);
    fixture.detectChanges();
    await fixture.whenStable();

    const fees = fixture.nativeElement.querySelector('[data-testid="kpi-fees"]').textContent;
    expect(fees).toContain('USD');
    expect(fees).toContain('-1.05');
    expect(fees).toContain('JPY');
    expect(fees).toContain('-120');
  });

  it('shows an em dash for fees when there are none', async () => {
    setup([row({ feesByCurrency: {} })], null);
    const fixture = TestBed.createComponent(KpiRow);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.querySelector('[data-testid="kpi-fees"]').textContent)
      .toContain('—');
  });

  it('warns when aggregated trades disagree with records published', async () => {
    setup([row({ tradeCount: 700 })], {
      configuredPath: 'sample-data/Input.txt',
      fileExists: true,
      fileSizeBytes: 127624,
      fileLastModified: null,
      lastIngestAt: '2026-08-12T14:31:52Z',
      fingerprint: 'fp',
      totalLines: 717,
      published: 717,
      skipped: 0,
      errorCount: 0,
    });
    const fixture = TestBed.createComponent(KpiRow);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.querySelector('[data-testid="reconcile-warning"]')).not.toBeNull();
  });

  it('does not warn when they agree', async () => {
    setup([row({ tradeCount: 717 })], {
      configuredPath: 'sample-data/Input.txt',
      fileExists: true,
      fileSizeBytes: 127624,
      fileLastModified: null,
      lastIngestAt: '2026-08-12T14:31:52Z',
      fingerprint: 'fp',
      totalLines: 717,
      published: 717,
      skipped: 0,
      errorCount: 0,
    });
    const fixture = TestBed.createComponent(KpiRow);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.querySelector('[data-testid="reconcile-warning"]')).toBeNull();
  });

  it('does not warn when the status endpoint is unavailable', async () => {
    setup([row({ tradeCount: 3 })], null);
    const fixture = TestBed.createComponent(KpiRow);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.querySelector('[data-testid="reconcile-warning"]')).toBeNull();
  });
});
