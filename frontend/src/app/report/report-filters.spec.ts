import { describe, expect, it } from 'vitest';
import { FilterCriteria, filterAndSort } from './report-filters';
import { NO_SELECTION } from './report-filter-dimensions';
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

const NO_FILTERS: FilterCriteria = {
  selection: { ...NO_SELECTION },
  search: '',
  sortColumnId: null,
  sortDirection: 'asc',
};

function withSelection(overrides: Partial<typeof NO_SELECTION>): FilterCriteria {
  return { ...NO_FILTERS, selection: { ...NO_SELECTION, ...overrides } };
}

describe('filterAndSort', () => {
  it('returns everything when no filter is set', () => {
    const rows = [row(), row({ clientNumber: '1234' })];

    expect(filterAndSort(rows, NO_FILTERS).length).toBe(2);
  });

  it('filters by client number, spanning that client\'s accounts', () => {
    const rows = [
      row({ clientNumber: '4321', accountNumber: '0002' }),
      row({ clientNumber: '4321', accountNumber: '0003' }),
      row({ clientNumber: '1234', accountNumber: '0002' }),
    ];

    const result = filterAndSort(rows, withSelection({ clientNumber: '4321' }));

    expect(result.length).toBe(2);
    expect(result.map((r) => r.accountNumber)).toEqual(['0002', '0003']);
  });

  it('filters by client type', () => {
    const rows = [row({ clientType: 'CL' }), row({ clientType: 'IN' })];

    const result = filterAndSort(rows, withSelection({ clientType: 'IN' }));

    expect(result.length).toBe(1);
    expect(result[0].clientType).toBe('IN');
  });

  it('filters by exchange, spanning every symbol traded there', () => {
    const rows = [
      row({ exchangeCode: 'CME', symbol: 'N1' }),
      row({ exchangeCode: 'CME', symbol: 'NK.' }),
      row({ exchangeCode: 'SGX', symbol: 'NK' }),
    ];

    const result = filterAndSort(rows, withSelection({ exchangeCode: 'CME' }));

    expect(result.length).toBe(2);
    expect(result.map((r) => r.symbol)).toEqual(['N1', 'NK.']);
  });

  it('filters by product group', () => {
    const rows = [row({ productGroupCode: 'FU' }), row({ productGroupCode: 'OP' })];

    expect(filterAndSort(rows, withSelection({ productGroupCode: 'OP' })).length).toBe(1);
  });

  it('filters by symbol', () => {
    const rows = [row({ symbol: 'NK' }), row({ symbol: 'N1' })];

    const result = filterAndSort(rows, withSelection({ symbol: 'N1' }));

    expect(result.length).toBe(1);
    expect(result[0].symbol).toBe('N1');
  });

  it('filters by expiry on the raw ISO value, not a formatted one', () => {
    const rows = [row({ expirationDate: '2010-09-10' }), row({ expirationDate: '2010-12-10' })];

    expect(filterAndSort(rows, withSelection({ expirationDate: '2010-12-10' })).length).toBe(1);
    // A formatted value must never match: the comparison is an exact string match.
    expect(filterAndSort(rows, withSelection({ expirationDate: '10 Dec 2010' })).length).toBe(0);
  });

  it('composes every dimension with AND', () => {
    const rows = [
      row({ clientNumber: '4321', exchangeCode: 'CME', symbol: 'N1' }),
      row({ clientNumber: '4321', exchangeCode: 'SGX', symbol: 'N1' }),
      row({ clientNumber: '1234', exchangeCode: 'CME', symbol: 'N1' }),
      row({ clientNumber: '4321', exchangeCode: 'CME', symbol: 'NK' }),
    ];

    const result = filterAndSort(
      rows,
      withSelection({ clientNumber: '4321', exchangeCode: 'CME', symbol: 'N1' }),
    );

    expect(result.length).toBe(1);
  });

  it('searches case-insensitively across fields', () => {
    // Product_Information must avoid the substring "nk" here (the default
    // 'SGXFUNK20100910' contains it via "FUNK"), or both rows match on that
    // field regardless of `symbol` and the assertion below is vacuous.
    const rows = [
      row({ symbol: 'NK' }),
      row({ symbol: 'N1', Product_Information: 'CMEFUEQ20100910' }),
    ];

    expect(filterAndSort(rows, { ...NO_FILTERS, search: 'nk' }).length).toBe(1);
  });

  it('matches a numeric value via search', () => {
    const rows = [row({ Total_Transaction_Amount: 46 }), row({ Total_Transaction_Amount: -215 })];

    expect(filterAndSort(rows, { ...NO_FILTERS, search: '-215' }).length).toBe(1);
  });

  it('returns an empty array when filters exclude everything', () => {
    expect(filterAndSort([row()], withSelection({ clientNumber: 'nope' }))).toEqual([]);
  });

  it('sorts ascending by a string column', () => {
    const rows = [row({ symbol: 'NK' }), row({ symbol: 'N1' })];

    const result = filterAndSort(rows, { ...NO_FILTERS, sortColumnId: 'symbol' });

    expect(result.map((r) => r.symbol)).toEqual(['N1', 'NK']);
  });

  it('sorts descending when direction is desc', () => {
    const rows = [row({ symbol: 'N1' }), row({ symbol: 'NK' })];

    const result = filterAndSort(rows, {
      ...NO_FILTERS,
      sortColumnId: 'symbol',
      sortDirection: 'desc',
    });

    expect(result.map((r) => r.symbol)).toEqual(['NK', 'N1']);
  });

  it('sorts numerically, not lexicographically', () => {
    const rows = [
      row({ Total_Transaction_Amount: 46 }),
      row({ Total_Transaction_Amount: -215 }),
      row({ Total_Transaction_Amount: 285 }),
    ];

    const result = filterAndSort(rows, { ...NO_FILTERS, sortColumnId: 'netQuantity' });

    // Lexicographic order would give -215, 285, 46.
    expect(result.map((r) => r.Total_Transaction_Amount)).toEqual([-215, 46, 285]);
  });

  it('preserves the server order when no sort column is set', () => {
    const rows = [row({ symbol: 'NK' }), row({ symbol: 'N1' })];

    expect(filterAndSort(rows, NO_FILTERS).map((r) => r.symbol)).toEqual(['NK', 'N1']);
  });

  it('does not mutate the input array', () => {
    const rows = [row({ symbol: 'NK' }), row({ symbol: 'N1' })];

    filterAndSort(rows, { ...NO_FILTERS, sortColumnId: 'symbol' });

    expect(rows.map((r) => r.symbol)).toEqual(['NK', 'N1']);
  });

  it('ignores an unknown sort column rather than throwing', () => {
    expect(filterAndSort([row()], { ...NO_FILTERS, sortColumnId: 'nope' }).length).toBe(1);
  });
});
