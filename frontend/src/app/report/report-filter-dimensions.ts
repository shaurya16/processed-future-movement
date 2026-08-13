/**
 * The dimensions offered as filter dropdowns, mirroring the REPORT_COLUMNS
 * pattern: one table drives both the state and the template.
 *
 * Every id names a ReportEntry field holding a short code, so filtering is an
 * exact string match. Account and subaccount are deliberately absent — an
 * account number means nothing without the client it hangs off, and the client
 * number filter already pins that.
 */
import { REPORT_COLUMNS } from './report-columns';

export type FilterDimensionId =
  | 'clientType'
  | 'clientNumber'
  | 'exchangeCode'
  | 'productGroupCode'
  | 'symbol'
  | 'expirationDate';

export interface FilterDimensionDef {
  id: FilterDimensionId;
  label: string;
  /**
   * Date-only fields display formatted ("10 Sep 2010") but the option value
   * stays the raw ISO string, because the comparison against the entry field is
   * an exact string match. Formatting must never leak into the filter value.
   */
  isDate?: boolean;
}

/**
 * Client dimensions first, then product dimensions, matching the table's column order.
 *
 * Labels are read from REPORT_COLUMNS rather than repeated here. Both tables described
 * the same six fields, and they had already drifted: the header said "Client" while the
 * filter above it said "Client number" for the same data. One source now, so a column
 * rename reaches the filter bar too.
 */
const FILTER_DIMENSION_IDS: readonly { id: FilterDimensionId; isDate?: boolean }[] = [
  { id: 'clientType' },
  { id: 'clientNumber' },
  { id: 'exchangeCode' },
  { id: 'productGroupCode' },
  { id: 'symbol' },
  { id: 'expirationDate', isDate: true },
];

export const FILTER_DIMENSIONS: readonly FilterDimensionDef[] = FILTER_DIMENSION_IDS.map(
  ({ id, isDate }) => {
    const column = REPORT_COLUMNS.find((candidate) => candidate.id === id);
    if (!column) {
      throw new Error(`Filter dimension '${id}' has no matching column in REPORT_COLUMNS`);
    }
    return isDate ? { id, label: column.label, isDate } : { id, label: column.label };
  },
);

/** '' means "all" for a dimension. */
export type FilterSelection = Record<FilterDimensionId, string>;

// Frozen so the "callers always spread this, never mutate it" guarantee is
// structural rather than conventional. Every caller does `{ ...NO_SELECTION }`,
// which copies own-enumerable properties and is unaffected by freezing the source.
export const NO_SELECTION: FilterSelection = Object.freeze({
  clientType: '',
  clientNumber: '',
  exchangeCode: '',
  productGroupCode: '',
  symbol: '',
  expirationDate: '',
});
