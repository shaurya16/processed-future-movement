import { ReportEntry } from './report-entry';

export type ColumnGroup = 'Client' | 'Product' | 'Position' | 'Activity' | 'Legacy';

/**
 * How a cell is drawn. `divergingBar` and `expiry` get bespoke templates.
 * `date` is for date-only LocalDate strings (e.g. "2010-09-10"); `dateTime` is
 * for full Instant timestamps (e.g. "2026-08-12T14:31:52Z") — they are NOT
 * interchangeable, since a date-only string parsed as an Instant renders the
 * wrong day in negative-offset timezones.
 */
export type ColumnRender = 'text' | 'date' | 'dateTime' | 'number' | 'divergingBar' | 'expiry';

export interface ColumnDef {
  id: string;
  label: string;
  group: ColumnGroup;
  /** Right-align numbers so digits line up; left-align identifiers. */
  align: 'left' | 'right';
  /** Applies tabular-nums so columns of figures align vertically. */
  numeric: boolean;
  defaultVisible: boolean;
  render: ColumnRender;
  /** Value used for sorting and for the global search. */
  sortValue: (entry: ReportEntry) => string | number;
}

export const REPORT_COLUMNS: readonly ColumnDef[] = [
  // --- Client ---
  // Order matters: this is also the render order (see ColumnPreferences.visibleColumns).
  { id: 'clientType', label: 'Client type', group: 'Client', align: 'left', numeric: false,
    defaultVisible: true, render: 'text', sortValue: (e) => e.clientType },
  { id: 'clientNumber', label: 'Client', group: 'Client', align: 'left', numeric: false,
    defaultVisible: true, render: 'text', sortValue: (e) => e.clientNumber },
  { id: 'accountNumber', label: 'Account', group: 'Client', align: 'left', numeric: false,
    defaultVisible: true, render: 'text', sortValue: (e) => e.accountNumber },
  { id: 'subaccountNumber', label: 'Subaccount', group: 'Client', align: 'left', numeric: false,
    defaultVisible: true, render: 'text', sortValue: (e) => e.subaccountNumber },

  // --- Product ---
  { id: 'symbol', label: 'Symbol', group: 'Product', align: 'left', numeric: false,
    defaultVisible: true, render: 'text', sortValue: (e) => e.symbol },
  { id: 'expirationDate', label: 'Expiry', group: 'Product', align: 'left', numeric: false,
    defaultVisible: false, render: 'expiry', sortValue: (e) => e.expirationDate },
  { id: 'exchangeCode', label: 'Exchange', group: 'Product', align: 'left', numeric: false,
    defaultVisible: false, render: 'text', sortValue: (e) => e.exchangeCode },
  { id: 'productGroupCode', label: 'Group', group: 'Product', align: 'left', numeric: false,
    defaultVisible: false, render: 'text', sortValue: (e) => e.productGroupCode },

  // --- Position ---
  { id: 'netQuantity', label: 'Net', group: 'Position', align: 'right', numeric: true,
    defaultVisible: true, render: 'divergingBar', sortValue: (e) => e.Total_Transaction_Amount },
  { id: 'grossLong', label: 'Gross long', group: 'Position', align: 'right', numeric: true,
    defaultVisible: true, render: 'number', sortValue: (e) => e.grossLong },
  { id: 'grossShort', label: 'Gross short', group: 'Position', align: 'right', numeric: true,
    defaultVisible: true, render: 'number', sortValue: (e) => e.grossShort },
  { id: 'tradeCount', label: 'Trades', group: 'Position', align: 'right', numeric: true,
    defaultVisible: true, render: 'number', sortValue: (e) => e.tradeCount },

  // --- Activity ---
  { id: 'firstTransactionDate', label: 'First trade', group: 'Activity', align: 'left',
    numeric: false, defaultVisible: false, render: 'date',
    sortValue: (e) => e.firstTransactionDate ?? '' },
  { id: 'lastTransactionDate', label: 'Last trade', group: 'Activity', align: 'left',
    numeric: false, defaultVisible: false, render: 'date',
    sortValue: (e) => e.lastTransactionDate ?? '' },
  { id: 'lastUpdatedAt', label: 'Updated', group: 'Activity', align: 'left', numeric: false,
    // A full Instant timestamp, not a LocalDate -- must go through formatDateTime.
    defaultVisible: false, render: 'dateTime', sortValue: (e) => e.lastUpdatedAt ?? '' },

  // --- Legacy: the concatenated strings, available but off by default ---
  { id: 'Client_Information', label: 'Client_Information', group: 'Legacy', align: 'left',
    numeric: false, defaultVisible: false, render: 'text', sortValue: (e) => e.Client_Information },
  { id: 'Product_Information', label: 'Product_Information', group: 'Legacy', align: 'left',
    numeric: false, defaultVisible: false, render: 'text',
    sortValue: (e) => e.Product_Information },
];

export const DEFAULT_VISIBLE_COLUMN_IDS: readonly string[] = REPORT_COLUMNS.filter(
  (column) => column.defaultVisible,
).map((column) => column.id);

export const COLUMN_GROUPS: readonly ColumnGroup[] = [
  'Client',
  'Product',
  'Position',
  'Activity',
  'Legacy',
];
