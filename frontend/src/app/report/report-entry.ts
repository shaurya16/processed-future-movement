/**
 * One report row. The three PascalCase properties are the frozen legacy contract
 * (they drive the CSV); everything else is additive.
 *
 * Dates arrive as ISO strings ("2010-09-10"), not Date objects — Jackson emits
 * ISO because Spring Boot disables WRITE_DATES_AS_TIMESTAMPS by default.
 */
export interface ReportEntry {
  Client_Information: string;
  Product_Information: string;
  Total_Transaction_Amount: number;

  clientType: string;
  clientNumber: string;
  accountNumber: string;
  subaccountNumber: string;
  exchangeCode: string;
  productGroupCode: string;
  symbol: string;
  expirationDate: string;

  grossLong: number;
  grossShort: number;
  tradeCount: number;
  firstTransactionDate: string | null;
  lastTransactionDate: string | null;
  lastUpdatedAt: string | null;
  /** Keyed by currency code. Values are negative for debits — see the plan's constraints. */
  feesByCurrency: Record<string, number>;
}

/** Provenance of the source file. Run fields are null until an ingestion has happened. */
export interface IngestionStatus {
  configuredPath: string;
  fileExists: boolean;
  fileSizeBytes: number | null;
  fileLastModified: string | null;
  lastIngestAt: string | null;
  fingerprint: string | null;
  totalLines: number | null;
  published: number | null;
  skipped: number | null;
  errorCount: number | null;
}
