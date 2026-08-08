# System A File Specification — PROCESSED FUTURE MOVEMENT

Fixed-width record, 303 bytes as specified, though the sample `Input.txt` has trailing
FILLER whitespace stripped so live records are 176 bytes (positions 1-176 below) plus a
trailing CRLF.

All positions are 1-indexed, inclusive, as given in the spec.

| Ref | Field | Length | Position | Notes |
|---|---|---|---|---|
| 1 | RECORD CODE | 3 | 1-3 | constant `"315"` |
| 2 | CLIENT TYPE | 4 | 4-7 | |
| 3 | CLIENT NUMBER | 4 | 8-11 | |
| 4 | ACCOUNT NUMBER | 4 | 12-15 | |
| 5 | SUBACCOUNT NUMBER | 4 | 16-19 | |
| 6 | OPPOSITE PARTY CODE | 6 | 20-25 | |
| 7 | PRODUCT GROUP CODE | 2 | 26-27 | |
| 8 | EXCHANGE CODE | 4 | 28-31 | |
| 9 | SYMBOL | 6 | 32-37 | |
| 11 | EXPIRATION DATE | 8 | 38-45 | `CCYYMMDD` |
| 13 | CURRENCY CODE | 3 | 46-48 | |
| 14 | MOVEMENT CODE | 2 | 49-50 | |
| 15 | BUY SELL CODE | 1 | 51 | `B` / `S` |
| 68 | QUANTITY LONG SIGN | 1 | 52 | |
| 16 | QUANTITY LONG | 10 | 53-62 | 0 decimals |
| 68 | QUANTITY SHORT SIGN | 1 | 63 | |
| 16 | QUANTITY SHORT | 10 | 64-73 | 0 decimals |
| 17 | EXCH/BROKER FEE / DEC | 12 | 74-85 | 2 decimals |
| 18 | EXCH/BROKER FEE D/C | 1 | 86 | |
| 17 | EXCH/BROKER FEE CUR CODE | 3 | 87-89 | |
| 19 | CLEARING FEE / DEC | 12 | 90-101 | 2 decimals |
| 18 | CLEARING FEE D/C | 1 | 102 | |
| 19 | CLEARING FEE CUR CODE | 3 | 103-105 | |
| 86 | COMMISSION | 12 | 106-117 | 2 decimals |
| 18 | COMMISSION D/C | 1 | 118 | |
| 86 | COMMISSION CUR CODE | 3 | 119-121 | |
| 34 | TRANSACTION DATE | 8 | 122-129 | `CCYYMMDD` |
| 36 | FUTURE REFERENCE | 6 | 130-135 | 0 decimals |
| 37 | TICKET NUMBER | 6 | 136-141 | |
| 38 | EXTERNAL NUMBER | 6 | 142-147 | 0 decimals |
| 20 | TRANSACTION PRICE / DEC | 15 | 148-162 | 7 decimals |
| 66 | TRADER INITIALS | 6 | 163-168 | |
| 156 | OPPOSITE TRADER ID | 7 | 169-175 | |
| 65 | OPEN CLOSE CODE | 1 | 176 | |
| — | FILLER | 127 | 177-303 | blank in practice — stripped from the sample file |

## Fields used by the daily summary report

Per the Requirements Specification:

- **Client_Information** = CLIENT TYPE + CLIENT NUMBER + ACCOUNT NUMBER + SUBACCOUNT NUMBER
- **Product_Information** = EXCHANGE CODE + PRODUCT GROUP CODE + SYMBOL + EXPIRATION DATE
- **Total_Transaction_Amount** = `sum(QUANTITY LONG - QUANTITY SHORT)` grouped by
  (Client_Information, Product_Information), where each quantity is signed by its
  adjacent SIGN field (`-` negates, blank/`+` is positive)
