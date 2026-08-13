#!/usr/bin/env python3
"""Generate PFM test fixtures from sample-data/Input.txt.

Every "valid" line is a real sample line with specific fixed-width slices
patched, so the 176-byte layout stays correct by construction instead of being
re-implemented here where it could drift. Offsets mirror
common/src/main/java/com/pfm/common/domain/FieldPositions.java (1-based).

Usage:  python3 scripts/gen-test-data.py
Output: sample-data/generated/*.txt (gitignored)
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "sample-data" / "Input.txt"
OUT_DIR = ROOT / "sample-data" / "generated"

# (1-based start, length), per FieldPositions.java
CLIENT_TYPE = (4, 4)
CLIENT_NUMBER = (8, 4)
PRODUCT_GROUP = (26, 2)
EXCHANGE = (28, 4)
SYMBOL = (32, 6)
EXPIRATION = (38, 8)
QUANTITY_LONG = (53, 10)

# 2 * 3 * 2 * 2 * 3 * 2 = 144 combinations. Over 7000 records that is ~48
# records per combination, so aggregation is visibly doing something, and every
# dropdown has at least two options to choose between.
CLIENT_TYPES = ["CL", "IN"]
CLIENT_NUMBERS = ["1234", "4321", "5678"]
EXCHANGES = ["CME", "SGX"]
GROUPS = ["FU", "OP"]
SYMBOLS = ["NK", "N1", "ES"]
EXPIRIES = ["20100910", "20101210"]

RECORD_COUNT = 7000


def patch(line: str, field: tuple[int, int], value: str) -> str:
    """Code fields are left-justified and space-padded."""
    start, length = field
    assert len(value) <= length, f"{value!r} is longer than field width {length}"
    return line[: start - 1] + value.ljust(length)[:length] + line[start - 1 + length :]


def patch_numeric(line: str, field: tuple[int, int], value: int) -> str:
    """Quantity fields are right-justified and zero-padded."""
    start, length = field
    text = str(value)
    assert len(text) <= length, f"{value} is longer than field width {length}"
    return line[: start - 1] + text.rjust(length, "0")[:length] + line[start - 1 + length :]


def vary(line: str, i: int) -> str:
    """Mixed-radix assignment, so every combination is hit evenly and deterministically."""
    line = patch(line, CLIENT_TYPE, CLIENT_TYPES[i % 2])
    line = patch(line, CLIENT_NUMBER, CLIENT_NUMBERS[(i // 2) % 3])
    line = patch(line, EXCHANGE, EXCHANGES[(i // 6) % 2])
    line = patch(line, PRODUCT_GROUP, GROUPS[(i // 12) % 2])
    line = patch(line, SYMBOL, SYMBOLS[(i // 24) % 3])
    line = patch(line, EXPIRATION, EXPIRIES[(i // 72) % 2])
    # Non-uniform quantities, so rows do not all aggregate to the same net.
    return patch_numeric(line, QUANTITY_LONG, (i % 9) + 1)


def write(name: str, lines: list[str]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / name
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    print(f"{path.relative_to(ROOT)}: {len(lines)} line(s)")


def main() -> None:
    if not SOURCE.exists():
        raise SystemExit(f"{SOURCE} not found")
    templates = SOURCE.read_text(encoding="utf-8").splitlines()
    if not templates:
        raise SystemExit(f"{SOURCE} is empty")

    valid = [vary(templates[i % len(templates)], i) for i in range(RECORD_COUNT)]
    write("large-7000.txt", valid)

    fifty = valid[:50]

    # Too short for the last field -- FixedWidthRecordParser rejects the line.
    truncated = list(fifty)
    truncated[24] = truncated[24][:80]
    write("truncated-line.txt", truncated)

    # Long.parseLong fails on the quantity.
    bad_quantity = list(fifty)
    bad_quantity[10] = patch(bad_quantity[10], QUANTITY_LONG, "ABCDEFGHIJ")
    write("bad-quantity.txt", bad_quantity)

    # Month 13, day 32: rejected by LocalDate.parse.
    bad_date = list(fifty)
    bad_date[10] = patch(bad_date[10], EXPIRATION, "20101332")
    write("bad-date.txt", bad_date)

    blanks = list(fifty)
    blanks.insert(5, "")
    blanks.insert(20, "    ")
    write("blank-lines.txt", blanks)

    # No parseable record at all: published 0, and no KafkaPublishException,
    # because that guard only fires when records exist but none published.
    write("all-invalid.txt", ["this is not a fixed-width record"] * 10)

    write("empty.txt", [])

    # The realistic case: 200 lines, 5 of them corrupted. Three of the five
    # (truncated, blank, garbage) all trip the same too-short-line guard in
    # FixedWidthRecordParser; only the quantity and date faults reach distinct
    # parser paths.
    mixed = valid[:200]
    mixed[30] = mixed[30][:80]
    mixed[60] = patch(mixed[60], QUANTITY_LONG, "ABCDEFGHIJ")
    mixed[90] = patch(mixed[90], EXPIRATION, "20101332")
    mixed[120] = ""
    mixed[150] = "garbage"
    write("mixed-errors.txt", mixed)


if __name__ == "__main__":
    main()
