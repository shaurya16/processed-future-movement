package com.pfm.common.domain;

import com.pfm.common.fixedwidth.FixedWidthParseException;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/** Converts a purely positional {@link RawFutureTransaction} into a typed {@link FutureTransaction}. */
public class FutureTransactionFactory {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    /** Money fields carry two implied decimal places; the price field carries seven. */
    private static final int MONEY_IMPLIED_DECIMALS = 2;
    private static final int PRICE_IMPLIED_DECIMALS = 7;

    /**
     * @param rawLine the actual 176-character source line this record was parsed from, used
     *                verbatim (not a reconstruction from {@code raw}) in any conversion-failure
     *                {@link FixedWidthParseException} so the error is replayable against the source file.
     */
    public FutureTransaction from(RawFutureTransaction raw, int lineNumber, String rawLine) {
        return new FutureTransaction(
                raw.recordCode(),
                raw.clientType(),
                raw.clientNumber(),
                raw.accountNumber(),
                raw.subaccountNumber(),
                raw.oppositePartyCode(),
                raw.productGroupCode(),
                raw.exchangeCode(),
                raw.symbol(),
                parseDate(raw.expirationDateRaw(), "expirationDate", lineNumber, rawLine),
                raw.currencyCode(),
                raw.movementCode(),
                parseChar(raw.buySellCode(), "buySellCode", lineNumber, rawLine),
                signedLong(raw.quantityLongSign(), raw.quantityLongRaw(), "quantityLong", lineNumber, rawLine),
                signedLong(raw.quantityShortSign(), raw.quantityShortRaw(), "quantityShort", lineNumber, rawLine),
                signedMoney(raw.exchBrokerFeeRaw(), raw.exchBrokerFeeDC(), MONEY_IMPLIED_DECIMALS, "exchBrokerFee", lineNumber, rawLine),
                raw.exchBrokerFeeCurrency(),
                parseDebitCredit(raw.exchBrokerFeeDC(), "exchBrokerFeeDC", lineNumber, rawLine),
                signedMoney(raw.clearingFeeRaw(), raw.clearingFeeDC(), MONEY_IMPLIED_DECIMALS, "clearingFee", lineNumber, rawLine),
                raw.clearingFeeCurrency(),
                parseDebitCredit(raw.clearingFeeDC(), "clearingFeeDC", lineNumber, rawLine),
                signedMoney(raw.commissionRaw(), raw.commissionDC(), MONEY_IMPLIED_DECIMALS, "commission", lineNumber, rawLine),
                raw.commissionCurrency(),
                parseDebitCredit(raw.commissionDC(), "commissionDC", lineNumber, rawLine),
                parseDate(raw.transactionDateRaw(), "transactionDate", lineNumber, rawLine),
                raw.futureReference(),
                raw.ticketNumber(),
                raw.externalNumber(),
                impliedDecimal(raw.transactionPriceRaw(), PRICE_IMPLIED_DECIMALS, "transactionPrice", lineNumber, rawLine),
                raw.traderInitials(),
                raw.oppositeTraderId(),
                parseChar(raw.openCloseCode(), "openCloseCode", lineNumber, rawLine)
        );
    }

    private long signedLong(String sign, String rawValue, String fieldName, int lineNumber, String rawLine) {
        long value = parseLong(rawValue, fieldName, lineNumber, rawLine);
        return "-".equals(sign) ? -value : value;
    }

    /**
     * Applies the D/C debit/credit indicator to a money field's magnitude.
     *
     * <p><b>Assumption flagged for implementation</b> (see
     * {@code docs/superpowers/specs/2026-08-09-common-fixed-width-parser-design.md}): the
     * standard accounting reading ({@code D} = debit = negative, {@code C} = credit = positive)
     * is assumed. Every money field in the 717-line sample data carries a {@code D} indicator;
     * there is no real {@code C} example to confirm the sign convention from data. Worth a
     * second look against the File Specification PDF's field description if this assumption
     * is ever load-bearing.
     */
    private BigDecimal signedMoney(String rawValue, String debitCreditCode, int decimals, String fieldName,
                                    int lineNumber, String rawLine) {
        BigDecimal magnitude = impliedDecimal(rawValue, decimals, fieldName, lineNumber, rawLine);
        char dc = parseDebitCredit(debitCreditCode, fieldName + "DC", lineNumber, rawLine);
        return dc == 'D' ? magnitude.negate() : magnitude;
    }

    /** Applies the format's implied decimal point: the digits are stored unscaled on the wire. */
    private BigDecimal impliedDecimal(String rawValue, int decimals, String fieldName, int lineNumber,
                                       String rawLine) {
        long digits = parseLong(rawValue, fieldName, lineNumber, rawLine);
        return BigDecimal.valueOf(digits, decimals);
    }

    private long parseLong(String rawValue, String fieldName, int lineNumber, String rawLine) {
        try {
            return Long.parseLong(rawValue);
        } catch (NumberFormatException e) {
            throw new FixedWidthParseException(lineNumber, rawLine,
                    "Field '" + fieldName + "' is not numeric: '" + rawValue + "'");
        }
    }

    private char parseChar(String rawValue, String fieldName, int lineNumber, String rawLine) {
        if (rawValue.length() != 1) {
            throw new FixedWidthParseException(lineNumber, rawLine,
                    "Field '" + fieldName + "' must be exactly one character: '" + rawValue + "'");
        }
        return rawValue.charAt(0);
    }

    /** Validates and parses a raw D/C indicator, rejecting anything other than exactly "D" or "C". */
    private char parseDebitCredit(String rawValue, String fieldName, int lineNumber, String rawLine) {
        if (!"D".equals(rawValue) && !"C".equals(rawValue)) {
            throw new FixedWidthParseException(lineNumber, rawLine,
                    "Field '" + fieldName + "' must be 'D' or 'C': '" + rawValue + "'");
        }
        return rawValue.charAt(0);
    }

    private LocalDate parseDate(String rawValue, String fieldName, int lineNumber, String rawLine) {
        try {
            return LocalDate.parse(rawValue, DATE_FORMAT);
        } catch (DateTimeParseException e) {
            throw new FixedWidthParseException(lineNumber, rawLine,
                    "Field '" + fieldName + "' is not a valid CCYYMMDD date: '" + rawValue + "'");
        }
    }
}
