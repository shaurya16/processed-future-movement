package com.pfm.common.domain;

import com.pfm.common.fixedwidth.FixedWidthParseException;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/** Converts a purely positional {@link RawFutureTransaction} into a typed {@link FutureTransaction}. */
public class FutureTransactionFactory {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    public FutureTransaction from(RawFutureTransaction raw, int lineNumber) {
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
                parseDate(raw.expirationDateRaw(), "expirationDate", lineNumber, raw),
                raw.currencyCode(),
                raw.movementCode(),
                parseChar(raw.buySellCode(), "buySellCode", lineNumber, raw),
                signedLong(raw.quantityLongSign(), raw.quantityLongRaw(), "quantityLong", lineNumber, raw),
                signedLong(raw.quantityShortSign(), raw.quantityShortRaw(), "quantityShort", lineNumber, raw),
                scaledDecimal(raw.exchBrokerFeeRaw(), raw.exchBrokerFeeDC(), 2, "exchBrokerFee", lineNumber, raw),
                raw.exchBrokerFeeCurrency(),
                scaledDecimal(raw.clearingFeeRaw(), raw.clearingFeeDC(), 2, "clearingFee", lineNumber, raw),
                raw.clearingFeeCurrency(),
                scaledDecimal(raw.commissionRaw(), raw.commissionDC(), 2, "commission", lineNumber, raw),
                raw.commissionCurrency(),
                parseDate(raw.transactionDateRaw(), "transactionDate", lineNumber, raw),
                raw.futureReference(),
                raw.ticketNumber(),
                raw.externalNumber(),
                unscaledDecimal(raw.transactionPriceRaw(), 7, "transactionPrice", lineNumber, raw),
                raw.traderInitials(),
                raw.oppositeTraderId(),
                parseChar(raw.openCloseCode(), "openCloseCode", lineNumber, raw)
        );
    }

    private long signedLong(String sign, String rawValue, String fieldName, int lineNumber, RawFutureTransaction raw) {
        long value = parseLong(rawValue, fieldName, lineNumber, raw);
        return "-".equals(sign) ? -value : value;
    }

    private BigDecimal scaledDecimal(String rawValue, String debitCreditCode, int decimals, String fieldName,
                                      int lineNumber, RawFutureTransaction raw) {
        BigDecimal magnitude = unscaledDecimal(rawValue, decimals, fieldName, lineNumber, raw);
        return "D".equals(debitCreditCode) ? magnitude.negate() : magnitude;
    }

    private BigDecimal unscaledDecimal(String rawValue, int decimals, String fieldName, int lineNumber,
                                        RawFutureTransaction raw) {
        long digits = parseLong(rawValue, fieldName, lineNumber, raw);
        return BigDecimal.valueOf(digits, decimals);
    }

    private long parseLong(String rawValue, String fieldName, int lineNumber, RawFutureTransaction raw) {
        try {
            return Long.parseLong(rawValue);
        } catch (NumberFormatException e) {
            throw new FixedWidthParseException(lineNumber, raw.toString(),
                    "Field '" + fieldName + "' is not numeric: '" + rawValue + "'");
        }
    }

    private char parseChar(String rawValue, String fieldName, int lineNumber, RawFutureTransaction raw) {
        if (rawValue.length() != 1) {
            throw new FixedWidthParseException(lineNumber, raw.toString(),
                    "Field '" + fieldName + "' must be exactly one character: '" + rawValue + "'");
        }
        return rawValue.charAt(0);
    }

    private LocalDate parseDate(String rawValue, String fieldName, int lineNumber, RawFutureTransaction raw) {
        try {
            return LocalDate.parse(rawValue, DATE_FORMAT);
        } catch (DateTimeParseException e) {
            throw new FixedWidthParseException(lineNumber, raw.toString(),
                    "Field '" + fieldName + "' is not a valid CCYYMMDD date: '" + rawValue + "'");
        }
    }
}
