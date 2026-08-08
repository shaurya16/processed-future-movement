package com.pfm.common.domain;

import com.pfm.common.fixedwidth.FixedWidthField;

import static com.pfm.common.domain.FieldPositions.*;

/** Purely positional extraction of every field in a System A future-transaction record — no type conversion yet. */
public record RawFutureTransaction(
        @FixedWidthField(start = RECORD_CODE_START, length = RECORD_CODE_LENGTH) String recordCode,
        @FixedWidthField(start = CLIENT_TYPE_START, length = CLIENT_TYPE_LENGTH) String clientType,
        @FixedWidthField(start = CLIENT_NUMBER_START, length = CLIENT_NUMBER_LENGTH) String clientNumber,
        @FixedWidthField(start = ACCOUNT_NUMBER_START, length = ACCOUNT_NUMBER_LENGTH) String accountNumber,
        @FixedWidthField(start = SUBACCOUNT_NUMBER_START, length = SUBACCOUNT_NUMBER_LENGTH) String subaccountNumber,
        @FixedWidthField(start = OPPOSITE_PARTY_CODE_START, length = OPPOSITE_PARTY_CODE_LENGTH) String oppositePartyCode,
        @FixedWidthField(start = PRODUCT_GROUP_CODE_START, length = PRODUCT_GROUP_CODE_LENGTH) String productGroupCode,
        @FixedWidthField(start = EXCHANGE_CODE_START, length = EXCHANGE_CODE_LENGTH) String exchangeCode,
        @FixedWidthField(start = SYMBOL_START, length = SYMBOL_LENGTH) String symbol,
        @FixedWidthField(start = EXPIRATION_DATE_START, length = EXPIRATION_DATE_LENGTH) String expirationDateRaw,
        @FixedWidthField(start = CURRENCY_CODE_START, length = CURRENCY_CODE_LENGTH) String currencyCode,
        @FixedWidthField(start = MOVEMENT_CODE_START, length = MOVEMENT_CODE_LENGTH) String movementCode,
        @FixedWidthField(start = BUY_SELL_CODE_START, length = BUY_SELL_CODE_LENGTH) String buySellCode,
        @FixedWidthField(start = QUANTITY_LONG_SIGN_START, length = QUANTITY_LONG_SIGN_LENGTH) String quantityLongSign,
        @FixedWidthField(start = QUANTITY_LONG_START, length = QUANTITY_LONG_LENGTH) String quantityLongRaw,
        @FixedWidthField(start = QUANTITY_SHORT_SIGN_START, length = QUANTITY_SHORT_SIGN_LENGTH) String quantityShortSign,
        @FixedWidthField(start = QUANTITY_SHORT_START, length = QUANTITY_SHORT_LENGTH) String quantityShortRaw,
        @FixedWidthField(start = EXCH_BROKER_FEE_START, length = EXCH_BROKER_FEE_LENGTH) String exchBrokerFeeRaw,
        @FixedWidthField(start = EXCH_BROKER_FEE_DC_START, length = EXCH_BROKER_FEE_DC_LENGTH) String exchBrokerFeeDC,
        @FixedWidthField(start = EXCH_BROKER_FEE_CURRENCY_START, length = EXCH_BROKER_FEE_CURRENCY_LENGTH) String exchBrokerFeeCurrency,
        @FixedWidthField(start = CLEARING_FEE_START, length = CLEARING_FEE_LENGTH) String clearingFeeRaw,
        @FixedWidthField(start = CLEARING_FEE_DC_START, length = CLEARING_FEE_DC_LENGTH) String clearingFeeDC,
        @FixedWidthField(start = CLEARING_FEE_CURRENCY_START, length = CLEARING_FEE_CURRENCY_LENGTH) String clearingFeeCurrency,
        @FixedWidthField(start = COMMISSION_START, length = COMMISSION_LENGTH) String commissionRaw,
        @FixedWidthField(start = COMMISSION_DC_START, length = COMMISSION_DC_LENGTH) String commissionDC,
        @FixedWidthField(start = COMMISSION_CURRENCY_START, length = COMMISSION_CURRENCY_LENGTH) String commissionCurrency,
        @FixedWidthField(start = TRANSACTION_DATE_START, length = TRANSACTION_DATE_LENGTH) String transactionDateRaw,
        @FixedWidthField(start = FUTURE_REFERENCE_START, length = FUTURE_REFERENCE_LENGTH) String futureReference,
        @FixedWidthField(start = TICKET_NUMBER_START, length = TICKET_NUMBER_LENGTH) String ticketNumber,
        @FixedWidthField(start = EXTERNAL_NUMBER_START, length = EXTERNAL_NUMBER_LENGTH) String externalNumber,
        @FixedWidthField(start = TRANSACTION_PRICE_START, length = TRANSACTION_PRICE_LENGTH) String transactionPriceRaw,
        @FixedWidthField(start = TRADER_INITIALS_START, length = TRADER_INITIALS_LENGTH) String traderInitials,
        @FixedWidthField(start = OPPOSITE_TRADER_ID_START, length = OPPOSITE_TRADER_ID_LENGTH) String oppositeTraderId,
        @FixedWidthField(start = OPEN_CLOSE_CODE_START, length = OPEN_CLOSE_CODE_LENGTH) String openCloseCode
) {
}
