package com.pfm.common.domain;

/** Byte offsets for every field in the System A 176-byte future-transaction record, per docs/file-spec.md. */
public final class FieldPositions {

    public static final int RECORD_CODE_START = 1;
    public static final int RECORD_CODE_LENGTH = 3;

    public static final int CLIENT_TYPE_START = 4;
    public static final int CLIENT_TYPE_LENGTH = 4;

    public static final int CLIENT_NUMBER_START = 8;
    public static final int CLIENT_NUMBER_LENGTH = 4;

    public static final int ACCOUNT_NUMBER_START = 12;
    public static final int ACCOUNT_NUMBER_LENGTH = 4;

    public static final int SUBACCOUNT_NUMBER_START = 16;
    public static final int SUBACCOUNT_NUMBER_LENGTH = 4;

    public static final int OPPOSITE_PARTY_CODE_START = 20;
    public static final int OPPOSITE_PARTY_CODE_LENGTH = 6;

    public static final int PRODUCT_GROUP_CODE_START = 26;
    public static final int PRODUCT_GROUP_CODE_LENGTH = 2;

    public static final int EXCHANGE_CODE_START = 28;
    public static final int EXCHANGE_CODE_LENGTH = 4;

    public static final int SYMBOL_START = 32;
    public static final int SYMBOL_LENGTH = 6;

    public static final int EXPIRATION_DATE_START = 38;
    public static final int EXPIRATION_DATE_LENGTH = 8;

    public static final int CURRENCY_CODE_START = 46;
    public static final int CURRENCY_CODE_LENGTH = 3;

    public static final int MOVEMENT_CODE_START = 49;
    public static final int MOVEMENT_CODE_LENGTH = 2;

    public static final int BUY_SELL_CODE_START = 51;
    public static final int BUY_SELL_CODE_LENGTH = 1;

    public static final int QUANTITY_LONG_SIGN_START = 52;
    public static final int QUANTITY_LONG_SIGN_LENGTH = 1;

    public static final int QUANTITY_LONG_START = 53;
    public static final int QUANTITY_LONG_LENGTH = 10;

    public static final int QUANTITY_SHORT_SIGN_START = 63;
    public static final int QUANTITY_SHORT_SIGN_LENGTH = 1;

    public static final int QUANTITY_SHORT_START = 64;
    public static final int QUANTITY_SHORT_LENGTH = 10;

    public static final int EXCH_BROKER_FEE_START = 74;
    public static final int EXCH_BROKER_FEE_LENGTH = 12;

    public static final int EXCH_BROKER_FEE_DC_START = 86;
    public static final int EXCH_BROKER_FEE_DC_LENGTH = 1;

    public static final int EXCH_BROKER_FEE_CURRENCY_START = 87;
    public static final int EXCH_BROKER_FEE_CURRENCY_LENGTH = 3;

    public static final int CLEARING_FEE_START = 90;
    public static final int CLEARING_FEE_LENGTH = 12;

    public static final int CLEARING_FEE_DC_START = 102;
    public static final int CLEARING_FEE_DC_LENGTH = 1;

    public static final int CLEARING_FEE_CURRENCY_START = 103;
    public static final int CLEARING_FEE_CURRENCY_LENGTH = 3;

    public static final int COMMISSION_START = 106;
    public static final int COMMISSION_LENGTH = 12;

    public static final int COMMISSION_DC_START = 118;
    public static final int COMMISSION_DC_LENGTH = 1;

    public static final int COMMISSION_CURRENCY_START = 119;
    public static final int COMMISSION_CURRENCY_LENGTH = 3;

    public static final int TRANSACTION_DATE_START = 122;
    public static final int TRANSACTION_DATE_LENGTH = 8;

    public static final int FUTURE_REFERENCE_START = 130;
    public static final int FUTURE_REFERENCE_LENGTH = 6;

    public static final int TICKET_NUMBER_START = 136;
    public static final int TICKET_NUMBER_LENGTH = 6;

    public static final int EXTERNAL_NUMBER_START = 142;
    public static final int EXTERNAL_NUMBER_LENGTH = 6;

    public static final int TRANSACTION_PRICE_START = 148;
    public static final int TRANSACTION_PRICE_LENGTH = 15;

    public static final int TRADER_INITIALS_START = 163;
    public static final int TRADER_INITIALS_LENGTH = 6;

    public static final int OPPOSITE_TRADER_ID_START = 169;
    public static final int OPPOSITE_TRADER_ID_LENGTH = 7;

    public static final int OPEN_CLOSE_CODE_START = 176;
    public static final int OPEN_CLOSE_CODE_LENGTH = 1;

    private FieldPositions() {
    }
}
