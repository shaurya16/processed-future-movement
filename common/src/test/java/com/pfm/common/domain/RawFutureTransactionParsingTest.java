package com.pfm.common.domain;

import com.pfm.common.fixedwidth.FixedWidthRecordParser;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RawFutureTransactionParsingTest {

    // Real line 1: client 4321, account 0002, SGX/NK buy, quantityLong=1.
    private static final String LINE_1 =
        "315CL  432100020001SGXDC FUSGX NK    20100910JPY01B 0000000001 0000000000000000000060DUSD000000000030DUSD000000000000DJPY201008200012380     688032000092500000000             O";

    // Real line 13: client 4321, account 0003, CME/N1 sell, quantityShort=3.
    private static final String LINE_13 =
        "315CL  432100030001FCC   FUCME N1    20100910JPY01S 0000000000 0000000003000000000000DUSD000000000015DUSD000000000000DJPY20100819059475      000308000093300000000             O";

    private final FixedWidthRecordParser parser = new FixedWidthRecordParser();

    @Test
    void parsesEveryFieldFromRealBuyRecord() {
        RawFutureTransaction raw = parser.parse(LINE_1, 1, RawFutureTransaction.class);

        assertEquals("315", raw.recordCode());
        assertEquals("CL", raw.clientType());
        assertEquals("4321", raw.clientNumber());
        assertEquals("0002", raw.accountNumber());
        assertEquals("0001", raw.subaccountNumber());
        assertEquals("SGXDC", raw.oppositePartyCode());
        assertEquals("FU", raw.productGroupCode());
        assertEquals("SGX", raw.exchangeCode());
        assertEquals("NK", raw.symbol());
        assertEquals("20100910", raw.expirationDateRaw());
        assertEquals("JPY", raw.currencyCode());
        assertEquals("01", raw.movementCode());
        assertEquals("B", raw.buySellCode());
        assertEquals("", raw.quantityLongSign());
        assertEquals("0000000001", raw.quantityLongRaw());
        assertEquals("", raw.quantityShortSign());
        assertEquals("0000000000", raw.quantityShortRaw());
        assertEquals("000000000060", raw.exchBrokerFeeRaw());
        assertEquals("D", raw.exchBrokerFeeDC());
        assertEquals("USD", raw.exchBrokerFeeCurrency());
        assertEquals("000000000030", raw.clearingFeeRaw());
        assertEquals("D", raw.clearingFeeDC());
        assertEquals("USD", raw.clearingFeeCurrency());
        assertEquals("000000000000", raw.commissionRaw());
        assertEquals("D", raw.commissionDC());
        assertEquals("JPY", raw.commissionCurrency());
        assertEquals("20100820", raw.transactionDateRaw());
        assertEquals("001238", raw.futureReference());
        assertEquals("0", raw.ticketNumber());
        assertEquals("688032", raw.externalNumber());
        assertEquals("000092500000000", raw.transactionPriceRaw());
        assertEquals("", raw.traderInitials());
        assertEquals("", raw.oppositeTraderId());
        assertEquals("O", raw.openCloseCode());
    }

    @Test
    void parsesEveryFieldFromRealSellRecord() {
        RawFutureTransaction raw = parser.parse(LINE_13, 13, RawFutureTransaction.class);

        assertEquals("4321", raw.clientNumber());
        assertEquals("0003", raw.accountNumber());
        assertEquals("FCC", raw.oppositePartyCode());
        assertEquals("CME", raw.exchangeCode());
        assertEquals("N1", raw.symbol());
        assertEquals("S", raw.buySellCode());
        assertEquals("", raw.quantityLongSign());
        assertEquals("0000000000", raw.quantityLongRaw());
        assertEquals("", raw.quantityShortSign());
        assertEquals("0000000003", raw.quantityShortRaw());
        assertEquals("000000000000", raw.exchBrokerFeeRaw());
        assertEquals("D", raw.exchBrokerFeeDC());
        assertEquals("USD", raw.exchBrokerFeeCurrency());
        assertEquals("000000000015", raw.clearingFeeRaw());
        assertEquals("D", raw.clearingFeeDC());
        assertEquals("USD", raw.clearingFeeCurrency());
        assertEquals("20100819", raw.transactionDateRaw());
        assertEquals("059475", raw.futureReference());
        assertEquals("", raw.ticketNumber());
        assertEquals("000308", raw.externalNumber());
        assertEquals("000093300000000", raw.transactionPriceRaw());
        assertEquals("O", raw.openCloseCode());
    }
}
