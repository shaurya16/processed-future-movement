package com.pfm.common.domain;

import com.pfm.common.fixedwidth.FixedWidthParseException;
import com.pfm.common.fixedwidth.FixedWidthRecordParser;
import org.junit.jupiter.api.Test;

import java.lang.reflect.RecordComponent;
import java.math.BigDecimal;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FutureTransactionFactoryTest {

    private static final String LINE_1 =
        "315CL  432100020001SGXDC FUSGX NK    20100910JPY01B 0000000001 0000000000000000000060DUSD000000000030DUSD000000000000DJPY201008200012380     688032000092500000000             O";

    private static final String LINE_13 =
        "315CL  432100030001FCC   FUCME N1    20100910JPY01S 0000000000 0000000003000000000000DUSD000000000015DUSD000000000000DJPY20100819059475      000308000093300000000             O";

    private final FixedWidthRecordParser recordParser = new FixedWidthRecordParser();
    private final FutureTransactionFactory factory = new FutureTransactionFactory();

    @Test
    void convertsRealBuyRecord() {
        RawFutureTransaction raw = recordParser.parse(LINE_1, 1, RawFutureTransaction.class);

        FutureTransaction result = factory.from(raw, 1, LINE_1);

        assertEquals("4321", result.clientNumber());
        assertEquals("0002", result.accountNumber());
        assertEquals("SGX", result.exchangeCode());
        assertEquals("FU", result.productGroupCode());
        assertEquals("NK", result.symbol());
        assertEquals(LocalDate.of(2010, 9, 10), result.expirationDate());
        assertEquals('B', result.buySellCode());
        assertEquals(1L, result.quantityLong());
        assertEquals(0L, result.quantityShort());
        assertEquals(new BigDecimal("-0.60"), result.exchBrokerFee());
        assertEquals('D', result.exchBrokerFeeDC());
        assertEquals(new BigDecimal("-0.30"), result.clearingFee());
        assertEquals('D', result.clearingFeeDC());
        assertEquals(new BigDecimal("0.00"), result.commission());
        assertEquals('D', result.commissionDC());
        assertEquals(LocalDate.of(2010, 8, 20), result.transactionDate());
        assertEquals(new BigDecimal("9250.0000000"), result.transactionPrice());
        assertEquals('O', result.openCloseCode());
    }

    @Test
    void convertsRealSellRecord() {
        RawFutureTransaction raw = recordParser.parse(LINE_13, 13, RawFutureTransaction.class);

        FutureTransaction result = factory.from(raw, 13, LINE_13);

        assertEquals("0003", result.accountNumber());
        assertEquals("CME", result.exchangeCode());
        assertEquals("N1", result.symbol());
        assertEquals('S', result.buySellCode());
        assertEquals(0L, result.quantityLong());
        assertEquals(3L, result.quantityShort());
        assertEquals(new BigDecimal("0.00"), result.exchBrokerFee());
        assertEquals('D', result.exchBrokerFeeDC());
        assertEquals(new BigDecimal("-0.15"), result.clearingFee());
        assertEquals('D', result.clearingFeeDC());
        assertEquals(LocalDate.of(2010, 8, 19), result.transactionDate());
        assertEquals(new BigDecimal("9330.0000000"), result.transactionPrice());
    }

    @Test
    void negativeQuantitySignProducesNegativeValue() {
        RawFutureTransaction base = recordParser.parse(LINE_1, 1, RawFutureTransaction.class);
        RawFutureTransaction withNegativeLong = withField(base, "quantityLongSign", "-");

        FutureTransaction result = factory.from(withNegativeLong, 1, LINE_1);

        assertEquals(-1L, result.quantityLong());
    }

    @Test
    void plusQuantitySignProducesPositiveValue() {
        RawFutureTransaction base = recordParser.parse(LINE_1, 1, RawFutureTransaction.class);
        RawFutureTransaction withPlusLong = withField(base, "quantityLongSign", "+");

        FutureTransaction result = factory.from(withPlusLong, 1, LINE_1);

        assertEquals(1L, result.quantityLong());
    }

    @Test
    void creditIndicatorProducesPositiveAmount() {
        RawFutureTransaction base = recordParser.parse(LINE_1, 1, RawFutureTransaction.class);
        RawFutureTransaction withCredit = withField(base, "exchBrokerFeeDC", "C");

        FutureTransaction result = factory.from(withCredit, 1, LINE_1);

        assertEquals(new BigDecimal("0.60"), result.exchBrokerFee());
        assertEquals('C', result.exchBrokerFeeDC());
    }

    @Test
    void invalidDebitCreditIndicatorThrows() {
        RawFutureTransaction base = recordParser.parse(LINE_1, 1, RawFutureTransaction.class);
        RawFutureTransaction withBadIndicator = withField(base, "exchBrokerFeeDC", "X");

        FixedWidthParseException exception = assertThrows(FixedWidthParseException.class,
            () -> factory.from(withBadIndicator, 1, LINE_1));

        assertEquals(1, exception.lineNumber());
        assertEquals(LINE_1, exception.rawLine());
    }

    @Test
    void nonNumericQuantityThrows() {
        RawFutureTransaction base = recordParser.parse(LINE_1, 1, RawFutureTransaction.class);
        RawFutureTransaction withBadQuantity = withField(base, "quantityLongRaw", "AAAAAAAAAA");

        FixedWidthParseException exception = assertThrows(FixedWidthParseException.class,
            () -> factory.from(withBadQuantity, 1, LINE_1));

        assertEquals(1, exception.lineNumber());
        assertEquals(LINE_1, exception.rawLine());
    }

    @Test
    void invalidDateThrows() {
        RawFutureTransaction base = recordParser.parse(LINE_1, 1, RawFutureTransaction.class);
        RawFutureTransaction withBadDate = withField(base, "expirationDateRaw", "20109999");

        assertThrows(FixedWidthParseException.class, () -> factory.from(withBadDate, 1, LINE_1));
    }

    /**
     * Copies {@code base}, substituting the named record component with {@code newValue}.
     * Generic over any field so individual tests don't each hand-roll a 33-argument
     * reconstruction of RawFutureTransaction.
     */
    private RawFutureTransaction withField(RawFutureTransaction base, String fieldName, String newValue) {
        RecordComponent[] components = RawFutureTransaction.class.getRecordComponents();
        Class<?>[] paramTypes = new Class<?>[components.length];
        Object[] args = new Object[components.length];
        try {
            for (int i = 0; i < components.length; i++) {
                RecordComponent component = components[i];
                paramTypes[i] = component.getType();
                args[i] = component.getName().equals(fieldName)
                        ? newValue
                        : component.getAccessor().invoke(base);
            }
            return RawFutureTransaction.class.getDeclaredConstructor(paramTypes).newInstance(args);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }
}
