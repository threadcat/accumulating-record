package com.threadcat.acm;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AcmRecordTest {
    private static final double DELTA = 0.001;
    AcmRecord record;
    TestColumns columns;
    ByteBuffer buffer;

    @BeforeEach
    void setUp() {
        record = new AcmRecord();
        columns = new TestColumns(record);
        buffer = ByteBuffer.allocate(record.size());
        record.setByteBuffer(buffer);
    }

    @Test
    void testSingleUpdate() {
        record.update(columns.priceLast, 1.234);
        record.commit();
        record.update(columns.priceLast, 2.222);
        assertEquals(2L, record.getLong(columns.revision));
        assertEquals(1.234, record.getDouble(columns.priceFirst), DELTA);
        assertEquals(1.234, record.getDouble(columns.priceLast), DELTA);
        assertEquals(1.234, record.getDouble(columns.priceMin), DELTA);
        assertEquals(1.234, record.getDouble(columns.priceMax), DELTA);
        assertEquals(1.234, record.getDouble(columns.priceSum), DELTA);
        assertEquals(1L, record.getLong(columns.priceCount));
        assertEquals(0., record.getDouble(columns.priceMaxDiff), DELTA);
        record.reset();
        assertEquals(3L, record.getLong(columns.revision));
        assertEquals(0., record.getDouble(columns.priceFirst), DELTA);
        assertEquals(0., record.getDouble(columns.priceLast), DELTA);
        assertEquals(0., record.getDouble(columns.priceMin), DELTA);
        assertEquals(0., record.getDouble(columns.priceMax), DELTA);
        assertEquals(0., record.getDouble(columns.priceSum), DELTA);
        assertEquals(0L, record.getLong(columns.priceCount));
        assertEquals(0., record.getDouble(columns.priceMaxDiff), DELTA);
    }

    @Test
    void testOnCommit() {
        record.update(columns.priceLast, 1.02);
        record.update(columns.quantityLast, 10.2);
        record.commit();
        assertEquals(10.404, record.getDouble(columns.product), DELTA);
    }

    @Test
    void testMultiColumnUpdate() {
        BiConsumer<Double, Integer> verifyPrice = (p, c) -> {
            assertEquals(p, record.getDouble(columns.priceLast), DELTA);
            assertEquals((long) c, record.getLong(columns.priceCount));
        };
        BiConsumer<Double, Integer> verifyQuantity = (q, c) -> {
            assertEquals(q, record.getDouble(columns.quantityLast), DELTA);
            assertEquals((long) c, record.getLong(columns.quantityCount));
        };
        // Checking that uncommitted data not available
        record.update(columns.priceLast, 1.234);
        record.update(columns.quantityLast, 1000.25);
        verifyPrice.accept(0.0, 0);
        verifyQuantity.accept(0.0, 0);
        // Checking committed updates
        record.commit();
        verifyPrice.accept(1.234, 1);
        verifyQuantity.accept(1000.25, 1);
        // Updating only one of two fields
        record.update(columns.priceLast, 1.234);
        record.commit();
        verifyPrice.accept(1.234, 2);
        verifyQuantity.accept(1000.25, 1);
    }

    @Test
    void testMultiUpdate() {
        generateMultiUpdate(record, columns);
        verifyMultiUpdate(record, columns);
    }

    @Test
    void testRestore() {
        generateMultiUpdate(record, columns);
        //
        AcmRecord record2 = new AcmRecord();
        TestColumns columns2 = new TestColumns(record2);
        record2.setByteBuffer(buffer);
        //
        verifyMultiUpdate(record2, columns2);
    }

    private static void generateMultiUpdate(AcmRecord record, TestColumns columns) {
        double[] prices = {1.234, 1.235, 1.232, 1.233};
        for (double price : prices) {
            record.update(columns.priceLast, price);
            record.commit();
        }
    }

    private static void verifyMultiUpdate(AcmRecord record, TestColumns columns) {
        assertEquals(5L, record.getLong(columns.revision));
        assertEquals(1.234, record.getDouble(columns.priceFirst), DELTA);
        assertEquals(1.233, record.getDouble(columns.priceLast), DELTA);
        assertEquals(1.232, record.getDouble(columns.priceMin), DELTA);
        assertEquals(1.235, record.getDouble(columns.priceMax), DELTA);
        assertEquals(4.935, record.getDouble(columns.priceSum), DELTA);
        assertEquals(4L, record.getLong(columns.priceCount));
        assertEquals(-0.003, record.getDouble(columns.priceMaxDiff), DELTA);
    }
}