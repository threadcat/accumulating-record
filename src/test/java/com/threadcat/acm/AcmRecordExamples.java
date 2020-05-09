package com.threadcat.acm;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static com.threadcat.acm.AcmDouble.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AcmRecordExamples {

    @Test
    void exampleAvg() {
        class Columns {
            final int sum;
            final int count;

            Columns(AcmRecord record) {
                sum = record.addColumn(SUM);
                count = record.addColumn(COUNT, sum);
            }
        }
        AcmRecord record = new AcmRecord();
        Columns columns = new Columns(record);

        ByteBuffer buffer = ByteBuffer.allocate(record.size());
        record.setByteBuffer(buffer);

        record.update(columns.sum, 1.5);
        record.commit();
        record.update(columns.sum, 1.6);
        record.commit();

        double expected = (1.5 + 1.6) / 2;
        double actual = record.getDouble(columns.sum) / record.getLong(columns.count);
        assertEquals(expected, actual, 1e-9);
    }

    @Test
    void exampleVWAP() {

        class Columns {
            final int lastPrice;
            final int lastQuantity;
            final int sumQuantity;
            final int sumPriceQuantity;

            Columns(AcmRecord record) {
                lastPrice = record.addColumn(LAST);
                lastQuantity = record.addColumn(LAST);
                sumQuantity = record.addColumn(SUM, lastQuantity);
                sumPriceQuantity = record.addColumn(sumPriceQuantity(lastPrice), lastQuantity);
            }

            AcmDoubleAction sumPriceQuantity(int priceColumn) {
                return (view, storeColumn, referenceColumn, quantity) ->
                        view.getDouble(storeColumn) + quantity * view.getDraftDouble(priceColumn);
            }
        }

        AcmRecord record = new AcmRecord();
        var columns = new Columns(record);
        record.setByteBuffer(ByteBuffer.allocate(record.size()));

        record.update(columns.lastPrice, 1.002);
        record.update(columns.lastQuantity, 100.0);
        record.commit();

        record.update(columns.lastPrice, 1.005);
        record.update(columns.lastQuantity, 200.0);
        record.commit();

        double expected = (1.002 * 100.0 + 1.005 * 200.0) / (100.0 + 200.0);
        double actual = record.getDouble(columns.sumPriceQuantity) / record.getDouble(columns.sumQuantity);
        assertEquals(expected, actual, 1e-9);
    }

    @Test
    void exampleOnCommit() {

        class Columns {
            final int revision;
            final int lastPrice;
            final int lastQuantity;
            final int sumQuantity;
            final int sumPriceQuantity;

            Columns(AcmRecord record) {
                revision = record.addRevisionColumn();
                lastPrice = record.addColumn(LAST);
                lastQuantity = record.addColumn(LAST);
                sumQuantity = record.addColumn(SUM, lastQuantity);
                sumPriceQuantity = record.addColumn(sumPriceQuantity(lastPrice, lastQuantity), revision);
            }

            AcmLongToDoubleAction sumPriceQuantity(int priceColumn, int quantityColumn) {
                return (view, storeColumn, referenceColumn, revision) ->
                        view.getDouble(storeColumn) +
                                view.getDraftDouble(priceColumn) * view.getDraftDouble(quantityColumn);
            }
        }

        AcmRecord record = new AcmRecord();
        var columns = new Columns(record);
        record.setByteBuffer(ByteBuffer.allocate(record.size()));

        record.update(columns.lastPrice, 1.002);
        record.update(columns.lastQuantity, 100.0);
        record.commit();

        record.update(columns.lastPrice, 1.005);
        record.update(columns.lastQuantity, 200.0);
        record.commit();

        double expected = (1.002 * 100.0 + 1.005 * 200.0) / (100.0 + 200.0);
        double actual = record.getDouble(columns.sumPriceQuantity) / record.getDouble(columns.sumQuantity);
        assertEquals(expected, actual, 1e-9);
    }

    @Test
    void exampleTWAP() {

        class Columns {
            final int firstTimestamp;
            final int lastTimestamp;
            final int lastPrice;
            final int sumPriceTime;

            Columns(AcmRecord record) {
                lastTimestamp = record.addColumn(AcmLong.LAST);
                lastPrice = record.addColumn(LAST);
                sumPriceTime = record.addColumn(priceTimeSum(lastPrice), lastTimestamp);
                firstTimestamp = record.addColumn(AcmLong::first, lastTimestamp);
            }

            AcmLongToDoubleAction priceTimeSum(int priceColumn) {
                return (view, storeColumn, referenceColumn, value) -> {
                    long timeDiff = value - view.getLong(referenceColumn);
                    return view.getDouble(storeColumn) + timeDiff * view.getDouble(priceColumn);
                };
            }
        }

        AcmRecord record = new AcmRecord();
        var columns = new Columns(record);
        record.setByteBuffer(ByteBuffer.allocate(record.size()));


        long timestamp = 1588942898841L;
        record.update(columns.lastTimestamp, timestamp);
        record.update(columns.lastPrice, 1.002);
        record.commit();

        timestamp += 10;
        record.update(columns.lastTimestamp, timestamp);
        record.update(columns.lastPrice, 1.005);
        record.commit();

        timestamp += 20;
        record.update(columns.lastTimestamp, timestamp);
        record.update(columns.lastPrice, 10.000);
        record.commit();

        double expected = (10 * 1.002 + 20 * 1.005) / (10 + 20);

        double cumulativeProduct = record.getDouble(columns.sumPriceTime);
        long timeSpan = record.getLong(columns.lastTimestamp) - record.getLong(columns.firstTimestamp);
        double actual = cumulativeProduct / timeSpan;

        assertEquals(expected, actual, 1e-9);
    }
}
