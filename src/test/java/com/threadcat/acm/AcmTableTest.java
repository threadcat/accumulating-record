package com.threadcat.acm;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AcmTableTest {
    static final double EPSILON = 1e-12;
    long[] samplesL = {3, 5, 7};
    double[] samplesD = {0.1, 0.2, 0.3};
    AcmTable table;
    int colL;
    int colD;

    @BeforeEach
    void onSetup() {
        table = new AcmTable();
        colL = table.addColumn(AcmLong.LAST);
        colD = table.addColumn(AcmDouble.LAST);
        ByteBuffer buffer = ByteBuffer.allocate(table.size(3));
        table.setByteBuffer(buffer);
        for (int i = 0; i < samplesL.length; i++) {
            table.setCursor(i);
            table.update(colL, samplesL[i]);
            table.update(colD, samplesD[i]);
            table.commit();
        }
    }

    @Test
    void testRandomAccess() {
        for (int i = 0; i < samplesL.length; i++) {
            assertEquals(samplesL[i], table.getLong(i, colL));
            assertEquals(samplesD[i], table.getDouble(i, colD));
        }
    }

    @Test
    void testInitialisation() {
        table.setCursor(0);
        int column_0 = 0;
        int column_1 = 1;
        assertEquals(table.getLong(column_0), samplesL[0]);
        assertEquals(table.getDouble(column_1), samplesD[0], EPSILON);
        long column_0_value = 11L;
        double column_1_value = 0.4;
        int row_1 = 1;
        table.reset(1);
        table.setLong(row_1, column_0, column_0_value);
        table.setDouble(row_1, column_1, column_1_value);
        table.setCursor(row_1);
        assertEquals(table.getLong(column_0), column_0_value);
        assertEquals(table.getDouble(column_1), column_1_value, EPSILON);
    }

    @Test
    void testReset() {
        BiConsumer<long[], double[]> verify = (long[] valuesL, double[] valuesD) -> {
            for (int i = 0; i < valuesL.length; i++) {
                assertEquals(valuesL[i], table.getLong(i, colL));
                assertEquals(valuesD[i], table.getDouble(i, colD), EPSILON);
            }
        };

        table.setCursor(0);
        table.reset();
        verify.accept(new long[]{0, 5, 7}, new double[]{0.0, 0.2, 0.3});
        table.update(colL, 3);
        table.update(colD, 0.1);
        table.commit();

        table.setCursor(1);
        table.reset();
        verify.accept(new long[]{3, 0, 7}, new double[]{0.1, 0.0, 0.3});
        table.update(colL, 5);
        table.update(colD, 0.2);
        table.commit();

        table.setCursor(2);
        table.reset();
        verify.accept(new long[]{3, 5, 0}, new double[]{0.1, 0.2, 0.0});
        table.update(colL, 7);
        table.update(colD, 0.3);
        table.commit();

        table.reset(1);
        verify.accept(new long[]{3, 0, 7}, new double[]{0.1, 0.0, 0.3});
    }
}
