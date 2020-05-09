package com.threadcat.acm;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static com.threadcat.acm.AcmDouble.LAST;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AcmTableExamples {

    @Test
    void exampleRandomAccess() {
        class Columns {
            final int x;
            final int y;
            final int xx;
            final int yy;
            final int xy;

            public Columns(AcmTable table) {
                this.x = table.addColumn(LAST);
                this.y = table.addColumn(LAST);
                this.xx = table.addColumn(square(), x);
                this.yy = table.addColumn(square(), y);
                this.xy = table.addColumn(product(x), y);
            }

            AcmDoubleAction square() {
                return (view, storeColumn, referenceColumn, value) -> view.getDouble(storeColumn) + value * value;
            }

            AcmDoubleAction product(int xColumn) {
                return (view, storeColumn, referenceColumn, y) ->
                        view.getDouble(storeColumn) + view.getDraftDouble(xColumn) * y;
            }
        }

        AcmTable table = new AcmTable();
        Columns columns = new Columns(table);
        int size = table.size(1024);
        table.setByteBuffer(ByteBuffer.allocate(size));

        table.update(columns.x, 2.);
        table.update(columns.y, 3.);
        table.commit();

        int nextRow = table.getCursor() + 1;
        table.reset(nextRow);
        table.setCursor(nextRow);

        table.update(columns.x, 5.);
        table.update(columns.y, 7.);
        table.commit();

        int rowA = table.getCursor() - 1;
        assertEquals(4, table.getDouble(rowA, columns.xx), 1e-9);
        assertEquals(9, table.getDouble(rowA, columns.yy), 1e-9);
        assertEquals(6, table.getDouble(rowA, columns.xy), 1e-9);
        int rowB = table.getCursor();
        assertEquals(25, table.getDouble(rowB, columns.xx), 1e-9);
        assertEquals(49, table.getDouble(rowB, columns.yy), 1e-9);
        assertEquals(35, table.getDouble(rowB, columns.xy), 1e-9);
    }
}
