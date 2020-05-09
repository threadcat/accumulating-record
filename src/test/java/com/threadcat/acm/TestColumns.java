package com.threadcat.acm;

import static com.threadcat.acm.AcmDouble.*;

public class TestColumns {
    public final int revision;
    public final int priceFirst;
    public final int priceLast;
    public final int priceMin;
    public final int priceMax;
    public final int priceSum;
    public final int priceCount;
    public final int priceMaxDiff;
    public final int quantityLast;
    public final int quantityCount;
    public final int product;

    public TestColumns(AcmRecord record) {
        revision = record.addRevisionColumn();
        priceLast = record.addColumn(LAST);
        priceFirst = record.addColumn(FIRST, priceLast);
        priceSum = record.addColumn(SUM, priceLast);
        priceCount = record.addColumn(COUNT, priceSum);
        priceMin = record.addColumn(MIN, priceLast);
        priceMax = record.addColumn(MAX, priceLast);
        priceMaxDiff = record.addColumn(maxDiff, priceLast);
        quantityLast = record.addColumn(LAST);
        quantityCount = record.addColumn(COUNT, quantityLast);
        product = record.addColumn(productAction(priceLast, quantityLast), revision);
    }

    AcmDoubleAction maxDiff = (view, storeCol, referenceCol, value) -> {
        if (view.isEmpty(referenceCol)) {
            return 0.;
        } else {
            double diff = value - view.getDouble(referenceCol);
            double oldMax = view.getDouble(storeCol);
            return Math.abs(diff) > Math.abs(oldMax) ? diff : oldMax;
        }
    };

    static AcmLongToDoubleAction productAction(int priceColumn, int quantityColumn) {
        return (view, storeColumn, referenceColumn, value) -> view.getDouble(storeColumn)
                + view.getDraftDouble(priceColumn) * view.getDraftDouble(quantityColumn);
    }
}
