#### Accumulating Record

Accumulating Record is a transactional store for custom cumulative functions.
It triggers execution of related functions and stores calculated results atomically.

Examples of cumulative and related functions:
calculating sum and keeping track of maximum value, calculating average, TWAP, VWAP.


#### Notes and constraints

* Accumulated data automatically recover if underlying byte buffer backed by memory mapped file.
* Maximum number of columns 64, assuming 64 bit platform.
* Revision column is optional. Can be used to trigger other actions on commit.
* Revision can also be used by concurrent reader before and after fetching record cells.
* Accumulating table is a fixed-size set of accumulating records sharing single byte buffer.

Performance ~3.8 million ops/s for test column set (sum,count,min,max,first,last) on i5-2500K.

#### TBD:
* Data migration tool


#### Concept by examples

Define columns:

```java
    class Columns {
        final int sum;
        final int count;

        Columns(AcmRecord record) {
            sum = record.addColumn(AcmDouble.SUM);
            count = record.addColumn(AcmDouble.COUNT, sum);
        }
    }

    AcmRecord record = new AcmRecord();
    Columns columns = new Columns(record);
```

Allocate storage:

```java
    int size = record.size();
    ByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size);
    record.setByteBuffer(buffer);
```

Accumulate data:

```java
    record.update(columns.sum, 1.5);
    record.commit();
    record.update(columns.sum, 1.6);
    record.commit();

    double average = record.getDouble(columns.sum) / record.getLong(columns.count);
```

Example with custom function:

```java
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
```

Using result after updates:

```java
    record.update(columns.lastPrice, 1.005);
    record.update(columns.lastQuantity, 200.0);
    record.commit();

    double vwap = record.getDouble(columns.sumPriceQuantity) / record.getDouble(columns.sumQuantity);
```

Example 'on commit':

```java
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
```

Calculation result is identical to 'custom function example':

```java
    record.update(columns.lastPrice, 1.005);
    record.update(columns.lastQuantity, 200.0);
    record.commit();

    double vwap = record.getDouble(columns.sumPriceQuantity) / record.getDouble(columns.sumQuantity);
```
