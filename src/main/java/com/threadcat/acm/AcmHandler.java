/*
 * Copyright 2020 threadcat
 *
 *     https://github.com/threadcat/accumulating-record
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.threadcat.acm;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Transactional store for custom cumulative functions.
 * It triggers execution of related functions and stores calculated results atomically.
 * Functions have access to committed and draft data.
 *
 * @author threadcat
 */
class AcmHandler implements AcmView {
    private int statusOffset;
    private int draftOffset;
    private int dataOffset;
    private final List<AcmColumn> columnList = new ArrayList<>();
    private int revisionColumn = -1;
    private long provisioned;
    protected ByteBuffer buffer;

    /**
     * Constructor defines offsets in ByteBuffer for 'empty' and 'committed' info and column data area.
     *
     * @param statusOffset - columns which were updated since last 'reset'.
     * @param draftOffset  - draft vs committed data offsets.
     * @param dataOffset   - start of column data area.
     */
    public AcmHandler(int statusOffset, int draftOffset, int dataOffset) {
        this.statusOffset = statusOffset;
        this.draftOffset = draftOffset;
        this.dataOffset = dataOffset;
    }

    protected void setStatusOffset(int statusOffset) {
        this.statusOffset = statusOffset;
    }

    protected void setDraftOffset(int draftOffset) {
        this.draftOffset = draftOffset;
    }

    protected void setDataOffset(int dataOffset) {
        this.dataOffset = dataOffset;
    }

    public int addColumn(AcmDoubleAction action) {
        AcmColumn col = new AcmColumn(action, columnList.size());
        columnList.add(col);
        return col.storeColumn;
    }

    public int addColumn(AcmLongAction action) {
        AcmColumn col = new AcmColumn(action, columnList.size());
        columnList.add(col);
        return col.storeColumn;
    }

    public int addColumn(AcmDoubleToLongAction action) {
        AcmColumn col = new AcmColumn(action, columnList.size());
        columnList.add(col);
        return col.storeColumn;
    }

    public int addColumn(AcmLongToDoubleAction action) {
        AcmColumn col = new AcmColumn(action, columnList.size());
        columnList.add(col);
        return col.storeColumn;
    }

    public int addRevisionColumn() {
        if (revisionColumn == -1) {
            AcmLongAction increment = (view, storeColumn, referenceColumn, value) -> value + 1L;
            AcmColumn col = new AcmColumn(increment, columnList.size());
            columnList.add(col);
            revisionColumn = col.storeColumn;
        }
        return revisionColumn;
    }

    public int addColumn(AcmDoubleAction action, int reference) {
        AcmColumn col = new AcmColumn(action, columnList.size());
        return linkColumn(col, reference);
    }

    public int addColumn(AcmLongAction action, int reference) {
        AcmColumn col = new AcmColumn(action, columnList.size());
        return linkColumn(col, reference);
    }

    public int addColumn(AcmDoubleToLongAction action, int reference) {
        AcmColumn col = new AcmColumn(action, columnList.size());
        return linkColumn(col, reference);
    }

    public int addColumn(AcmLongToDoubleAction action, int reference) {
        AcmColumn col = new AcmColumn(action, columnList.size());
        return linkColumn(col, reference);
    }

    protected int size() {
        return columnList.size() * 16;
    }

    public void setByteBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
        // Completing eventual consistency cycle for 'status' (emptiness flags).
        // This is needed if process terminated at the moment between storing 'committed' and 'status'.
        long status = buffer.getLong(statusOffset) | buffer.getLong(draftOffset);
        buffer.putLong(statusOffset, status);
    }

    /**
     * Updates specified column draft value and triggers functions on linked columns.
     */
    public void update(int col, long value) {
        AcmColumn column = columnList.get(col);
        switch (column.type) {
            case LONG:
                long newLong = column.longAction.apply(this, col, column.referenceColumn, value);
                provision(col, newLong);
                processLinks(column.linkedColumns, newLong);
                break;
            case LONG_TO_DOUBLE:
                double newDouble = column.longToDoubleAction.apply(this, col, column.referenceColumn, value);
                provision(col, newDouble);
                processLinks(column.linkedColumns, newDouble);
                break;
            default:
                throw new AcmException("Can not process 'long', column type is " + column.type);
        }
    }

    /**
     * Updates specified column draft value and triggers functions on linked columns.
     */
    public void update(int col, double value) {
        AcmColumn column = columnList.get(col);
        switch (column.type) {
            case DOUBLE:
                double newDouble = column.doubleAction.apply(this, col, column.referenceColumn, value);
                provision(col, newDouble);
                processLinks(column.linkedColumns, newDouble);
                break;
            case DOUBLE_TO_LONG:
                long newLong = column.doubleToLongAction.apply(this, col, column.referenceColumn, value);
                provision(col, newLong);
                processLinks(column.linkedColumns, newLong);
                break;
            default:
                throw new AcmException("Can not process 'double', column type is " + column.type);
        }
    }

    @Override
    public boolean isEmpty(int col) {
        return (buffer.getLong(statusOffset) >> col & 1L) == 0L;
    }

    @Override
    public long getLong(int col) {
        int committedOffset = dataOffset(col, true);
        return buffer.getLong(committedOffset);
    }

    @Override
    public double getDouble(int col) {
        int committedOffset = dataOffset(col, true);
        return buffer.getDouble(committedOffset);
    }

    @Override
    public long getDraftLong(int col) {
        int committedOffset = dataOffset(col, false);
        return buffer.getLong(committedOffset);
    }

    @Override
    public double getDraftDouble(int col) {
        int committedOffset = dataOffset(col, false);
        return buffer.getDouble(committedOffset);
    }

    /**
     * Sets value not triggering any calculations e.g. initial or cascading values.
     * {@link #commit()} still required.
     */
    public void setLong(int col, long value) {
        provision(col, value);
    }

    /**
     * Sets value not triggering any calculations e.g. initial or cascading values.
     * {@link #commit()} still required.
     */
    public void setDouble(int col, double value) {
        provision(col, value);
    }

    /**
     * Increments 'revision' column (if present) and makes all draft values persisted.
     */
    public void commit() {
        if (revisionColumn != -1) {
            long revision = getLong(revisionColumn);
            update(revisionColumn, revision);
        }
        long committed = buffer.getLong(draftOffset) ^ provisioned;
        buffer.putLong(draftOffset, committed);
        long empty = buffer.getLong(statusOffset) | provisioned;
        buffer.putLong(statusOffset, empty);
        provisioned = 0L;
    }

    /**
     * Increments 'revision' column (if present) and sets other columns to zero. {@link #commit()} not needed.
     */
    public void reset() {
        for (int i = 0; i < columnList.size(); i++) {
            AcmColumn column = columnList.get(i);
            if (i == revisionColumn) {
                continue;
            }
            switch (column.type) {
                case DOUBLE, LONG_TO_DOUBLE -> provision(i, 0.);
                case LONG, DOUBLE_TO_LONG -> provision(i, 0L);
            }
        }
        if (revisionColumn != -1) {
            long revision = getLong(revisionColumn);
            update(revisionColumn, revision);
        }
        long committed = buffer.getLong(draftOffset) ^ provisioned;
        buffer.putLong(draftOffset, committed);
        buffer.putLong(statusOffset, 0L);
        provisioned = 0L;
    }

    private int linkColumn(AcmColumn col, int reference) {
        AcmColumn ref = columnList.get(reference);
        validate(col.type, ref.type);
        col.referenceColumn = reference;
        ref.linkedColumns.add(col);
        columnList.add(col);
        return col.storeColumn;
    }

    // Column input type should be the same as reference column output type.
    private void validate(AcmColumn.Type type, AcmColumn.Type refType) {
        boolean inputLong = type == AcmColumn.Type.LONG || type == AcmColumn.Type.LONG_TO_DOUBLE;
        boolean outputDouble = refType == AcmColumn.Type.DOUBLE || refType == AcmColumn.Type.LONG_TO_DOUBLE;
        if (inputLong == outputDouble) {
            throw new AcmException(String.format("Can not link %s column to %s column, input/output type mismatch",
                    type, refType));
        }
    }

    private void processLinks(List<AcmColumn> links, long value) {
        for (AcmColumn link : links) {
            if (link.type == AcmColumn.Type.LONG) {
                long newValue = link.longAction.apply(this, link.storeColumn, link.referenceColumn, value);
                provision(link.storeColumn, newValue);
                processLinks(link.linkedColumns, newValue);
            } else {
                double newValue = link.longToDoubleAction.apply(this, link.storeColumn, link.referenceColumn, value);
                provision(link.storeColumn, newValue);
                processLinks(link.linkedColumns, newValue);
            }
        }
    }

    private void processLinks(List<AcmColumn> linkedColumns, double value) {
        for (AcmColumn link : linkedColumns) {
            if (link.type == AcmColumn.Type.DOUBLE) {
                double newValue = link.doubleAction.apply(this, link.storeColumn, link.referenceColumn, value);
                provision(link.storeColumn, newValue);
                processLinks(link.linkedColumns, newValue);
            } else {
                long newValue = link.doubleToLongAction.apply(this, link.storeColumn, link.referenceColumn, value);
                provision(link.storeColumn, newValue);
                processLinks(link.linkedColumns, newValue);
            }
        }
    }

    private void provision(int col, long value) {
        int provisionalOffset = dataOffset(col, false);
        buffer.putLong(provisionalOffset, value);
        provisioned |= (1L << col);
    }

    private void provision(int col, double value) {
        int provisionalOffset = dataOffset(col, false);
        buffer.putDouble(provisionalOffset, value);
        provisioned |= (1L << col);
    }

    private int dataOffset(int col, boolean committed) {
        long savedState = buffer.getLong(draftOffset);
        return dataOffset + shift(col, committed, savedState);
    }

    // Calculating draft/committed cell place inside 'data' area
    protected int shift(int col, boolean committed, long savedState) {
        int shift = (int) (savedState >> col & 0x1L);
        if (committed) {
            shift ^= 0x1; // inverted
        }
        shift *= columnList.size();
        return 8 * (col + shift);
    }
}
