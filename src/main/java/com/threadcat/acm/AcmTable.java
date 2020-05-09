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
import java.nio.charset.StandardCharsets;

import static com.threadcat.acm.AcmTool.startsWith;

/**
 * Adapter to {@link AcmHandler} provides storage layout for a set of records.
 * {@inheritDoc}
 *
 * @author threadcat
 */
public class AcmTable extends AcmHandler {
    static final byte[] MARKER = "ACM_TABLE".getBytes(StandardCharsets.US_ASCII);
    static final int VERSION = 12;
    static final int CURSOR = VERSION + 4;
    static final int ROWS = CURSOR + 4;
    static final int DATA = ROWS + 4;

    public AcmTable() {
        super(DATA, DATA + 8, DATA + 16);
    }

    /**
     * Calculates buffer size required for specified table capacity.
     */
    public int size(int rows) {
        return size(rows, size());
    }

    public void setByteBuffer(ByteBuffer buffer) {
        if (size() == 0) {
            throw new AcmException("Columns have to be defined first for buffer size calculation");
        }
        if (startsWith(buffer, MARKER)) {
            int rows = buffer.getInt(ROWS);
            if (size(rows) != buffer.capacity()) {
                int cols = (buffer.capacity() - DATA) / rows;
                throw new AcmException(String.format("Incorrect number of columns, expected %s columns and %s rows", cols, rows));
            }
            super.setByteBuffer(buffer);
        } else {
            if ((buffer.capacity() - DATA) % (16 + size()) != 0) {
                throw new AcmException(String.format("Incorrect byte buffer capacity %s", buffer.capacity()));
            }
            super.setByteBuffer(buffer);
            int rows = (buffer.capacity() - DATA) / (16 + size());
            buffer.put(0, MARKER).putInt(VERSION, 1)
                    .putInt(CURSOR, 0)
                    .putInt(ROWS, rows);
            reset();
        }
    }

    public int getCursor() {
        return buffer.getInt(CURSOR);
    }

    public void setCursor(int row) {
        buffer.putInt(CURSOR, row);
        setOffsets(row);
    }

    /**
     * @return table capacity.
     */
    public int getRows() {
        return rows(buffer);
    }

    /**
     * Reset specified row. Allows to clear data before moving cursor.
     * Data reset and cursor move are two separate atomic operations.
     */
    public void reset(int row) {
        setOffsets(row);
        reset();
        setOffsets(buffer.getInt(CURSOR));
    }

    /**
     * Random data access.
     */
    public long getLong(int row, int col) {
        int dataOffset = dataOffset(row, col);
        return buffer.getLong(dataOffset);
    }

    /**
     * Random data access.
     */
    public double getDouble(int row, int col) {
        int dataOffset = dataOffset(row, col);
        return buffer.getDouble(dataOffset);
    }

    static int size(int rowCount, int rowSize) {
        return DATA + rowCount * (16 + rowSize);
    }

    static int rows(ByteBuffer buffer) {
        return buffer.getInt(ROWS);
    }

    private int dataOffset(int row, int col) {
        int baseOffset = DATA + row * (16 + super.size());
        int stateOffset = baseOffset + 8;
        long state = buffer.getLong(stateOffset);
        return baseOffset + 16 + shift(col, true, state);
    }

    private void setOffsets(int row) {
        int base = DATA + row * (16 + super.size());
        setStatusOffset(base);
        setDraftOffset(base + 8);
        setDataOffset(base + 16);
    }
}
