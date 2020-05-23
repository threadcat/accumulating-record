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

/**
 * Adapter for {@link AcmHandler} to provide storage layout.
 * {@inheritDoc}
 *
 * @author threadcat
 */
public class AcmRecord extends AcmHandler {
    static final byte[] MARKER = "ACM_RECORD".getBytes(StandardCharsets.US_ASCII);
    static final int VERSION = 12;
    static final int EMPTY = VERSION + 4;
    static final int COMMITTED = EMPTY + 8;
    static final int DATA = COMMITTED + 8;

    public AcmRecord() {
        super(EMPTY, COMMITTED, DATA);
    }

    @Override
    public int size() {
        return DATA + super.size();
    }

    @Override
    public void setByteBuffer(ByteBuffer buffer) {
        if (buffer.capacity() != this.size()) {
            throw new AcmException("Incorrect byte buffer capacity " + buffer);
        }
        super.setByteBuffer(buffer);
        if (!AcmTool.startsWith(buffer, MARKER)) {
            buffer.put(0, MARKER);
            reset();
        }
    }
}
