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

import java.util.ArrayList;
import java.util.List;

/**
 * Column descriptor holds a function to calculate, list of columns to notify after calculation
 * and optional reference to upstream column.
 *
 * @author threadcat
 */
class AcmColumn {
    final List<AcmColumn> linkedColumns = new ArrayList<>();
    final Type type;
    final int storeColumn;
    int referenceColumn = -1;
    AcmDoubleAction doubleAction;
    AcmLongAction longAction;
    AcmDoubleToLongAction doubleToLongAction;
    AcmLongToDoubleAction longToDoubleAction;

    public enum Type {DOUBLE, LONG, DOUBLE_TO_LONG, LONG_TO_DOUBLE}

    public AcmColumn(AcmDoubleAction action, int storeColumn) {
        this.doubleAction = action;
        this.storeColumn = storeColumn;
        type = Type.DOUBLE;
    }

    public AcmColumn(AcmLongAction action, int storeColumn) {
        this.longAction = action;
        this.storeColumn = storeColumn;
        type = Type.LONG;
    }

    public AcmColumn(AcmDoubleToLongAction action, int storeColumn) {
        this.doubleToLongAction = action;
        this.storeColumn = storeColumn;
        type = Type.DOUBLE_TO_LONG;
    }

    public AcmColumn(AcmLongToDoubleAction action, int storeColumn) {
        this.longToDoubleAction = action;
        this.storeColumn = storeColumn;
        type = Type.LONG_TO_DOUBLE;
    }
}
