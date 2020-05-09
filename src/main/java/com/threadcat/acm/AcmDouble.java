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

/**
 * Most used cumulative functions: sum, count, min, max, first, last.
 *
 * @author threadcat
 */
public abstract class AcmDouble {
    public static final AcmDoubleAction SUM = AcmDouble::sum;
    public static final AcmDoubleToLongAction COUNT = AcmDouble::count;
    public static final AcmDoubleAction MIN = AcmDouble::min;
    public static final AcmDoubleAction MAX = AcmDouble::max;
    public static final AcmDoubleAction FIRST = AcmDouble::first;
    public static final AcmDoubleAction LAST = AcmDouble::last;

    public static double sum(AcmView view, int storeColumns, int referenceColumn, double value) {
        return view.getDouble(storeColumns) + value;
    }

    public static long count(AcmView view, int storeColumns, int referenceColumn, double value) {
        return view.getLong(storeColumns) + 1;
    }

    public static double min(AcmView view, int storeColumns, int referenceColumn, double value) {
        return view.isEmpty(storeColumns) ? value : Math.min(value, view.getDouble(storeColumns));
    }

    public static double max(AcmView view, int storeColumns, int referenceColumn, double value) {
        return view.isEmpty(storeColumns) ? value : Math.max(value, view.getDouble(storeColumns));
    }

    public static double first(AcmView view, int storeColumns, int referenceColumn, double value) {
        return view.isEmpty(storeColumns) ? value : view.getDouble(storeColumns);
    }

    public static double last(AcmView view, int storeColumns, int referenceColumn, double value) {
        return value;
    }
}
