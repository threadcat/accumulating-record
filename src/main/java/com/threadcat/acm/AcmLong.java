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
public abstract class AcmLong {
    public static final AcmLongAction SUM = AcmLong::sum;
    public static final AcmLongAction COUNT = AcmLong::count;
    public static final AcmLongAction MIN = AcmLong::min;
    public static final AcmLongAction MAX = AcmLong::max;
    public static final AcmLongAction FIRST = AcmLong::first;
    public static final AcmLongAction LAST = AcmLong::last;

    public static long sum(AcmView view, int storeColumn, int referenceColumn, long value) {
        return view.getLong(storeColumn) + value;
    }

    public static long count(AcmView view, int storeColumn, int referenceColumn, long value) {
        return view.getLong(storeColumn) + 1;
    }

    public static long min(AcmView view, int storeColumn, int referenceColumn, long value) {
        return view.isEmpty(storeColumn) ? value : Math.min(value, view.getLong(storeColumn));
    }

    public static long max(AcmView view, int storeColumn, int referenceColumn, long value) {
        return view.isEmpty(storeColumn) ? value : Math.max(value, view.getLong(storeColumn));
    }

    public static long first(AcmView view, int storeColumn, int referenceColumn, long value) {
        return view.isEmpty(storeColumn) ? value : view.getLong(storeColumn);
    }

    public static long last(AcmView view, int storeColumn, int referenceColumn, long value) {
        return value;
    }
}
