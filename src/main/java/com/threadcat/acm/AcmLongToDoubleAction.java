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
 * Function type to be invoked on {@link AcmColumn} change.
 *
 * @author threadcat
 * @see AcmLongAction
 * @see AcmDoubleAction
 * @see AcmDoubleToLongAction
 */
public interface AcmLongToDoubleAction {
    double apply(AcmView view, int storeColumn, int referenceColumn, long value);
}
