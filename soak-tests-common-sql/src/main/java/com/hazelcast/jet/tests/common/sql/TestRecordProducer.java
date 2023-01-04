/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.tests.common.sql;

public final class TestRecordProducer {

    private TestRecordProducer() {
    }

    public static String produceTradeRecords(long startItem, long itemCount, int timeInterval) {
        if (itemCount <= 0 || timeInterval <= 0) {
            throw new IllegalArgumentException("itemCount and timeInterval must be greater than 0!");
        }

        StringBuilder sb = new StringBuilder();

        long nextItem = startItem;
        for (long i = startItem; i < startItem + itemCount - 1; i++) {
            createSingleRecord(sb, nextItem).append(",");
            nextItem += timeInterval;
        }

        createSingleRecord(sb, nextItem);

        return sb.toString();
    }

    private static StringBuilder createSingleRecord(StringBuilder sb, long idx) {
        sb.append(" (TO_TIMESTAMP_TZ(")
                .append(idx + "), ")
                .append(idx)
                .append(")");
        return sb;
    }
}
