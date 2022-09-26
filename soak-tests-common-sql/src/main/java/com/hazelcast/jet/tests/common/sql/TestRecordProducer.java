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

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;

public class TestRecordProducer {

    public TestRecordProducer() {
    }

    public static String produceTradeRecords(long itemsSubmitted, long itemsCountToSubmit, int timeInterval) {
        StringBuilder sb = new StringBuilder();

        // ingest regular monotonic events
        for (long i = itemsSubmitted; i < itemsSubmitted + itemsCountToSubmit - 1; i = i + timeInterval) {
            createSingleRecord(sb, i).append(",");
        }

        // next append an advanced event
        long j = itemsSubmitted + itemsCountToSubmit - 1;
        createSingleRecord(sb, j);

        return sb.toString();
    }

    private static StringBuilder createSingleRecord(StringBuilder sb, long idx) {
        sb.append(" (TO_TIMESTAMP_TZ(")
                .append(idx + "), ")
                .append(idx)
                .append(")");
        return sb;
    }

    public static OffsetDateTime timestampTz(long epochMillis) {
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault());
    }
}
