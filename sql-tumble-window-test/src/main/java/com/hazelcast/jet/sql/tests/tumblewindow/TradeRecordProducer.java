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

package com.hazelcast.jet.sql.tests.tumblewindow;

public class TradeRecordProducer {

    public TradeRecordProducer() {
    }

    /* function returns a list of SQL json-flat records in format
            (<int>, <varchar>, <int> )(,)
            where one column serves as a time source,
            and second is used for agg. operations  i.e MIN,MAX,AVG,SUM
     */

    public static String produceTradeRecords(int startTime, int endTIme, int timeInterval, int lagTime) {
        StringBuilder sb = new StringBuilder();

        // ingest regular monotonic events
        for (int i = startTime; i < endTIme; i = i + timeInterval) {
            createSingleRecord(sb, i).append(",");
        }

        // next append an advanced event
        int j = endTIme + lagTime;
        createSingleRecord(sb, j).append(",");

        // append late event
        int k = j - 2 * lagTime;
        createSingleRecord(sb, k);

        return sb.toString();
    }

    private static StringBuilder createSingleRecord(StringBuilder sb, int a) {
        sb.append("(")
                .append(a + ",")
                .append("'" + "value-" + a + "'" + ",")
                .append(a)
                .append(")");
        return sb;
    }
}
