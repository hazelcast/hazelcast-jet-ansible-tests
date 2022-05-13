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

import com.hazelcast.sql.SqlRow;

import java.util.stream.IntStream;

public class AvgTumbleWindowTest extends AbstractTumbleWindowTest {

    private static final String SOURCE = "AVG_TRADES_SOURCE";
    private static final String SINK = "AVG_TRADES_SINK";
    private static final String AGGREGATION_TYPE = "AVG";

    public AvgTumbleWindowTest(String sourceName, String sinkName, String aggregationType) {
        super(sourceName, sinkName, aggregationType);
    }

    public static void main(String[] args) throws Exception {
        new AvgTumbleWindowTest(SOURCE, SINK, AGGREGATION_TYPE).run(args);
    }

    @Override
    protected void assertQuerySuccessful(SqlRow sqlRow, int currentEventStartTime, int currentEventEndTime) {
        int actualAvgValue = sqlRow.getObject(0);
        int expectedValue = (int) (IntStream.range(currentEventStartTime, currentEventEndTime).average().getAsDouble());
        assertEquals("The avg count over aggregate window does not match", expectedValue, actualAvgValue);
    }

}
