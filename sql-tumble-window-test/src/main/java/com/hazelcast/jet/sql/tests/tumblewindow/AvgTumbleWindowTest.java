/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import java.math.BigDecimal;

public class AvgTumbleWindowTest extends AbstractTumbleWindowTest {

    private static final String SOURCE = "AVG_TRADES_SOURCE";
    private static final String AGGREGATION_TYPE = "AVG";

    public AvgTumbleWindowTest(String sourceName, String aggregationType) {
        super(sourceName, aggregationType);
    }

    public static void main(String[] args) throws Exception {
        new AvgTumbleWindowTest(SOURCE + randomName(), AGGREGATION_TYPE).run(args);
    }

    @Override
    protected void assertQuerySuccessful(SqlRow sqlRow, int currentEventStartTime, int currentEventEndTime) {
        BigDecimal actualAvgValue = sqlRow.getObject(2);
        // average of arithmetic series formula: first + (last - first) / 2
        BigDecimal expectedValue =
                BigDecimal.valueOf(currentEventStartTime + (currentEventEndTime - 1 - currentEventStartTime) / 2.0);
        String assertionErr = String.format("The avg over aggregate window does not match.\n " +
                "Expected: %d Actual: %d -- Row: %s", expectedValue.longValue(), actualAvgValue.longValue(), sqlRow);
        assertEquals(assertionErr, expectedValue.intValue(), actualAvgValue.intValue());
    }

}
