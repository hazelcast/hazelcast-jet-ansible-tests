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

package com.hazelcast.jet.tests.sql.tests;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.jet.tests.common.Util;
import com.hazelcast.jet.tests.sql.pojo.Key;
import com.hazelcast.jet.tests.sql.pojo.Pojo;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;


public abstract class AbstractSqlMapTest extends AbstractSoakTest {

    protected static final int DEFAULT_DATA_SET_SIZE = 65536;
    protected static final int DEFAULT_QUERY_TIMEOUT_MILLIS = 100;
    private static final int PROGRESS_PRINT_QUERIES_INTERVAL = 500;

    protected boolean isIndexed;
    protected HazelcastInstance client;
    protected int dataSetSize = propertyInt("dataSetSize", DEFAULT_DATA_SET_SIZE);
    protected int queryTimeout = propertyInt("queryTimeout", DEFAULT_QUERY_TIMEOUT_MILLIS);
    private String mapName;
    private long begin;
    private long currentQueryCount;
    private long lastQueryCount;
    private long lastProgressPrintCount;

    public AbstractSqlMapTest(String mapName, boolean isIndexed) {
        this.mapName = mapName;
        this.isIndexed = isIndexed;
    }

    protected void runTest() {
        int index = 0;
        begin = System.currentTimeMillis();

        SqlService sql = client.getSql();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            int queryKey = index++;
            //Execute query
            String query = getSqlQuery(queryKey);
            SqlResult sqlResult = sql.execute(query);

            //Check that query returned results
            assertTrue("The following query returned not results: " + query,
                    isQuerySuccessful(sqlResult, queryKey));
            currentQueryCount++;

            //Print progress
            printProgress();

            //Reset index if reached to the end of sql table
            if (index == dataSetSize - 1) {
                index = 0;
            }

            //Timeout between queries to not stress out the cluster
            Util.sleepMillis(queryTimeout);
        }
        logger.info(String.format("Test completed successfully. Executed %d queries in %s.",
                currentQueryCount, getTimeElapsed()));
    }

    private void printProgress() {
        long nextPrintCount = lastProgressPrintCount + PROGRESS_PRINT_QUERIES_INTERVAL;
        boolean toPrint = currentQueryCount >= nextPrintCount;
        if (toPrint) {
            logger.info(String.format("Time elapsed: %s. Executed %d queries", getTimeElapsed(), currentQueryCount));
            lastProgressPrintCount = currentQueryCount;
        }
        assertNotStuck();
        lastQueryCount = currentQueryCount;
    }

    private String getTimeElapsed() {
        Duration timeElapsed = Duration.ofMillis(System.currentTimeMillis() - begin);
        long days = timeElapsed.toDays();
        long hours = timeElapsed.minusDays(days).toHours();
        long minutes = timeElapsed.minusDays(days).minusHours(hours).toMinutes();
        long seconds = timeElapsed.minusDays(days).minusHours(hours).minusMinutes(minutes).toMillis() / 1000;
        return String.format("%dd, %dh, %dm, %ds", days, hours, minutes, seconds);
    }

    private void assertNotStuck() {
        assertNotEquals(
                String.format("No queries executed in %d seconds.", MILLISECONDS.toSeconds(durationInMillis)),
                lastQueryCount, currentQueryCount);
    }

    protected void populateMap() {
        IMap<Key, Pojo> map = client.getMap(mapName);
        for (long i = 0; i < DEFAULT_DATA_SET_SIZE; i++) {
            map.put(new Key(i), new Pojo(i));
        }
        if (isIndexed) {
            addIndexing();
        }
        assertEquals(
                String.format("Failed to populate the map. Map size should be %d but actually is %d.",
                        DEFAULT_DATA_SET_SIZE, map.size()),
                DEFAULT_DATA_SET_SIZE, map.size());
    }

    protected String getSqlQuery(int key) {
        List<String> fields = getFieldNames();
        StringBuilder res = new StringBuilder("SELECT ");

        for (int i = 0; i < fields.size(); i++) {
            if (i != 0) {
                res.append(", ");
            }
            res.append(fields.get(i));
        }
        res.append(" FROM ").append(mapName).append(" WHERE key = ").append(key);

        return res.toString();
    }

    protected static List<String> getFieldNames() {
        return Arrays.asList(
                "key",
                "booleanVal",
                "tinyIntVal",
                "smallIntVal",
                "intVal",
                "bigIntVal",
                "realVal",
                "doubleVal",
                "decimalBigIntegerVal",
                "decimalVal",
                "charVal",
                "varcharVal",
                "dateVal",
                "timeVal",
                "timestampVal",
                "tsTzDateVal",
                "tsTzCalendarVal",
                "tsTzInstantVal",
                "tsTzOffsetDateTimeVal",
                "tsTzZonedDateTimeVal",
                "objectVal"
        );
    }

    protected static List<String> getFieldNamesForIndexing() {
        return Arrays.asList(
                "bigIntVal",
                "doubleVal",
                "decimalBigIntegerVal",
                "charVal",
                "varcharVal",
                "dateVal",
                "timestampVal",
                "tsTzOffsetDateTimeVal"
        );
    }

    private boolean isQuerySuccessful(SqlResult sqlResult, int queryKey) {
        return sqlResult.iterator().next().getObject("intVal").equals(queryKey);
    }

    protected void addIndexing() {
        IndexConfig indexConfig = new IndexConfig().setName("Index_" + UuidUtil.newUnsecureUuidString())
                .setType(IndexType.SORTED);

        for (String fieldName : getFieldNamesForIndexing()) {
            indexConfig.addAttribute(fieldName);
        }

        client.getMap(mapName).addIndex(indexConfig);
    }
}
