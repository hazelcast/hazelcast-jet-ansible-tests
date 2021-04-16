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

import com.hazelcast.config.*;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.jet.tests.sql.pojo.Key;
import com.hazelcast.jet.tests.sql.pojo.Pojo;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import org.junit.Assert;

import java.util.Arrays;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;


public abstract class AbstractSqlMapTest extends AbstractSoakTest {

    protected static final int DEFAULT_DATA_SET_SIZE = 65536;
    protected static final int DEFAULT_QUERY_TIMEOUT_MILLIS = 100;

    protected int dataSetSize = propertyInt("dataSetSize", DEFAULT_DATA_SET_SIZE);
    protected int queryTimeout = propertyInt("queryTimeout", DEFAULT_QUERY_TIMEOUT_MILLIS);

    protected HazelcastInstance hazelcastInstance;
    private String mapName;
    boolean isIndexed;
    private long begin;
    private long onePercentInMillis;
    private long currentIteration = 0;
    private long currentQueryCount = 0;
    private long lastQueryCount = 0;

    public AbstractSqlMapTest(String mapName, boolean isIndexed) {
        this.mapName = mapName;
        this.isIndexed = isIndexed;
    }

    protected void runTest() {
        int index = 0;
        begin = System.currentTimeMillis();
        onePercentInMillis = durationInMillis / 100;

        SqlService sql = hazelcastInstance.getSql();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            //Execute query
            String query = getSqlQuery(index++);
            SqlResult sqlResult = sql.execute(query);

            //Check that query returned results
            Assert.assertTrue("The following query returned not results: " + query,
                    isQuerySuccessful(sqlResult));
            currentQueryCount++;

            //Print progress
            printProgress();

            //Reset index if reached to the end of sql table
            if(index == dataSetSize -1) {
                index = 0;
            }

            //Timeout between queries to not stress out the cluster
            sleep(queryTimeout);
        }
        logger.info(String.format("Test completed successfully. Executed %d queries in %d minutes",
                currentQueryCount, MILLISECONDS.toMinutes(durationInMillis)));
    }

    private void printProgress() {
        long timer = System.currentTimeMillis() - begin;
        long nextPrintTime = onePercentInMillis * currentIteration;
        boolean toPrint = timer >= nextPrintTime;
        if(toPrint) {
            logger.info(String.format("Progress: %d%%. Executed %d queries", currentIteration, currentQueryCount));
            currentIteration++;
        }
        assertNotStuck();
    }

    private void assertNotStuck() {
        Assert.assertNotEquals(
                String.format("No queries executed in %d seconds.", MILLISECONDS.toSeconds(durationInMillis)),
                lastQueryCount, currentQueryCount);
    }

    protected void populateMap() {
        IMap<Key, Pojo> map = hazelcastInstance.getMap(mapName);
        for (long i = 0; i < DEFAULT_DATA_SET_SIZE; i++) {
            map.put(new Key(i), new Pojo(i));
        }
        if(isIndexed) {
            addIndexing();
        }
        assertEquals("Failed to populate the map", DEFAULT_DATA_SET_SIZE, map.size());
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

    private void sleep(int timeout) {
        try {
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private boolean isQuerySuccessful(SqlResult sqlResult) {
        return sqlResult.iterator().hasNext();
    }

    protected void setInMemoryFormat(JetInstance client, InMemoryFormat inMemoryFormat) {
        client.getConfig().configureHazelcast(config1 ->
                new Config().addMapConfig(new MapConfig(mapName).setInMemoryFormat(inMemoryFormat)));
    }

    protected void addIndexing() {
        IndexConfig indexConfig = new IndexConfig().setName("Index_" + UuidUtil.newUnsecureUuidString())
                .setType(IndexType.SORTED);

        for (String fieldName : getFieldNamesForIndexing()) {
            indexConfig.addAttribute(fieldName);
        }

        hazelcastInstance.getMap(mapName).addIndex(indexConfig);
    }
}
