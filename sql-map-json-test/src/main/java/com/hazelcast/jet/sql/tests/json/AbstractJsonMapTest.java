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

package com.hazelcast.jet.sql.tests.json;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.jet.tests.common.Util;
import com.hazelcast.jet.tests.common.sql.SqlResultProcessor;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.tests.common.Util.getTimeElapsed;

public abstract class AbstractJsonMapTest extends AbstractJetSoakTest {

    protected static final int DEFAULT_QUERY_TIMEOUT_MILLIS = 100;
    private static final int PROGRESS_PRINT_QUERIES_INTERVAL = 500;

    protected final String mapName;
    protected final String sqlQuery;
    protected final Boolean resultRequiredSort;
    protected HazelcastInstance client;

    protected int queryTimeout = propertyInt("queryTimeout", DEFAULT_QUERY_TIMEOUT_MILLIS);

    private long begin;
    private long currentQueryCount;
    private long lastProgressPrintCount;

    private ExecutorService threadPool;

    public AbstractJsonMapTest(String mapName, String sqlQuery, Boolean resultRequiredSort) {
        this.mapName = mapName;
        this.sqlQuery = sqlQuery;
        this.resultRequiredSort = resultRequiredSort;
    }

    @Override
    protected void init(HazelcastInstance client) {
        this.client = client;
        createTable();
        populateMap();
        threadPool = Executors.newSingleThreadExecutor();
    }

    @Override
    protected void test(HazelcastInstance client, String name) {
        runTest();
    }

    @Override
    protected void teardown(Throwable t) {
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(10, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException ex) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }


    protected void runTest() {
        begin = System.currentTimeMillis();

        SqlResultProcessor sqlResultProcessor =
                new SqlResultProcessor(sqlQuery, client.getSql(), threadPool);

        while (System.currentTimeMillis() - begin < durationInMillis) {

            //Execute query
            Future<SqlResult> sqlResultFuture = sqlResultProcessor.runQueryAsync();
            SqlResult sqlResult = sqlResultProcessor.awaitQueryExecutionWithTimeout(sqlResultFuture, 10);

            //Check that query returned results
            assertQuerySuccessful(sqlResult, getExpectedJsonResult());
            currentQueryCount++;

            //Print progress
            printProgress();

            //Timeout between queries to not stress out the cluster
            Util.sleepMillis(queryTimeout);
        }
        logger.info(String.format("Test completed successfully. Executed %d queries in %s.", currentQueryCount,
                getTimeElapsed(begin)));
    }

    private void printProgress() {
        long nextPrintCount = lastProgressPrintCount + PROGRESS_PRINT_QUERIES_INTERVAL;
        boolean toPrint = currentQueryCount >= nextPrintCount;
        if (toPrint) {
            logger.info(String.format("Time elapsed: %s. Executed %d queries", getTimeElapsed(begin), currentQueryCount));
            lastProgressPrintCount = currentQueryCount;
        }
    }

    protected abstract void createTable();
    protected abstract ArrayList<String> getExpectedJsonResult();

    protected abstract void populateMap();

    protected void assertQuerySuccessful(SqlResult sqlResult, ArrayList<String> expectedJsonResults) {
        assertNotEquals("The SQL results is null: ", sqlResult, null);

        Iterator<SqlRow> sqlRowIterator = sqlResult.iterator();
        assertTrue("The SQL result contains no rows: ", sqlRowIterator.hasNext());

        for (String expectedResult : expectedJsonResults) {
            assertTrue("Too few results: ", sqlRowIterator.hasNext());

            String jsonQueryResult = sqlRowIterator.next().getObject(0).toString();
            String failureMessage = "The following query result is different than expected: ";

            // when comparing node records sort is mandatory as JSONObject keys in records are unsorted
            if (resultRequiredSort) {
                assertEqualsJsonSorted(failureMessage, jsonQueryResult, expectedResult);
            } else {
                assertEquals(failureMessage, expectedResult, jsonQueryResult);
            }
        }
    }

    private void assertEqualsJsonSorted(String message, String expected, String actual) {
        assertEquals(
                message,
                JsonSorter.sortJsonAsCharArray(expected),
                JsonSorter.sortJsonAsCharArray(actual)
        );
    }
}
