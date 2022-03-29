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

package com.hazelcast.jet.sql.tests.json;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.jet.tests.common.Util;
import com.hazelcast.jet.tests.common.sql.SqlResultProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public abstract class AbstractJsonMapTest extends AbstractSoakTest {

    protected static final int DEFAULT_QUERY_TIMEOUT_MILLIS = 100;
    private static final int PROGRESS_PRINT_QUERIES_INTERVAL = 500;
    private static final String JSON_DATA_PATH_DEFAULT = "/home/ec2-user/ansible/dataFile.json";

    protected final String mapName;
    protected final String sqlQuery;
    protected final Boolean resultRequiredSort;
    protected final String expectedJsonResultString;
    protected HazelcastInstance client;

    protected int queryTimeout = propertyInt("queryTimeout", DEFAULT_QUERY_TIMEOUT_MILLIS);
    protected String inputJsonFile = property("jsonDataFilePath", JSON_DATA_PATH_DEFAULT);

    protected String jsonInputString;
    private long begin;
    private long currentQueryCount;
    private long lastProgressPrintCount;

    private ExecutorService threadPool;

    public AbstractJsonMapTest(String mapName, String sqlQuery, String expectedJsonPath, Boolean resultRequiredSort)
            throws IOException {
        this.mapName = mapName;
        this.sqlQuery = sqlQuery;
        this.jsonInputString = readJsonFromFile(inputJsonFile);
        this.expectedJsonResultString = retrieveExpectedJsonStructure(jsonInputString, expectedJsonPath);
        this.resultRequiredSort = resultRequiredSort;
    }

    @Override
    protected void init(HazelcastInstance client) {
        this.client = client;
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

    protected abstract String retrieveExpectedJsonStructure(String jsonInputString, String jsonPath);


    protected void runTest() {
        begin = System.currentTimeMillis();

        SqlService sqlService = client.getSql();

        String sqlCreateMappingQueryString = "CREATE OR REPLACE MAPPING " + mapName +
                " TYPE " + IMapSqlConnector.TYPE_NAME + "\n" +
                "OPTIONS (\n" +
                '\'' + OPTION_KEY_FORMAT + "'='bigint'\n" + "," +
                '\'' + OPTION_VALUE_FORMAT + "'='json'\n" +
                ")";

        try (SqlResult result = sqlService.execute(sqlCreateMappingQueryString)) {
            assertEquals(result.updateCount(), 0L);
        }

        SqlResultProcessor sqlResultProcessor = new SqlResultProcessor(sqlQuery, sqlService, threadPool);

        while (System.currentTimeMillis() - begin < durationInMillis) {

            //Execute query
            SqlResult sqlResult = null;
            Future<SqlResult> sqlResultFuture = sqlResultProcessor.runQueryAsync();
            sqlResult = sqlResultProcessor.awaitQueryExecutionWithTimeout(sqlResultFuture, 10);

            //Check that query returned results
            assertQuerySuccessful(sqlResult, expectedJsonResultString);
            currentQueryCount++;

            //Print progress
            printProgress();

            //Timeout between queries to not stress out the cluster
            Util.sleepMillis(queryTimeout);
        }
        logger.info(String.format("Test completed successfully. Executed %d queries in %s.", currentQueryCount,
                getTimeElapsed()));
    }

    private void printProgress() {
        long nextPrintCount = lastProgressPrintCount + PROGRESS_PRINT_QUERIES_INTERVAL;
        boolean toPrint = currentQueryCount >= nextPrintCount;
        if (toPrint) {
            logger.info(String.format("Time elapsed: %s. Executed %d queries", getTimeElapsed(), currentQueryCount));
            lastProgressPrintCount = currentQueryCount;
        }
    }

    private String getTimeElapsed() {
        Duration timeElapsed = Duration.ofMillis(System.currentTimeMillis() - begin);
        long days = timeElapsed.toDays();
        long hours = timeElapsed.minusDays(days).toHours();
        long minutes = timeElapsed.minusDays(days).minusHours(hours).toMinutes();
        long seconds = timeElapsed.minusDays(days).minusHours(hours).minusMinutes(minutes).toMillis() / 1000;
        return String.format("%dd, %dh, %dm, %ds", days, hours, minutes, seconds);
    }

    protected void populateMap() {
        IMap<Long, HazelcastJsonValue> map = client.getMap(mapName);
        map.put(1L, new HazelcastJsonValue(jsonInputString));
        assertThat(map.get(1L), is(notNullValue()));
    }

    protected String readJsonFromFile(String fileName) throws IOException {
        Path inputJsonDataFilePath = Paths.get(fileName);
        assertTrue("Json data input file does not exist", Files.exists(inputJsonDataFilePath));
        try (Stream<String> lines = Files.lines(inputJsonDataFilePath)) {
            jsonInputString = lines.collect(Collectors.joining("\n"));
        }
        assertThat(jsonInputString.length(), is(not(0)));
        return jsonInputString;
    }

    protected void assertQuerySuccessful(SqlResult sqlResult, String expectedJsonQueryResult) {
        assertNotEquals("The SQL results is null: ", sqlResult, null);

        Iterator<SqlRow> sqlRowIterator = sqlResult.iterator();
        assertTrue("The SQL result contains no rows: ", sqlRowIterator.hasNext());

        String jsonQueryResult = sqlRowIterator.next().getObject(0).toString();

        // when comparing node records sort is mandatory as JSONObject keys in records are unsorted
        if (resultRequiredSort) {
            jsonQueryResult = JsonSorter.sortJsonAsCharArray(jsonQueryResult);
            expectedJsonQueryResult = JsonSorter.sortJsonAsCharArray(expectedJsonResultString);
        }
        assertEquals("The following query result is different than expected: ", expectedJsonQueryResult, jsonQueryResult);
    }
}
