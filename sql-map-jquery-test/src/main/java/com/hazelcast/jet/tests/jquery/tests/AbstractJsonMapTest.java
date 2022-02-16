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

package com.hazelcast.jet.tests.jquery.tests;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.jet.tests.common.Util;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public abstract class AbstractJsonMapTest extends AbstractSoakTest {
    protected static final int DEFAULT_QUERY_TIMEOUT_MILLIS = 100;
    private static final int PROGRESS_PRINT_QUERIES_INTERVAL = 500;
    protected final String mapName;
    protected final String sqlQuery;
    protected final Boolean requiredSort;
    protected final String expectedJqueryResultString;
    protected HazelcastInstance client;
    protected int queryTimeout = propertyInt("queryTimeout", DEFAULT_QUERY_TIMEOUT_MILLIS);
    protected String jsonInputString;
    private long begin;
    private long currentQueryCount;
    private long lastQueryCount;
    private long lastProgressPrintCount;

    public AbstractJsonMapTest(String mapName, String inputJsonFile, String sqlQuery, Boolean requiredSort,
                               String expectedJsonPath, Boolean resultIsArray) throws IOException, URISyntaxException {
        this.mapName = mapName;
        this.jsonInputString = readJsonFromFile(inputJsonFile);
        this.sqlQuery = sqlQuery;
        this.requiredSort = requiredSort;
        this.expectedJqueryResultString = retrieveExpectedJsonStructure(jsonInputString, expectedJsonPath,
                resultIsArray);
    }

    protected abstract String retrieveExpectedJsonStructure(String expectedJsonInputString, String expectedJsonPath,
                                                            Boolean resultIsArray);

    protected void runTest() {
        begin = System.currentTimeMillis();

        SqlService sql = client.getSql();

        String sqlString = "CREATE OR REPLACE MAPPING " + mapName +
                " TYPE " + IMapSqlConnector.TYPE_NAME + "\n" +
                "OPTIONS (\n" + '\''
                + OPTION_KEY_FORMAT + "'='" + "bigint" + "'\n" + ", '"
                + OPTION_VALUE_FORMAT + "'='" + "json" + "'\n" + ")";

        try (SqlResult result = client.getSql().execute(sqlString)) {
            assertEquals(result.updateCount(), 0L);
        }

        while (System.currentTimeMillis() - begin < durationInMillis) {

            //Execute query
            SqlResult sqlResult = sql.execute(sqlQuery);

            //Check that query returned results
            assertTrue("The following query was not successful: " + sqlQuery, isQuerySuccessful(sqlResult,
                    expectedJqueryResultString));
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
        IMap<Long, HazelcastJsonValue> map = client.getMap(mapName);
        map.put(1L, new HazelcastJsonValue(jsonInputString));
        assertThat(map.get(1L), is(notNullValue()));
    }

    // this is a nasty workaround for uber jar accessing a packaged resource - check on local needed
    protected String readJsonFromFile(String fileName) throws URISyntaxException, IOException {
        URI uri = Objects.requireNonNull(getClass().getResource("/" + fileName)).toURI();

        final Map<String, String> env = new HashMap<>();
        final String[] array = uri.toString().split("!");
        final FileSystem fs = FileSystems.newFileSystem(URI.create(array[0]), env);
        final Path path = fs.getPath(array[1]);

        Stream<String> lines = Files.lines(path);
        jsonInputString = lines.collect(Collectors.joining("\n"));

        lines.close();
        fs.close();

        assertThat(jsonInputString, is(notNullValue()));

        return jsonInputString;
    }

    protected boolean isQuerySuccessful(SqlResult sqlResult, String expectedJsonQueryResult) {
        String jsonQueryResult = sqlResult.iterator().next().getObject(0).toString();

        // when comparing node records sort is mandatory as JSONObject keys in records are unsorted
        if (requiredSort) {
            jsonQueryResult = JsonSorter.getInstance().sortJsonAsCharArray(jsonQueryResult);
            expectedJsonQueryResult = JsonSorter.getInstance().sortJsonAsCharArray(expectedJqueryResultString);
        }

        return jsonQueryResult.equals(expectedJsonQueryResult);
    }
}
