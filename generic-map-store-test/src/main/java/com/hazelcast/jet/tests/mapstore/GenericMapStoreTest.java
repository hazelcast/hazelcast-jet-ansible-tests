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

package com.hazelcast.jet.tests.mapstore;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.jet.tests.common.Util;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.sql.SqlResult;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import static com.hazelcast.jet.tests.common.Util.getTimeElapsed;

public class GenericMapStoreTest extends AbstractJetSoakTest {

    private static final String DB_AND_USER = "/generic-mapstore-test?user=root&password=Soak-test,1";
    private static final String TABLE_NAME_PREFIX = "PERSON_";
    private static final int PERSON_COUNT = 20_000;
    private static final int DEFAULT_QUERY_TIMEOUT_MILLIS = 100;
    private static final int PROGRESS_PRINT_QUERIES_INTERVAL = 5_000;

    private String connectionUrl;
    private String tableName;
    private IMap<Integer, GenericRecord> map;
    private final int queryTimeout = propertyInt("queryTimeout", DEFAULT_QUERY_TIMEOUT_MILLIS);
    private long lastProgressPrintCount;
    private long currentIterationCount;
    private long startTime;

    public static void main(String[] args) throws Exception {
        new GenericMapStoreTest().run(args);
    }

    @Override
    protected void init(HazelcastInstance client) throws Exception {
        String dbAndUser = System.getProperty("dbAndUser", DB_AND_USER);
        connectionUrl = System.getProperty("connectionUrl", "jdbc:mysql://localhost") + dbAndUser;
        tableName = TABLE_NAME_PREFIX + Util.randomName();

        createAndFillTable();

        SqlResult createDataConnResult = client.getSql().execute("CREATE DATA CONNECTION mysql "
            + "TYPE JDBC "
            + "OPTIONS('jdbcUrl'='" + connectionUrl + "')"
        );
        AbstractJetSoakTest.assertEquals(0L, createDataConnResult.updateCount());

        MapConfig mapConfig = new MapConfig(tableName);
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setClassName("com.hazelcast.mapstore.GenericMapStore")
                .setProperty("data-connection-ref", "mysql");
        mapConfig.setMapStoreConfig(mapStoreConfig);
        client.getConfig().addMapConfig(mapConfig);

        map = client.getMap(tableName);
    }

    @Override
    protected void test(HazelcastInstance client, String name) {
        Random rand = new Random();

        startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < durationInMillis) {
            int key = rand.nextInt(PERSON_COUNT) + 1;
            runAndVerifyMapStoreOperations(key);

            currentIterationCount++;
            printProgress();
            Util.sleepMillis(queryTimeout);
        }

        logger.info(String.format("Test completed successfully. Executed %d GenericMapStore iterations in %s.",
                currentIterationCount, getTimeElapsed(startTime)));
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
        try (Connection connection = DriverManager.getConnection(connectionUrl);
             Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE " + tableName);
        }
    }

    private void runAndVerifyMapStoreOperations(int key) {
        // MapStore.store()
        GenericRecord person = map.get(key);
        AbstractJetSoakTest.assertEquals("name-" + key, person.getString("name"));

        // MapStore.delete()
        map.delete(key);

        // MapStore.load()
        AbstractJetSoakTest.assertNull(map.put(key, person));

        // Make sure data is loaded from MapStore each time
        map.evict(key);
    }

    private void createAndFillTable() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionUrl);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE " + tableName + "(id int primary key, name varchar(255))");
            try (PreparedStatement stmt = connection.prepareStatement(
                    "INSERT INTO " + tableName + "(id, name) VALUES(?, ?)")) {
                for (int i = 1; i <= PERSON_COUNT; i++) {
                    stmt.setInt(1, i);
                    stmt.setString(2, "name-" + i);
                    stmt.executeUpdate();
                }
            }
        }
    }

    private void printProgress() {
        long nextPrintCount = lastProgressPrintCount + PROGRESS_PRINT_QUERIES_INTERVAL;
        boolean toPrint = currentIterationCount >= nextPrintCount;
        if (toPrint) {
            logger.info(
                    String.format(
                            "Time elapsed: %s. Completed %d iterations.",
                            getTimeElapsed(startTime),
                            currentIterationCount
                    ));
            lastProgressPrintCount = currentIterationCount;
        }
    }
}
