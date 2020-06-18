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

package com.hazelcast.jet.tests.cdc.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.RecordPart;
import com.hazelcast.jet.cdc.mysql.MySqlCdcSources;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.logging.ILogger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static com.hazelcast.jet.tests.common.Util.entry;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static com.hazelcast.jet.tests.common.Util.waitForJobStatus;

public class CdcSourceTest extends AbstractSoakTest {

    public static final String TABLE_NAME = "CdcSourceTest";
    private static final String DATABASE_NAME = "cdc_source_test_db";

    private static final String DEFAULT_DATABASE_URL = "jdbc:mysql://localhost";
    private static final int DEFAULT_SLEEP_MS_BETWEEN_ITEM = 50;
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;
    private static final int ASSERTION_RETRY_COUNT = 60;

    private String connectionUrlWithDb;
    private String connectionUrl;
    private String connectionIp;

    private int sleepMsBetweenItem;
    private long snapshotIntervalMs;

    public static void main(String[] args) throws Exception {
        new CdcSourceTest().run(args);
    }

    @Override
    public void init(JetInstance client) throws Exception {
        String connectionUrlProperty = property("connectionUrl", DEFAULT_DATABASE_URL);
        connectionUrl = connectionUrlProperty + "?useSSL=false";
        connectionIp = connectionUrlProperty.split("//")[1];
        connectionUrlWithDb = connectionUrlProperty + "/" + DATABASE_NAME + "?useSSL=false";


        sleepMsBetweenItem = propertyInt("sleepMsBetweenItem", DEFAULT_SLEEP_MS_BETWEEN_ITEM);
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);

        try (Connection connection = DriverManager.getConnection(connectionUrl, "root", "soak-test")) {
            connection
                    .prepareStatement("CREATE DATABASE " + DATABASE_NAME)
                    .executeUpdate();
        }
    }

    @Override
    protected boolean runOnBothClusters() {
        return true;
    }

    @Override
    public void test(JetInstance client, String name) throws Exception {
        String tableName = TABLE_NAME + name.replace("-", "_");
        try (Connection connection = DriverManager.getConnection(connectionUrlWithDb, "root", "soak-test")) {
            connection.createStatement().execute("DROP TABLE IF EXISTS " + tableName);
            connection.createStatement().execute("CREATE TABLE " + tableName
                    + "(id int PRIMARY KEY AUTO_INCREMENT, value int)");
        }

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(name);
        if (name.startsWith(STABLE_CLUSTER)) {
            jobConfig.addClass(CdcSourceTest.class, VerificationProcessor.class);
        } else {
            jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
            jobConfig.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
        }
        Job job = client.newJob(pipeline(name, tableName), jobConfig);

        waitForJobStatus(job, RUNNING);

        MessageProducer producer = new MessageProducer(connectionUrlWithDb, tableName, sleepMsBetweenItem);
        producer.start();

        int cycles = 0;
        int latestCheckedCount = 0;
        long begin = System.currentTimeMillis();
        int expectedTotalCount = 0;
        try {
            while (System.currentTimeMillis() - begin < durationInMillis) {
                if (getJobStatusWithRetry(job) == FAILED) {
                    job.join();
                }
                cycles++;
                if (cycles % 10 == 0) {
                    latestCheckedCount = checkNewDataProcessed(client, name, latestCheckedCount);
                    log(logger, "Check for insert changes succeeded, latestCheckedCount: " + latestCheckedCount, name);
                }
                sleepMinutes(1);
            }
        } finally {
            expectedTotalCount = producer.stop();
        }
        assertTrue(expectedTotalCount > 0);
        log(logger, "Producer stopped, expectedTotalCount: " + expectedTotalCount, name);
        assertCountEventually(client, expectedTotalCount, logger, name);
        job.cancel();
        log(logger, "Job completed", name);
    }

    private int checkNewDataProcessed(JetInstance client, String clusterName, int latestChecked) {
        Map<String, Integer> latestCounterMap = client.getMap(VerificationProcessor.CONSUMED_MESSAGES_MAP_NAME);
        int latest = 0;
        for (int i = 0; i < ASSERTION_RETRY_COUNT; i++) {
            Integer latestInteger = latestCounterMap.get(clusterName + "_insert");
            if (latestInteger != null) {
                latest = latestInteger;
                break;
            }
            sleepSeconds(1);
        }
        assertTrue("LatestChecked was: " + latestChecked + ", but after 10minutes latest is: " + latest,
                latest > latestChecked);
        return latest;
    }

    private Pipeline pipeline(String clusterName, String tableName) {

        Pipeline pipeline = Pipeline.create();

        StreamSource<ChangeRecord> source = MySqlCdcSources.mysql(tableName)
                .setDatabaseAddress(connectionIp)
                .setDatabasePort(3306)
                .setDatabaseUser("debezium")
                .setDatabasePassword("dbz")
                .setClusterName("dbserver1")
                .setDatabaseWhitelist(DATABASE_NAME)
                .setTableWhitelist(DATABASE_NAME + "." + tableName)
                .build();

        Sink<Integer> insertSink = Sinks.fromProcessor("insertVerificationProcessor",
                VerificationProcessor.supplier(clusterName + "_insert"));
        Sink<Integer> updateSink = Sinks.fromProcessor("updateVerificationProcessor",
                VerificationProcessor.supplier(clusterName + "_update"));
        Sink<Integer> deleteSink = Sinks.fromProcessor("deleteVerificationProcessor",
                VerificationProcessor.supplier(clusterName + "_delete"));

        StreamStage<Map.Entry<Operation, Integer>> afterMappingStage = pipeline.readFrom(source)
                .withNativeTimestamps(0)
                .map(record -> {
                    Operation operation = record.operation();
                    RecordPart value = record.value();
                    TableRow tableRow = value.toObject(TableRow.class);
                    return entry(operation, tableRow.getId());
                });

        afterMappingStage.filter(t -> t.getKey().equals(Operation.INSERT))
                .map(t -> t.getValue())
                .writeTo(insertSink);
        afterMappingStage.filter(t -> t.getKey().equals(Operation.UPDATE))
                .map(t -> t.getValue())
                .writeTo(updateSink);
        afterMappingStage.filter(t -> t.getKey().equals(Operation.DELETE))
                .map(t -> t.getValue())
                .writeTo(deleteSink);

        return pipeline;
    }

    private static void assertCountEventually(JetInstance client, long expectedTotalCount, ILogger logger,
            String clusterName) throws Exception {
        Map<String, Integer> latestCounterMap = client.getMap(VerificationProcessor.CONSUMED_MESSAGES_MAP_NAME);
        for (int i = 0; i < ASSERTION_RETRY_COUNT; i++) {
            int insertActualTotalCount = latestCounterMap.get(clusterName + "_insert");
            int updateActualTotalCount = latestCounterMap.get(clusterName + "_update");
            int deleteActualTotalCount = latestCounterMap.get(clusterName + "_delete");
            log(logger, "expected: " + expectedTotalCount
                    + ", actual insert: " + insertActualTotalCount
                    + ", actual update: " + updateActualTotalCount
                    + ", actual delete: " + deleteActualTotalCount,
                     clusterName);
            if (expectedTotalCount == insertActualTotalCount
                    && expectedTotalCount == updateActualTotalCount
                    && expectedTotalCount == deleteActualTotalCount) {
                break;
            }
            sleepSeconds(1);
        }
        int insertActualTotalCount = latestCounterMap.get(clusterName + "_insert");
        int updateActualTotalCount = latestCounterMap.get(clusterName + "_update");
        int deleteActualTotalCount = latestCounterMap.get(clusterName + "_delete");
        assertEquals(expectedTotalCount, insertActualTotalCount);
        assertEquals(expectedTotalCount, updateActualTotalCount);
        assertEquals(expectedTotalCount, deleteActualTotalCount);
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
        try (Connection connection = DriverManager.getConnection(connectionUrl, "root", "soak-test")) {
            connection
                    .prepareStatement("DROP DATABASE " + DATABASE_NAME)
                    .executeUpdate();
        }
    }

    private static void log(ILogger logger, String message, String clusterName) {
        logger.info("Cluster" + clusterName + "\t\t" + message);
    }

    static class TableRow {

        @JsonProperty("id")
        private int id;

        @JsonProperty("value")
        private int value;

        TableRow() {
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, value);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            TableRow other = (TableRow) obj;
            return id == other.id
                    && Objects.equals(value, other.value);
        }

        @Override
        public String toString() {
            return "TableRow {id=" + id + ", value=" + value + "}";
        }
    }

    private static class MessageProducer {

        private final String connectionUrl;
        private final String tableName;
        private final int sleepMs;
        private final Thread producerThread;
        private volatile boolean running = true;
        private volatile int producedItems;

        MessageProducer(String connectionUrl, String tableName, int sleepMs) {
            this.connectionUrl = connectionUrl;
            this.tableName = tableName;
            this.sleepMs = sleepMs;
            this.producerThread = new Thread(() -> uncheckRun(this::run));
        }

        private void run() throws Exception {
            try (Connection connection = DriverManager.getConnection(connectionUrl, "root", "soak-test")) {
                int id = 1;
                while (running) {
                    executeAndCloseStatement(connection,
                            "INSERT INTO " + tableName + " VALUES (" + id + ", " + id + ")");
                    sleepMillis(sleepMs);
                    executeAndCloseStatement(connection,
                            "UPDATE " + tableName + " SET value=" + (id + 1) + " WHERE id=" + id);
                    sleepMillis(sleepMs);
                    executeAndCloseStatement(connection,
                            "DELETE FROM " + tableName + " WHERE id=" + id);
                    sleepMillis(sleepMs);
                    id++;
                }
                producedItems = id;
            }
        }

        public void start() {
            producerThread.start();
        }

        public int stop() throws InterruptedException {
            running = false;
            producerThread.join();
            return producedItems;
        }

        private void executeAndCloseStatement(Connection connection, String statement) throws SQLException {
            try (PreparedStatement prepareStatement = connection.prepareStatement(statement)) {
                prepareStatement.executeUpdate();
            }
        }
    }
}
