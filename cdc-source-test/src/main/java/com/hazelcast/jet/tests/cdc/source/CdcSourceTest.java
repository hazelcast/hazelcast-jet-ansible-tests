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

package com.hazelcast.jet.tests.cdc.source;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.enterprise.jet.cdc.ChangeRecord;
import com.hazelcast.enterprise.jet.cdc.Operation;
import com.hazelcast.enterprise.jet.cdc.RecordPart;
import com.hazelcast.enterprise.jet.cdc.mysql.MySqlCdcSources;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.tests.common.Util.entry;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static com.hazelcast.jet.tests.common.Util.waitForJobStatus;

public class CdcSourceTest extends AbstractJetSoakTest {

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
    public void init(HazelcastInstance client) throws Exception {
        String connectionUrlProperty = property("connectionUrl", DEFAULT_DATABASE_URL);
        connectionUrl = connectionUrlProperty + "?useSSL=false";
        connectionIp = connectionUrlProperty.split("//")[1];
        connectionUrlWithDb = connectionUrlProperty + "/" + DATABASE_NAME + "?useSSL=false";

        sleepMsBetweenItem = propertyInt("sleepMsBetweenItem", DEFAULT_SLEEP_MS_BETWEEN_ITEM);
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);

        createDatabase();
    }

    @Override
    protected boolean runOnBothClusters() {
        return true;
    }

    @Override
    public void test(HazelcastInstance client, String name) throws Exception {
        String tableName = TABLE_NAME + name.replace("-", "_");
        createTable(tableName);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(name);
        if (name.startsWith(STABLE_CLUSTER)) {
            jobConfig.addClass(CdcSourceTest.class, VerificationProcessor.class);
        } else {
            jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
            jobConfig.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
        }
        Job job = client.getJet().newJob(pipeline(name, tableName), jobConfig);

        waitForJobStatus(job, RUNNING);

        MessageProducer producer = new MessageProducer(connectionUrlWithDb, tableName, sleepMsBetweenItem, logger);
        producer.start();

        int cycles = 0;
        int latestCheckedCount = 0;
        long begin = System.currentTimeMillis();
        int expectedTotalCount;
        try {
            while (System.currentTimeMillis() - begin < durationInMillis) {
                if (getJobStatusWithRetry(job) == FAILED) {
                    job.join();
                }
                cycles++;
                if (cycles % 20 == 0) {
                    latestCheckedCount = checkNewDataProcessed(client, name, latestCheckedCount);
                    log("Check for insert changes succeeded, latestCheckedCount: " + latestCheckedCount, name);
                }
                sleepMinutes(1);
            }
        } finally {
            expectedTotalCount = producer.stop();
        }
        assertTrue(expectedTotalCount > 0);
        log("Producer stopped, expectedTotalCount: " + expectedTotalCount, name);
        assertCountEventually(client, name, expectedTotalCount);
        job.cancel();
        log("Job completed", name);
    }

    private int checkNewDataProcessed(HazelcastInstance client, String clusterName, int latestChecked) {
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
        assertTrue("LatestChecked was: " + latestChecked + ", but after 20 minutes latest is: " + latest,
                latest > latestChecked);
        return latest;
    }

    private Pipeline pipeline(String clusterName, String tableName) {

        Pipeline pipeline = Pipeline.create();

        StreamSource<ChangeRecord> source = MySqlCdcSources
                .mysql(tableName)
                .setDatabaseAddress(connectionIp)
                .setDatabasePort(3306)
                .setDatabaseUser("debezium")
                .setDatabasePassword("Dbz,1234")
                .setDatabaseName("cdc_source_test_db")
                .setDatabaseClientId(clusterName.contains(STABLE_CLUSTER) ? 444444 : 555555)
                .setSchemaIncludeList(DATABASE_NAME)
                .setTableIncludeList(DATABASE_NAME + "." + tableName)
                .build();

        Sink<Integer> insertSink = Sinks.fromProcessor("insertVerificationProcessor",
                VerificationProcessor.supplier(clusterName + "_insert"));
        Sink<Integer> updateSink = Sinks.fromProcessor("updateVerificationProcessor",
                VerificationProcessor.supplier(clusterName + "_update"));
        Sink<Integer> deleteSink = Sinks.fromProcessor("deleteVerificationProcessor",
                VerificationProcessor.supplier(clusterName + "_delete"));

        StreamStage<Map.Entry<Operation, Integer>> afterMappingStage
                = pipeline.readFrom(source)
                          .withNativeTimestamps(0)
                          .map(record -> {
                              RecordPart value = record.value();
                              int id = -1;
                              if (value != null && value.toMap().get("id") != null) {
                                   id = (int) value.toMap().get("id");
                              }
                              return entry(record.operation(), id);
                          });

        afterMappingStage.filter(t -> t.getKey().equals(Operation.INSERT))
                         .map(Map.Entry::getValue)
                         .writeTo(insertSink);
        afterMappingStage.filter(t -> t.getKey().equals(Operation.UPDATE))
                         .map(Map.Entry::getValue)
                         .writeTo(updateSink);
        afterMappingStage.filter(t -> t.getKey().equals(Operation.DELETE))
                         .map(Map.Entry::getValue)
                         .writeTo(deleteSink);

        return pipeline;
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
        // remove database only if test finished without failure
        if (t == null) {
            try (
                    Connection connection = DriverManager.getConnection(connectionUrl, "root", "Soak-test,1");
                    Statement statement = connection.createStatement()
            ) {
                statement.executeUpdate("DROP DATABASE " + DATABASE_NAME);
            }
        }
    }

    private void createDatabase() throws SQLException {
        try (
                Connection connection = DriverManager.getConnection(connectionUrl, "root", "Soak-test,1");
                Statement statement = connection.createStatement()
        ) {
            statement.executeUpdate("DROP DATABASE IF EXISTS " + DATABASE_NAME);
            statement.executeUpdate("CREATE DATABASE " + DATABASE_NAME);
        }
    }

    private void createTable(String tableName) throws SQLException {
        try (
                Connection connection = DriverManager.getConnection(connectionUrlWithDb, "root", "Soak-test,1");
                Statement statement = connection.createStatement()
        ) {
            statement.executeUpdate("DROP TABLE IF EXISTS " + tableName);
            statement.executeUpdate("CREATE TABLE " + tableName + "(id int PRIMARY KEY AUTO_INCREMENT, value int)");
        }
    }

    private void assertCountEventually(HazelcastInstance client, String clusterName, long expectedTotalCount) {
        Map<String, Integer> latestCounterMap = client.getMap(VerificationProcessor.CONSUMED_MESSAGES_MAP_NAME);
        for (int i = 0; i < ASSERTION_RETRY_COUNT; i++) {
            int insertActualTotalCount = latestCounterMap.get(clusterName + "_insert");
            int updateActualTotalCount = latestCounterMap.get(clusterName + "_update");
            int deleteActualTotalCount = latestCounterMap.get(clusterName + "_delete");
            log("expected: " + expectedTotalCount
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

    private void log(String message, String clusterName) {
        logger.info("Cluster" + clusterName + "\t\t" + message);
    }

}
