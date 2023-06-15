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

package com.hazelcast.jet.tests.sql;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.kafka.impl.HazelcastJsonValueDeserializer;
import com.hazelcast.jet.kafka.impl.HazelcastJsonValueSerializer;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.kafka.KafkaSqlConnector;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.jet.tests.common.Util;
import com.hazelcast.jet.tests.common.sql.DataIngestionTask;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.tests.common.Util.randomName;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static com.hazelcast.jet.tests.common.sql.DataIngestionTask.DEFAULT_QUERY_TIMEOUT_MILLIS;

public class SqlStreamToStreamFaultToleranceTest extends AbstractSoakTest {

    private static final String EVENTS_SOURCE_PREFIX = "source_topic_";
    private static final String EVENTS_SINK_PREFIX = "sink_topic_";
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;
    private static final int DEFAULT_PRODUCER_THREAD_COUNT = 2;

    private final int queryTimeout = propertyInt("queryTimeout", Integer.parseInt(DEFAULT_QUERY_TIMEOUT_MILLIS));
    private final String sourceName;
    private final String viewName1 = "view1_" + randomName();
    private final String viewName2 = "view2_" + randomName();
    private final String sinkName;
    private ExecutorService producerExecutorService;
    private int snapshotIntervalMs;
    private Properties kafkaProps;

    public SqlStreamToStreamFaultToleranceTest(String sourceName, String sinkName) {
       this.sourceName = sourceName;
       this.sinkName = sinkName;
    }

    public static void main(String[] args) throws Exception {
        new SqlStreamToStreamFaultToleranceTest(
                EVENTS_SOURCE_PREFIX + randomName(),
                EVENTS_SINK_PREFIX + randomName()
        ).run(args);
    }

    @Override
    protected void init(HazelcastInstance client) {
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);
        producerExecutorService = Executors.newFixedThreadPool(
                propertyInt("producerThreadCount", DEFAULT_PRODUCER_THREAD_COUNT));
        String brokerUri = property("brokerUri", "localhost:9092");

        kafkaProps = new Properties();
        kafkaProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        kafkaProps.setProperty("value.serializer", HazelcastJsonValueSerializer.class.getName());
        kafkaProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        kafkaProps.setProperty("value.deserializer", HazelcastJsonValueDeserializer.class.getName());
        kafkaProps.setProperty("bootstrap.servers", brokerUri);
        kafkaProps.setProperty("auto.offset.reset", "earliest");
    }

    @Override
    protected void test(HazelcastInstance client, String name) throws Throwable {
        String sqlName = dehyphenate(name);
        createKafkaDataConnection(client, sqlName);
        createMappingAndViews(client.getSql(), sqlName);

        DataIngestionTask producerTask = new DataIngestionTask(
                client.getSql(),
                sqlName + "_" + sourceName,
                SqlStreamToStreamFaultToleranceTest::createSingleRecord
        );
        producerTask.produceTradeRecords(0, false);
        Util.sleepMillis(queryTimeout);

        producerExecutorService.execute(producerTask);

        String sinkTopic = sqlName + "_" + sinkName;
        KafkaSinkVerifier verifier = new KafkaSinkVerifier(kafkaProps, sinkTopic);
        verifier.start();

        Util.sleepSeconds(30);

        try {
            createSQLJob(client, sqlName, sinkTopic);
            Util.sleepSeconds(15);
            verifySQLJob(client, sqlName, verifier);
        } finally {
            try (SqlResult dropJobResult = client.getSql().execute("DROP JOB IF EXISTS \"" + sqlName + "\"")) {
                assertEquals(0L, dropJobResult.updateCount());
            }
            producerTask.stopProducingEvents();
            verifier.finish();
        }
    }

    @Override
    protected void teardown(Throwable t) {
        producerExecutorService.shutdown();
        try {
            if (!producerExecutorService.awaitTermination(10, TimeUnit.SECONDS)) {
                producerExecutorService.shutdownNow();
            }
        } catch (InterruptedException ex) {
            producerExecutorService.shutdownNow();
            Thread.currentThread().interrupt();

        }
    }

    @Override
    protected boolean runOnBothClusters() {
        return true;
    }

    private static StringBuilder createSingleRecord(StringBuilder sb, Number idx) {
        sb.append(" (TO_TIMESTAMP_TZ(").append(idx).append("), ")
                .append(idx)
                .append(")");
        return sb;
    }

    private static String propertiesToOptions(Properties properties) {
        StringBuilder sb = new StringBuilder();
        properties.forEach((key, value) -> sb.append("'" + key + "'")
                .append("=")
                .append("'" + value + "'")
                .append(","));
        return sb.substring(0, sb.length() - 1);
    }

    private void createMappingAndViews(SqlService sqlService, String name) {
        String sourceTopicName = name + "_" + sourceName;
        SqlResult sourceMappingCreateResult = sqlService.execute(
                "CREATE MAPPING " + sourceTopicName + " ("
                        + "event_time TIMESTAMP WITH TIME ZONE,"
                        + "event_tick BIGINT )"
                        + " DATA CONNECTION " + name + "_kafka "
                        + "OPTIONS ( "
                        + '\'' + SqlConnector.OPTION_KEY_FORMAT + "'='int'"
                        + ", '" + SqlConnector.OPTION_VALUE_FORMAT + "'='json-flat'"
                        + ")"
        );
        AbstractSoakTest.assertEquals(0L, sourceMappingCreateResult.updateCount());

        String sinkTopicName = name + "_" + sinkName;
        SqlResult sinkMappingCreateResult = sqlService.execute(
                "CREATE MAPPING " + sinkTopicName + " ("
                        + "l_event_time TIMESTAMP WITH TIME ZONE,"
                        + "l_event_tick BIGINT, "
                        + "r_event_time TIMESTAMP WITH TIME ZONE,"
                        + "r_event_tick BIGINT )"
                        + " DATA CONNECTION " + name + "_kafka "
                        + "OPTIONS ( "
                        + '\'' + SqlConnector.OPTION_KEY_FORMAT + "'='int'"
                        + ", '" + SqlConnector.OPTION_VALUE_FORMAT + "'='json-flat'"
                        + ")"
        );
        AbstractSoakTest.assertEquals(0L, sinkMappingCreateResult.updateCount());

        sqlService.execute("CREATE VIEW " + name + "_" + viewName1 + " AS "
                + "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE "
                + sourceTopicName
                + ", DESCRIPTOR(event_time), INTERVAL '1' SECOND))");

        sqlService.execute("CREATE VIEW " + name + "_" + viewName2 + " AS "
                + "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE "
                + sourceTopicName
                + ", DESCRIPTOR(event_time), INTERVAL '1' SECOND))");
    }

    private void verifySQLJob(HazelcastInstance client, String sqlName, KafkaSinkVerifier verifier) {
        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            boolean active = false;
            try (SqlResult showJobsResult = client.getSql().execute("SHOW JOBS")) {
                for (SqlRow row : showJobsResult) {
                    if (sqlName.equals(row.getObject(0))) {
                        active = true;
                        break;
                    }
                }
            }
            if (!active) {
                throw new RuntimeException("Job " + sqlName + " is inactive!");
            }

            verifier.checkStatus();
            sleepMinutes(1);
        }
    }

    private void createSQLJob(HazelcastInstance client, String sqlName, String sinkTopic) {
        try (SqlResult createJobResult = client.getSql().execute(
                "CREATE JOB \"" + sqlName + "\" " +
                        "OPTIONS (" +
                        "  'processingGuarantee' = 'exactlyOnce'," +
                        "  'snapshotIntervalMillis' = '" + snapshotIntervalMs + "'" +
                        ") AS " +
                        "SINK INTO " + sinkTopic +
                        " (__key, l_event_time, l_event_tick, r_event_time, r_event_tick) " +
                        "SELECT v1.event_tick, * FROM " + sqlName + "_" + viewName1 + " v1 " +
                        "JOIN " + sqlName + "_" + viewName2 + " v2 ON v1.event_time = v2.event_time"
        )) {
            assertEquals(0L, createJobResult.updateCount());
        }
    }

    private static String dehyphenate(String identifier) {
        return identifier.replace("-", "_");
    }

    private void createKafkaDataConnection(HazelcastInstance client, String name) {
        logger.info("Creating Data Connection to Kafka with brokerUri: " + kafkaProps.get("bootstrap.servers"));
        SqlResult createDataConnResult = client.getSql().execute(
                "CREATE DATA CONNECTION " + name + "_kafka "
                        + "TYPE " + KafkaSqlConnector.TYPE_NAME
                        + " NOT SHARED "
                        + "OPTIONS ( " + propertiesToOptions(kafkaProps) + ")"
        );
        AbstractSoakTest.assertEquals(0L, createDataConnResult.updateCount());
    }
}
