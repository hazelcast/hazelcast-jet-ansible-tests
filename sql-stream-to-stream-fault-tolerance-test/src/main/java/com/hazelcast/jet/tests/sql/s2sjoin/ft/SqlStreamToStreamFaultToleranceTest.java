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

package com.hazelcast.jet.tests.sql.s2sjoin.ft;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.kafka.impl.HazelcastJsonValueDeserializer;
import com.hazelcast.jet.kafka.impl.HazelcastJsonValueSerializer;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.kafka.KafkaSqlConnector;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.jet.tests.common.Util;
import com.hazelcast.jet.tests.common.sql.DataIngestionTask;
import com.hazelcast.jet.tests.common.sql.ItemProducer;
import com.hazelcast.shaded.org.json.JSONObject;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.randomName;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static com.hazelcast.jet.tests.common.sql.DataIngestionTask.DEFAULT_QUERY_TIMEOUT_MILLIS;

public class SqlStreamToStreamFaultToleranceTest extends AbstractJetSoakTest {

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
    private String brokerUri;
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
        brokerUri = property("brokerUri", "localhost:9092");
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);
        producerExecutorService = Executors.newFixedThreadPool(
                propertyInt("producerThreadCount", DEFAULT_PRODUCER_THREAD_COUNT));

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

        try (ItemProducer producer = new ItemProducer(brokerUri)) {
            producer.produceItems(sqlName + "_" + sourceName, 0, 10, 1,
                    SqlStreamToStreamFaultToleranceTest::createValue);
        }
        Util.sleepMillis(queryTimeout);

        DataIngestionTask producerTask = new DataIngestionTask(
                brokerUri, sqlName + "_" + sourceName,
                SqlStreamToStreamFaultToleranceTest::createValue);
        producerExecutorService.execute(producerTask);

        String sinkTopic = sqlName + "_" + sinkName;
        KafkaSinkVerifier verifier = new KafkaSinkVerifier(name, kafkaProps, sinkTopic);
        verifier.start();

        Util.sleepSeconds(30);

        try {
            createSQLJob(client, sqlName, sinkTopic);
            Util.sleepSeconds(15);
            verifySQLJob(client, sqlName, verifier);
        } catch (Exception e) {
            logger.severe("Failure in job verify", e);
            throw e;
        } finally {
            Job sqlJob = client.getJet().getJob(sqlName);
            if (sqlJob != null && !sqlJob.getStatus().isTerminal()) {
                sqlJob.cancel();
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

    private static String createValue(Number idx) {
        JSONObject value = new JSONObject();
        LocalDateTime dateTime = LocalDateTime.ofEpochSecond((Long) idx, 0, ZoneOffset.UTC);
        value.put("event_time", OffsetDateTime.of(dateTime, ZoneOffset.UTC));
        value.put("event_tick", idx);
        return value.toString();
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
        AbstractJetSoakTest.assertEquals(0L, sourceMappingCreateResult.updateCount());

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
        AbstractJetSoakTest.assertEquals(0L, sinkMappingCreateResult.updateCount());

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
            Job sqlJob = client.getJet().getJob(sqlName);
            if (sqlJob == null) {
                // It's possible after cluster restart client can be reconnected but job not restarted
                logger.warning("Job " + sqlName + " not found");
            } else if (getJobStatusWithRetry(sqlJob) == FAILED) {
                sqlJob.join();
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
        AbstractJetSoakTest.assertEquals(0L, createDataConnResult.updateCount());
    }
}
