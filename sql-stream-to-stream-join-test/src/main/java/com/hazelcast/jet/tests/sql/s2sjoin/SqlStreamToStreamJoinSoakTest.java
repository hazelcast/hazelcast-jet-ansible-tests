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

package com.hazelcast.jet.tests.sql.s2sjoin;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.connector.kafka.KafkaSqlConnector;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.jet.tests.common.Util;
import com.hazelcast.jet.tests.common.sql.DataIngestionTask;
import com.hazelcast.jet.tests.common.sql.ItemProducer;
import com.hazelcast.shaded.org.json.JSONObject;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.jet.tests.common.Util.getTimeElapsed;
import static com.hazelcast.jet.tests.common.Util.randomName;

public class SqlStreamToStreamJoinSoakTest extends AbstractJetSoakTest {
    private static final String EVENTS_SOURCE_PREFIX = "events_topic_";

    private static final int PROGRESS_PRINT_QUERIES_INTERVAL = 20_000;
    private static final int EVENTS_START_TIME = 0;
    private static final int EVENTS_COUNT_PER_BATCH = 100;
    private static final int EVENT_TIME_INTERVAL = 1;
    private static final int STREAM_RESULTS_TIMEOUT_SECONDS = 600;
    private static final int PRODUCER_RETRY_DELAY_SECONDS = 10;

    private long startTime;
    private long currentQueryCount;
    private long lastProgressPrintCount;
    private SqlService sqlService;
    private ExecutorService ingestionExecutorService;
    private ExecutorService streamingResultsExecutor;
    private int streamingResultsTimeout;
    private String brokerUri;

    private final String sourceName;
    private final String viewName1 = "view1_" + randomName();
    private final String viewName2 = "view2_" + randomName();

    public SqlStreamToStreamJoinSoakTest(String sourceName) {
        this.sourceName = sourceName;
    }

    @Override
    protected void init(HazelcastInstance client) {
        sqlService = client.getSql();
        ingestionExecutorService = Executors.newSingleThreadExecutor();
        streamingResultsExecutor = Executors.newSingleThreadExecutor();
        streamingResultsTimeout = propertyInt("streamingResultsTimeout", STREAM_RESULTS_TIMEOUT_SECONDS);
        startTime = System.currentTimeMillis();
        brokerUri = property("brokerUri", "localhost:9092");

        logger.info("Creating mapping to Kafka with brokerUri: " + brokerUri);
        SqlResult sourceMappingCreateResult = sqlService.execute("CREATE MAPPING " + sourceName + " ("
                + "event_time TIMESTAMP WITH TIME ZONE,"
                + "event_tick BIGINT )"
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + SqlConnector.OPTION_KEY_FORMAT + "'='int'"
                + ", '" + SqlConnector.OPTION_VALUE_FORMAT + "'='json-flat'"
                + ", 'bootstrap.servers'='" + brokerUri + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );
        AbstractJetSoakTest.assertEquals(0L, sourceMappingCreateResult.updateCount());

        try (ItemProducer producer = new ItemProducer(brokerUri)) {
            producer.produceItems(
                sourceName,
                EVENTS_START_TIME,
                EVENTS_COUNT_PER_BATCH,
                EVENT_TIME_INTERVAL,
                SqlStreamToStreamJoinSoakTest::createValue
            );
        }

        sqlService.execute("CREATE VIEW " + viewName1 + " AS "
                + "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE "
                + sourceName
                + ", DESCRIPTOR(event_time), INTERVAL '1' SECOND))");

        sqlService.execute("CREATE VIEW " + viewName2 + " AS "
                + "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE "
                + sourceName
                + ", DESCRIPTOR(event_time), INTERVAL '1' SECOND))");
    }

    @Override
    protected final void test(HazelcastInstance client, String name) throws ExecutionException, InterruptedException {

        DataIngestionTask producerTask = new DataIngestionTask(
                brokerUri, sourceName, EVENTS_START_TIME + EVENTS_COUNT_PER_BATCH,
                EVENTS_COUNT_PER_BATCH, EVENT_TIME_INTERVAL, PRODUCER_RETRY_DELAY_SECONDS,
                SqlStreamToStreamJoinSoakTest::createValue);
        ingestionExecutorService.execute(producerTask);

        Util.sleepMillis(30_000L);

        try (SqlResult sqlResult = sqlService.execute("SELECT * FROM " + viewName1 + " v1 "
                + "JOIN " + viewName2 + " v2 ON v1.event_time=v2.event_time")) {

            Iterator<SqlRow> iterator = sqlResult.iterator();
            while (System.currentTimeMillis() - startTime < durationInMillis) {

                final Future<SqlRow> sqlRowFuture = streamingResultsExecutor.submit(iterator::next);
                try {
                    SqlRow sqlRow = sqlRowFuture.get(streamingResultsTimeout, TimeUnit.SECONDS);
                    // l_event       r_event
                    // time(0) | 0 | time(0) | 0  <-- joint row

                    // checking only event_tick to simplify our destiny.
                    Long leftRowIndex = sqlRow.getObject(1);
                    Long rightRowIndex = sqlRow.getObject(3);
                    AbstractJetSoakTest.assertEquals(leftRowIndex, rightRowIndex);
                } catch (TimeoutException e) {
                    sqlRowFuture.cancel(true);
                    throw new RuntimeException("Timed out after " + streamingResultsTimeout
                            + " secs waiting for new records", e);
                }

                currentQueryCount++;
                printProgress();
            }
        }

        producerTask.stopProducingEvents();
    }

    @Override
    protected void teardown(Throwable t) {
        ingestionExecutorService.shutdown();
        streamingResultsExecutor.shutdown();
        try {
            if (!ingestionExecutorService.awaitTermination(10, TimeUnit.SECONDS)) {
                ingestionExecutorService.shutdownNow();
            }

            if (!streamingResultsExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                streamingResultsExecutor.shutdownNow();
            }
        } catch (InterruptedException ex) {
            ingestionExecutorService.shutdownNow();
            streamingResultsExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private void printProgress() {
        long nextPrintCount = lastProgressPrintCount + PROGRESS_PRINT_QUERIES_INTERVAL;
        boolean toPrint = currentQueryCount >= nextPrintCount;
        if (toPrint) {
            logger.info(
                    String.format(
                            "Time elapsed: %s. Executed %d queries.",
                            getTimeElapsed(startTime),
                            currentQueryCount
                    ));
            lastProgressPrintCount = currentQueryCount;
        }
    }


    private static String createValue(Number idx) {
        JSONObject value = new JSONObject();
        LocalDateTime dateTime = LocalDateTime.ofEpochSecond((Long) idx, 0, ZoneOffset.UTC);
        value.put("event_time", OffsetDateTime.of(dateTime, ZoneOffset.UTC));
        value.put("event_tick", idx);
        return value.toString();
    }

    public static void main(String[] args) throws Exception {
        new SqlStreamToStreamJoinSoakTest(EVENTS_SOURCE_PREFIX + randomName()).run(args);
    }
}
