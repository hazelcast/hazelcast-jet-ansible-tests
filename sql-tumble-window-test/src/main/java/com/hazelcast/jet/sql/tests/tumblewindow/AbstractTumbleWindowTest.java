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

package com.hazelcast.jet.sql.tests.tumblewindow;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.sql.impl.connector.kafka.KafkaSqlConnector;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.jet.tests.common.Util;
import com.hazelcast.jet.tests.common.sql.TestRecordProducer;
import com.hazelcast.logging.ILogger;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.jet.tests.common.Util.getTimeElapsed;

public abstract class AbstractTumbleWindowTest extends AbstractJetSoakTest {
    private static final int DEFAULT_QUERY_TIMEOUT_MILLIS = 100;
    private static final int PROGRESS_PRINT_QUERIES_INTERVAL = 500;

    private static final int EVENT_START_TIME = 0;
    private static final int EVENT_WINDOW_COUNT = 10;
    private static final int LAG_TIME = 2;
    private static final int STREAM_RESULTS_TIMEOUT_SECONDS = 600;

    private final int queryTimeout = propertyInt("queryTimeout", DEFAULT_QUERY_TIMEOUT_MILLIS);
    private final String brokerUri = property("brokerUri", "localhost:9092");
    private final int streamingResultsTimeout = propertyInt("streamingResultsTimeout", STREAM_RESULTS_TIMEOUT_SECONDS);

    private long begin;
    private long currentRowCount;
    private long nextPrintCount = PROGRESS_PRINT_QUERIES_INTERVAL;
    private SqlService sqlService;
    private ExecutorService ingestionExecutor;
    private ExecutorService aggregationResultsExecutor;

    private final String sourceName;
    private final String aggregationType;
    private DataIngestionTask producerTask;

    protected AbstractTumbleWindowTest(String sourceName, String aggregationType) {
        this.sourceName = sourceName;
        this.aggregationType = aggregationType;
    }

    @Override
    protected void init(HazelcastInstance client) {
        sqlService = client.getSql();
        ingestionExecutor = Executors.newSingleThreadExecutor();
        producerTask = new DataIngestionTask(sqlService, sourceName, queryTimeout, logger);
        aggregationResultsExecutor = Executors.newSingleThreadExecutor();

        SqlResult sourceMappingCreateResult = sqlService.execute("CREATE MAPPING " + sourceName + " ("
                + "tick BIGINT,"
                + "ticker VARCHAR,"
                + "price DECIMAL" + ") "
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='json-flat'"
                + ", 'bootstrap.servers'='" + brokerUri + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );
        assertEquals(0L, sourceMappingCreateResult.updateCount());

        producerTask.produceTradeRecords(EVENT_START_TIME, false);
        Util.sleepMillis(queryTimeout);
    }

    @Override
    protected void test(HazelcastInstance client, String name) throws ExecutionException, InterruptedException {
        runTest();
    }

    protected void runTest() throws ExecutionException, InterruptedException {
        begin = System.currentTimeMillis();
        ingestionExecutor.execute(producerTask);

        int currentEventStartTime = EVENT_START_TIME;

        Util.sleepSeconds(30);

        String streamingSql = "SELECT window_start, window_end, " + aggregationType + "(price) FROM" +
                " TABLE(TUMBLE(" +
                "    (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + sourceName + ", DESCRIPTOR(tick), " + LAG_TIME + ")))," +
                "    DESCRIPTOR(tick), " +
                EVENT_WINDOW_COUNT +
                " ))" +
                " GROUP BY window_start, window_end";

        try (SqlResult sqlResult = sqlService.execute(streamingSql)) {
            Iterator<SqlRow> iterator = sqlResult.iterator();

            while (System.currentTimeMillis() - begin < durationInMillis) {
                final Future<SqlRow> sqlRowFuture = aggregationResultsExecutor.submit(iterator::next);
                try {
                    SqlRow sqlRow = sqlRowFuture.get(streamingResultsTimeout, TimeUnit.SECONDS);
                    assertQuerySuccessful(sqlRow, currentEventStartTime,
                            currentEventStartTime + EVENT_WINDOW_COUNT);

                    currentEventStartTime += EVENT_WINDOW_COUNT;
                } catch (TimeoutException e) {
                    throw new RuntimeException("Timed out after " + streamingResultsTimeout
                            + " secs waiting for new records", e);
                }

                currentRowCount++;
                printProgress();
            }
        }

        producerTask.stopProducingEvents();
        logger.info(String.format("Test completed successfully. Processed %d rows in %s.", currentRowCount,
                getTimeElapsed(begin)));
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
        aggregationResultsExecutor.shutdown();
        try {
            aggregationResultsExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            aggregationResultsExecutor.shutdownNow();
        }

        ingestionExecutor.shutdown();
        try {
            ingestionExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            ingestionExecutor.shutdownNow();
        }
    }

    protected abstract void assertQuerySuccessful(SqlRow sqlRow, int currentEventStartTime, int currentEventEndTime);

    private void printProgress() {
        if (currentRowCount >= nextPrintCount) {
            logger.info(String.format("Time elapsed: %s. Processed %d rows", getTimeElapsed(begin), currentRowCount));
            nextPrintCount = currentRowCount + PROGRESS_PRINT_QUERIES_INTERVAL;
        }
    }

    private static StringBuilder createSingleRecord(StringBuilder sb, Number a) {
        sb.append("(")
                .append(a + ",")
                .append("'value-" + a + "',")
                .append(a)
                .append(")");
        return sb;
    }

    public static String randomName() {
        return "o_" + UuidUtil.newUnsecureUuidString().replace('-', '_');
    }

    static class DataIngestionTask implements Runnable {
        private static final int PRODUCER_RETRY_DELAY_SECONDS = 10;
        private final SqlService sqlService;
        private final String sourceName;
        private final long queryTimeout;
        private boolean continueProducing;
        private final ILogger logger;

        DataIngestionTask(
                SqlService sqlService,
                String sourceName,
                long queryTimeout,
                ILogger logger) {
            this.sqlService = sqlService;
            this.sourceName = sourceName;
            this.queryTimeout = queryTimeout;
            this.continueProducing = true;
            this.logger = logger;
        }

        @Override
        public void run() {
            AtomicInteger currentEventStartTime = new AtomicInteger(EVENT_WINDOW_COUNT);

            while (continueProducing) {
                // produce late events at rate inversely proportional to queryTimeout
                boolean includeLateEvent = currentEventStartTime.get() % (queryTimeout * 120) == 0;

                // ingest data to Kafka using new timestamps
                try (SqlResult res = produceTradeRecords(
                        currentEventStartTime.getAndAdd(EVENT_WINDOW_COUNT), includeLateEvent)) {
                    AbstractJetSoakTest.assertEquals(0L, res.updateCount());
                } catch (HazelcastSqlException e) {
                    logger.warning("Failed to produce new records. Retrying in 10 seconds.", e);
                    Util.sleepSeconds(PRODUCER_RETRY_DELAY_SECONDS);
                    continue;
                }

                Util.sleepMillis(queryTimeout);
            }
        }

        public void stopProducingEvents() {
            this.continueProducing = false;
        }

        private SqlResult produceTradeRecords(int currentEventStartTime, boolean includeLate) {
            StringBuilder queryBuilder = new StringBuilder("INSERT INTO " + sourceName + " VALUES");
            queryBuilder.append(TestRecordProducer.produceTradeRecords(
                    currentEventStartTime,
                    EVENT_WINDOW_COUNT, 1,
                    AbstractTumbleWindowTest::createSingleRecord
            ));

            if (includeLate) {
                queryBuilder.append(", ").append(TestRecordProducer.produceTradeRecords(
                        Long.max(0, currentEventStartTime - LAG_TIME * 10_000L),
                        1, 1,
                        AbstractTumbleWindowTest::createSingleRecord
                ));
            }

            return sqlService.execute(queryBuilder.toString());
        }
    }
}
