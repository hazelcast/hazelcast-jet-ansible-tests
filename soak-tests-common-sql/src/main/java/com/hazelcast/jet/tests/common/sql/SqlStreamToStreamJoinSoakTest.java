package com.hazelcast.jet.tests.common.sql;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.sql.impl.connector.kafka.KafkaSqlConnector;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.jet.tests.common.Util;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.jet.tests.common.sql.TestRecordProducer.produceTradeRecords;

public class SqlStreamToStreamJoinSoakTest extends AbstractSoakTest {
    private static final String EVENTS_SOURCE_PREFIX = "events_topic_";

    private final static int DEFAULT_QUERY_TIMEOUT_MILLIS = 10;
    private final static int PROGRESS_PRINT_QUERIES_INTERVAL = 500;

    private final static int EVENTS_START_TIME = 0;
    private final static int EVENTS_COUNT_PER_BATCH = 100;
    private final static int EVENT_TIME_INTERVAL = 1;

    private final int queryTimeout = propertyInt("queryTimeout", DEFAULT_QUERY_TIMEOUT_MILLIS);
    private final String brokerUri = property("brokerUri", "localhost:9092");

    private long startTime;
    private long currentQueryCount;
    private long lastProgressPrintCount;
    private SqlService sqlService;
    private ExecutorService ingestionExecutorService;

    private final String sourceName;

    public SqlStreamToStreamJoinSoakTest(String sourceName) {
        this.sourceName = sourceName;
    }

    @Override
    protected void init(HazelcastInstance client) {
        sqlService = client.getSql();
        ingestionExecutorService = Executors.newSingleThreadExecutor();
        startTime = System.currentTimeMillis();

        SqlResult sourceMappingCreateResult = sqlService.execute("CREATE MAPPING " + sourceName + " ("
                + "event_time TIMESTAMP WITH TIME ZONE,"
                + "event_tick BIGINT )"
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='json-flat'"
                + ", 'bootstrap.servers'='" + brokerUri + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );
        assertEquals(0L, sourceMappingCreateResult.updateCount());

        String initialIngestionQuery = "INSERT INTO " + sourceName + " VALUES" +
                produceTradeRecords(EVENTS_START_TIME, EVENTS_COUNT_PER_BATCH, EVENT_TIME_INTERVAL);
        logger.info("Initial ingestion query: " + initialIngestionQuery);
        SqlResult initialDataIngestionResult = sqlService.execute(initialIngestionQuery);
        assertEquals(0L, initialDataIngestionResult.updateCount());

        sqlService.execute("CREATE VIEW v1 AS "
                + "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE "
                + sourceName
                + ", DESCRIPTOR(event_time), INTERVAL '1' SECOND))");

        sqlService.execute("CREATE VIEW v2 AS "
                + "SELECT * FROM TABLE(IMPOSE_ORDER(TABLE "
                + sourceName
                + ", DESCRIPTOR(event_time), INTERVAL '1' SECOND))");
    }

    @Override
    protected final void test(HazelcastInstance client, String name) {

        DataIngestionTask producerTask = new DataIngestionTask(sqlService, sourceName, queryTimeout);
        ingestionExecutorService.execute(producerTask);

        Util.sleepMillis(30_000L);

        SqlResult sqlResult = sqlService.execute("SELECT * FROM v1 JOIN v2 ON v1.event_time=v2.event_time");

        Iterator<SqlRow> iterator = sqlResult.iterator();
        while (System.currentTimeMillis() - startTime < durationInMillis) {
            SqlRow sqlRow = iterator.next();

            // l_event       r_event
            // time(0) | 0 | time(0) | 0  <-- joint row

            // checking only event_tick to simplify our destiny.
            Long leftRowIndex = sqlRow.getObject(1);
            Long rightRowIndex = sqlRow.getObject(3);
            assertEquals(leftRowIndex, rightRowIndex);

            currentQueryCount++;
            printProgress();
        }

        producerTask.stopProducingEvents();
    }

    @Override
    protected void teardown(Throwable t) {
        ingestionExecutorService.shutdown();
        try {
            if (!ingestionExecutorService.awaitTermination(10, TimeUnit.SECONDS)) {
                ingestionExecutorService.shutdownNow();
            }
        } catch (InterruptedException ex) {
            ingestionExecutorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    static class DataIngestionTask implements Runnable {
        private final SqlService sqlService;
        private final String sourceName;
        private final long queryTimeout;
        private final AtomicBoolean continueProducing;

        public DataIngestionTask(
                SqlService sqlService,
                String sourceName,
                long queryTimeout) {
            this.sqlService = sqlService;
            this.sourceName = sourceName;
            this.queryTimeout = queryTimeout;
            this.continueProducing = new AtomicBoolean(true);
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            AtomicInteger currentEventStartTime = new AtomicInteger(EVENTS_START_TIME);
            AtomicInteger currentEventEndTime = new AtomicInteger();

            while (continueProducing.get()) {
                // ingest data to Kafka using new timestamps
                currentEventStartTime.set(currentEventStartTime.get() + EVENTS_COUNT_PER_BATCH);
                currentEventEndTime.set(currentEventStartTime.get() + EVENTS_COUNT_PER_BATCH);

                String sql = "INSERT INTO " + sourceName + " VALUES" +
                        produceTradeRecords(
                                currentEventStartTime.get(),
                                currentEventEndTime.get(),
                                EVENT_TIME_INTERVAL);

                try (SqlResult res = sqlService.execute(sql)) {
                    assertEquals(0L, res.updateCount());
                } catch (HazelcastSqlException e) {
                    continue;
                }
                Util.sleepMillis(queryTimeout);
            }
        }

        public void stopProducingEvents() {
            continueProducing.set(false);
        }
    }

    private void printProgress() {
        long nextPrintCount = lastProgressPrintCount + PROGRESS_PRINT_QUERIES_INTERVAL;
        boolean toPrint = currentQueryCount >= nextPrintCount;
        if (toPrint) {
            logger.info(
                    String.format(
                            "Time elapsed: %s. Executed %d queries.",
                            getTimeElapsed(),
                            currentQueryCount
                    ));
            lastProgressPrintCount = currentQueryCount;
        }
    }

    private String getTimeElapsed() {
        Duration timeElapsed = Duration.ofMillis(System.currentTimeMillis() - startTime);
        long days = timeElapsed.toDays();
        long hours = timeElapsed.minusDays(days).toHours();
        long minutes = timeElapsed.minusDays(days).minusHours(hours).toMinutes();
        long seconds = timeElapsed.minusDays(days).minusHours(hours).minusMinutes(minutes).toMillis() / 1000;
        return String.format("%dd, %dh, %dm, %ds", days, hours, minutes, seconds);
    }

    public static String randomName() {
        return "o_" + UuidUtil.newUnsecureUuidString().replace('-', '_');
    }

    public static void main(String[] args) throws Exception {
        new SqlStreamToStreamJoinSoakTest(EVENTS_SOURCE_PREFIX + randomName()).run(args);
    }
}
