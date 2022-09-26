package com.hazelcast.jet.tests.common.sql;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.sql.impl.connector.kafka.KafkaSqlConnector;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.jet.tests.common.Util;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;

import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.jet.tests.common.sql.TestRecordProducer.produceTradeRecords;

public class SqlStreamToStreamJoinSoakTest extends AbstractSoakTest {
    private static final String EVENTS_SOURCE = "events_topic";

    private final static int DEFAULT_QUERY_TIMEOUT_MILLIS = 10;
    private final static int PROGRESS_PRINT_QUERIES_INTERVAL = 500;

    private final static int EVENTS_START_TIME = 0;
    private final static int EVENTS_COUNT_PER_BATCH = DEFAULT_QUERY_TIMEOUT_MILLIS;
    private final static int EVENT_TIME_INTERVAL = 1;
//    private final static int LAG_TIME = 2;

    private final int queryTimeout = propertyInt("queryTimeout", DEFAULT_QUERY_TIMEOUT_MILLIS);
    private final String brokerUri = property("brokerUri", "localhost:9092");

    private long startTime;
    private long currentQueryCount;
    private long lastProgressPrintCount;
    private SqlService sqlService;
    private ExecutorService ingestionExecutorService;

    private final String sourceName;
    private String sqlQuery;

    private final Set<Long> expectedRows = ConcurrentHashMap.newKeySet(1024 * 32);
    private final AtomicLong assertionCounter = new AtomicLong(0L);

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
        System.out.println(initialIngestionQuery);
        SqlResult initialDataIngestionResult = sqlService.execute(initialIngestionQuery);
        assertEquals(0L, initialDataIngestionResult.updateCount());

        for (long i = EVENTS_START_TIME; i < EVENTS_COUNT_PER_BATCH; i += EVENT_TIME_INTERVAL) {
            expectedRows.add(i);
        }

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
        ingestionExecutorService.execute(
                new DataIngestionTask(sqlService, expectedRows, sourceName, durationInMillis, queryTimeout));

        Util.sleepMillis(10000L);

        SqlResult sqlResult = sqlService.execute("SELECT * FROM v1 JOIN v2 ON v1.event_time=v2.event_time");

        Iterator<SqlRow> iterator = sqlResult.iterator();
        while (System.currentTimeMillis() - startTime < durationInMillis) {
            SqlRow sqlRow = iterator.next();

            // checking only event_tick to simplify our destiny.
            Long leftRowIndex = sqlRow.getObject(1);
            Long rightRowIndex = sqlRow.getObject(3);
            assertEquals(leftRowIndex, rightRowIndex);
            assertTrue("Unexpected row index : " + leftRowIndex, expectedRows.contains(leftRowIndex));
            expectedRows.remove(leftRowIndex);

            printProgress();
        }
        if (expectedRows.size() <= Duration.ofSeconds(1).toMillis()) {
            throw new AssertionError("Too much unprocessed rows " + expectedRows.size());
        }
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
        private final Set<Long> expectedRows;
        private final long durationInMillis;
        private final long queryTimeout;

        public DataIngestionTask(
                SqlService sqlService,
                Set<Long> expectedRows,
                String sourceName,
                long durationInMillis,
                long queryTimeout) {
            this.sqlService = sqlService;
            this.expectedRows = expectedRows;
            this.sourceName = sourceName;
            this.durationInMillis = durationInMillis;
            this.queryTimeout = queryTimeout;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            long executedQueries = 0L;
            AtomicInteger sourceCurrentEventStartTime = new AtomicInteger(EVENTS_START_TIME);
            AtomicInteger sourceCurrentEventEndTime = new AtomicInteger();

            System.err.println(System.currentTimeMillis() + " -> " + startTime + " -> " + durationInMillis);
            while (System.currentTimeMillis() - startTime < durationInMillis) {

                // ingest data to Kafka using new timestamps
                sourceCurrentEventStartTime.set(sourceCurrentEventStartTime.get() + EVENTS_COUNT_PER_BATCH);
                sourceCurrentEventEndTime.set(sourceCurrentEventStartTime.get() + EVENTS_COUNT_PER_BATCH);

                String sql = "INSERT INTO " + sourceName + " VALUES" +
                        produceTradeRecords(
                                sourceCurrentEventStartTime.get(),
                                sourceCurrentEventEndTime.get(),
                                EVENT_TIME_INTERVAL);

                for (long i = sourceCurrentEventStartTime.get(); i < sourceCurrentEventStartTime.get(); i += EVENT_TIME_INTERVAL) {
                    expectedRows.add(i);
                }

                assertEquals(0L, sqlService.execute(sql).updateCount());
                ++executedQueries;
                Util.sleepMillis(queryTimeout);
                if (executedQueries % 100 == 0) {
                    System.out.println("Executed " + executedQueries + " queries, "
                            + (System.currentTimeMillis() - startTime) + " ms left.");
                }
            }
        }
    }

    private void printProgress() {
        long nextPrintCount = lastProgressPrintCount + PROGRESS_PRINT_QUERIES_INTERVAL;
        boolean toPrint = currentQueryCount >= nextPrintCount;
        if (toPrint) {
            System.err.printf("Time elapsed: %s. Executed %d queries.", getTimeElapsed(), currentQueryCount);
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

    public static void main(String[] args) throws Exception {
        new SqlStreamToStreamJoinSoakTest(EVENTS_SOURCE).run(args);
    }
}
