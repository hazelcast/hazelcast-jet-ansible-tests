package com.hazelcast.jet.sql.tests.tumblewindow;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.sql.impl.connector.kafka.KafkaSqlConnector;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.jet.tests.common.Util;
import com.hazelcast.jet.tests.common.sql.SqlRowProcessor;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;

public abstract class AbstractTumbleWindowTest extends AbstractSoakTest {
    private final static int DEFAULT_QUERY_TIMEOUT_MILLIS = 100;
    private final static int PROGRESS_PRINT_QUERIES_INTERVAL = 500;

    private final static int EVENT_START_TIME = 0;
    private final static int EVENT_WINDOW_COUNT = 10;
    private final static int EVENT_TIME_INTERVAL = 1;
    private final static int LAG_TIME = 2;

    private final int queryTimeout = propertyInt("queryTimeout", DEFAULT_QUERY_TIMEOUT_MILLIS);
    private final String brokerUri = property("brokerUri", "localhost:9092");

    private long begin;
    private long currentQueryCount;
    private long lastProgressPrintCount;
    private SqlService sqlService;
    private ExecutorService threadPoolIngestion;
    private ExecutorService threadPoolSinkVerification;

    private final String sourceName;
    private final String sinkName;
    private final String aggregationType;

    public AbstractTumbleWindowTest(String sourceName, String sinkName, String aggregationType) {
        this.sourceName = sourceName;
        this.sinkName = sinkName;
        this.aggregationType = aggregationType;
    }

    @Override
    protected void init(HazelcastInstance client) throws InterruptedException {
        sqlService = client.getSql();
        threadPoolIngestion = Executors.newSingleThreadExecutor();
        threadPoolSinkVerification = Executors.newSingleThreadExecutor();

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

        SqlResult sinkMappingCreateResult = sqlService.execute("CREATE OR REPLACE MAPPING " + sinkName + " ("
                + "__key INT,"
                + "windowsend INT,"
                + "countsc INT"
                + ") "
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='json-flat'"
                + ")"
        );
        assertEquals(0L, sinkMappingCreateResult.updateCount());

        SqlResult initialDataIngestionResult = sqlService.execute("INSERT INTO " + sourceName + " VALUES" +
                TradeRecordProducer.produceTradeRecords(EVENT_START_TIME, EVENT_WINDOW_COUNT, EVENT_TIME_INTERVAL, LAG_TIME));
        assertEquals(0L, initialDataIngestionResult.updateCount());

        Util.sleepMillis(queryTimeout);

        SqlResult createJobResult = sqlService.execute("CREATE JOB job" + aggregationType + " AS SINK INTO " + sinkName +
                " SELECT window_start, window_end, " + aggregationType + "(price) FROM " +
                "TABLE(TUMBLE(" +
                "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + sourceName + ", DESCRIPTOR(tick), " + LAG_TIME + ")))" +
                "  , DESCRIPTOR(tick)" +
                " ," + EVENT_WINDOW_COUNT +
                ")) " +
                "GROUP BY window_start, window_end");
        assertEquals(0L, createJobResult.updateCount());
    }

    @Override
    protected void test(HazelcastInstance client, String name) throws Throwable {
        runTest();
    }

    protected void runTest() throws InterruptedException {
        // initiate counters
        begin = System.currentTimeMillis();

        String sinkRowSqlQuery;
        int sinkCurrentEventStartTime = EVENT_START_TIME;
        int sinkCurrentEventEndTime;

        AtomicInteger sourceCurrentEventStartTime = new AtomicInteger(EVENT_START_TIME);
        AtomicInteger sourceCurrentEventEndTime = new AtomicInteger();

        threadPoolIngestion.submit(
                () -> {
                    while (System.currentTimeMillis() - begin < durationInMillis) {
                        // ingest data to Kafka using new timestamps
                        sourceCurrentEventStartTime.set(sourceCurrentEventStartTime.get() + EVENT_WINDOW_COUNT);
                        sourceCurrentEventEndTime.set(sourceCurrentEventStartTime.get() + EVENT_WINDOW_COUNT);

                        sqlService.execute("INSERT INTO " + sourceName + " VALUES" +
                                TradeRecordProducer.produceTradeRecords(sourceCurrentEventStartTime.get(),
                                        sourceCurrentEventEndTime.get(), EVENT_TIME_INTERVAL, LAG_TIME));

                        Util.sleepMillis(queryTimeout);
                    }
                    return null;
                }
        );

        // pace between initial ingestion and sink verification
        Util.sleepMillis(120000);

        while (System.currentTimeMillis() - begin < durationInMillis) {
            // prepare sink row query
            sinkCurrentEventStartTime = sinkCurrentEventStartTime + EVENT_WINDOW_COUNT;
            sinkCurrentEventEndTime = sinkCurrentEventStartTime + EVENT_WINDOW_COUNT;

            sinkRowSqlQuery = "SELECT countsc FROM " + sinkName + " WHERE __key=" + sinkCurrentEventStartTime +
                    " AND windowsend=" + sinkCurrentEventEndTime;

            // execute query with timeout and  polling wait interval
            SqlRowProcessor sqlRowProcessor = new SqlRowProcessor(sinkRowSqlQuery, sqlService, threadPoolSinkVerification);
            SqlRow sqlRow = null;
            Future<SqlRow> sqlRowFuture = sqlRowProcessor.runQueryAsyncUntilRowFetched(1, 200);
            sqlRow = sqlRowProcessor.awaitQueryExecutionWithTimeout(sqlRowFuture, 10);

            // assert query successful
            assertQuerySuccessful(sqlRow, sinkCurrentEventStartTime, sinkCurrentEventEndTime);

            // timeout between queries to not stress out the cluster
            Util.sleepMillis(queryTimeout);
            currentQueryCount++;

            // print progress
            printProgress();
        }
        logger.info(String.format("Test completed successfully. Executed %d queries in %s.", currentQueryCount,
                getTimeElapsed()));
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
        threadPoolSinkVerification.shutdown();
        try {
            if (!threadPoolSinkVerification.awaitTermination(10, TimeUnit.SECONDS)) {
                threadPoolSinkVerification.shutdownNow();
            }
        } catch (InterruptedException ex) {
            threadPoolSinkVerification.shutdownNow();
            Thread.currentThread().interrupt();
        }

        threadPoolIngestion.shutdown();
        try {
            if (!threadPoolIngestion.awaitTermination(10, TimeUnit.SECONDS)) {
                threadPoolIngestion.shutdownNow();
            }
        } catch (InterruptedException ex) {
            threadPoolIngestion.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    protected abstract void assertQuerySuccessful(SqlRow sqlRow, int currentEventStartTime, int currentEventEndTime);

    private void printProgress() {
        long nextPrintCount = lastProgressPrintCount + PROGRESS_PRINT_QUERIES_INTERVAL;
        boolean toPrint = currentQueryCount >= nextPrintCount;
        if (toPrint) {
            logger.info(String.format("Time elapsed: %s. Executed %d queries", getTimeElapsed(), currentQueryCount));
            lastProgressPrintCount = currentQueryCount;
        }
    }

    private String getTimeElapsed() {
        Duration timeElapsed = Duration.ofMillis(System.currentTimeMillis() - begin);
        long days = timeElapsed.toDays();
        long hours = timeElapsed.minusDays(days).toHours();
        long minutes = timeElapsed.minusDays(days).minusHours(hours).toMinutes();
        long seconds = timeElapsed.minusDays(days).minusHours(hours).minusMinutes(minutes).toMillis() / 1000;
        return String.format("%dd, %dh, %dm, %ds", days, hours, minutes, seconds);
    }
}
