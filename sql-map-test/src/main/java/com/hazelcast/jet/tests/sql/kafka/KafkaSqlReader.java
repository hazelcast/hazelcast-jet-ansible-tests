package com.hazelcast.jet.tests.sql.kafka;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import org.junit.Assert;

import java.time.LocalDateTime;
import java.util.Iterator;

public class KafkaSqlReader{

    public final Thread readerThread;
    private JetInstance jetInstance;
    private String topicName;
    private LocalDateTime finish;
    private volatile boolean finished;
    private ILogger logger;
    private int queryCount;

    public KafkaSqlReader(ILogger logger, JetInstance jetInstance, String topicName, LocalDateTime finish) {
        this.jetInstance = jetInstance;
        this.topicName = topicName;
        this.finish = finish;
        this.logger = logger;
        this.readerThread = new Thread(() -> {
            try {
                System.out.println("Starting reader thread");

                run();
            } catch (Exception exception) {
                logger.severe("Exception while reading with SQL from topic: " + topicName, exception);
            }
        });
    }

    public void run() {
        SqlService sql = jetInstance.getSql();
        while(!finished) {
            String query = "SELECT * FROM " + topicName;
            logger.info("Executing query: " + query);
            SqlResult sqlResult = sql.execute(query);
            verifySqlResultSuccessful(sqlResult);
            logger.info("SQL query executed");
            queryCount++;
        }
    }

    public void start() {
        readerThread.start();
    }

    private void verifySqlResultSuccessful(SqlResult sqlResult) {
        Iterator<SqlRow> iterator = sqlResult.iterator();
        SqlRow sqlRow = iterator.hasNext() ? iterator.next() : null;
        Assert.assertNotNull("SQL query returned no results.", sqlRow);
    }

    public void finish() throws Exception {
        logger.info("Finishing execution after");
        finished = true;
        Thread.sleep(1000);
        readerThread.interrupt();

        //TODO: Find out why join causes deadlock
//        readerThread.join();
    }

    public int getQueryCount() {
        return queryCount;
    }
}
