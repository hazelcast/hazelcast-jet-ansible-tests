package com.hazelcast.jet.tests.sql.kafka;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;

import java.util.Iterator;

public class KafkaSqlReader{

    public final Thread readerThread;
    private JetInstance jetInstance;
    private String topicName;
    private volatile boolean finished;
    private ILogger logger;
    private int queryCount;
    long iteratorLastUpdated = System.currentTimeMillis();

    public KafkaSqlReader(ILogger logger, JetInstance jetInstance, String topicName) {
        this.jetInstance = jetInstance;
        this.topicName = topicName;
        this.logger = logger;
        this.readerThread = new Thread(() -> {
            try {
                logger.info("Starting reader thread");
                run();
            } catch (Exception exception) {
                logger.severe("Exception while reading with SQL from topic: " + topicName, exception);
            }
        });
    }

    public void run() {
        SqlService sql = jetInstance.getSql();
        String query = "SELECT * FROM " + topicName;
        logger.info("Executing query: " + query);
        SqlResult sqlResult = sql.execute(query);
        Iterator<SqlRow> iterator = sqlResult.iterator();
        while(!finished) {
            verifyIteratorIsUpdating(iterator);
            logger.info("SQL query executed");
            queryCount++;
        }
    }

    public void start() {
        readerThread.start();
    }

    private void verifyIteratorIsUpdating(Iterator<SqlRow> iterator) {
        SqlRow sqlRow;
        while (!finished && iterator.hasNext()) {
            sqlRow = iterator.next();
            logger.info("Retreived row from sql: " + sqlRow.getObject("intVal"));
            iteratorLastUpdated = System.currentTimeMillis();
        }
    }

    public void finish() throws Exception {
        logger.info("Finishing execution after");
        finished = true;
//        readerThread.interrupt();

        //TODO: Find out why join causes deadlock
        readerThread.join();
    }

    public int getQueryCount() {
        return queryCount;
    }
}
