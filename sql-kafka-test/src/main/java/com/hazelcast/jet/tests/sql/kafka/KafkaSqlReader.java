package com.hazelcast.jet.tests.sql.kafka;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.tests.common.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import org.junit.Assert;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class KafkaSqlReader implements Callable<Integer> {

    private static final long SQL_UPDATE_MAX_TIMEOUT = TimeUnit.SECONDS.toMillis(10);
    private static final long PROGRESS_PRINT_INTERVAL = TimeUnit.SECONDS.toMillis(10);

    private JetInstance jetInstance;
    private String topicName;
    private ILogger logger;
    private long iteratorLastUpdated = System.currentTimeMillis();
    private long begin;
    private long durationInMillis;
    private long lastProgressPrintTime;
    private int currentQueryCount;

    public KafkaSqlReader(ILogger logger, JetInstance jetInstance, String topicName, long begin, long durationInMillis) {
        this.jetInstance = jetInstance;
        this.topicName = topicName;
        this.logger = logger;
        this.begin = begin;
        this.durationInMillis = durationInMillis;
    }

    @Override
    public Integer call() {
        SqlResult sqlResult = jetInstance.getSql().execute(getSQLQuery());
        readFromIterator(sqlResult.iterator());
        return currentQueryCount;
    }

    private void readFromIterator(Iterator<SqlRow> iterator) {
        while (System.currentTimeMillis() - begin < durationInMillis) {
            checkIfNotStuck();
            //The following line would throw an exception in case SqlRow does not contain the value we expect
            iterator.next().getObject("intVal");
            iteratorLastUpdated = System.currentTimeMillis();
            currentQueryCount++;
            printProgress();
        }
    }

    private void checkIfNotStuck() {
        Assert.assertFalse("Too big interval between SQL updates. Reading seems stuck.",
                (System.currentTimeMillis()-iteratorLastUpdated) > SQL_UPDATE_MAX_TIMEOUT);
    }

    private void printProgress() {
        boolean toPrint = (System.currentTimeMillis() - lastProgressPrintTime) >= PROGRESS_PRINT_INTERVAL;
        if (toPrint) {
            logger.info(String.format("Time elapsed: %s. Read %d records from Kafka with SQL.",
                    Util.getTimeElapsed(begin), currentQueryCount));
            lastProgressPrintTime = System.currentTimeMillis();
        }
    }

    private String getSQLQuery() {
        return "SELECT * FROM " + topicName;
    }
}
