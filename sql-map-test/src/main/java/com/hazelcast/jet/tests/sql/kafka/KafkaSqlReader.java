package com.hazelcast.jet.tests.sql.kafka;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;

import java.util.Iterator;

public class KafkaSqlReader extends Thread{

    private JetInstance jetInstance;
    private String topicName;
    private ILogger logger;
    private long iteratorLastUpdated = System.currentTimeMillis();
    long begin;
    long durationInMillis;

    public KafkaSqlReader(ILogger logger, JetInstance jetInstance, String topicName, long begin, long durationInMillis) {
        this.jetInstance = jetInstance;
        this.topicName = topicName;
        this.logger = logger;
        this.begin = begin;
        this.durationInMillis = durationInMillis;
    }

    @Override
    public void run() {
        SqlResult sqlResult = jetInstance.getSql().execute("SELECT * FROM " + topicName);
        readFromIterator(sqlResult.iterator());
    }

    private void readFromIterator(Iterator<SqlRow> iterator) {
        SqlRow sqlRow;
        while (System.currentTimeMillis() - begin < durationInMillis) {
            sqlRow = iterator.next();
            logger.info("Retreived row from sql: " + sqlRow.getObject("intVal"));
            //TODO: Add verification that it's not stuck
            iteratorLastUpdated = System.currentTimeMillis();
        }
    }
}
