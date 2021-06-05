package com.hazelcast.jet.tests.sql.common;

import com.hazelcast.jet.tests.common.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.jet.tests.sql.pojo.Key;
import com.hazelcast.jet.tests.sql.pojo.Pojo;
import org.junit.Assert;

import java.util.concurrent.TimeUnit;

public class BaseExecutor {

    private static final long PROGRESS_PRINT_INTERVAL = TimeUnit.SECONDS.toMillis(5);
    private static final long MAXIMUM_TIME_BETWEEN_QUERIES = TimeUnit.SECONDS.toMillis(30);

    private ILogger logger;
    private long lastCheckpoint = System.currentTimeMillis();
    protected int currentQueryCount;
    protected long begin;
    protected long threshold;

    public BaseExecutor(ILogger logger, long begin, long threshold) {
        this.logger = logger;
        this.begin = begin;
        this.threshold = threshold;
    }

    protected void printProgress(String joinType) {
        boolean toPrint = (System.currentTimeMillis() - lastCheckpoint) >= PROGRESS_PRINT_INTERVAL;
        if (toPrint) {
            logger.info(String.format("Time elapsed: %s. Read %d records from SQL stream with " + joinType,
                    Util.getTimeElapsed(begin), currentQueryCount));
            lastCheckpoint = System.currentTimeMillis();
        }
    }

    protected void verifyNotStuck() {
        long millisSinceLastCheckpoint = System.currentTimeMillis() - lastCheckpoint;
        boolean isStuck = millisSinceLastCheckpoint > (MAXIMUM_TIME_BETWEEN_QUERIES + threshold);
        Assert.assertFalse(String.format("It's been more than %d seconds since last SQL query. " +
                        "The test seems to be stuck", TimeUnit.MILLISECONDS.toSeconds(millisSinceLastCheckpoint)),
                isStuck);
    }

    protected void populateMap(IMap<Key, Pojo> map, int entries) {
        for (int i=0; i<entries; i++) {
            map.put(new Key(i), new Pojo(i));
        }
    }
}
