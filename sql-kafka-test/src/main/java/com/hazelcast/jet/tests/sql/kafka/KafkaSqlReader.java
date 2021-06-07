/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.tests.sql.kafka;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.tests.common.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import org.junit.Assert;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class KafkaSqlReader implements Callable<Integer> {

    private static final long PROGRESS_PRINT_INTERVAL = TimeUnit.SECONDS.toMillis(60);

    private HazelcastInstance client;
    private String topicName;
    private ILogger logger;
    private long begin;
    private long durationInMillis;
    private long lastProgressPrintTime;
    private long readFromKafkaThreshold;
    private int currentQueryCount;

    public KafkaSqlReader(ILogger logger, HazelcastInstance client, String topicName, long begin,
                          long durationInMillis, long readFromKafkaThreshold) {
        this.client = client;
        this.topicName = topicName;
        this.logger = logger;
        this.begin = begin;
        this.durationInMillis = durationInMillis;
        this.readFromKafkaThreshold = readFromKafkaThreshold;
    }

    @Override
    public Integer call() {
        SqlResult sqlResult = client.getSql().execute(getSQLQuery());
        readFromIterator(sqlResult.iterator());
        return currentQueryCount;
    }

    private void readFromIterator(Iterator<SqlRow> iterator) {
        while (System.currentTimeMillis() - begin < durationInMillis) {
            //The following line would throw an exception in case SqlRow does not contain the value we expect
            SqlRow sqlRow = iterator.next();
            checkReadTime(sqlRow);
            currentQueryCount++;
            printProgress();
        }
    }

    /**
     * Verifies that it's been less than {@link #readFromKafkaThreshold} to consume a message from Kafka
     * @param sqlRow
     */
    private void checkReadTime(SqlRow sqlRow) {
        long timestamp = sqlRow.getObject("timestampVal");
        long timeTakenToRead = System.currentTimeMillis() - timestamp;
        Assert.assertTrue(String.format("It took %d seconds to read message from Kafka while expected under %d seconds",
                TimeUnit.MILLISECONDS.toSeconds(timeTakenToRead), TimeUnit.MILLISECONDS.toSeconds(readFromKafkaThreshold)),
                timeTakenToRead < readFromKafkaThreshold);
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
