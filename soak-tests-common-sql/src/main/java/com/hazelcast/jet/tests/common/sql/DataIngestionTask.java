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

package com.hazelcast.jet.tests.common.sql;

import com.hazelcast.jet.tests.common.Util;
import com.hazelcast.logging.ILogger;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.hazelcast.logging.Logger.getLogger;

public class DataIngestionTask implements Runnable {

    public static final String DEFAULT_QUERY_TIMEOUT_MILLIS = "100";
    private static final int DEFAULT_COUNT_PER_BATCH = 10;
    private static final int DEFAULT_START_TIME = DEFAULT_COUNT_PER_BATCH;
    private static final int DEFAULT_TIME_INTERVAL = 1;
    private static final int DEFAULT_RETRY_DELAY_SECONDS = 10;
    private final Function<Number, String> createValue;
    private final String brokerUrl;
    private final String sourceName;
    private final long queryTimeout;
    private boolean continueProducing;
    private final ILogger logger;
    private final int startTime;
    private final int countPerBatch;
    private final int timeInterval;
    private final int retryDelaySeconds;

    public DataIngestionTask(String brokerUrl, String sourceName, int startTime, int countPerBatch, int timeInterval,
             int retryDelaySeconds, Function<Number, String> createValue) {
        this.brokerUrl = brokerUrl;
        this.sourceName = sourceName;
        this.startTime = startTime;
        this.countPerBatch = countPerBatch;
        this.timeInterval = timeInterval;
        this.retryDelaySeconds = retryDelaySeconds;
        this.createValue = createValue;

        this.continueProducing = true;
        this.queryTimeout = Long.parseLong(
                System.getProperty("queryTimeout", DEFAULT_QUERY_TIMEOUT_MILLIS));
        this.logger = getLogger(getClass());
    }

    public DataIngestionTask(String brokerUrl, String sourceName,
             Function<Number, String> createValue) {
        this(brokerUrl, sourceName, DEFAULT_START_TIME, DEFAULT_COUNT_PER_BATCH,
            DEFAULT_TIME_INTERVAL, DEFAULT_RETRY_DELAY_SECONDS, createValue);
    }

    @Override
    public void run() {
        AtomicInteger currentEventStartTime = new AtomicInteger(startTime);

        try (ItemProducer producer = new ItemProducer(brokerUrl)) {
            while (continueProducing) {
                // produce late events at rate inversely proportional to queryTimeout
                boolean includeLateEvent = currentEventStartTime.get() % (queryTimeout * 120) == 0;

                // ingest data to Kafka using new timestamps
                try {
                    produceTradeRecords(producer, currentEventStartTime.getAndAdd(countPerBatch), includeLateEvent);
                } catch (Exception e) {
                    String warnMessage = String.format(
                            "Failed to produce new records. Retrying in %d seconds.", retryDelaySeconds);
                    logger.warning(warnMessage, e);
                    Util.sleepSeconds(retryDelaySeconds);
                    continue;
                }

                Util.sleepMillis(queryTimeout);
            }
        }
    }

    public void stopProducingEvents() {
        this.continueProducing = false;
    }

    private void produceTradeRecords(ItemProducer producer, int currentEventStartTime, boolean includeLate) {
        producer.produceItems(sourceName, currentEventStartTime, countPerBatch, timeInterval, createValue);

        if (includeLate) {
            int lagTime = timeInterval * 2;
            producer.produceItems(
                    sourceName,
                    Long.max(0, currentEventStartTime - lagTime * 10_000L),
                    1,
                    timeInterval,
                    createValue
            );
        }
    }
}
