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

package com.hazelcast.jet.tests.stateful;

import com.hazelcast.logging.ILogger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.tests.stateful.StatefulMapTest.WAIT_TX_TIMEOUT_FACTOR;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

/**
 * It produces events with String key like [type],[id] where type is 0 for start
 * event and 1 for end event. Value is timestamp.
 * <p>
 * In every cycle it emits "batchCount" times start event + "batchCount" times
 * end event for them; and 1 transaction event (with negative id) which has only
 * start and which will never get the END event.
 * <p>
 * Ids are increasing by 1 for each start event and for each end event (i.e.
 * there is just one start event with id x and just one end event with id x).
 */
public class KafkaTradeProducer implements AutoCloseable {

    private static final int PRODUCE_WAIT_TIMEOUT_MILLIS = 10_000;
    private static final int MAX_WAIT_IN_FINISH_MS = 120_000;

    private final KafkaProducer<String, Long> producer;
    private final String topic;
    private final long nanosBetweenEvents;
    private final int batchCount;
    private final int txTimeout;

    private final Thread producerThread;
    private volatile boolean finished;
    private volatile Exception exception;

    private long txId;

    public KafkaTradeProducer(
            ILogger logger, String broker, String topic, int txPerSeconds, int batchCount, int txTimeout
    ) {
        this.producerThread = new Thread(() -> {
            try {
                run();
            } catch (Exception exception) {
                logger.severe("Exception while producing trades to topic: " + topic, exception);
                this.exception = exception;
            }
        });
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", broker);
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", LongSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.batchCount = batchCount;
        this.txTimeout = txTimeout;
        this.nanosBetweenEvents = SECONDS.toNanos(1) / txPerSeconds;
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    private void run() throws Exception {
        List<Future<RecordMetadata>> futureList = new ArrayList<>(batchCount);
        while (!finished) {
            // generate start events
            for (int i = 0; i < batchCount; i++) {
                long id = txId + i;
                futureList.add(produce(0, id, id));
                parkNanos(nanosBetweenEvents);
            }
            waitForCompleteAndClearList(futureList);

            // generate send events
            for (int i = 0; i < batchCount; i++) {
                long id = txId + i;
                futureList.add(produce(1, id, id));
                parkNanos(nanosBetweenEvents);
            }
            waitForCompleteAndClearList(futureList);

            txId += batchCount;

            //This adds a transaction event of type START which will never get the END event
            //Eventually the transaction will be evicted and marked as timeout
            //a single tx is produced per batch and txId<0
            produce(0, -txId, txId).get(PRODUCE_WAIT_TIMEOUT_MILLIS, MILLISECONDS);
            parkNanos(nanosBetweenEvents);
        }
        //this is to advance wm and eventually evict expired transactions
        parkNanos(MILLISECONDS.toNanos(WAIT_TX_TIMEOUT_FACTOR * txTimeout));
        produce(-1, Long.MAX_VALUE, Long.MAX_VALUE - 1).get(PRODUCE_WAIT_TIMEOUT_MILLIS, MILLISECONDS);
    }

    void start() {
        producerThread.start();
    }

    public long finish() throws Exception {
        checkStatus();
        finished = true;
        producerThread.join(MAX_WAIT_IN_FINISH_MS);
        checkStatus();
        return txId;
    }

    public void checkStatus() throws Exception {
        if (exception != null) {
            throw exception;
        }
    }

    private Future<RecordMetadata> produce(int type, long id, long timestamp) {
        String key = type + "," + id;
        return producer.send(new ProducerRecord<>(topic, key, timestamp));
    }

    private static void waitForCompleteAndClearList(List<Future<RecordMetadata>> futureList) {
        futureList.forEach(f -> uncheckCall(() -> f.get(PRODUCE_WAIT_TIMEOUT_MILLIS, MILLISECONDS)));
        futureList.clear();
    }

}
