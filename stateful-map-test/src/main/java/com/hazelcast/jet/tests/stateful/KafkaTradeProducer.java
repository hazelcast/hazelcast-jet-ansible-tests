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

package com.hazelcast.jet.tests.stateful;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

/**
 * It produces events with String key like [type],[id] where type is 0 for start
 * event and 1 for end event. Value is timestamp.
 *
 * In every cycle it emits "batchCount" times start event + "batchCount" times
 * end event for them; and 1 transaction event (with negative id) which has only
 * start and which will never get the END event.
 *
 * Ids are increasing by 1 for each start event and for each end event (i.e.
 * there is just one start event with id x and just one end event with id x).
 */
public class KafkaTradeProducer implements AutoCloseable {

    private final KafkaProducer<String, Long> producer;
    private final String topic;
    private final long nanosBetweenEvents;
    private final int batchCount;

    private final Thread producerThread;
    private volatile boolean finished;

    private long txId;

    public KafkaTradeProducer(String broker, String topic, int txPerSeconds, int batchCount) {
        this.producerThread = new Thread(() -> uncheckRun(this::run));
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", broker);
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", LongSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
        nanosBetweenEvents = SECONDS.toNanos(1) / txPerSeconds;
        this.batchCount = batchCount;
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    private void run() {
        while (!finished) {
            // generate start events
            for (int i = 0; i < batchCount; i++) {
                long id = txId + i;
                produce(0, id, id);
                parkNanos(nanosBetweenEvents);
            }

            // generate send events
            for (int i = 0; i < batchCount; i++) {
                long id = txId + i;
                produce(1, id, id);
                parkNanos(nanosBetweenEvents);
            }

            txId += batchCount;

            //This adds a transaction event of type START which will never get the END event
            //Eventually the transaction will be evicted and marked as timeout
            //a single tx is produced per batch and txId<0
            produce(0, -txId, txId);
            parkNanos(nanosBetweenEvents);
        }
        //this is to advance wm and eventually evict expired transactions
        produce(-1, Long.MAX_VALUE, Long.MAX_VALUE - 1);
        parkNanos(nanosBetweenEvents);
    }

    void start() {
        producerThread.start();
    }

    public long finish() throws InterruptedException {
        finished = true;
        producerThread.join();
        return txId;
    }

    private void produce(int type, long id, long timestamp) {
        String key = type + "," + id;
        producer.send(new ProducerRecord<>(topic, key, timestamp));
    }

}
