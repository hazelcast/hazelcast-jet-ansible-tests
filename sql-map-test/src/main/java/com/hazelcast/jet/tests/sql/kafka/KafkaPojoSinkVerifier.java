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

import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.tests.sql.pojo.Key;
import com.hazelcast.jet.tests.sql.pojo.Pojo;
import com.hazelcast.logging.ILogger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Properties;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;

public class KafkaPojoSinkVerifier {

    private static final int SLEEP_AFTER_VERIFICATION_CYCLE_MS = 1000;
    private static final int ALLOWED_NO_INPUT_MS = 600000;
    private static final int POLL_TIMEOUT = 1000;
    private static final int QUEUE_SIZE_LIMIT = 200_000;
    private static final int PRINT_LOG_ITEMS = 200_000;

    private final Thread consumerThread;
    private final String name;
    private final ILogger logger;
    private final String topic;
    private final KafkaConsumer<Key, Pojo> consumer;

    private volatile boolean finished;
    private volatile Throwable error;
    private long counter;
    private final PriorityQueue<Key> verificationQueue = new PriorityQueue<>();

    public KafkaPojoSinkVerifier(String name, String broker, String topic, ILogger logger) {
        this.consumerThread = new Thread(() -> uncheckRun(this::run));
        this.name = name;
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", broker);
        props.setProperty("group.id", UuidUtil.newUnsecureUuidString());
        props.setProperty("key.serializer", "com.hazelcast.jet.tests.sql.serializer.KeySerializer");
        props.setProperty("key.deserializer", "com.hazelcast.jet.tests.sql.serializer.KeyDeserializer");
        props.setProperty("value.serializer", "com.hazelcast.jet.tests.sql.serializer.PojoSerializer");
        props.setProperty("value.deserializer", "com.hazelcast.jet.tests.sql.serializer.PojoDeserializer");
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("max.poll.records", "32768");
        props.setProperty("isolation.level", "read_committed");
        this.consumer = new KafkaConsumer<>(props);
        this.topic = topic;
        this.logger = logger;
    }

    private void run() throws Exception {
        List<String> topicList = new ArrayList<>();
        topicList.add(topic);
        consumer.subscribe(topicList);

        long lastInputTime = System.currentTimeMillis();
        while (!finished) {
            try {
                ConsumerRecords<Key, Pojo> records = consumer.poll(POLL_TIMEOUT);
                for (ConsumerRecord<Key, Pojo> record : records) {
                    verificationQueue.add(record.key());
                    if (Objects.isNull(record.value())) {
                        throw new AssertionError("Value is null for Key: " + record.key());
                    }
                }

                long now = System.currentTimeMillis();
                if (records.isEmpty()) {
                    if (now - lastInputTime > ALLOWED_NO_INPUT_MS) {
                        throw new AssertionError(
                                String.format("[%s] No new data was added during last %s", name, ALLOWED_NO_INPUT_MS));
                    }
                } else {
                    verifyQueue();
                    lastInputTime = now;
                }
                Thread.sleep(SLEEP_AFTER_VERIFICATION_CYCLE_MS);
            } catch (Throwable e) {
                logger.severe("[" + name + "] Exception thrown during processing files.", e);
                error = e;
                finished = true;
            }
        }
    }

    private void verifyQueue() {
        // try to verify head of verification queue
        for (Key peeked; (peeked = verificationQueue.peek()) != null;) {
            if (peeked.getKey() > counter) {
                // the item might arrive later
                break;
            } else if (peeked.getKey() == counter) {
                if (counter % PRINT_LOG_ITEMS == 0) {
                    logger.info(String.format("[%s] Processed correctly item %d", name, counter));
                }
                // correct head of queue
                verificationQueue.remove();
                counter++;
            } else if (peeked.getKey() < counter) {
                // duplicate key
                throw new AssertionError(
                        String.format("Duplicate key %d, but counter was %d", peeked.getKey(), counter));
            }
        }
        if (verificationQueue.size() >= QUEUE_SIZE_LIMIT) {
            throw new AssertionError(String.format("[%s] Queue size exceeded while waiting for the next "
                    + "item. Limit=%d, expected next=%d, next in queue: %s, %s, %s, %s, ...",
                    name, QUEUE_SIZE_LIMIT, counter, verificationQueue.poll(), verificationQueue.poll(),
                    verificationQueue.poll(), verificationQueue.poll()));
        }
    }

    public void start() {
        consumerThread.start();
    }

    public void finish() throws InterruptedException {
        finished = true;
        consumerThread.join();
    }

    public long getProcessedCount() {
        return counter;
    }

    public void checkStatus() {
        if (error != null) {
            throw new RuntimeException(error);
        }
        if (finished) {
            throw new RuntimeException("[" + name + "] Verifier is not running");
        }
    }

}
