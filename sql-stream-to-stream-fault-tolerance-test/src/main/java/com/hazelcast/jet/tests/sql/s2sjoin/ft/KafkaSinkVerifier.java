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

package com.hazelcast.jet.tests.sql.s2sjoin.ft;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.shaded.org.json.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.math.BigInteger;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.logging.Logger.getLogger;

public class KafkaSinkVerifier {

    private static final int ALLOWED_NO_INPUT_MS = 600_000;
    private static final int SLEEP_AFTER_VERIFICATION_CYCLE_MS = 1000;
    private static final int PRINT_LOG_ITEMS = 10_000;
    private static final int QUEUE_SIZE_LIMIT = 20_000;
    private final Thread consumerThread;
    private final String name;
    private final ILogger logger;
    private final KafkaConsumer<Integer, HazelcastJsonValue> consumer;
    private volatile boolean finished;
    private final PriorityQueue<ConsumerRecord<Integer, HazelcastJsonValue>> verificationQueue;
    private final AtomicReference<Throwable> error;
    private long counter;

    public KafkaSinkVerifier(String name, Properties kafkaProps, String sinkTopic) {
        this.consumerThread = new Thread(() -> uncheckRun(this::run));
        this.name = name;
        this.logger = getLogger(getClass());

        verificationQueue = new PriorityQueue<>(Comparator.comparing(ConsumerRecord::key));

        kafkaProps.setProperty("group.id", UuidUtil.newUnsecureUuidString());
        consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(Collections.singletonList(sinkTopic));

        error = new AtomicReference<>();
    }

    private void run() {
        long lastInputTime = System.currentTimeMillis();
        while (!finished) {
            ConsumerRecords<Integer, HazelcastJsonValue> records = consumer.poll(Duration.ofSeconds(1));
            try {
                long now = System.currentTimeMillis();
                if (records.isEmpty()) {
                    if (now - lastInputTime > ALLOWED_NO_INPUT_MS) {
                        throw new AssertionError("No new data was added during last " + ALLOWED_NO_INPUT_MS);
                    }
                } else {
                    records.forEach(verificationQueue::add);
                    verifyQueue();
                    lastInputTime = now;
                }
                sleepMillis(SLEEP_AFTER_VERIFICATION_CYCLE_MS);
            } catch (Throwable e) {
                logger.severe("Exception thrown while processing consumer records.", e);
                error.set(e);
                finished = true;
            }
        }
    }

    private void verifyQueue() {
        // try to verify head of verification queue
        for (ConsumerRecord<Integer, HazelcastJsonValue> peeked; (peeked = verificationQueue.peek()) != null;) {
            if (peeked.key() > counter) {
                // the item might arrive later
                break;
            } else if (peeked.key() == counter) {
                if (counter % PRINT_LOG_ITEMS == 0) {
                    logger.info(String.format("[%s] Processed correctly item %d", name, counter));
                }

                verifyRow(peeked);  // Verify the item was joined correctly
                verificationQueue.remove();  // correct head of queue
                counter++;
            } else if (peeked.key() < counter) {
                // duplicate key
                throw new AssertionError(
                        String.format("Duplicate key %d, but counter was %d", peeked.key(), counter));
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

    public void checkStatus() {
        if (error.get() != null) {
            throw new RuntimeException(error.get());
        }
        if (finished) {
            throw new RuntimeException("Verifier is not running");
        }
    }

    public void finish() throws InterruptedException {
        finished = true;
        consumerThread.join();
        consumer.close();
    }

    private static void verifyRow(ConsumerRecord<Integer, HazelcastJsonValue> peeked) {
        JSONObject value = new JSONObject(peeked.value().getValue());
        BigInteger leftTick = value.getBigInteger("l_event_tick");
        BigInteger rightTick = value.getBigInteger("r_event_tick");
        if (!leftTick.equals(rightTick)) {
            throw new AssertionError(String.format("Left tick %d != right tick %d", leftTick, rightTick));
        }
    }
}
