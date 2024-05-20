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

package com.hazelcast.jet.tests.snapshot.jmssink;

import com.hazelcast.logging.ILogger;
import java.util.PriorityQueue;

import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.jet.tests.snapshot.jmssink.JmsSinkTest.SINK_QUEUE;

public class JmsSinkVerifier {

    private static final int SLEEP_AFTER_VERIFICATION_CYCLE_MS = 1000;
    private static final int ALLOWED_NO_INPUT_MS = 600000;
    private static final int QUEUE_SIZE_LIMIT = 20_000;
    private static final int PRINT_LOG_ITEMS = 10_000;
    private static final int MAX_VERIFY_BULK_SIZE = 2_000;

    private final Thread consumerThread;
    private final String name;
    private final ILogger logger;
    private final String brokerURL;
    private final String queueName;

    private volatile boolean finished;
    private volatile Throwable error;
    private long counter;
    private final PriorityQueue<Long> verificationQueue = new PriorityQueue<>();

    public JmsSinkVerifier(String name, String brokerURL, ILogger logger) {
        this.consumerThread = new Thread(() -> uncheckRun(this::run));
        this.brokerURL = brokerURL;
        this.queueName = SINK_QUEUE + name;
        this.name = name;
        this.logger = logger;
    }

    private void run() throws Exception {
        Connection connection = new ActiveMQConnectionFactory(brokerURL).createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(session.createQueue(queueName));


        long lastInputTime = System.currentTimeMillis();
        while (!finished) {
            try {
                int lastBulkSize = 0;
                for (Message msg; (msg = consumer.receiveNoWait()) != null && lastBulkSize < MAX_VERIFY_BULK_SIZE;
                        lastBulkSize++) {
                    verificationQueue.add(Long.valueOf(((TextMessage) msg).getText()));
                }

                long now = System.currentTimeMillis();
                if (lastBulkSize == 0) {
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
        for (Long peeked; (peeked = verificationQueue.peek()) != null;) {
            if (peeked > counter) {
                // the item might arrive later
                break;
            } else if (peeked == counter) {
                if (counter % PRINT_LOG_ITEMS == 0) {
                    logger.info(String.format("[%s] Processed correctly item %d", name, counter));
                }
                // correct head of queue
                verificationQueue.remove();
                counter++;
            } else if (peeked < counter) {
                // duplicate key
                throw new AssertionError(
                        String.format("Duplicate key %d, but counter was %d", peeked, counter));
            }
        }
        if (verificationQueue.size() >= QUEUE_SIZE_LIMIT) {
            throw new AssertionError(String.format("[%s] Queue size exceeded while waiting for the next "
                    + "item. Limit=%d, expected next=%d, next in queue: %s, %s, %s, %s, ...",
                    name, QUEUE_SIZE_LIMIT, counter, verificationQueue.poll(), verificationQueue.poll(),
                    verificationQueue.poll(), verificationQueue.poll()));
        }
    }

    void start() {
        consumerThread.start();
    }

    public void finish() throws InterruptedException {
        finished = true;
        consumerThread.join();
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
