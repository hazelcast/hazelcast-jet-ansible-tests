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

package com.hazelcast.jet.tests.snapshot;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;

import java.util.concurrent.PriorityBlockingQueue;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This class verifies the output windows of the job
 * using a priority blocking queue.
 * De-duplicates the windows and in case of a missing one
 * waits for a fixed amount of time and throws assertion error
 */
public class QueueVerifier extends Thread {

    private static final long TIMEOUT = 50_000;
    private static final int WAIT_SLEEP_SECONDS = 10;
    private static final int INITIAL_QUEUE_SIZE = 1_000;
    private static final int LOG_QUEUE_LIMIT = 30;

    private final ILogger logger;
    private final PriorityBlockingQueue<Long> queue;
    private final int totalWindowCount;
    private final String name;

    private int windowCount;
    private long key;
    private long lastCheck = -1;

    private volatile boolean running = true;

    public QueueVerifier(LoggingService loggingService, String name, int windowCount) {
        this.logger = loggingService.getLogger(name);
        this.queue = new PriorityBlockingQueue<>(INITIAL_QUEUE_SIZE);
        this.name = name;
        this.totalWindowCount = windowCount;
        this.windowCount = windowCount;
    }

    public void offer(long item) {
        if (logger.isFinestEnabled()) {
            logger.finest("item: " + item);
        }
        if (!running) {
            StringBuilder builder = new StringBuilder("key: ").append(key).append(" - items: ");
            queue.stream().limit(LOG_QUEUE_LIMIT).forEachOrdered(i -> builder.append(i).append(", "));
            logger.severe(builder.toString());
            throw new AssertionError(name + " failed at key: " + key +
                    ", remaining window count: " + windowCount + ", total window count per key: " + totalWindowCount);
        }
        queue.offer(item);
    }

    public QueueVerifier startVerification() {
        super.start();
        return this;
    }

    @Override
    public void run() {
        while (running) {
            Long next = queue.peek();
            if (next == null) {
                //Queue is empty, sleep
                logger.info("Queue is empty");
                sleepSeconds(WAIT_SLEEP_SECONDS);
            } else if (next == key) {
                //Happy path
                queue.poll();
                lastCheck = -1;
                if (--windowCount == 0) {
                    //we have enough windows for this key, increment the key
                    key++;
                    windowCount = totalWindowCount;
                }
            } else if (next < key) {
                //we have a duplicate
                queue.poll();
            } else if (lastCheck == -1) {
                //mark last check for timeout
                lastCheck = System.currentTimeMillis();
            } else if ((System.currentTimeMillis() - lastCheck) > TIMEOUT) {
                //time is up
                running = false;
            } else {
                //sleep for timeout
                sleepSeconds(WAIT_SLEEP_SECONDS);
                logger.info("key: " + key);
            }
        }
    }

    public void close() throws Exception {
        running = false;
        join();
    }

    private static void sleepSeconds(int seconds) {
        uncheckRun(() -> SECONDS.sleep(seconds));
    }
}
