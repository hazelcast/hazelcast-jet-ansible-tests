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

package com.hazelcast.jet.tests.eventjournal;

import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

import static com.hazelcast.jet.impl.util.Util.toLocalTime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class EventJournalTradeProducer extends Thread {

    private static final int ASYNC_OP_LIMIT = 10_000;

    private final int countPerTicker;
    private final ILogger logger;
    private final IMap<Long, Long> map;
    private final int timestampPerSecond;
    private final AtomicLong realTimestamp = new AtomicLong();
    private final AtomicInteger pendingAsyncOps = new AtomicInteger();
    private volatile boolean running = true;

    EventJournalTradeProducer(int countPerTicker, IMap<Long, Long> map, int timestampPerSecond, ILogger logger) {
        this.countPerTicker = countPerTicker;
        this.map = map;
        this.timestampPerSecond = timestampPerSecond;
        this.logger = logger;
    }

    void close() throws InterruptedException {
        running = false;
        join();
    }

    @Override
    public void run() {
        long key = 0;
        long timestamp = 0;

        final long begin = System.nanoTime();
        while (running) {
            while (pendingAsyncOps.get() > ASYNC_OP_LIMIT) {
                logger.info("Too many async ops pending, waiting 1 second");
                LockSupport.parkNanos(SECONDS.toNanos(1));
            }
            for (int i = 0; i < countPerTicker; i++) {
                map.setAsync(key, timestamp)
                        .whenComplete(new RetryRecursivelyBiConsumer(key, timestamp));
                pendingAsyncOps.incrementAndGet();
                key++;
            }
            if (timestamp % timestampPerSecond == 0) {
                long elapsedRT = System.nanoTime() - begin;
                long sleepTime = SECONDS.toNanos(timestamp) / timestampPerSecond - elapsedRT;
                if (sleepTime > 0) {
                    LockSupport.parkNanos(sleepTime);
                }
                long realTimestamp1 = realTimestamp.get();
                logger.info("Produced realEventTime=" + realTimestamp1 + ", " + toLocalTime(realTimestamp1) + " lag="
                        + (NANOSECONDS.toSeconds(elapsedRT * timestampPerSecond) - realTimestamp1)
                        + ", pendingAsyncOps=" + pendingAsyncOps.get());
            }
            timestamp++;
        }
    }

    /**
     * on success removes the key from map
     * on failure retries the set operation recursively
     */
    private final class RetryRecursivelyBiConsumer implements BiConsumer<Object, Throwable> {

        private final long key;
        private final long timestamp;

        private RetryRecursivelyBiConsumer(long key, long timestamp) {
            this.key = key;
            this.timestamp = timestamp;
        }

        @Override
        public void accept(Object response, Throwable t) {
            if (t == null) {
                realTimestamp.updateAndGet(val -> Math.max(val, timestamp));
                map.removeAsync(key);
                pendingAsyncOps.decrementAndGet();
            } else {
                logger.info("setAsync for (" + key + "," + timestamp + ") failed, retrying: " + t);
                map.setAsync(key, timestamp).whenComplete(this);
            }
        }
    }
}
