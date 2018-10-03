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

package tests.eventjournal;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.jet.impl.util.Util.toLocalTime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class EventJournalTradeProducer extends Thread {

    private static final ILogger LOGGER = Logger.getLogger(EventJournalTradeProducer.class);

    private static final int ASYNC_OP_LIMIT = 50_000;

    private final int countPerTicker;
    private final IMapJet<Long, Long> map;
    private final int timestampPerSecond;
    private volatile boolean running = true;
    private final AtomicLong realTimestamp = new AtomicLong();
    private final AtomicInteger pendingAsyncOps = new AtomicInteger();

    public EventJournalTradeProducer(int countPerTicker, IMapJet<Long, Long> map, int timestampPerSecond) {
        this.countPerTicker = countPerTicker;
        this.map = map;
        this.timestampPerSecond = timestampPerSecond;
    }

    public void close() throws InterruptedException {
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
                LOGGER.info("Too many async ops pending, waiting 1 second");
                LockSupport.parkNanos(SECONDS.toNanos(1));
            }
            for (int i = 0; i < countPerTicker; i++) {
                map.setAsync(key, timestamp)
                   .andThen(new RetryRecursivelyCallback(key, timestamp));
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
                LOGGER.info("Produced realEventTime=" + realTimestamp1 + ", " + toLocalTime(realTimestamp1) + " lag="
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
    private final class RetryRecursivelyCallback implements ExecutionCallback<Void> {

        private final long key;
        private final long timestamp;

        private RetryRecursivelyCallback(long key, long timestamp) {
            this.key = key;
            this.timestamp = timestamp;
        }

        @Override
        public void onResponse(Void response) {
            realTimestamp.updateAndGet(val -> Math.max(val, timestamp));
            map.removeAsync(key);
            pendingAsyncOps.decrementAndGet();
        }

        @Override
        public void onFailure(Throwable t) {
            LOGGER.info("setAsync for (" + key + "," + timestamp + ") failed, retrying: " + t);
            map.setAsync(key, timestamp)
               .andThen(this);
        }
    }
}
