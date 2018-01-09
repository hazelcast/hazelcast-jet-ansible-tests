/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package tests.snapshot;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.PriorityBlockingQueue;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * todo add proper javadoc
 */
public class QueueVerifier extends Thread implements Closeable {

    private static final long TIMEOUT = 300_000;

    private static final int INITIAL_QUEUE_SIZE = 10_000;

    private final PriorityBlockingQueue<Long> queue;

    private final int totalWindowCount;

    private int windowCount;

    private long key;

    private long lastCheck = -1;

    private volatile boolean running = true;

    public QueueVerifier(int windowCount) {
        this.queue = new PriorityBlockingQueue<>(INITIAL_QUEUE_SIZE);
        this.totalWindowCount = windowCount;
        this.windowCount = windowCount;
    }

    public void offer(long item) {
        if (!running) {
            throw new AssertionError("Failed at key: " + key +
                    ", remaining window count: " + windowCount + ", total window count per key: " + totalWindowCount);
        }
        queue.offer(item);
    }

    @Override
    public void run() {
        while (running) {
            Long next = queue.peek();
            if (next == null) {
                //sleep
                System.out.println("Queue is empty, sleeping");
                sleepSeconds(1);
            } else if (next == key) {
                //Happy path
                System.out.println("Polled: " + next);
                queue.poll();
                lastCheck = -1;
                if (--windowCount == 0) {
                    //we have enough windows for this key, increment the key
                    key++;
                    System.out.println("Incremented key to: " + key);
                    windowCount = totalWindowCount;
                }
            } else if (next < key) {
                queue.poll();
                System.out.println("---------> Duplicate: " + next);
            } else if (lastCheck == -1) {
                //mark last check for timeout
                System.out.println("marking lastCheck, key: " + key);
                lastCheck = System.currentTimeMillis();
            } else if ((System.currentTimeMillis() - lastCheck) > TIMEOUT) {
                //time is up
                running = false;
            } else {
                //sleep
                System.out.println("waiting for timeout ");
                sleepSeconds(1);
            }
        }
    }

    @Override
    public void close() throws IOException {
        running = false;
    }

    private static void sleepSeconds(int seconds) {
        uncheckRun(() -> SECONDS.sleep(seconds));
    }
}
