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

package com.hazelcast.jet.tests.management;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;

import java.util.PriorityQueue;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;

public final class VerificationProcessor extends AbstractProcessor {

    private static final int MAX_QUEUE_SIZE = 50_000;

    private final boolean odds;

    private boolean processed;
    private long counter;
    private PriorityQueue<Long> queue = new PriorityQueue<>();

    private VerificationProcessor(boolean odds) {
        this.odds = odds;
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) {
        processed = true;
        long value = (Long) item;
        assertValue(value);
        if (value < counter) {
            // discard stale value
        } else if (value != counter) {
            queue.offer(value);
        } else {
            incrementCounter();
            consumeQueue();
        }
        if (queue.size() > MAX_QUEUE_SIZE) {
            throw new IllegalStateException("Queue size reached the threshold(" + MAX_QUEUE_SIZE + ") = " + queue.size());
        }
        return true;
    }

    @Override
    public boolean saveToSnapshot() {
        return !processed || tryEmitToSnapshot(BroadcastKey.broadcastKey(counter), queue);
    }

    @Override
    protected void restoreFromSnapshot(Object key, Object value) {
        counter = (Long) ((BroadcastKey) key).key();
        queue = (PriorityQueue<Long>) value;

        if ((odds && !isOdd(counter)) || (!odds && isOdd(counter))) {
            counter++;
        }
        queue.removeIf(v -> (odds && !isOdd(v)) || (!odds && isOdd(v)));
    }

    private void consumeQueue() {
        while (true) {
            Long peeked = queue.peek();
            if (peeked == null || counter != peeked) {
                break;
            }
            incrementCounter();
            queue.poll();
        }
    }

    private void incrementCounter() {
        counter += 2;
    }

    private void assertValue(long value) {
        if ((odds && !isOdd(value)) || (!odds && isOdd(value))) {
            throw new AssertionError("Value should not be odd. odds: " + odds + ", value: " + value);
        }
    }

    static ProcessorMetaSupplier supplier(boolean odds) {
        return preferLocalParallelismOne(ProcessorSupplier.of(() -> new VerificationProcessor(odds)));
    }

    private static boolean isOdd(long value) {
        return value % 2 != 0;
    }
}

