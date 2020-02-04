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

package com.hazelcast.jet.tests.snapshot.jms;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.logging.ILogger;
import java.util.Map;
import java.util.PriorityQueue;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.preferLocalParallelismOne;

public class VerificationProcessor extends AbstractProcessor {

    public static final String CONSUMED_MESSAGES_MAP_NAME = "JmsSourceTest_latestCounter";
    private static final int QUEUE_SIZE_LIMIT = 5_000;
    private static final int PRINT_LOG_ITEMS = 5_000;

    private final String name;

    private boolean processed;
    private long counter;
    private final PriorityQueue<Long> queue = new PriorityQueue<>();
    private ILogger logger;
    private Map<String, Long> map;

    public VerificationProcessor(String name) {
        this.name = name;
    }

    @Override
    protected void init(Context context) {
        logger = context.logger();
        map = context.jetInstance().getMap(CONSUMED_MESSAGES_MAP_NAME);
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) {
        processed = true;
        long value = (Long) item;
        queue.offer(value);
        // try to verify head of verification queue
        for (Long peeked; (peeked = queue.peek()) != null;) {
            if (peeked > counter) {
                // the item might arrive later
                break;
            } else if (peeked == counter) {
                if (counter % PRINT_LOG_ITEMS == 0) {
                    logger.info(String.format("[%s] Processed correctly item %d", name, counter));
                }
                // correct head of queue
                queue.remove();
                counter++;
                map.put(name, counter);
            } else if (peeked < counter) {
                // duplicate key
                throw new AssertionError(
                        String.format("[%s] Duplicate key %d, but counter was %d", name, peeked, counter));
            }
        }
        if (queue.size() >= QUEUE_SIZE_LIMIT) {
            throw new AssertionError(String.format("[%s] Queue size exceeded while waiting for the next "
                    + "item. Limit=%d, expected next=%d, next in queue: %s, %s, %s, %s, ...",
                    name, QUEUE_SIZE_LIMIT, counter, queue.poll(), queue.poll(), queue.poll(), queue.poll()));
        }
        return true;
    }

    @Override
    public boolean saveToSnapshot() {
        if (!processed) {
            return true;
        }
        logger.info(String.format("saveToSnapshot counter: %d, size: %d, peek: %d",
                counter, queue.size(), queue.peek()));
        return tryEmitToSnapshot(BroadcastKey.broadcastKey(counter), queue);
    }

    @Override
    protected void restoreFromSnapshot(Object key, Object value) {
        counter = (Long) ((BroadcastKey) key).key();
        queue.addAll((PriorityQueue<Long>) value);

        logger.info(String.format("restoreFromSnapshot counter: %d, size: %d, peek: %d",
                counter, queue.size(), queue.peek()));
    }

    static ProcessorMetaSupplier supplier(String name) {
        return preferLocalParallelismOne(ProcessorSupplier.of(() -> new VerificationProcessor(name)));
    }

}
