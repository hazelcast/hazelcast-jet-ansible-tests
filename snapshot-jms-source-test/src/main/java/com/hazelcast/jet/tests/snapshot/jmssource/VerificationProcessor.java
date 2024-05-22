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

package com.hazelcast.jet.tests.snapshot.jmssource;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.pipeline.SinkImpl;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;

import java.util.PriorityQueue;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.forceTotalParallelismOne;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.impl.pipeline.SinkImpl.Type.TOTAL_PARALLELISM_ONE;
import static java.lang.String.format;

public class VerificationProcessor extends AbstractProcessor {

    public static final String CONSUMED_MESSAGES_MAP_NAME = "JmsSourceTest_latestCounter";
    private static final int QUEUE_SIZE_LIMIT = 5_000;
    private static final int PRINT_LOG_ITEMS = 5_000;

    private final String name;

    private long counter;
    private final PriorityQueue<Long> queue = new PriorityQueue<>();
    private ILogger logger;
    private IMap<String, Long> map;

    public VerificationProcessor(String name) {
        this.name = name;
    }

    @Override
    protected void init(Context context) {
        logger = context.logger();
        map = context.hazelcastInstance().getMap(CONSUMED_MESSAGES_MAP_NAME);
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) {
        long value = (Long) item;
        queue.offer(value);
        // try to verify head of verification queue
        long counterBeforeProcess = counter;
        for (Long peeked; (peeked = queue.peek()) != null; ) {
            if (peeked > counter) {
                // the item might arrive later
                break;
            } else if (peeked == counter) {
                if (counter % PRINT_LOG_ITEMS == 0) {
                    logger.info(format("[%s] Processed correctly item %d", name, counter));
                }
                // correct head of queue
                queue.remove();
                counter++;
            } else {
                // duplicate key
                logger.warning(format("[%s] Duplicate key %d, but counter was %d", name, peeked, counter));
                queue.remove();
            }
        }
        if (counter != counterBeforeProcess) {
            try {
                map.setAsync(name, counter);
            } catch (HazelcastInstanceNotActiveException e) {
                logger.warning(format("Setting the counter[%s] to %d failed with instance not active exception",
                        name, counter), e);
            }
        }
        if (queue.size() >= QUEUE_SIZE_LIMIT) {
            throw new AssertionError(format("[%s] Queue size exceeded while waiting for the next "
                            + "item. Limit=%d, expected next=%d, next in queue: %s, %s, %s, %s, ...",
                    name, QUEUE_SIZE_LIMIT, counter, queue.poll(), queue.poll(), queue.poll(), queue.poll()));
        }
        return true;
    }

    @Override
    public boolean saveToSnapshot() {
        logger.info(format("saveToSnapshot counter: %d, size: %d, peek: %d",
                counter, queue.size(), queue.peek()));
        return tryEmitToSnapshot(name, tuple2(counter, queue));
    }

    @Override
    protected void restoreFromSnapshot(Object ignored, Object value) {
        Tuple2<Long, PriorityQueue<Long>> tuple = (Tuple2) value;
        counter = tuple.f0();
        queue.addAll(tuple.f1());

        logger.info(format("restoreFromSnapshot counter: %d, size: %d, peek: %d",
                counter, queue.size(), queue.peek()));
    }

    static Sink<Long> sink(String name) {
        return new SinkImpl<>(name,
                forceTotalParallelismOne(ProcessorSupplier.of(() -> new VerificationProcessor(name)), name),
                TOTAL_PARALLELISM_ONE);
    }

}
