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

package com.hazelcast.jet.tests.management;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.pipeline.SinkImpl;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.logging.ILogger;

import java.util.PriorityQueue;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.forceTotalParallelismOne;
import static com.hazelcast.jet.impl.pipeline.SinkImpl.Type.TOTAL_PARALLELISM_ONE;

public final class VerificationProcessor extends AbstractProcessor {

    public static final String SINK_NAME = "jobManagementSink";

    private static final int MAX_QUEUE_SIZE = 50_000;
    private final PriorityQueue<Long> queue = new PriorityQueue<>();
    private long counter;
    private ILogger logger;

    private VerificationProcessor() {
    }

    @Override
    protected void init(Context context) {
        logger = context.logger();
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) {
        long value = (Long) item;
        if (value < counter) {
            logger.info("discard stale value: " + value + ", counter: " + counter);
        } else if (value != counter) {
            queue.offer(value);
        } else {
            counter++;
            consumeQueue();
        }
        if (queue.size() > MAX_QUEUE_SIZE) {
            StringBuilder builder = new StringBuilder();
            builder.append(String.format("Queue reached the threshold(%d) size: %d,", MAX_QUEUE_SIZE, queue.size()));
            builder.append(" Counter: ").append(counter).append(", queue: ");
            for (int i = 0; i < 10; i++) {
                builder.append(queue.poll()).append("\t");
            }
            throw new IllegalStateException(builder.toString());
        }
        return true;
    }

    @Override
    public boolean saveToSnapshot() {
        logger.info(String.format("saveToSnapshot counter: %d, size: %d, peek: %d", counter, queue.size(), queue.peek()));
        return tryEmitToSnapshot(SINK_NAME, Tuple2.tuple2(counter, queue));
    }

    @Override
    protected void restoreFromSnapshot(Object ignored, Object value) {
        Tuple2<Long, PriorityQueue<Long>> tuple = (Tuple2) value;
        counter = tuple.f0();
        queue.addAll(tuple.f1());

        logger.info(String.format("restoreFromSnapshot counter: %d, size: %d, peek: %d",
                counter, queue.size(), queue.peek()));
    }

    private void consumeQueue() {
        while (true) {
            Long peeked = queue.peek();
            if (peeked == null || counter != peeked) {
                break;
            }
            counter++;
            queue.poll();
        }
    }

    static Sink<Long> sink() {
        return new SinkImpl<>(SINK_NAME,
                forceTotalParallelismOne(ProcessorSupplier.of(VerificationProcessor::new), SINK_NAME),
                TOTAL_PARALLELISM_ONE);
    }
}

