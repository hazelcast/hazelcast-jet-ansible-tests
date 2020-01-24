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

package com.hazelcast.jet.tests.stateful;

import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.tests.stateful.TransactionEvent.Type;
import com.hazelcast.logging.ILogger;

import java.util.List;

import static com.hazelcast.jet.tests.stateful.StatefulMapTest.CURRENT_TX_ID;
import static com.hazelcast.jet.tests.stateful.StatefulMapTest.MESSAGING_MAP;
import static com.hazelcast.jet.tests.stateful.StatefulMapTest.STOP_GENERATION_MESSAGE;
import static com.hazelcast.jet.tests.stateful.StatefulMapTest.TOTAL_KEY_COUNT;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

/**
 * Generates transaction events in batches. First creates a batch of `start`
 * events and then creates a batch of `end` events. After these batches it
 * creates a `start` event with a negative txId which will never get an `end`
 * event and eventually evicted by `mapStateful`.
 * <p>
 * Uses a `messagingMap` to check if the generation should stop. After stopping
 * the generation it still puts some events to the buffer just to advance the wm.
 * These events will be ignored by `mapStateful`.
 * <p>
 * When the job finished, the generator puts the total-key-count to the
 * messaging map for verification.
 */
public final class TransactionGenerator {
    private final IMapJet<String, Long> messagingMap;
    private final long nanosBetweenEvents;
    private final int batchCount;
    private final ILogger logger;

    private long txId;
    private boolean start = true;

    private TransactionGenerator(Context context, int txPerSeconds, int batchCount) {
        logger = context.jetInstance().getHazelcastInstance().getLoggingService().getLogger(getClass());
        messagingMap = context.jetInstance().getMap(MESSAGING_MAP);
        nanosBetweenEvents = SECONDS.toNanos(1) / txPerSeconds;
        this.batchCount = batchCount;
    }

    public static StreamSource<TransactionEvent> transactionEventSource(int txPerSeconds, int batchCount) {
        return SourceBuilder
                .stream("tx-generator", c -> new TransactionGenerator(c, txPerSeconds, batchCount))
                .fillBufferFn(TransactionGenerator::generateTrades)
                .createSnapshotFn(TransactionGenerator::createSnapshot)
                .restoreSnapshotFn(TransactionGenerator::restoreSnapshot)
                .destroyFn(TransactionGenerator::close)
                .build();
    }

    private void generateTrades(SourceBuilder.SourceBuffer<TransactionEvent> buf) {
        if (start && messagingMap.get(STOP_GENERATION_MESSAGE) != null) {
            //this is to advance wm and eventually evict expired transactions
            buf.add(new TransactionEvent(null, Long.MAX_VALUE, Long.MAX_VALUE - 1));
            parkNanos(nanosBetweenEvents);
            return;
        }
        Type type = start ? Type.START : Type.END;
        for (int i = 0; i < batchCount; i++) {
            long id = txId + i;
            buf.add(new TransactionEvent(type, id, id));
            parkNanos(nanosBetweenEvents);
        }
        start = !start;
        txId += start ? batchCount : 0;
        //This adds a transaction event of type START which will never get the END event
        //Eventually the transaction will be evicted and marked as timeout
        //a single tx is produced per batch and txId<0
        if (start) {
            buf.add(new TransactionEvent(Type.START, -txId, txId));
            messagingMap.put(CURRENT_TX_ID, txId);
        }
    }

    private Object[] createSnapshot() {
        logger.finest(String.format("Create snapshot txId: %d, isStart: %b", txId, start));
        return new Object[]{txId, start};
    }

    private void restoreSnapshot(List<Object[]> list) {
        Object[] pair = list.get(0);
        txId = (Long) pair[0];
        start = (Boolean) pair[1];
        logger.finest(String.format("Restore snapshot txId: %d, isStart: %b", txId, start));
    }

    private void close() {
        //put the result to map only if we get the stop message
        if (messagingMap.get(STOP_GENERATION_MESSAGE) != null) {
            messagingMap.put(TOTAL_KEY_COUNT, txId);
        }
    }
}
