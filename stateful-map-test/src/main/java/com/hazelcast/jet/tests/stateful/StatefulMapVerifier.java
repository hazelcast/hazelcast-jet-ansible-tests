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

import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.jet.tests.stateful.StatefulMapTest.TIMEOUT_KEY_COUNT_PREFIX;
import static com.hazelcast.jet.tests.stateful.StatefulMapTest.TOTAL_KEY_COUNT_PREFIX;

/**
 * todo add proper javadoc
 */
public class StatefulMapVerifier {

    private final Map<Long, Long> map = new HashMap<>();
    private final Map<Long, Long> timeoutMap = new HashMap<>();
    private final ReplicatedMap<String, Long> replicatedMap;
    private final int processorIndex;

    private long keyNumber;
    private long timeoutKeyNumber;

    public StatefulMapVerifier(Context context) {
        replicatedMap = context.jetInstance().getHazelcastInstance().getReplicatedMap(StatefulMapTest.REPLICATED_MAP);
        processorIndex = context.globalProcessorIndex();
    }

    public static Sink<Map.Entry<Long, Long>> verifier() {
        return SinkBuilder
                .sinkBuilder("stateful-map-verifier", StatefulMapVerifier::new)
                .receiveFn(StatefulMapVerifier::receive)
                .flushFn(StatefulMapVerifier::flush)
                .destroyFn(StatefulMapVerifier::close)
                .preferredLocalParallelism(1)
                .build();
    }

    private void receive(Map.Entry<Long, Long> entry) {
        if (entry.getKey() < 0) {
            timeoutMap.put(entry.getKey(), entry.getValue());
        } else {
            map.put(entry.getKey(), entry.getValue());
        }
    }

    private void flush() {
        int sizeBefore = map.size();
        map.values().removeIf(value -> value >= 0);
        keyNumber += sizeBefore - map.size();

        sizeBefore = timeoutMap.size();
        timeoutMap.values().removeIf(value -> value == StatefulMapTest.TIMED_OUT_CODE);
        timeoutKeyNumber += sizeBefore - timeoutMap.size();
    }

    private void close() {
        replicatedMap.put(TOTAL_KEY_COUNT_PREFIX + processorIndex, keyNumber);
        replicatedMap.put(TIMEOUT_KEY_COUNT_PREFIX + processorIndex, timeoutKeyNumber);
    }


}
