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

package com.hazelcast.jet.tests.largesnapshotchunk;

import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.impl.util.AsyncSnapshotWriterImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Bits;

import java.util.stream.IntStream;

import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;

class LargeSnapshotChunkProducer {

    private static final int PRODUCER_SLEEP_MILLIS = 1000;

    private final ILogger logger;
    private final String[] keys;
    private final JetInstance instance;
    private final int windowSize;
    private final IMapJet<String, int[]> map;
    private final Thread thread;

    private volatile boolean shutdown;

    LargeSnapshotChunkProducer(ILogger logger, JetInstance instance, int windowSize, IMapJet<String, int[]> map) {
        this.logger = logger;
        this.instance = instance;
        this.windowSize = windowSize;
        this.map = map;
        this.thread = new Thread(this::run);

        // find one string key for each partition
        int partitionCount = instance.getHazelcastInstance().getPartitionService().getPartitions().size();
        keys = new String[partitionCount];
        for (int i = 0; partitionCount > 0; i++) {
            String key = "key-" + i;
            int partition = instance.getHazelcastInstance().getPartitionService().getPartition(key).getPartitionId();
            if (keys[partition] == null) {
                keys[partition] = key;
                partitionCount--;
            }
        }
    }

    private void run() {
        for (long time = 0; !shutdown; time++) {
            int intTime = (int) time;
            int[] bigValue = IntStream.generate(() -> intTime)
                                      .limit(AsyncSnapshotWriterImpl.DEFAULT_CHUNK_SIZE / Bits.INT_SIZE_IN_BYTES / windowSize)
                                      .toArray();
            int[] smallValue = {bigValue[0]};
            try {
                for (int i = 0; i < keys.length; i++) {
                    // Use big value only for the 1st partition. For other partitions use small value to
                    // not use too much memory. We produce value to all partitions in order to not
                    // have to wait for idle timeout to have the output.
                    map.set(keys[i], i == 0 ? bigValue : smallValue);
                }
            } catch (Exception e) {
                logger.severe("Exception while producing, time: " + time, e);
                sleepSeconds(1);
                continue;
            }
            sleepMillis(PRODUCER_SLEEP_MILLIS);
        }
    }

    void start() {
        thread.start();
    }

    void stop() throws InterruptedException {
        shutdown = true;
        thread.join();
    }
}
