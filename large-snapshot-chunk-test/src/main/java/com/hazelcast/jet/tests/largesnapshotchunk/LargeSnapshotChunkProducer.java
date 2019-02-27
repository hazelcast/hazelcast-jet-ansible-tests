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
import com.hazelcast.logging.ILogger;

import java.util.stream.IntStream;

import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;

class LargeSnapshotChunkProducer {

    private static final int PRODUCER_SLEEP_MILLIS = 500;
    private static final int INTS_IN_KILOBYTE = 256;

    private final ILogger logger;
    private final String[] keys;
    private final IMapJet<String, int[]> map;
    private final Thread thread;

    private volatile boolean shutdown;

    LargeSnapshotChunkProducer(ILogger logger, JetInstance instance, IMapJet<String, int[]> map) {
        this.logger = logger;
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
            int[] value = IntStream.generate(() -> intTime).limit(INTS_IN_KILOBYTE).toArray();
            value[0] = (int) time;
            try {
                for (String key : keys) {
                    map.set(key, value);
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
