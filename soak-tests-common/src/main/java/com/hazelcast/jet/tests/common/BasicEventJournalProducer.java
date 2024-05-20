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

package com.hazelcast.jet.tests.common;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;

import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;

/**
 * Produces events sequentially (0, 1, 2...) using {@link IMap#set}
 * Event journal is configured with given {@code capacity}
 * Map is cleared periodically to limit the memory consumption
 */
public class BasicEventJournalProducer {

    private static final int MAP_CLEAR_THRESHOLD = 5000;
    private static final int PRODUCER_SLEEP_MILLIS = 5;

    private final IMap<Long, Long> map;
    private final Thread thread;
    private final ILogger logger;


    private volatile boolean producing = true;

    public BasicEventJournalProducer(HazelcastInstance client, String mapName, int capacity) {
        Config config = client.getConfig();
        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.getEventJournalConfig()
                .setCapacity(capacity)
                .setEnabled(true);
        config.addMapConfig(mapConfig);
        this.logger = client.getLoggingService()
                .getLogger(BasicEventJournalProducer.class.getSimpleName() + "-" + mapName);

        this.map = client.getMap(mapName);
        this.map.destroy();

        this.thread = new Thread(this::run);
    }

    private void run() {
        long counter = 0;
        while (producing) {
            try {
                map.set(counter, counter);
            } catch (Exception e) {
                logger.severe("Exception during producing, counter: " + counter, e);
                sleepSeconds(1);
                continue;
            }
            counter++;
            if (counter % MAP_CLEAR_THRESHOLD == 0) {
                map.clear();
            }
            sleepMillis(PRODUCER_SLEEP_MILLIS);
        }
    }

    public void start() {
        thread.start();
    }

    public void stop() throws InterruptedException {
        producing = false;
        thread.join();
    }
}
