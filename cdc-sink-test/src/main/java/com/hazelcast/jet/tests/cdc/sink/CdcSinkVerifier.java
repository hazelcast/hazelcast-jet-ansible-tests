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

package com.hazelcast.jet.tests.cdc.sink;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;

import java.sql.SQLException;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.jet.tests.cdc.sink.CdcSinkTest.EXPECTED_VALUE_PATTERN;
import static com.hazelcast.jet.tests.cdc.sink.CdcSinkTest.SINK_MAP_NAME;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;

public class CdcSinkVerifier {

    private static final int SLEEP_AFTER_VERIFICATION_CYCLE_SECONDS = 10;
    private static final int MAX_ALLOWED_TIME_WITHOUT_NEW_ITEM_MS = 600_000;

    private final Thread consumerThread;
    private final IMap<Integer, String> map;
    private final String name;
    private final int preserveItem;
    private final ILogger logger;
    private volatile boolean finished;
    private volatile Throwable error;
    private volatile int lastPreserved;

    public CdcSinkVerifier(HazelcastInstance client, String name, int preserveItem, ILogger logger) throws SQLException {
        this.consumerThread = new Thread(() -> uncheckRun(this::run));
        this.map = client.getMap(SINK_MAP_NAME + name);
        this.name = name;
        this.preserveItem = preserveItem;
        this.logger = logger;
    }

    private void run() {
        int counter = 0;
        String expectedValue = prepareExpectedValue(counter);
        long lastCorrectCheckTime = System.currentTimeMillis();
        while (!finished) {
            try {
                String value = map.get(counter);
                if (expectedValue.equals(value)) {
                    logger.info(String.format("[%s] Processed correctly item %d", name, counter));
                    lastCorrectCheckTime = System.currentTimeMillis();
                    counter += preserveItem;
                    expectedValue = prepareExpectedValue(counter);
                } else {
                    long currentTime = System.currentTimeMillis();
                    // check whether time for the new issue was not elapsed
                    if (currentTime - lastCorrectCheckTime > MAX_ALLOWED_TIME_WITHOUT_NEW_ITEM_MS) {
                        throw new AssertionError(String.format(
                                "[%s] No new item was processed during the last %d ms. Missing item is %d. "
                                        + "Item with this key is %s.",
                                name, MAX_ALLOWED_TIME_WITHOUT_NEW_ITEM_MS, counter, value));
                    }
                    sleepSeconds(SLEEP_AFTER_VERIFICATION_CYCLE_SECONDS);
                }
            } catch (Throwable e) {
                logger.severe("[" + name + "] Exception thrown during processing.", e);
                error = e;
                finished = true;
            }
        }
        lastPreserved = counter - preserveItem;
    }

    void start() {
        consumerThread.start();
    }

    public int finish() throws Exception {
        finished = true;
        consumerThread.join();
        if (error != null) {
            throw new RuntimeException(error);
        }
        return lastPreserved;
    }

    public void checkStatus() {
        if (error != null) {
            throw new RuntimeException(error);
        }
        if (finished) {
            throw new RuntimeException("[" + name + "] Verifier is not running");
        }
    }

    private static String prepareExpectedValue(int entryId) {
        return String.format(EXPECTED_VALUE_PATTERN, entryId);
    }

}
