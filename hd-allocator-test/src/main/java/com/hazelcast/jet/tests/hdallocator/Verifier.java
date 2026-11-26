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

package com.hazelcast.jet.tests.hdallocator;

import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.jet.tests.common.Util.sleepSeconds;

/** Pauses the workers and compares every key of both maps with the workers' models. */
final class Verifier {

    private static final int PERIODIC_ATTEMPTS = 3;
    private static final int BATCH_SIZE = 500;
    private static final int RETRY_DELAY_SECONDS = 5;

    private final Context context;
    private final Worker[] workers;

    Verifier(Context context, Worker[] workers) {
        this.context = context;
        this.workers = workers;
    }

    /** Returns true when a complete verification was done. */
    boolean verifyWithWorkersPaused() throws InterruptedException {
        if (!context.pauseWorkers()) {
            return false;
        }
        try {
            return verify(PERIODIC_ATTEMPTS);
        } finally {
            context.resumeWorkers();
        }
    }

    boolean verify(int attempts) {
        for (int attempt = 1; attempt <= attempts; attempt++) {
            long before = context.disruptions();
            try {
                checkAllKeys();
                return true;
            } catch (AssertionError e) {
                if (context.disruptions() == before) {
                    throw e;
                }
                context.logger.warning(context.testName + ": mismatch during a disruption, repeating: " + e);
            } catch (Exception e) {
                context.logger.warning(context.testName + ": verification failed, repeating: " + e);
            }
            sleepSeconds(RETRY_DELAY_SECONDS);
        }
        context.logger.warning(context.testName + ": verification skipped, the cluster was not available");
        return false;
    }

    private void checkAllKeys() {
        checkMainMap();
        int[] countPerValue = checkIndexMapAndCountValues();
        checkIndexQueries(countPerValue);
    }

    private void checkMainMap() {
        IMap<Integer, byte[]> map = context.mainMap();
        int found = 0;
        for (int from = 0; from < context.settings.keys; from += BATCH_SIZE) {
            Set<Integer> batch = keyRange(from, context.settings.keys);
            Map<Integer, byte[]> values = map.getAll(batch);
            for (int key : batch) {
                byte[] actual = values.get(key);
                ownerOf(key).mainModel.check(key, actual, context.disruptions());
                found += actual == null ? 0 : 1;
            }
        }
        assertEqual("main map size differs from the number of keys found", found, map.size());
    }

    /** Checks every key of the indexed map and returns how many keys hold each number. */
    private int[] checkIndexMapAndCountValues() {
        IMap<Integer, Long> indexMap = context.indexMap();
        int[] countPerValue = new int[Values.INDEX_VALUES];
        int found = 0;
        for (int from = 0; from < context.settings.indexKeys; from += BATCH_SIZE) {
            Set<Integer> batch = keyRange(from, context.settings.indexKeys);
            Map<Integer, Long> values = indexMap.getAll(batch);
            for (int key : batch) {
                Long actual = values.get(key);
                ownerOf(key).indexModel.check(key, actual, context.disruptions());
                if (actual != null) {
                    countPerValue[actual.intValue()]++;
                    found++;
                }
            }
        }
        assertEqual("index map size differs from the number of keys found", found, indexMap.size());
        return countPerValue;
    }

    /** Compares index queries with the numbers counted from the stored data. */
    private void checkIndexQueries(int[] countPerValue) {
        IMap<Integer, Long> indexMap = context.indexMap();
        for (int value = 0; value < Values.INDEX_VALUES; value++) {
            assertEqual("index query for value " + value, countPerValue[value],
                    indexMap.values(Predicates.equal("this", (long) value)).size());
        }
        int low = Values.INDEX_VALUES / 4;
        int high = Values.INDEX_VALUES / 2;
        int expectedInRange = 0;
        for (int value = low; value <= high; value++) {
            expectedInRange += countPerValue[value];
        }
        assertEqual("sorted index range query", expectedInRange,
                indexMap.values(Predicates.between("this", (long) low, (long) high)).size());
    }

    private Worker ownerOf(int key) {
        return workers[key % workers.length];
    }

    private static Set<Integer> keyRange(int from, int limit) {
        Set<Integer> range = new HashSet<>();
        for (int key = from; key < Math.min(limit, from + BATCH_SIZE); key++) {
            range.add(key);
        }
        return range;
    }

    private static void assertEqual(String what, long expected, long actual) {
        if (expected != actual) {
            throw new AssertionError(what + ": expected " + expected + " but was " + actual);
        }
    }
}
