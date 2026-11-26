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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static com.hazelcast.jet.tests.common.Util.sleepSeconds;

/**
 * Runs random operations on its own keys, which are the keys {@code k} with {@code k % threads == id}. No other worker
 * touches them, so the worker knows exactly what each key must hold.
 */
final class Worker implements Runnable {

    /** The number of keys in one batch operation. */
    static final int BATCH_KEYS = 20;

    private static final int BURST_MIN_KEYS = 32;
    private static final int BURST_KEY_RANGE = 64;
    /** A burst writes about this many bytes at most, so large values mean fewer keys. */
    private static final int BURST_BYTES = 4 * 1024 * 1024;
    private static final int BURST_FEWEST_KEYS = 8;

    final KeyModel<byte[]> mainModel;
    final KeyModel<Long> indexModel;

    private final Context context;
    private final int id;
    private final Random random;

    Worker(Context context, int id, Random random) {
        this.context = context;
        this.id = id;
        this.random = random;
        this.mainModel = new KeyModel<>("main map", context.settings.keys);
        this.indexModel = new KeyModel<>("index map", context.settings.indexKeys);
    }

    @Override
    public void run() {
        while (!context.isStopped()) {
            boolean failed = false;
            context.lockForOperation();
            try {
                runRandomOperation();
                context.operationSucceeded();
            } catch (Exception e) {
                failed = true;
                context.operationFailed(e);
            } catch (AssertionError e) {
                context.fail(e);
            } finally {
                context.unlockAfterOperation();
            }
            if (failed) {
                sleepSeconds(1);
            }
        }
    }

    private void runRandomOperation() {
        IMap<Integer, byte[]> map = context.mainMap();
        IMap<Integer, Long> indexMap = context.indexMap();
        int choice = random.nextInt(100);
        if (choice < 28) {
            put(map);
        } else if (choice < 42) {
            get(map);
        } else if (choice < 52) {
            remove(map);
        } else if (choice < 59) {
            putIfAbsent(map);
        } else if (choice < 67) {
            putAll(map);
        } else if (choice < 79) {
            putIndexed(indexMap);
        } else if (choice < 85) {
            removeIndexed(indexMap);
        } else if (choice < 91) {
            getIndexed(indexMap);
        } else if (choice < 97) {
            getAll(map, ownMainKeys(BATCH_KEYS));
        } else {
            burst(map);
        }
    }

    private int ownMainKey() {
        int threads = context.settings.threads;
        return random.nextInt(context.settings.keys / threads) * threads + id;
    }

    private int ownIndexKey() {
        int threads = context.settings.threads;
        return random.nextInt(context.settings.indexKeys / threads) * threads + id;
    }

    private Set<Integer> ownMainKeys(int count) {
        Set<Integer> keys = new LinkedHashSet<>();
        while (keys.size() < count) {
            keys.add(ownMainKey());
        }
        return keys;
    }

    private byte[] newValue(int key) {
        return Values.create(key, random, context.settings.maxValueExponent);
    }

    private void put(IMap<Integer, byte[]> map) {
        int key = ownMainKey();
        putValue(map, key, newValue(key));
    }

    private void putValue(IMap<Integer, byte[]> map, int key, byte[] value) {
        long before = context.disruptions();
        try {
            map.put(key, value);
        } catch (RuntimeException e) {
            mainModel.writeMayHaveHappened(key, value);
            throw e;
        }
        mainModel.confirmWrite(key, value, before);
        mainModel.check(key, map.get(key), context.disruptions());
    }

    /**
     * Writes many values of one size class, and then removes them all. Blocks of one size that were allocated together
     * and are freed together can merge with their neighbours, and the merged block can merge again. This is the way
     * the test reaches long chains of merges.
     */
    private void burst(IMap<Integer, byte[]> map) {
        int exponent = Values.randomExponent(random, context.settings.maxValueExponent);
        int wanted = BURST_MIN_KEYS + random.nextInt(BURST_KEY_RANGE);
        int count = Math.min(wanted, Math.max(BURST_FEWEST_KEYS, BURST_BYTES >> exponent));
        count = Math.min(count, context.settings.keys / context.settings.threads);
        List<Integer> keys = new ArrayList<>(ownMainKeys(count));
        for (int key : keys) {
            putValue(map, key, Values.createInClass(key, random, exponent));
        }
        // Free in the order of writing, in reverse, or shuffled.
        int order = random.nextInt(3);
        if (order == 1) {
            Collections.reverse(keys);
        } else if (order == 2) {
            Collections.shuffle(keys, random);
        }
        for (int key : keys) {
            removeKey(map, key);
        }
    }

    private void get(IMap<Integer, byte[]> map) {
        int key = ownMainKey();
        mainModel.check(key, map.get(key), context.disruptions());
    }

    private void remove(IMap<Integer, byte[]> map) {
        removeKey(map, ownMainKey());
    }

    private void removeKey(IMap<Integer, byte[]> map, int key) {
        long before = context.disruptions();
        byte[] previous;
        try {
            previous = map.remove(key);
        } catch (RuntimeException e) {
            mainModel.writeMayHaveHappened(key, null);
            throw e;
        }
        mainModel.check(key, previous, context.disruptions());
        mainModel.confirmWrite(key, null, before);
        mainModel.check(key, map.get(key), context.disruptions());
    }

    private void putIfAbsent(IMap<Integer, byte[]> map) {
        int key = ownMainKey();
        byte[] value = newValue(key);
        long before = context.disruptions();
        byte[] previous;
        try {
            previous = map.putIfAbsent(key, value);
        } catch (RuntimeException e) {
            mainModel.writeMayHaveHappened(key, value);
            throw e;
        }
        mainModel.check(key, previous, context.disruptions());
        if (previous == null) {
            mainModel.confirmWrite(key, value, before);
        }
        mainModel.check(key, map.get(key), context.disruptions());
    }

    private void putAll(IMap<Integer, byte[]> map) {
        Map<Integer, byte[]> batch = new HashMap<>();
        for (int key : ownMainKeys(BATCH_KEYS)) {
            batch.put(key, newValue(key));
        }
        long before = context.disruptions();
        try {
            map.putAll(batch);
        } catch (RuntimeException e) {
            batch.forEach(mainModel::writeMayHaveHappened);
            throw e;
        }
        batch.forEach((key, value) -> mainModel.confirmWrite(key, value, before));
        getAll(map, batch.keySet());
    }

    private void getAll(IMap<Integer, byte[]> map, Set<Integer> keys) {
        Map<Integer, byte[]> found = map.getAll(keys);
        for (int key : keys) {
            mainModel.check(key, found.get(key), context.disruptions());
        }
    }

    private void putIndexed(IMap<Integer, Long> indexMap) {
        int key = ownIndexKey();
        Long value = (long) random.nextInt(Values.INDEX_VALUES);
        long before = context.disruptions();
        try {
            indexMap.put(key, value);
        } catch (RuntimeException e) {
            indexModel.writeMayHaveHappened(key, value);
            throw e;
        }
        indexModel.confirmWrite(key, value, before);
        indexModel.check(key, indexMap.get(key), context.disruptions());
    }

    private void removeIndexed(IMap<Integer, Long> indexMap) {
        int key = ownIndexKey();
        long before = context.disruptions();
        try {
            indexMap.remove(key);
        } catch (RuntimeException e) {
            indexModel.writeMayHaveHappened(key, null);
            throw e;
        }
        indexModel.confirmWrite(key, null, before);
        indexModel.check(key, indexMap.get(key), context.disruptions());
    }

    private void getIndexed(IMap<Integer, Long> indexMap) {
        int key = ownIndexKey();
        indexModel.check(key, indexMap.get(key), context.disruptions());
    }
}
