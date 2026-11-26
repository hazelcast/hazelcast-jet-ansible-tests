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

import java.util.Objects;

/**
 * What a worker knows about its keys: the last value it wrote successfully, the value of a write that failed but may
 * still have been applied, and the disruption counter at the time the key was last confirmed.
 */
final class KeyModel<T> {

    private final String mapName;
    private final Object[] expected;
    private final Object[] pending;
    private final boolean[] writeUnsure;
    private final long[] confirmedAt;

    KeyModel(String mapName, int size) {
        this.mapName = mapName;
        this.expected = new Object[size];
        this.pending = new Object[size];
        this.writeUnsure = new boolean[size];
        this.confirmedAt = new long[size];
    }

    /** A write succeeded. A null value means the key was removed. */
    void confirmWrite(int key, T value, long disruptionsBeforeWrite) {
        expected[key] = value;
        pending[key] = null;
        writeUnsure[key] = false;
        confirmedAt[key] = disruptionsBeforeWrite;
    }

    /** A write failed, so the member may or may not have applied it. */
    void writeMayHaveHappened(int key, T value) {
        pending[key] = value;
        writeUnsure[key] = true;
    }

    /** Throws an AssertionError when the value read from the map is not allowed for the key. */
    void check(int key, T actual, long disruptionsNow) {
        if (actual == null) {
            checkMissing(key, disruptionsNow);
            return;
        }
        checkContent(key, actual);
        if (!isAllowed(key, actual)) {
            throw new AssertionError(mapName + ": key " + key + " holds " + Values.describe(actual)
                    + ", expected " + Values.describe(expected[key])
                    + (writeUnsure[key] ? " or " + Values.describe(pending[key]) : ""));
        }
    }

    /** Finds a damaged value first, so that the error says what is wrong with it. */
    private void checkContent(int key, Object actual) {
        if (actual instanceof byte[]) {
            String problem = Values.findProblem(key, (byte[]) actual);
            if (problem != null) {
                throw new AssertionError(mapName + ": key " + key + " holds a damaged value ("
                        + Values.describe(actual) + "): " + problem);
            }
        }
    }

    private void checkMissing(int key, long disruptionsNow) {
        boolean restartPossible = confirmedAt[key] != disruptionsNow;
        if (expected[key] != null && !writeUnsure[key] && !restartPossible) {
            throw new AssertionError(mapName + ": key " + key + " disappeared, expected "
                    + Values.describe(expected[key]));
        }
        if (restartPossible) {
            // A restart may have wiped the key, so from now on the model expects it to be missing.
            expected[key] = null;
            confirmedAt[key] = disruptionsNow;
        }
    }

    private boolean isAllowed(int key, Object actual) {
        return Objects.deepEquals(actual, expected[key])
                || (writeUnsure[key] && Objects.deepEquals(actual, pending[key]));
    }
}
