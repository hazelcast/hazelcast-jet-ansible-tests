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

import java.util.function.ToIntBiFunction;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/** The test arguments, written as key=value on the command line. */
final class Settings {

    private static final int DEFAULT_THREADS = 6;
    private static final int DEFAULT_KEYS = 5_000;
    private static final int DEFAULT_INDEX_KEYS = 2_000;
    private static final int DEFAULT_MAX_OUTAGE_MINUTES = 10;
    private static final int DEFAULT_VERIFY_INTERVAL_SECONDS = 30;
    private static final int DEFAULT_MAX_VALUE_BYTES = 64 * 1024;

    final int threads;
    final int keys;
    final int indexKeys;
    /** The test fails when no operation succeeds for this long. */
    final long maxOutageMillis;
    final long verifyIntervalMillis;
    /** The largest value is at most 2 to the power of this number bytes. */
    final int maxValueExponent;

    private Settings(ToIntBiFunction<String, Integer> property) {
        threads = property.applyAsInt("hdThreads", DEFAULT_THREADS);
        keys = property.applyAsInt("hdKeys", DEFAULT_KEYS);
        indexKeys = property.applyAsInt("hdIndexKeys", DEFAULT_INDEX_KEYS);
        maxOutageMillis = MINUTES.toMillis(property.applyAsInt("hdMaxOutageMinutes", DEFAULT_MAX_OUTAGE_MINUTES));
        verifyIntervalMillis = SECONDS.toMillis(
                property.applyAsInt("hdVerifyIntervalSeconds", DEFAULT_VERIFY_INTERVAL_SECONDS));
        int maxValueBytes = property.applyAsInt("hdMaxValueBytes", DEFAULT_MAX_VALUE_BYTES);
        maxValueExponent = 31 - Integer.numberOfLeadingZeros(maxValueBytes);
    }

    /** Reads the arguments with the given function, which returns the argument value or the default. */
    static Settings read(ToIntBiFunction<String, Integer> property) {
        Settings settings = new Settings(property);
        if (settings.keys < settings.threads * Worker.BATCH_KEYS || settings.indexKeys < settings.threads) {
            throw new IllegalArgumentException("hdKeys must be at least hdThreads * " + Worker.BATCH_KEYS
                    + " and hdIndexKeys must be at least hdThreads");
        }
        if (settings.maxValueExponent <= Values.MIN_EXPONENT) {
            throw new IllegalArgumentException("hdMaxValueBytes must be at least " + (2 << Values.MIN_EXPONENT));
        }
        return settings;
    }
}
