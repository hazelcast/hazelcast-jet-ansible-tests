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

import java.util.Random;

final class Values {

    /** The indexed map stores numbers from 0 up to this value (not included). */
    static final int INDEX_VALUES = 500;

    /** The smallest value is the 16 byte header, which is 2 to the power of 4. */
    static final int MIN_EXPONENT = 4;

    private static final int HEADER_SIZE = 16;
    private static final int CHECKSUM_SALT = 0x5bd1e995;

    private Values() {
    }

    /** Creates a value of a random size class, see {@link #createInClass}. */
    static byte[] create(int key, Random random, int maxExponent) {
        return createInClass(key, random, randomExponent(random, maxExponent));
    }

    /** Every size class from the smallest up to the largest is equally likely. */
    static int randomExponent(Random random, int maxExponent) {
        return MIN_EXPONENT + random.nextInt(maxExponent - MIN_EXPONENT + 1);
    }

    /** Creates a value that is longer than 2^(exponent-1) bytes and at most 2^exponent bytes. */
    static byte[] createInClass(int key, Random random, int exponent) {
        int half = 1 << (exponent - 1);
        int length = Math.max(HEADER_SIZE, half + 1 + random.nextInt(half));
        return build(key, random.nextInt(), length);
    }

    static byte[] build(int key, int version, int length) {
        byte[] value = new byte[length];
        putInt(value, 0, key);
        putInt(value, 4, version);
        putInt(value, 8, length);
        putInt(value, 12, key ^ version ^ length ^ CHECKSUM_SALT);
        for (int i = HEADER_SIZE; i < length; i++) {
            value[i] = (byte) (key * 31 + version * 17 + i);
        }
        return value;
    }

    /**
     * Checks that the bytes are a value that {@link #build} could have created for this key.
     *
     * @return null when the value is intact, otherwise a short description of what is wrong
     */
    static String findProblem(int key, byte[] value) {
        if (value.length < HEADER_SIZE) {
            return "only " + value.length + " bytes, the header needs " + HEADER_SIZE;
        }
        int keyInHeader = getInt(value, 0);
        int version = getInt(value, 4);
        int lengthInHeader = getInt(value, 8);
        if (keyInHeader != key) {
            return "the header holds key " + keyInHeader;
        }
        if (lengthInHeader != value.length) {
            return "the header holds length " + lengthInHeader + " but there are " + value.length + " bytes";
        }
        if (getInt(value, 12) != (key ^ version ^ lengthInHeader ^ CHECKSUM_SALT)) {
            return "the header checksum is wrong";
        }
        for (int i = HEADER_SIZE; i < value.length; i++) {
            byte expected = (byte) (key * 31 + version * 17 + i);
            if (value[i] != expected) {
                return "byte " + i + " is " + value[i] + " but should be " + expected;
            }
        }
        return null;
    }

    /** Short text for error messages. */
    static String describe(Object value) {
        if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;
            return "byte[" + bytes.length + "]" + (bytes.length >= 8 ? " version " + getInt(bytes, 4) : "");
        }
        return String.valueOf(value);
    }

    private static void putInt(byte[] bytes, int offset, int value) {
        for (int i = 0; i < 4; i++) {
            bytes[offset + i] = (byte) (value >>> (24 - 8 * i));
        }
    }

    private static int getInt(byte[] bytes, int offset) {
        int result = 0;
        for (int i = 0; i < 4; i++) {
            result = (result << 8) | (bytes[offset + i] & 0xff);
        }
        return result;
    }
}
