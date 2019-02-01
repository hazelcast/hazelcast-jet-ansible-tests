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

package com.hazelcast.jet.tests.common;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.server.JetBootstrap;
import com.hazelcast.logging.ILogger;

import java.io.Serializable;

import static com.hazelcast.jet.tests.common.Util.parseArguments;
import static java.util.concurrent.TimeUnit.MINUTES;

public abstract class AbstractSoakTest implements Serializable {

    private static final int DEFAULT_DURATION_MINUTES = 30;

    protected transient JetInstance jet;
    protected transient ILogger logger;

    protected final void run(String[] args) throws Exception {
        parseArguments(args);

        jet = JetBootstrap.getInstance();
        logger = getLogger(getClass());

        logger.info("Initializing...");
        try {
            init();
        } catch (Throwable t) {
            logger.severe(t);
            teardown();
            System.exit(1);
        }
        logger.info("Running...");
        try {
            test();
        } catch (Throwable t) {
            logger.severe(t);
            teardown();
            System.exit(1);
        }
        logger.info("Teardown...");
        teardown();
        logger.info("Finished OK");
        System.exit(0);
    }

    protected abstract void init() throws Exception;

    protected abstract void test() throws Exception;

    protected abstract void teardown() throws Exception;

    protected String property(String name, String defaultValue) {
        return System.getProperty(name, defaultValue);
    }

    protected long durationInMillis() {
        return MINUTES.toMillis(propertyInt("durationInMinutes", DEFAULT_DURATION_MINUTES));
    }

    protected int propertyInt(String name, int defaultValue) {
        String value = System.getProperty(name);
        if (value != null) {
            return Integer.parseInt(value);
        }
        return defaultValue;
    }

    protected ILogger getLogger(Class clazz) {
        return jet.getHazelcastInstance().getLoggingService().getLogger(clazz);
    }

    protected void assertEquals(int expected, int actual) {
        assertEquals("expected: " + expected + ", actual: " + actual, expected, actual);
    }

    protected void assertEquals(String message, int expected, int actual) {
        if (expected != actual) {
            throw new AssertionError(message);
        }
    }

    protected void assertEquals(long expected, long actual) {
        assertEquals("expected: " + expected + ", actual: " + actual, expected, actual);
    }

    protected void assertEquals(String message, long expected, long actual) {
        if (expected != actual) {
            throw new AssertionError(message);
        }
    }

    protected void assertEquals(Object expected, Object actual) {
        assertEquals("expected: " + expected + ", actual: " + actual, expected, actual);
    }

    protected void assertEquals(String message, Object expected, Object actual) {
        if (!expected.equals(actual)) {
            throw new AssertionError(message);
        }
    }

    protected void assertNotEquals(Object expected, Object actual) {
        assertNotEquals("expected: " + expected + ", actual: " + actual, expected, actual);
    }

    protected void assertNotEquals(String message, Object expected, Object actual) {
        if (expected.equals(actual)) {
            throw new AssertionError(message);
        }
    }

    protected void assertTrue(boolean actual) {
        assertTrue("expected: true, actual: " + actual, actual);
    }

    protected void assertTrue(String message, boolean actual) {
        if (!actual) {
            throw new AssertionError(message);
        }
    }

    protected void assertFalse(boolean actual) {
        assertFalse("expected: false, actual: " + actual, actual);
    }

    protected void assertFalse(String message, boolean actual) {
        if (actual) {
            throw new AssertionError(message);
        }
    }


}
