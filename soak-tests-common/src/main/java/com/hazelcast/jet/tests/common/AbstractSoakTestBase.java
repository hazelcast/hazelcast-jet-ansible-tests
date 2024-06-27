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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.YamlClientConfigBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.logging.ILogger;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.concurrent.TimeUnit.MINUTES;

public abstract class AbstractSoakTestBase {

    public static final String STABLE_CLUSTER = ClusterType.STABLE.getPrettyName();
    public static final String DYNAMIC_CLUSTER = ClusterType.DYNAMIC.getPrettyName();

    protected static final double WAIT_TIMEOUT_FACTOR = 1.1;
    protected static final int DELAY_BETWEEN_INIT_AND_TEST_SECONDS = 15;
    private static final int DEFAULT_DURATION_MINUTES = 30;

    protected transient ClientConfig stableClusterClientConfig;
    protected long durationInMillis;

    protected abstract void run(String[] args) throws Exception;

    protected abstract void teardown(Throwable t) throws Exception;

    protected String property(String name, String defaultValue) {
        return System.getProperty(name, defaultValue);
    }

    protected long durationInMillis() {
        return MINUTES.toMillis(propertyInt("durationInMinutes", DEFAULT_DURATION_MINUTES));
    }

    protected Config localClusterConfig() {
        Config config = new Config();
        config.setJetConfig(new JetConfig()
                .setEnabled(true)
                .setResourceUploadEnabled(true));
        return config;
    }

    protected ClientConfig remoteClusterClientConfig() throws IOException {
        if (isRunLocal()) {
            return new ClientConfig();
        }
        String remoteClusterYaml = property("remoteClusterYaml", null);
        if (remoteClusterYaml == null) {
            throw new IllegalArgumentException("Remote cluster yaml should be set, use -DremoteClusterYaml to specify it");
        }

        return new YamlClientConfigBuilder(remoteClusterYaml).build();
    }

    protected int propertyInt(String name, int defaultValue) {
        String value = System.getProperty(name);
        if (value != null) {
            return Integer.parseInt(value);
        }
        return defaultValue;
    }

    protected boolean propertyBoolean(String name, boolean defaultValue) {
        String value = System.getProperty(name);
        if (value != null) {
            return Boolean.parseBoolean(value);
        }
        return defaultValue;
    }

    protected void handleErrorAndShutdownJvmWithErrorStatus(ILogger logger, Throwable t, String msg) throws Exception {
        t.printStackTrace();
        logger.severe(t);
        teardown(t);
        logger.info(msg);
        System.exit(1);
    }

    protected void handleErrorAndShutdownJvmWithErrorStatus(Logger logger, Throwable t, String msg) throws Exception {
        t.printStackTrace();
        logger.log(Level.SEVERE, "Exception", t);
        teardown(t);
        logger.info(msg);
        System.exit(1);
    }

    protected static boolean isRunLocal() {
        return System.getProperty("runLocal") != null;
    }

    protected static void setRunLocal() {
        System.setProperty("runLocal", "true");
    }

    protected static void assertEquals(int expected, int actual) {
        assertEquals("expected: " + expected + ", actual: " + actual, expected, actual);
    }

    protected static void assertEquals(String message, int expected, int actual) {
        if (expected != actual) {
            throw new AssertionError(message);
        }
    }

    protected static void assertEquals(long expected, long actual) {
        assertEquals("expected: " + expected + ", actual: " + actual, expected, actual);
    }

    protected static void assertEquals(String message, long expected, long actual) {
        if (expected != actual) {
            throw new AssertionError(message);
        }
    }

    protected static void assertEquals(Object expected, Object actual) {
        assertEquals("expected: " + expected + ", actual: " + actual, expected, actual);
    }

    protected static void assertEquals(String message, Object expected, Object actual) {
        if (!expected.equals(actual)) {
            throw new AssertionError(message);
        }
    }

    protected static void assertNotEquals(Object expected, Object actual) {
        assertNotEquals("not expected: " + expected + ", actual: " + actual, expected, actual);
    }

    protected static void assertNotEquals(String message, Object expected, Object actual) {
        if (expected.equals(actual)) {
            throw new AssertionError(message);
        }
    }

    protected static void assertTrue(boolean actual) {
        assertTrue("expected: true, actual: " + actual, actual);
    }

    protected static void assertTrue(String message, boolean actual) {
        if (!actual) {
            throw new AssertionError(message);
        }
    }

    protected static void assertFalse(boolean actual) {
        assertFalse("expected: false, actual: " + actual, actual);
    }

    protected static void assertFalse(String message, boolean actual) {
        if (actual) {
            throw new AssertionError(message);
        }
    }

    protected static void assertNotNull(Object actual) {
        assertNotNull("expected: not null, actual: null", actual);
    }

    protected static void assertNotNull(String message, Object actual) {
        if (actual == null) {
            throw new AssertionError(message);
        }
    }

    protected static void assertNull(Object actual) {
        assertNull(String.format("expected: null, actual: %s", actual), actual);
    }

    protected static void assertNull(String message, Object actual) {
        if (actual != null) {
            throw new AssertionError(message);
        }
    }
}
