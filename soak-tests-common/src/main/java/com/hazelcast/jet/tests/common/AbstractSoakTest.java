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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetClientConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.EventJournalMapEvent;

import java.io.IOException;

import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.tests.common.Util.parseArguments;
import static java.util.concurrent.TimeUnit.MINUTES;

public abstract class AbstractSoakTest {

    private static final int DEFAULT_DURATION_MINUTES = 30;
    private static final int CACHE_EVICTION_SIZE = 2000000;

    protected transient JetInstance jet;
    protected transient ILogger logger;
    protected long durationInMillis;

    protected final void run(String[] args) throws Exception {
        parseArguments(args);

        JetInstance[] instances = null;
        if (isRunLocal()) {
            JetConfig config = new JetConfig();
            CacheSimpleConfig cacheConfig = new CacheSimpleConfig()
                    .setName("CooperativeMapCacheSourceTest_SourceCache");
            cacheConfig.getEvictionConfig().setSize(CACHE_EVICTION_SIZE);
            config.getHazelcastConfig().addCacheConfig(cacheConfig);

            instances = new JetInstance[]{Jet.newJetInstance(config), Jet.newJetInstance(config)};
            jet = Jet.newJetClient();
        } else {
            jet = Jet.bootstrappedInstance();
        }
        logger = getLogger(getClass());

        logger.info("Initializing...");
        try {
            durationInMillis = durationInMillis();
            init();
        } catch (Throwable t) {
            t.printStackTrace();
            logger.severe(t);
            teardown(t);
            logger.info("Finished with failure at init");
            System.exit(1);
        }
        logger.info("Running...");
        try {
            test();
        } catch (Throwable t) {
            t.printStackTrace();
            logger.severe(t);
            teardown(t);
            logger.info("Finished with failure at test");
            System.exit(1);
        }
        logger.info("Teardown...");
        teardown(null);
        if (jet != null) {
            jet.shutdown();
        }
        if (instances != null) {
            Jet.shutdownAll();
        }
        logger.info("Finished OK");
        System.exit(0);
    }

    protected abstract void init() throws Exception;

    protected abstract void test() throws Throwable;

    protected abstract void teardown(Throwable t) throws Exception;

    protected String property(String name, String defaultValue) {
        return System.getProperty(name, defaultValue);
    }

    protected long durationInMillis() {
        return MINUTES.toMillis(propertyInt("durationInMinutes", DEFAULT_DURATION_MINUTES));
    }

    protected ClientConfig remoteClusterClientConfig() throws IOException {
        if (isRunLocal()) {
            return new JetClientConfig();
        }
        String remoteClusterXml = property("remoteClusterXml", null);
        if (remoteClusterXml == null) {
            throw new IllegalArgumentException("Remote cluster xml should be set, use -DremoteClusterXml to specify it");
        }

        return new XmlClientConfigBuilder(remoteClusterXml).build();
    }

    private static boolean isRunLocal() {
        return System.getProperty("runLocal") != null;
    }

    protected static void setRunLocal() {
        System.setProperty("runLocal", "true");
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

    protected static ILogger getLogger(JetInstance instance, Class clazz) {
        return instance.getHazelcastInstance().getLoggingService().getLogger(clazz);
    }

    protected static PredicateEx<EventJournalMapEvent<Long, Long>> filter(boolean odds) {
        PredicateEx<EventJournalMapEvent<Long, Long>> putEvents = mapPutEvents();
        return e -> putEvents.test(e) && (e.getNewValue() % 2 == (odds ? 1 : 0));
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
}
