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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hazelcast.jet.tests.common.Util.parseArguments;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * This class allows to create test which will be run from a server perspective side
 */
public abstract class AbstractJetSoakTest extends AbstractSoakTestBase {

    private static final int CACHE_EVICTION_SIZE = 2000000;

    protected transient HazelcastInstance stableClusterClient;
    protected transient ILogger logger;

    private transient HazelcastInstance hz;

    /**
     * Implementation of this method is invoked before test, similar to @BeforeClass from JUnit
     */
    protected abstract void init(HazelcastInstance client) throws Exception;

    /**
     * Implementation of this method is invoked before test for each of cluster, similar to @Before from JUnit
     */
    protected abstract void test(HazelcastInstance client, String name) throws Throwable;

    /**
     * If {@code true} then {@link #test(HazelcastInstance, String)} method will be
     * called with the dynamic cluster client (which should be the bootstrapped
     * instance) and stable cluster client (which needs a `remoteClusterYaml`
     * defined).
     */
    protected boolean runOnBothClusters() {
        return false;
    }

    @Override
    protected final void run(String[] args) throws Exception {
        parseArguments(args);

        HazelcastInstance[] instances = null;
        if (isRunLocal()) {
            Config config = localClusterConfig();
            CacheSimpleConfig cacheConfig = new CacheSimpleConfig()
                    .setName("CooperativeMapCacheSourceTest_SourceCache");
            cacheConfig.getEvictionConfig().setSize(CACHE_EVICTION_SIZE);
            config.addCacheConfig(cacheConfig);

            instances = new HazelcastInstance[]{
                    Hazelcast.newHazelcastInstance(config), Hazelcast.newHazelcastInstance(config)};
            hz = HazelcastClient.newHazelcastClient();
        } else {
            hz = Hazelcast.bootstrappedInstance();
        }
        logger = getLogger(getClass());

        logger.info("Initializing...");
        try {
            durationInMillis = durationInMillis();
            init(hz);
            sleepSeconds(DELAY_BETWEEN_INIT_AND_TEST_SECONDS);
        } catch (Throwable t) {
            handleErrorAndShutdownJvmWithErrorStatus(logger, t, "Finished with failure at init");
        }
        logger.info("Running...");
        try {
            testInternal();
        } catch (Throwable t) {
            handleErrorAndShutdownJvmWithErrorStatus(logger, t, "Finished with failure at test");
        }
        logger.info("Teardown...");
        teardown(null);
        if (hz != null) {
            hz.shutdown();
        }
        if (stableClusterClient != null) {
            stableClusterClient.shutdown();
        }
        if (instances != null) {
            Hazelcast.shutdownAll();
        }
        logger.info("Finished OK");
        System.exit(0);
    }

    protected boolean runOnlyAsClient() {
        return false;
    }

    private void testInternal() throws Throwable {
        if (!runOnBothClusters() && !runOnlyAsClient()) {
            test(hz, getClass().getSimpleName());
            return;
        }

        stableClusterClientConfig = remoteClusterClientConfig();
        stableClusterClient = HazelcastClient.newHazelcastClient(stableClusterClientConfig);

        if (runOnlyAsClient()) {
            test(stableClusterClient, getClass().getSimpleName());
            return;
        }

        Throwable[] exceptions = new Throwable[2];
        String dynamicName = DYNAMIC_CLUSTER + "-" + getClass().getSimpleName();
        String stableName = STABLE_CLUSTER + "-" + getClass().getSimpleName();
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(() -> {
            try {
                test(hz, dynamicName);
            } catch (Throwable t) {
                logger.severe("Exception in " + dynamicName, t);
                exceptions[0] = t;
            }
        });
        executorService.execute(() -> {
            try {
                test(stableClusterClient, stableName);
            } catch (Throwable t) {
                logger.severe("Exception in " + stableName, t);
                exceptions[1] = t;
            }
        });
        executorService.shutdown();
        executorService.awaitTermination((long) (durationInMillis * WAIT_TIMEOUT_FACTOR), MILLISECONDS);

        if (exceptions[0] != null) {
            logger.severe("Exception in " + dynamicName, exceptions[0]);
        }
        if (exceptions[1] != null) {
            logger.severe("Exception in " + stableName, exceptions[1]);
        }
        if (exceptions[0] != null) {
            throw exceptions[0];
        }
        if (exceptions[1] != null) {
            throw exceptions[1];
        }
    }

    protected ILogger getLogger(Class clazz) {
        return hz.getLoggingService().getLogger(clazz);
    }

    protected static ILogger getLogger(HazelcastInstance instance, Class clazz) {
        return instance.getLoggingService().getLogger(clazz);
    }

}
