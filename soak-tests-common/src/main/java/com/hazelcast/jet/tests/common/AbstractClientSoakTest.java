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
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.YamlClientConfigBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.hazelcast.jet.tests.common.ClusterType.DYNAMIC;
import static com.hazelcast.jet.tests.common.ClusterType.STABLE;
import static com.hazelcast.jet.tests.common.Util.parseArguments;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * This class allows to create test which will be run from a client perspective side
 */
public abstract class AbstractClientSoakTest extends AbstractSoakTestBase {

    protected transient HazelcastInstance stableClusterClient;
    protected transient HazelcastInstance dynamicClusterClient;
    protected transient ClientConfig dynamicClusterClientConfig;
    protected transient Logger logger;

    /**
     * Implementation of this method is invoked before test for each of cluster, similar to @Before from JUnit
     */
    protected abstract void beforeTest(HazelcastInstance client, ClusterType clusterType) throws Exception;

    /**
     * Implementation of this method is invoked before test, similar to @BeforeClass from JUnit
     */
    protected abstract void init() throws Exception;

    /**
     * Implementation of this method should be a test
     */
    protected abstract void test(HazelcastInstance client, ClusterType clusterType, String name) throws Throwable;

    /**
     * Implementation of this method configures for which clusters test will be run
     */
    protected abstract List<ClusterType> runOnClusters();

    /**
     * It can be overridden to provide special configuration for client during local run of test
     */
    protected ClientConfig localClientConfig() {
        return new ClientConfig();
    }

    @Override
    protected final void run(String[] args) throws Exception {
        parseArguments(args);

        HazelcastInstance[] instances = null;
        if (isRunLocal()) {
            Config config = localClusterConfig();

            instances = new HazelcastInstance[]{
                    Hazelcast.newHazelcastInstance(config), Hazelcast.newHazelcastInstance(config)};
        }

        logger = getLogger(getClass());

        durationInMillis = durationInMillis();

        logger.info("Initializing ");
        try {
            init();
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
        if (stableClusterClient != null) {
            stableClusterClient.shutdown();
        }
        if (dynamicClusterClient != null) {
            dynamicClusterClient.shutdown();
        }

        if (instances != null) {
            Hazelcast.shutdownAll();
        }
        logger.info("Finished OK");
        System.exit(0);
    }

    protected ClientConfig stableClusterClientConfig() throws IOException {
        if (isRunLocal()) {
            return localClientConfig();
        }
        String remoteClusterYaml = property("remoteStableClusterYaml", null);
        if (remoteClusterYaml == null) {
            throw new IllegalArgumentException("Remote cluster yaml should be set, " +
                    "use -DremoteStableClusterYaml to specify it");
        }

        return new YamlClientConfigBuilder(remoteClusterYaml).build();
    }

    protected ClientConfig dynamicClusterClientConfig() throws IOException {
        if (isRunLocal()) {
            return localClientConfig();
        }
        String remoteClusterYaml = property("remoteDynamicClusterYaml", null);
        if (remoteClusterYaml == null) {
            throw new IllegalArgumentException("Remote cluster yaml should be set, " +
                    "use -DremoteDynamicClusterYaml to specify it");
        }

        return new YamlClientConfigBuilder(remoteClusterYaml).build();
    }

    protected void severeLog(String log, Throwable t) {
        logger.log(Level.SEVERE, log, t);
    }

    private void testInternal() throws Throwable {
        Map<ClusterType, Throwable> exceptions = new HashMap<>();
        ExecutorService executorService = Executors.newFixedThreadPool(runOnClusters().size());

        if (runOnClusters().contains(DYNAMIC)) {
            String dynamicName = DYNAMIC_CLUSTER + "-" + getClass().getSimpleName();
            dynamicClusterClientConfig = dynamicClusterClientConfig();
            dynamicClusterClient = HazelcastClient.newHazelcastClient(dynamicClusterClientConfig);

            initialize(dynamicClusterClient, DYNAMIC);

            executorService.execute(() -> {
                try {
                    logger.info("Starting " + dynamicName);
                    test(dynamicClusterClient, DYNAMIC, dynamicName);
                } catch (Throwable t) {
                    severeLog("Exception in " + dynamicName, t);
                    exceptions.put(DYNAMIC, t);
                }
            });
        }

        if (runOnClusters().contains(STABLE)) {
            String testName = STABLE_CLUSTER + "-" + getClass().getSimpleName();
            stableClusterClientConfig = stableClusterClientConfig();
            stableClusterClient = HazelcastClient.newHazelcastClient(stableClusterClientConfig);

            initialize(stableClusterClient, STABLE);

            executorService.execute(() -> {
                try {
                    test(stableClusterClient, STABLE, testName);
                } catch (Throwable t) {
                    severeLog("Exception in " + testName, t);
                    exceptions.put(STABLE, t);
                }
            });
        }

        executorService.shutdown();
        executorService.awaitTermination((long) (durationInMillis * WAIT_TIMEOUT_FACTOR), MILLISECONDS);

        if (!exceptions.isEmpty()) {
            exceptions.forEach((clusterType, throwable) -> severeLog("Exception in " + clusterType, throwable));
            //throw the first exception
            throw exceptions.values().stream().findFirst().orElseThrow();
        }
    }

    private void initialize(HazelcastInstance client, ClusterType clusterType) throws Exception {
        logger.info("Evaluate beforeTest... on " + clusterType.getPrettyName() + " cluster");
        try {
            beforeTest(client, clusterType);
            sleepSeconds(DELAY_BETWEEN_INIT_AND_TEST_SECONDS);
        } catch (Throwable t) {
            handleErrorAndShutdownJvmWithErrorStatus(logger, t, "Finished with failure at beforeTest");
        }
    }

    private Logger getLogger(Class<? extends AbstractClientSoakTest> aClass) {
        return Logger.getLogger(aClass.getName());
    }

}
