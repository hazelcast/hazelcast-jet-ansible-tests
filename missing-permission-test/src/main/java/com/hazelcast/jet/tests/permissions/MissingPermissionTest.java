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

package com.hazelcast.jet.tests.permissions;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.map.IMap;
import java.security.AccessControlException;

import static com.hazelcast.jet.tests.common.Util.sleepMillis;

public class MissingPermissionTest extends AbstractJetSoakTest {

    private static final String SOURCE_MAP_NAME = "missingMapPermissionSourceMap";
    private static final String SINK_MAP_NAME = "missingMapPermissionSinkMap";

    private static final int DEFAULT_ITEM_COUNT = 1_000;
    private static final int LOG_JOB_COUNT_THRESHOLD = 200;
    private static final int DEFAULT_SLEEP_AFTER_TEST_MS = 2_000;
    private static final boolean DEFAULT_RUN_ON_SECURED_CLUSTER = false;

    private IMap<Integer, Integer> sourceMap;
    private IMap<Integer, Integer> sinkMap;
    private int itemCount;
    private int sleepAfterTestMs;
    private boolean runOnSecuredCluster;

    public static void main(String[] args) throws Exception {
        new MissingPermissionTest().run(args);
    }

    @Override
    protected void init(HazelcastInstance client) throws Exception {
        sourceMap = client.getMap(SOURCE_MAP_NAME);
        sinkMap = client.getMap(SINK_MAP_NAME);

        sourceMap.clear();
        sinkMap.clear();

        itemCount = propertyInt("itemCount", DEFAULT_ITEM_COUNT);
        sleepAfterTestMs = propertyInt("sleepAfterTestMs", DEFAULT_SLEEP_AFTER_TEST_MS);
        runOnSecuredCluster = propertyBoolean("runOnSecuredCluster", DEFAULT_RUN_ON_SECURED_CLUSTER);

        for (int i = 0; i < itemCount; i++) {
            sourceMap.put(i, i);
        }
    }

    @Override
    protected void test(HazelcastInstance client, String name) {
        if (!runOnSecuredCluster) {
            logger.info("MissingPermissionTest wasn't executed because it can run only on secured cluster!!!");
            return;
        }

        long begin = System.currentTimeMillis();
        long jobCount = 0;
        while (System.currentTimeMillis() - begin < durationInMillis) {
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("MissingPermissionTest" + jobCount);
            try {
                client.getJet().newJob(pipeline(), jobConfig);
                throw new AssertionError("Job " + jobCount + " is expected to throw AccessControlException");
            } catch (AccessControlException ex) {
                // expected
                assertTrue("Permission should contain name of the Map but was: " + ex.getMessage(),
                        ex.getMessage().contains(SOURCE_MAP_NAME));
            }

            assertTrue(sinkMap.isEmpty());

            if (jobCount % LOG_JOB_COUNT_THRESHOLD == 0) {
                logger.info("Job count: " + jobCount);
            }
            jobCount++;
            sleepMillis(sleepAfterTestMs);
        }
        logger.info("Final job count: " + jobCount);
    }

    @Override
    protected void teardown(Throwable t) {
    }

    private Pipeline pipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.map(SOURCE_MAP_NAME))
                .writeTo(Sinks.map(SINK_MAP_NAME));
        return p;
    }

}
