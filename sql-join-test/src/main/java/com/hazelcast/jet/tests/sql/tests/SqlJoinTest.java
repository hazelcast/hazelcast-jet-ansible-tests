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

package com.hazelcast.jet.tests.sql.tests;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.jet.tests.common.Util;
import com.hazelcast.jet.tests.sql.common.JoinByKeyExecutor;
import com.hazelcast.jet.tests.sql.common.JoinByNonKeyExecutor;

import java.util.concurrent.FutureTask;

public class SqlJoinTest extends AbstractSoakTest {

    public static final int DEFAULT_THRESHOLD = 100;

    private long begin;
    private int threshold;

    public static void main(String[] args) throws Exception {
        new SqlJoinTest().run(args);
    }

    @Override
    protected void init(HazelcastInstance jetInstance) {
        begin = System.currentTimeMillis();
        threshold = propertyInt("generatorBatchCount", DEFAULT_THRESHOLD);
    }

    @Override
    protected void test(HazelcastInstance jetInstance, String s) throws Exception {
        FutureTask<Integer> joinByKeyFuture = new FutureTask<>(
                new JoinByKeyExecutor(jetInstance, logger, begin, durationInMillis, threshold));
        Thread joinByKeyThread = new Thread(joinByKeyFuture);

        FutureTask<Integer> joinByNonKeyFuture = new FutureTask<>(
                new JoinByNonKeyExecutor(jetInstance, logger, begin, durationInMillis, threshold));
        Thread joinByNonKeyThread = new Thread(joinByNonKeyFuture);

        joinByKeyThread.start();
        joinByNonKeyThread.start();

        joinByKeyThread.join();
        joinByNonKeyThread.join();

        int keyQueriesRun = joinByKeyFuture.get();
        int nonKeyQueriesRun = joinByNonKeyFuture.get();

        logger.info(String.format(
                "Test completed successfully. Executed %d JOIN by key and %d JOIN by non key queries in: %s",
                keyQueriesRun, nonKeyQueriesRun, Util.getTimeElapsed(begin)));

        // The following executor uses the BETWEEN function, which is not available in Jet 4.5
        // Please enable it only when testing on Jet 5+
//        FutureTask<Integer> nonEqualJoinFuture = new FutureTask<>(
//                new NonEqualJoinExecutor(jetInstance, logger, begin, durationInMillis, threshold));
//        Thread nonEqualJoinThread = new Thread(nonEqualJoinFuture);
//        nonEqualJoinThread.start();
//        nonEqualJoinThread.join();
    }

    @Override
    protected void teardown(Throwable throwable) throws Exception {
    }
}
