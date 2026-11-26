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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;

import java.util.Random;

import static com.hazelcast.jet.tests.common.Util.sleepSeconds;

public class HDAllocatorTest extends AbstractJetSoakTest {

    private static final int FINAL_VERIFICATION_ATTEMPTS = 16;
    private static final int WORKER_JOIN_TIMEOUT_MILLIS = 60_000;

    private Settings settings;

    public static void main(String[] args) throws Exception {
        new HDAllocatorTest().run(args);
    }

    @Override
    protected void init(HazelcastInstance hz) {
        settings = Settings.read(this::propertyInt);
        clearMaps(hz);
    }

    @Override
    protected void test(HazelcastInstance hz, String name) throws Throwable {
        long seed = System.nanoTime();
        Context context = new Context(hz, name, settings, logger);
        logger.info(name + ": seed=" + seed + " threads=" + settings.threads + " keys=" + settings.keys
                + " verifyIntervalMs=" + settings.verifyIntervalMillis);
        hz.getLifecycleService().addLifecycleListener(event -> {
            LifecycleState state = event.getState();
            if (state == LifecycleState.CLIENT_CONNECTED || state == LifecycleState.CLIENT_DISCONNECTED) {
                context.countDisruption();
            }
        });

        Worker[] workers = new Worker[settings.threads];
        Thread[] threads = new Thread[settings.threads];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker(context, i, new Random(seed + i));
            threads[i] = new Thread(workers[i], "hd-allocator-worker-" + i);
            threads[i].start();
        }
        Verifier verifier = new Verifier(context, workers);

        int completeSweeps = runUntilDone(context, verifier);

        context.stop();
        for (Thread thread : threads) {
            thread.join(WORKER_JOIN_TIMEOUT_MILLIS);
            if (thread.isAlive()) {
                throw new AssertionError(thread.getName() + " did not finish");
            }
        }
        logger.info(name + ": finished, operations=" + context.operations() + " failedOperations="
                + context.failedOperations() + " completeSweeps=" + completeSweeps
                + " disruptions=" + context.disruptions());
        if (context.hasFailed()) {
            throw new AssertionError("seed=" + seed + ": " + context.firstFailure(), context.firstFailure());
        }

        if (!verifier.verify(FINAL_VERIFICATION_ATTEMPTS)) {
            throw new IllegalStateException(name + ": the final verification could not complete");
        }
        clearMaps(hz);
        assertEquals("main map not empty after clear", 0, hz.getMap(Context.MAIN_MAP).size());
        assertEquals("index map not empty after clear", 0, hz.getMap(Context.INDEX_MAP).size());
    }

    @Override
    protected void teardown(Throwable t) {
        // Nothing to release. The workers are stopped at the end of test().
    }

    /** Waits for the test duration, starts a verification at every interval, and returns the complete sweeps. */
    private int runUntilDone(Context context, Verifier verifier) throws InterruptedException {
        long begin = System.currentTimeMillis();
        long nextVerification = begin + settings.verifyIntervalMillis;
        int completeSweeps = 0;
        while (System.currentTimeMillis() - begin < durationInMillis && !context.hasFailed()) {
            context.checkOutage();
            if (System.currentTimeMillis() < nextVerification) {
                sleepSeconds(1);
                continue;
            }
            if (verifier.verifyWithWorkersPaused()) {
                completeSweeps++;
            }
            nextVerification = System.currentTimeMillis() + settings.verifyIntervalMillis;
        }
        return completeSweeps;
    }

    private static void clearMaps(HazelcastInstance hz) {
        hz.getMap(Context.MAIN_MAP).clear();
        hz.getMap(Context.INDEX_MAP).clear();
    }
}
