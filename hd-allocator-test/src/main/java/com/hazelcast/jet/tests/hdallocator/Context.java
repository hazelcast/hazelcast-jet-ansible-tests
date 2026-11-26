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
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/** Everything the workers and the verifier share: the client, the logger, the counters, the failures and the lock. */
final class Context {

    static final String MAIN_MAP = HDAllocatorTest.class.getSimpleName() + "-main";
    static final String INDEX_MAP = HDAllocatorTest.class.getSimpleName() + "-idx";

    private static final int FAILURE_LOG_INTERVAL_MILLIS = 10_000;

    final HazelcastInstance client;
    final Settings settings;
    final ILogger logger;
    final String testName;

    private final AtomicLong disruptions = new AtomicLong();
    private final AtomicLong lastSuccess = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong operations = new AtomicLong();
    private final AtomicLong failedOperations = new AtomicLong();
    private final AtomicLong lastFailureLog = new AtomicLong();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final Queue<Throwable> failures = new ConcurrentLinkedQueue<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    Context(HazelcastInstance client, String testName, Settings settings, ILogger logger) {
        this.client = client;
        this.testName = testName;
        this.settings = settings;
        this.logger = logger;
    }

    IMap<Integer, byte[]> mainMap() {
        return client.getMap(MAIN_MAP);
    }

    IMap<Integer, Long> indexMap() {
        return client.getMap(INDEX_MAP);
    }

    /**
     * The disruption counter goes up when the client connects or disconnects and when an operation fails. An entry
     * that was confirmed at an earlier value may be gone after a restart.
     */
    long disruptions() {
        return disruptions.get();
    }

    void countDisruption() {
        disruptions.incrementAndGet();
    }

    long operations() {
        return operations.get();
    }

    long failedOperations() {
        return failedOperations.get();
    }

    void operationSucceeded() {
        operations.incrementAndGet();
        lastSuccess.set(System.currentTimeMillis());
    }

    /** Counts the failure, logs it at most once per interval, and stops the test if the client gave up. */
    void operationFailed(Exception e) {
        failedOperations.incrementAndGet();
        countDisruption();
        long now = System.currentTimeMillis();
        long last = lastFailureLog.get();
        if (now - last >= FAILURE_LOG_INTERVAL_MILLIS && lastFailureLog.compareAndSet(last, now)) {
            logger.warning(testName + ": operation failed, retrying: " + e, e);
        }
        if (!client.getLifecycleService().isRunning()) {
            fail(new AssertionError("The client shut itself down, so the cluster was unreachable for longer "
                    + "than the client connection timeout: " + e, e));
        }
    }

    /** Fails the test when no operation has succeeded for too long. */
    void checkOutage() {
        long sinceSuccess = System.currentTimeMillis() - lastSuccess.get();
        if (sinceSuccess > settings.maxOutageMillis) {
            fail(new AssertionError("No operation succeeded for " + MILLISECONDS.toSeconds(sinceSuccess)
                    + " seconds (limit " + MILLISECONDS.toSeconds(settings.maxOutageMillis) + ")"));
        }
    }

    void fail(Throwable failure) {
        failures.add(failure);
        stop();
    }

    boolean hasFailed() {
        return !failures.isEmpty();
    }

    Throwable firstFailure() {
        return failures.peek();
    }

    boolean isStopped() {
        return stopped.get();
    }

    void stop() {
        stopped.set(true);
    }

    void lockForOperation() {
        lock.readLock().lock();
    }

    void unlockAfterOperation() {
        lock.readLock().unlock();
    }

    /** Waits until all workers are between operations. Returns false if the test failed while waiting. */
    boolean pauseWorkers() throws InterruptedException {
        while (!lock.writeLock().tryLock(1, SECONDS)) {
            checkOutage();
            if (hasFailed()) {
                return false;
            }
        }
        return true;
    }

    void resumeWorkers() {
        lock.writeLock().unlock();
    }
}
