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

import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.logging.ILogger;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.CancellationException;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class Util {

    private static final int JOB_STATUS_RETRY_COUNT = 20;

    private Util() {
    }

    public static JobStatus getJobStatusWithRetry(Job job) {
        try {
            return job.getStatus();
        } catch (Exception e) {
            uncheckRun(() -> sleepSeconds(1));
            return getJobStatusWithRetry(job);
        }
    }

    public static void waitForJobStatus(Job job, JobStatus expectedStatus) {
        for (int i = 0; i < JOB_STATUS_RETRY_COUNT; i++) {
            JobStatus currentStatus = getJobStatusWithRetry(job);
            if (currentStatus == FAILED) {
                job.join();
            }
            if (currentStatus.equals(expectedStatus)) {
                return;
            }
            sleepSeconds(1);
        }
        throw new IllegalStateException(String.format("Wait for status[%s] timed out. current status: %s",
                expectedStatus, job.getStatus()));
    }

    public static void cancelJobAndJoin(Job job, ILogger logger) {
        logger.info("STARTING CANCELLATION FOR JOB " + job.getName() + " " + job.getIdString());
        job.cancel();
        try {
            logger.info("TRY TO CALL JOIN FOR JOB " + job.getName() + " " + job.getIdString());
            job.join();
            logger.info("JOIN FINISHED FOR JOB " + job.getName() + " " + job.getIdString());
        } catch (CancellationException ignored) {
        }
    }

    public static <K, V> Map.Entry<K, V> entry(K key, V value) {
        return new AbstractMap.SimpleImmutableEntry<>(key, value);
    }

    public static void sleepMinutes(int minutes) {
        uncheckRun(() -> MINUTES.sleep(minutes));
    }

    public static void sleepSeconds(int seconds) {
        uncheckRun(() -> SECONDS.sleep(seconds));
    }

    public static void sleepMillis(int millis) {
        uncheckRun(() -> MILLISECONDS.sleep(millis));
    }

    public static void parseArguments(String[] args) {
        for (String arg : args) {
            String[] split = arg.split("=");
            if (split.length != 2) {
                throw new IllegalArgumentException(arg);
            }
            System.setProperty(split[0], split[1]);
        }
    }
}
