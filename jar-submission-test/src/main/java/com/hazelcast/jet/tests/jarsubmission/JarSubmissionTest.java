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

package com.hazelcast.jet.tests.jarsubmission;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SubmitJobParameters;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.map.IMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class JarSubmissionTest extends AbstractSoakTest {

    private static final int SMALL_JAR_LOG_JOB_COUNT_THRESHOLD = 100;
    private static final int LARGE_JAR_LOG_JOB_COUNT_THRESHOLD = 50;

    private static final int ASSERTION_ATTEMPS = 1200;
    private static final int ASSERTION_SLEEP_MS = 100;

    private static final String SMALL_JAR_PATH_DEFAULT = "/tmp/jar-submission-job.jar";
    private static final String LARGE_JAR_PATH_DEFAULT = "/tmp/jar-submission-job-large.jar";

    private static final int PAUSE_BETWEEN_SMALL_JAR_JOBS = 1_000;
    private static final int PAUSE_BETWEEN_LARGE_JAR_JOBS = 3_000;
    private static final int DELAY_AFTER_TEST_FINISHED = 120_000;

    private String smallJarPath;
    private String largeJarPath;
    private int pauseBetweenSmallJarJobs;
    private int pauseBetweenLargeJarJobs;

    public static void main(String[] args) throws Exception {
        new JarSubmissionTest().run(args);
    }

    @Override
    protected void init(HazelcastInstance client) throws Exception {
        smallJarPath = property("smallJarPath", SMALL_JAR_PATH_DEFAULT);
        largeJarPath = property("largeJarPath", LARGE_JAR_PATH_DEFAULT);
        pauseBetweenSmallJarJobs = propertyInt("pauseBetweenSmallJarJobs", PAUSE_BETWEEN_SMALL_JAR_JOBS);
        pauseBetweenLargeJarJobs = propertyInt("pauseBetweenLargeJarJobs", PAUSE_BETWEEN_LARGE_JAR_JOBS);
    }

    @Override
    protected void test(HazelcastInstance client, String name) throws Throwable {
        Throwable[] exceptions = new Throwable[4];
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        executorService.execute(() -> {
            try {
                testBatchSmallJar(client);
            } catch (Throwable t) {
                logger.severe("Exception in small batch jar.", t);
                exceptions[0] = t;
            }
        });
        executorService.execute(() -> {
            try {
                testBatchLargeJar(client);
            } catch (Throwable t) {
                logger.severe("Exception in large batch jar.", t);
                exceptions[1] = t;
            }
        });
        executorService.execute(() -> {
            try {
                testStreamSmallJar(client);
            } catch (Throwable t) {
                logger.severe("Exception in small stream jar.", t);
                exceptions[2] = t;
            }
        });
        executorService.execute(() -> {
            try {
                testStreamLargeJar(client);
            } catch (Throwable t) {
                logger.severe("Exception in small stream jar.", t);
                exceptions[3] = t;
            }
        });
        executorService.shutdown();
        executorService.awaitTermination((long) (durationInMillis + DELAY_AFTER_TEST_FINISHED), MILLISECONDS);

        if (exceptions[0] != null) {
            logger.severe("Exception in small batch jar.", exceptions[0]);
        }
        if (exceptions[1] != null) {
            logger.severe("Exception in large batch jar.", exceptions[1]);
        }
        if (exceptions[2] != null) {
            logger.severe("Exception in small stream jar.", exceptions[2]);
        }
        if (exceptions[3] != null) {
            logger.severe("Exception in large stream jar.", exceptions[3]);
        }

        for (Throwable exception : exceptions) {
            if (exception != null) {
                throw exception;
            }
        }
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
    }

    protected void testBatchSmallJar(HazelcastInstance client) throws IOException {
        testBatchJar(client, smallJarPath, "JarSubmissionTestBatchSmall", pauseBetweenSmallJarJobs,
                SMALL_JAR_LOG_JOB_COUNT_THRESHOLD);
    }

    protected void testBatchLargeJar(HazelcastInstance client) throws IOException {
        testBatchJar(client, largeJarPath, "JarSubmissionTestBatchLarge", pauseBetweenLargeJarJobs,
                LARGE_JAR_LOG_JOB_COUNT_THRESHOLD);
    }

    protected void testStreamSmallJar(HazelcastInstance client) throws IOException {
        testStreamJar(client, smallJarPath, "JarSubmissionTestStreamSmall", pauseBetweenSmallJarJobs,
                SMALL_JAR_LOG_JOB_COUNT_THRESHOLD);
    }

    protected void testStreamLargeJar(HazelcastInstance client) throws IOException {
        testStreamJar(client, largeJarPath, "JarSubmissionTestStreamLarge", pauseBetweenLargeJarJobs,
                LARGE_JAR_LOG_JOB_COUNT_THRESHOLD);
    }

    protected void testBatchJar(HazelcastInstance client, String jarPath, String prefix, int sleep, int logThreshold) {
        Path jarFile = Paths.get(jarPath);
        assertTrue("testing file for " + prefix + " does not exist", Files.exists(jarFile));

        JetService jet = client.getJet();

        long begin = System.currentTimeMillis();
        long jobCount = 0;
        while (System.currentTimeMillis() - begin < durationInMillis) {
            ArrayList<String> params = new ArrayList<>();
            params.add(Long.toString(jobCount));
            params.add(prefix);
            // true means batch job
            params.add("true");
            SubmitJobParameters jobParams = new SubmitJobParameters()
                    .setJobParameters(params)
                    .setJobName(prefix + jobCount)
                    .setJarPath(jarFile);
            // set non-existing main class for 10% of jobs
            if (jobCount % 10 == 5) {
                jobParams.setMainClass("not.existing.Main");
            }

            try {
                jet.submitJobFromJar(jobParams);
            } catch (Exception ex) {
                if (jobCount % 10 == 5) {
                    // expected
                } else {
                    throw ex;
                }
            }

            if (jobCount % 10 != 5) {
                Job job = getJobEventually(jet, prefix + jobCount);
                Exception jobException = null;
                try {
                    job.join();
                } catch (Exception ex) {
                    jobException = ex;
                }

                // check result
                IMap<String, String> map = client.getMap(prefix);
                if (jobCount % 10 == 0) {
                    assertNotNull(jobException);
                    assertNotEquals(prefix + jobCount, map.get(prefix));
                } else {
                    assertEquals(prefix + jobCount, map.get(prefix));
                }
            }

            if (jobCount % logThreshold == 0) {
                logger.info("Job count for " + prefix + ": " + jobCount);
            }
            jobCount++;
            sleepMillis(sleep);
        }
        assertTrue(jobCount > 0);
        logger.info("Job count for " + prefix + ": " + jobCount);
    }

    protected void testStreamJar(HazelcastInstance client, String jarPath, String prefix, int sleep, int logThreshold) {
        Path jarFile = Paths.get(jarPath);
        assertTrue("testing file for " + prefix + " does not exist", Files.exists(jarFile));

        JetService jet = client.getJet();

        long begin = System.currentTimeMillis();
        long jobCount = 0;
        while (System.currentTimeMillis() - begin < durationInMillis) {
            ArrayList<String> params = new ArrayList<>();
            params.add(Long.toString(jobCount));
            params.add(prefix);
            // false means stream job
            params.add("false");
            SubmitJobParameters jobParams = new SubmitJobParameters()
                    .setJobParameters(params)
                    .setJobName(prefix + jobCount)
                    .setJarPath(jarFile);
            // set non-existing main class for 10% of jobs
            if (jobCount % 10 == 5) {
                jobParams.setMainClass("not.existing.Main");
            }

            try {
                jet.submitJobFromJar(jobParams);
            } catch (Exception ex) {
                if (jobCount % 10 == 5) {
                    // expected
                } else {
                    throw ex;
                }
            }

            if (jobCount % 10 != 5) {
                Job job = getJobEventually(jet, prefix + jobCount);

                // check result
                IMap<String, String> map = client.getMap(prefix);
                if (jobCount % 10 == 0) {
                    assertJobStatusEventually(job, JobStatus.FAILED);
                    assertNotEquals(prefix + jobCount, map.get(prefix));
                } else {
                    assertJobStatusEventually(job, JobStatus.RUNNING);
                    assertMapItemEventually(map, prefix, prefix + jobCount);
                    job.cancel();
                }
            }

            if (jobCount % logThreshold == 0) {
                logger.info("Job count for " + prefix + ": " + jobCount);
            }
            jobCount++;
            sleepMillis(sleep);
        }
        assertTrue(jobCount > 0);
        logger.info("Job count for " + prefix + ": " + jobCount);
    }

    private static Job getJobEventually(JetService jet, String jobName) {
        for (int i = 0; i < ASSERTION_ATTEMPS; i++) {
            Job job = jet.getJob(jobName);
            if (job != null) {
                return job;
            } else {
                sleepMillis(ASSERTION_SLEEP_MS);
            }
        }
        throw new AssertionError("Job " + jobName + " was not found.");
    }

    private static void assertMapItemEventually(IMap<String, String> map, String key, String expectedValue) {
        for (int i = 0; i < ASSERTION_ATTEMPS; i++) {
            String value = map.get(key);
            if (value != null && value.equals(expectedValue)) {
                return;
            } else {
                sleepMillis(ASSERTION_SLEEP_MS);
            }
        }
        throw new AssertionError("Map " + map.getName() + " does not contain expected value for key: " + key
                + ". Expected value: " + expectedValue + " actual value: " + map.get(key));
    }

    private static void assertJobStatusEventually(Job job, JobStatus expectedStatus) {
        for (int i = 0; i < ASSERTION_ATTEMPS; i++) {
            if (job.getStatus().equals(expectedStatus)) {
                return;
            } else {
                sleepMillis(ASSERTION_SLEEP_MS);
            }
        }
        throw new AssertionError("Job " + job.getName() + " does not have expected status: " + expectedStatus
                + ". Job status: " + job.getStatus());
    }

}
