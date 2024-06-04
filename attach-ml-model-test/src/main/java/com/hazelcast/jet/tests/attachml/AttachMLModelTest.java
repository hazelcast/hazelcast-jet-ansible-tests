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

package com.hazelcast.jet.tests.attachml;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static com.hazelcast.jet.core.processor.Processors.noopP;
import static com.hazelcast.jet.pipeline.Sinks.fromProcessor;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

public class AttachMLModelTest extends AbstractJetSoakTest {

    private static final int LOG_JOB_COUNT_THRESHOLD = 100;
    private static final int LOG_JOB_COUNT_LARGE_FILE_THRESHOLD = 10;
    private static final int EXPECTED_LINES_IN_MODEL = 434337;
    private static final int EXPECTED_LINES_IN_LARGE_FILE_MODEL = 4343370;
    private static final String EXPECTED_FIRST_LINE_IN_MODEL = "2016-01-01T00:00,15,47";
    private static final String MODEL = "model";
    private static final String MODEL_LARGE = "model-large";

    private static final String ML_DATA_PATH_DEFAULT = "/home/ec2-user/ansible/15-minute-counts-sorted.csv";
    private static final String ML_LARGE_DATA_PATH_DEFAULT = "/home/ec2-user/ansible/ml_100mb";

    private static final int PAUSE_BETWEEN_SMALL_FILE_JOBS = 2_000;
    private static final int PAUSE_BETWEEN_LARGE_FILE_JOBS = 30_000;
    private static final int DELAY_AFTER_TEST_FINISHED = 120_000;

    private String ml10mbDataPath;
    private String ml100mbDataPath;

    public static void main(String[] args) throws Exception {
        new AttachMLModelTest().run(args);
    }

    @Override
    protected void init(HazelcastInstance client) throws Exception {
        ml10mbDataPath = property("ml10mbDataPath", ML_DATA_PATH_DEFAULT);
        ml100mbDataPath = property("ml100mbDataPath", ML_LARGE_DATA_PATH_DEFAULT);
    }

    @Override
    protected void test(HazelcastInstance client, String name) throws Throwable {
        Throwable[] exceptions = new Throwable[2];
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(() -> {
            try {
                test10mbFile(client);
            } catch (Throwable t) {
                logger.severe("Exception in 10MB file test", t);
                exceptions[0] = t;
            }
        });
        executorService.execute(() -> {
            try {
                test100mbDirectory(client);
            } catch (Throwable t) {
                logger.severe("Exception in 100MB directory test", t);
                exceptions[1] = t;
            }
        });
        executorService.shutdown();
        executorService.awaitTermination((long) (durationInMillis + DELAY_AFTER_TEST_FINISHED), MILLISECONDS);

        if (exceptions[0] != null) {
            logger.severe("Exception in 10MB file test", exceptions[0]);
        }
        if (exceptions[1] != null) {
            logger.severe("Exception in 100MB directory test", exceptions[1]);
        }
        if (exceptions[0] != null) {
            throw exceptions[0];
        }
        if (exceptions[1] != null) {
            throw exceptions[1];
        }
    }

    protected void test10mbFile(HazelcastInstance client) throws IOException {
        Path mlData = Paths.get(ml10mbDataPath);
        assertTrue("testing ML 10MB data does not exists", Files.exists(mlData));
        long count = Files.lines(mlData).count();
        assertEquals(EXPECTED_LINES_IN_MODEL, count);
        String firstLine = Files.lines(mlData).findFirst().get();
        assertEquals(EXPECTED_FIRST_LINE_IN_MODEL, firstLine);

        long begin = System.currentTimeMillis();
        long jobCount = 0;
        while (System.currentTimeMillis() - begin < durationInMillis) {
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("AttachMLModelTest_10mbFile" + jobCount);
            jobConfig.attachFile(ml10mbDataPath, MODEL + jobCount);
            client.getJet().newJob(pipeline10mbFile(jobCount), jobConfig).join();
            if (jobCount % LOG_JOB_COUNT_THRESHOLD == 0) {
                logger.info("Job count 10MB: " + jobCount);
            }
            jobCount++;
            sleepMillis(PAUSE_BETWEEN_SMALL_FILE_JOBS);
        }
        assertTrue(jobCount > 0);
        logger.info("Final job count 10MB: " + jobCount);
    }

    protected void test100mbDirectory(HazelcastInstance client) throws IOException {
        Path mlDir = Paths.get(ml100mbDataPath);
        assertTrue("testing ML 100MB data does not exists", Files.exists(mlDir));
        assertTrue(Files.isDirectory(mlDir));
        List<Path> dirContent = Files.list(mlDir).collect(toList());
        assertEquals(1, dirContent.size());
        Path mlData = dirContent.get(0);
        long count = Files.lines(mlData).count();
        assertEquals(EXPECTED_LINES_IN_LARGE_FILE_MODEL, count);
        String firstLine = Files.lines(mlData).findFirst().get();
        assertEquals(EXPECTED_FIRST_LINE_IN_MODEL, firstLine);

        long begin = System.currentTimeMillis();
        long jobCount = 0;
        while (System.currentTimeMillis() - begin < durationInMillis) {
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("AttachMLModelTest_100mbDirectory" + jobCount);
            jobConfig.attachDirectory(ml100mbDataPath, MODEL_LARGE + jobCount);
            client.getJet().newJob(pipeline100mbDirectory(jobCount), jobConfig).join();
            if (jobCount % LOG_JOB_COUNT_LARGE_FILE_THRESHOLD == 0) {
                logger.info("Job count 100MB: " + jobCount);
            }
            jobCount++;
            sleepMillis(PAUSE_BETWEEN_LARGE_FILE_JOBS);
        }
        assertTrue(jobCount > 0);
        logger.info("Final job count 100MB: " + jobCount);
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
    }

    private Pipeline pipeline10mbFile(long i) {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(1))
         .mapUsingService(
                 ServiceFactories.sharedService(context -> context.attachedFile(MODEL + i)),
                 (file, integer) -> {
                     Path mlData = file.toPath();
                     assertTrue("testing ML 10MB data does not exists", Files.exists(mlData));
                     try (Stream<String> lines = Files.lines(mlData)) {
                         assertEquals(EXPECTED_LINES_IN_MODEL, lines.count());
                     }
                     try (Stream<String> lines = Files.lines(mlData)) {
                         assertEquals(EXPECTED_FIRST_LINE_IN_MODEL, lines.findFirst().get());
                     }
                     return true;
                 })
         .writeTo(fromProcessor("noopSink", ProcessorMetaSupplier.of(noopP())));
        return p;
    }

    private Pipeline pipeline100mbDirectory(long i) {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(1))
         .mapUsingService(
                 ServiceFactories.sharedService(context -> context.attachedDirectory(MODEL_LARGE + i)),
                 (dir, integer) -> {
                     Path mlDir = dir.toPath();
                     assertTrue("testing ML 100MB data does not exists", Files.exists(mlDir));
                     assertTrue(Files.isDirectory(mlDir));
                     List<Path> dirContent = Files.list(mlDir).collect(toList());
                     assertEquals(1, dirContent.size());
                     Path mlData = dirContent.get(0);
                     try (Stream<String> lines = Files.lines(mlData)) {
                         assertEquals(EXPECTED_LINES_IN_LARGE_FILE_MODEL, lines.count());
                     }
                     try (Stream<String> lines = Files.lines(mlData)) {
                         assertEquals(EXPECTED_FIRST_LINE_IN_MODEL, lines.findFirst().get());
                     }
                     return true;
                 })
         .writeTo(fromProcessor("noopSink", ProcessorMetaSupplier.of(noopP())));
        return p;
    }

}
