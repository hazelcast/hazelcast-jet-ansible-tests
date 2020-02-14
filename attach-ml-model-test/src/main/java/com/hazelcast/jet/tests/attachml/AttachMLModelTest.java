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

package com.hazelcast.jet.tests.attachml;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static com.hazelcast.jet.core.processor.Processors.noopP;
import static com.hazelcast.jet.pipeline.Sinks.fromProcessor;

public class AttachMLModelTest extends AbstractSoakTest {

    private static final int LOG_JOB_COUNT_THRESHOLD = 100;
    private static final int EXPECTED_LINES_IN_MODEL = 434337;
    private static final String EXPECTED_FIRST_LINE_IN_MODEL = "2016-01-01T00:00,15,47";
    private static final String MODEL = "model";

    private static final String ML_DATA_PATH_DEFAULT = "/home/ec2-user/ansible/15-minute-counts-sorted.csv";

    private String mlDataPath;

    public static void main(String[] args) throws Exception {
        new AttachMLModelTest().run(args);
    }

    @Override
    protected void init() throws Exception {
        mlDataPath = property("mlDataPath", ML_DATA_PATH_DEFAULT);
    }

    @Override
    protected void test() throws IOException {
        Path mlData = Paths.get(mlDataPath);
        assertTrue("testing ML data does not exists", Files.exists(mlData));
        long count = Files.lines(mlData).count();
        assertEquals(EXPECTED_LINES_IN_MODEL, count);
        String firstLine = Files.lines(mlData).findFirst().get();
        assertEquals(EXPECTED_FIRST_LINE_IN_MODEL, firstLine);

        long begin = System.currentTimeMillis();
        long jobCount = 0;
        while (System.currentTimeMillis() - begin < durationInMillis) {
            JobConfig jobConfig = new JobConfig();
            jobConfig.attachFile(mlDataPath, MODEL + jobCount);
            jet.newJob(pipeline(jobCount), jobConfig).join();
            if (jobCount % LOG_JOB_COUNT_THRESHOLD == 0) {
                logger.info("Job count: " + jobCount);
            }
            jobCount++;
        }
        assertTrue(jobCount > 0);
        logger.info("Final job count: " + jobCount);
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
    }

    private Pipeline pipeline(long i) {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(1))
                .mapUsingService(
                        ServiceFactories.sharedService(context -> context.attachedFile(MODEL + i)),
                        (file, integer) -> {
                            Path mlData = file.toPath();
                            assertTrue("testing ML data does not exists", Files.exists(mlData));
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

}
