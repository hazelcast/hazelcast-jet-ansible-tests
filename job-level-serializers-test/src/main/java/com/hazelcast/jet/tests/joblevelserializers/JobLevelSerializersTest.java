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

package com.hazelcast.jet.tests.joblevelserializers;

import com.hazelcast.collection.IList;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.pipeline.ServiceFactories.sharedService;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;

public class JobLevelSerializersTest extends AbstractSoakTest {

    private static final int PAUSE_BETWEEN_JOBS_MS = 1_000;
    private static final int LOG_JOB_COUNT_THRESHOLD = 500;

    private static final int DEFAULT_SOURCE_LIST_SIZE = 1_000;

    private static final String SOURCE_LIST_NAME = "JobLevelSerializersTest_sourceList";
    private static final String SINK_LIST_NAME = "JobLevelSerializersTest_sinkList";

    private static int sourceListSize;

    private List<Integer> expectedList;

    public static void main(String[] args) throws Exception {
        new JobLevelSerializersTest().run(args);
    }

    @Override
    protected void init(JetInstance client) throws Exception {
        sourceListSize = propertyInt("sourceListSize", DEFAULT_SOURCE_LIST_SIZE);
    }

    @Override
    protected void test(JetInstance client, String name) throws Throwable {
        prepareSourceData(client);
        prepareExpectedList();

        long begin = System.currentTimeMillis();
        long jobCount = 0;
        while (System.currentTimeMillis() - begin < durationInMillis) {
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("JobLevelSerializersTest" + jobCount);
            jobConfig.addPackage(JobLevelSerializersTest.class.getPackage().getName());
            jobConfig.registerSerializer(IntValue.class, IntValueSerializer.class);

            client.newJob(pipeline(jobCount), jobConfig).join();

            verifySink(client, jobCount);
            if (jobCount % LOG_JOB_COUNT_THRESHOLD == 0) {
                logger.info("Job count executed: " + jobCount);
            }

            jobCount++;
            sleepMillis(PAUSE_BETWEEN_JOBS_MS);
        }
        assertTrue(jobCount > 0);
        logger.info("Final job count for JobLevelSerializersTest: " + jobCount);
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
    }

    private void prepareSourceData(JetInstance client) {
        List<Integer> list = client.getList(SOURCE_LIST_NAME);
        for (int i = 0; i < sourceListSize; i++) {
            list.add(i);
        }
    }

    private void prepareExpectedList() {
        expectedList = IntStream.range(0, sourceListSize)
                .boxed()
                .collect(Collectors.toList());
    }

    private void verifySink(JetInstance client, long jobCount) {
        IList<Integer> list = client.getList(SINK_LIST_NAME + jobCount);
        List<Integer> copiedList = new ArrayList<>(list);
        Collections.sort(copiedList);
        assertEquals(expectedList, copiedList);
        list.destroy();
    }

    private Pipeline pipeline(long jobCount) {
        Pipeline p = Pipeline.create();
        p.<Integer>readFrom(Sources.list(SOURCE_LIST_NAME))
                .map(t -> new IntValue(t))
                .groupingKey(wholeItem())
                .filterUsingService(sharedService(ctx -> null), (s, k, v) -> true)
                .map(t -> t.getValue())
                .writeTo(Sinks.list(SINK_LIST_NAME + jobCount));
        return p;
    }

}
