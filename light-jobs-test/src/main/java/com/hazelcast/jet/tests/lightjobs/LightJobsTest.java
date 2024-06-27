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

package com.hazelcast.jet.tests.lightjobs;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.map.IMap;

import java.util.Map;

import static com.hazelcast.jet.tests.common.Util.sleepMillis;

public class LightJobsTest extends AbstractJetSoakTest {

    private static final String SOURCE_BATCH_MAP_NAME = "lightJobBatchInputMap";
    private static final String SOURCE_JOURNAL_MAP_NAME = "lightJobJournalInputMap";
    private static final String SOURCE_INCORRECT_MAP_NAME = "lightJobIncorrectMap";
    private static final String OUTPUT_BATCH_LIST_PREFIX = "lightJobBatchOutputSinkList";
    private static final String OUTPUT_STREAM_LIST_PREFIX = "lightJobJournalOutputSinkList";
    private static final String OUTPUT_INCORRECT_MAP_NAME = "lightJobIncorrectSinkMap";

    private static final String EXPECTED_FAIL_MESSAGE = "LightJobsTest - incorrectPipeline - expected exception";

    private static final int DEFAULT_ITEM_COUNT = 400;
    private static final int DEFAULT_LOG_JOB_COUNT_THRESHOLD = 100;
    private static final int DEFAULT_SLEEP_BETWEEN_JOBS_MS = 2_000;

    private IMap<String, String> sourceBatchMap;
    private IMap<String, String> sourceJournalMap;
    private IMap<String, String> sourceIncorrectMap;

    private int itemCount;
    private int sleepAfterJobMillis;
    private int logJobCountThreshold;

    public static void main(String[] args) throws Exception {
        new LightJobsTest().run(args);
    }

    @Override
    protected void init(HazelcastInstance client) throws Exception {
        sourceBatchMap = client.getMap(SOURCE_BATCH_MAP_NAME);
        sourceBatchMap.clear();
        sourceJournalMap = client.getMap(SOURCE_JOURNAL_MAP_NAME);
        sourceJournalMap.clear();
        sourceIncorrectMap = client.getMap(SOURCE_INCORRECT_MAP_NAME);
        sourceIncorrectMap.clear();

        itemCount = propertyInt("itemCount", DEFAULT_ITEM_COUNT);
        sleepAfterJobMillis = propertyInt("sleepAfterJobMillis", DEFAULT_SLEEP_BETWEEN_JOBS_MS);
        logJobCountThreshold = propertyInt("logJobCountThreshold", DEFAULT_LOG_JOB_COUNT_THRESHOLD);

        for (int i = 0; i < itemCount; i++) {
            String item = Integer.toString(i);
            sourceBatchMap.put(item, item);
            sourceJournalMap.put(item, item);
        }
        sourceIncorrectMap.put("ignore", "ignore");
    }

    @Override
    protected void test(HazelcastInstance client, String name) {
        long begin = System.currentTimeMillis();
        long jobCount = 0;
        while (System.currentTimeMillis() - begin < durationInMillis) {
            runBatchJob(client, jobCount);
            sleepMillis(sleepAfterJobMillis);
            runStreamJob(client, jobCount);
            sleepMillis(sleepAfterJobMillis);
            runIncorrectJob(client);
            sleepMillis(sleepAfterJobMillis);

            if (jobCount % logJobCountThreshold == 0) {
                logger.info("Job count: " + jobCount);
            }
            jobCount++;
        }
        logger.info("Final job count: " + jobCount);
    }

    private void runBatchJob(HazelcastInstance client, long jobCount) {
        String outputName = OUTPUT_BATCH_LIST_PREFIX + jobCount;
        IList<String> list = client.getList(outputName);
        assertTrue(list.isEmpty());

        client.getJet().newLightJob(batchPipeline(outputName)).join();
        assertEquals(itemCount, list.size());

        list.destroy();
    }

    private void runStreamJob(HazelcastInstance client, long jobCount) {
        String outputName = OUTPUT_STREAM_LIST_PREFIX + jobCount;
        IList<String> list = client.getList(outputName);
        assertTrue(list.isEmpty());

        Job job = client.getJet().newLightJob(streamPipeline(outputName));
        assertListSize(list, 20, 1000);
        job.cancel();

        list.destroy();
    }

    private void runIncorrectJob(HazelcastInstance client) {
        Job job = client.getJet().newLightJob(incorrectPipeline());
        try {
            job.join();
        } catch (RuntimeException ex) {
            assertTrue(ex.getMessage(),
                    ex.getMessage().contains(EXPECTED_FAIL_MESSAGE));
            return;
        }
        throw new AssertionError("Job is expected to fail");
    }

    private Pipeline batchPipeline(String sinkName) {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.map(sourceBatchMap))
                .map(t -> t.getKey())
                .writeTo(Sinks.list(sinkName));
        return p;
    }

    private Pipeline streamPipeline(String sinkName) {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.mapJournal(sourceJournalMap, JournalInitialPosition.START_FROM_OLDEST))
                .withoutTimestamps()
                .map(t -> t.getKey())
                .writeTo(Sinks.list(sinkName));
        return p;
    }

    private Pipeline incorrectPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.map(SOURCE_INCORRECT_MAP_NAME))
                .<Map.Entry<Object, Object>>map(t -> {
                    RuntimeException ex = new RuntimeException(EXPECTED_FAIL_MESSAGE);
                    // set empty stacktrace to decrease log size
                    ex.setStackTrace(new StackTraceElement[0]);
                    throw ex;
                }).setLocalParallelism(1)
                .writeTo(Sinks.map(OUTPUT_INCORRECT_MAP_NAME));
        return p;
    }

    private void assertListSize(IList<String> list, int sleepBetweenAttempts, int attempts) {
        int counter = 0;
        while (counter < attempts) {
            if (list.size() == itemCount) {
                return;
            }
            sleepMillis(sleepBetweenAttempts);
            counter++;
        }
        throw new AssertionError("List size for " + list.getName() + " was not expected. It was: " + list.size());
    }

    @Override
    protected void teardown(Throwable t) {
    }

}
