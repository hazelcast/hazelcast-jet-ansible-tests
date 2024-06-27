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

package com.hazelcast.jet.tests.sort;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.map.IMap;
import java.util.Iterator;

import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static com.hazelcast.query.Predicates.alwaysTrue;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public class SortTest extends AbstractJetSoakTest {

    private static final String SOURCE_MAP_NAME = "sortSourceMap";
    private static final String SINK_LIST_NAME = "sortSinkList";

    private static final int DEFAULT_ITEM_COUNT = 10_000;
    private static final int LOG_JOB_COUNT_THRESHOLD = 100;
    private static final int DEFAULT_SLEEP_AFTER_TEST_SECONDS = 5;

    private IMap<Integer, String> sourceMap;
    private IList<String> sinkList;
    private List<String> expectedList;
    private int itemCount;
    private int sleepAfterTestSeconds;

    public static void main(String[] args) throws Exception {
        new SortTest().run(args);
    }

    @Override
    protected void init(HazelcastInstance client) throws Exception {
        sourceMap = client.getMap(SOURCE_MAP_NAME);
        sinkList = client.getList(SINK_LIST_NAME);

        sourceMap.clear();
        sinkList.clear();

        itemCount = propertyInt("itemCount", DEFAULT_ITEM_COUNT);
        sleepAfterTestSeconds = propertyInt("sleepAfterTestSeconds", DEFAULT_SLEEP_AFTER_TEST_SECONDS);

        range(0, itemCount)
                .mapToObj(i
                        -> sourceMap.putAsync(i, UuidUtil.newUnsecureUuidString().substring(0, 4))
                                .toCompletableFuture())
                .collect(toList())
                .forEach(f -> uncheckCall(f::get));
        expectedList = sourceMap.values().stream().sorted().collect(toList());
    }

    @Override
    protected void test(HazelcastInstance client, String name) {
        long begin = System.currentTimeMillis();
        long jobCount = 0;
        while (System.currentTimeMillis() - begin < durationInMillis) {
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("SortTest" + jobCount);
            client.getJet().newJob(pipeline(), jobConfig).join();

            verifyListSize();
            verifyListContent();

            sinkList.clear();
            if (jobCount % LOG_JOB_COUNT_THRESHOLD == 0) {
                logger.info("Job count: " + jobCount);
            }
            jobCount++;
            sleepSeconds(sleepAfterTestSeconds);
        }
        logger.info("Final job count: " + jobCount);
    }

    @Override
    protected void teardown(Throwable t) {
    }

    private Pipeline pipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.map(sourceMap, alwaysTrue(), Map.Entry::getValue))
                .sort(String::compareTo)
                .writeTo(Sinks.list(sinkList));
        return p;
    }

    private void verifyListSize() {
        try {
            assertEquals(itemCount, sinkList.size());
        } catch (Throwable ex) {
            logger.info("Printing content of sink list:");
            for (int i = 0; i < sinkList.size(); i++) {
                logger.info(sinkList.get(i));
            }
            throw ex;
        }
    }

    private void verifyListContent() {
        try {
            Iterator<String> sinkIterator = sinkList.iterator();
            int i = 0;
            while (sinkIterator.hasNext()) {
                assertEquals(expectedList.get(i), sinkIterator.next());
                i++;
            }
        } catch (Throwable ex) {
            for (int i = 0; i < itemCount; i++) {
                logger.info(expectedList.get(i) + " = " + sinkList.get(i));
            }
            throw ex;
        }
    }
}
