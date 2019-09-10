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

package com.hazelcast.jet.tests.cooperativesource;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.GenericPredicates;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.projection.Projections;

import javax.cache.Cache;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class CooperativeMapCacheSourceTest extends AbstractSoakTest {

    private static final String TEST_PREFIX = CooperativeMapCacheSourceTest.class.getSimpleName();
    private static final String SOURCE_MAP = TEST_PREFIX + "_SourceMap";
    private static final String SINK_LOCAL_MAP = TEST_PREFIX + "_SinkLocalMap";
    private static final String SINK_REMOTE_MAP = TEST_PREFIX + "_SinkRemoteMap";
    private static final String SINK_LOCAL_CACHE = TEST_PREFIX + "_SinkLocalCache";
    private static final String SINK_REMOTE_CACHE = TEST_PREFIX + "_SinkRemoteCache";
    private static final String SINK_QUERY_LOCAL_MAP = TEST_PREFIX + "_SinkQueryLocalMap";
    private static final String SINK_QUERY_REMOTE_MAP = TEST_PREFIX + "_SinkQueryRemoteMap";
    private static final String SOURCE_CACHE = TEST_PREFIX + "_SourceCache";
    private static final int SOURCE_MAP_ITEMS = 100_000;
    private static final int SOURCE_MAP_LAST_KEY = SOURCE_MAP_ITEMS - 1;
    private static final int SOURCE_CACHE_ITEMS = 100_000;
    private static final int SOURCE_CACHE_LAST_KEY = SOURCE_CACHE_ITEMS - 1;
    private static final int PREDICATE_FROM = 50_000;
    private static final int EXPECTED_SIZE_AFTER_PREDICATE = SOURCE_MAP_ITEMS - PREDICATE_FROM;

    private static final int DEFAULT_THREAD_COUNT = 1;

    private int threadCount;
    /**
     * I deliberately use only one instance of Exception instead of something like Exception per source type.
     * <br/>
     * We will stop all tests when any exception occurs.
     */
    private volatile Exception exception;

    private int[] localMapSequence;
    private int[] remoteMapSequence;
    private int[] queryLocalMapSequence;
    private int[] queryRemoteMapSequence;
    private int[] localCacheSequence;
    private int[] remoteCacheSequence;

    public static void main(String[] args) throws Exception {
        new CooperativeMapCacheSourceTest().run(args);
    }

    @Override
    public void init() {
        threadCount = propertyInt("cooperative_map_cache_thread_count", DEFAULT_THREAD_COUNT);
        localMapSequence = new int[threadCount];
        remoteMapSequence = new int[threadCount];
        queryLocalMapSequence = new int[threadCount];
        queryRemoteMapSequence = new int[threadCount];
        localCacheSequence = new int[threadCount];
        remoteCacheSequence = new int[threadCount];

        initializeSourceMap();
        initializeSourceCache();
    }
    @Override
    public void test() throws Exception {
        List<ExecutorService> executorServices = new ArrayList<>();
        executorServices.add(runTestInExecutorService(
                threadIndex -> executeLocalMapJob(threadIndex),
                threadIndex -> verifyLocalMapJob(threadIndex),
                localMapSequence
        ));
        executorServices.add(runTestInExecutorService(
                threadIndex -> executeRemoteMapJob(threadIndex),
                threadIndex -> verifyRemoteMapJob(threadIndex),
                remoteMapSequence
        ));
        executorServices.add(runTestInExecutorService(
                threadIndex -> executeQueryLocalMapJob(threadIndex),
                threadIndex -> verifyQueryLocalMapJob(threadIndex),
                queryLocalMapSequence
        ));
        executorServices.add(runTestInExecutorService(
                threadIndex -> executeQueryRemoteMapJob(threadIndex),
                threadIndex -> verifyQueryRemoteMapJob(threadIndex),
                queryRemoteMapSequence
        ));
        executorServices.add(runTestInExecutorService(
                threadIndex -> executeLocalCacheJob(threadIndex),
                threadIndex -> verifyLocalCacheJob(threadIndex),
                localCacheSequence
        ));
        executorServices.add(runTestInExecutorService(
                threadIndex -> executeRemoteCacheJob(threadIndex),
                threadIndex -> verifyRemoteCacheJob(threadIndex),
                remoteCacheSequence
        ));

        awaitExecutorServiceTermination(executorServices);

        if (exception != null) {
            throw exception;
        }
    }

    @Override
    public void teardown() {
    }

    private ExecutorService runTestInExecutorService(Consumer<Integer> executeJob, Consumer<Integer> verify,
            int[] sequenceArray) {
        long begin = System.currentTimeMillis();
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            final int threadIndex = i;
            executorService.submit(() -> {
                while ((System.currentTimeMillis() - begin) < durationInMillis && exception == null) {
                    try {
                        executeJob.accept(threadIndex);
                        verify.accept(threadIndex);
                        sequenceArray[threadIndex]++;
                    } catch (Throwable e) {
                        exception = new Exception(e);
                    }
                }
            });
        }
        executorService.shutdown();
        return executorService;
    }

    private void executeLocalMapJob(int threadIndex) {
        Pipeline pipeline = Pipeline.create();
        int sequence = localMapSequence[threadIndex];
        pipeline.drawFrom(Sources.map(SOURCE_MAP))
                .map((t) -> {
                    t.setValue(t.getValue() + "_" + sequence);
                    return t;
                })
                .drainTo(Sinks.map(SINK_LOCAL_MAP + threadIndex));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Cooperative Source - Local Map [thread " + threadIndex + "]");
        jet.newJob(pipeline, jobConfig).join();
    }

    private void executeRemoteMapJob(int threadIndex) {
        Pipeline pipeline = Pipeline.create();
        int sequence = remoteMapSequence[threadIndex];
        pipeline.drawFrom(Sources.remoteMap(SOURCE_MAP, wrappedRemoteClusterClientConfig()))
                .map((t) -> {
                    t.setValue(t.getValue() + "_" + sequence);
                    return t;
                })
                .drainTo(Sinks.map(SINK_REMOTE_MAP + threadIndex));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Cooperative Source - Remote Map [thread " + threadIndex + "]");
        jet.newJob(pipeline, jobConfig).join();
    }

    private void executeQueryLocalMapJob(int threadIndex) {
        Pipeline pipeline = Pipeline.create();
        BatchSource<Map.Entry<Integer, String>> source
                = Sources.<Map.Entry<Integer, String>, Integer, String>map(
                        SOURCE_MAP,
                        GenericPredicates.greaterEqual("__key", PREDICATE_FROM),
                        Projections.identity());
        int sequence = queryLocalMapSequence[threadIndex];
        pipeline.drawFrom(source)
                .map((t) -> {
                    t.setValue(t.getValue() + "_" + sequence);
                    return t;
                })
                .drainTo(Sinks.map(SINK_QUERY_LOCAL_MAP + threadIndex));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Cooperative Source - Query Local Map [thread " + threadIndex + "]");
        jet.newJob(pipeline, jobConfig).join();
    }

    private void executeQueryRemoteMapJob(int threadIndex) {
        Pipeline pipeline = Pipeline.create();
        BatchSource<Map.Entry<Integer, String>> source
                = Sources.<Map.Entry<Integer, String>, Integer, String>remoteMap(
                        SOURCE_MAP,
                        wrappedRemoteClusterClientConfig(),
                        GenericPredicates.greaterEqual("__key", PREDICATE_FROM),
                        Projections.identity());
        int sequence = queryRemoteMapSequence[threadIndex];
        pipeline.drawFrom(source)
                .map((t) -> {
                    t.setValue(t.getValue() + "_" + sequence);
                    return t;
                })
                .drainTo(Sinks.map(SINK_QUERY_REMOTE_MAP + threadIndex));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Cooperative Source - Query Remote Map [thread " + threadIndex + "]");
        jet.newJob(pipeline, jobConfig).join();
    }

    private void executeLocalCacheJob(int threadIndex) {
        Pipeline pipeline = Pipeline.create();
        int sequence = localCacheSequence[threadIndex];
        pipeline.drawFrom(Sources.cache(SOURCE_CACHE))
                .map((t) -> {
                    t.setValue(t.getValue() + "_" + sequence);
                    return t;
                })
                .drainTo(Sinks.map(SINK_LOCAL_CACHE + threadIndex));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Cooperative Source - Local Cache [thread " + threadIndex + "]");
        jet.newJob(pipeline, jobConfig).join();
    }

    private void executeRemoteCacheJob(int threadIndex) {
        Pipeline pipeline = Pipeline.create();
        int sequence = remoteCacheSequence[threadIndex];
        pipeline.drawFrom(Sources.remoteCache(SOURCE_CACHE, wrappedRemoteClusterClientConfig()))
                .map((t) -> {
                    t.setValue(t.getValue() + "_" + sequence);
                    return t;
                })
                .drainTo(Sinks.map(SINK_REMOTE_CACHE + threadIndex));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Cooperative Source - Remote Cache [thread " + threadIndex + "]");
        jet.newJob(pipeline, jobConfig).join();
    }

    private void verifyLocalMapJob(int threadIndex) {
        Map<Integer, String> map = jet.getMap(SINK_LOCAL_MAP + threadIndex);
        assertEquals(SOURCE_MAP_ITEMS, map.size());
        String value = map.get(SOURCE_MAP_LAST_KEY);
        String expectedValue = SOURCE_MAP_LAST_KEY + "_" + localMapSequence[threadIndex];
        assertEquals(expectedValue, value);
        map.clear();
    }

    private void verifyRemoteMapJob(int threadIndex) {
        Map<Integer, String> map = jet.getMap(SINK_REMOTE_MAP + threadIndex);
        assertEquals(SOURCE_MAP_ITEMS, map.size());
        String value = map.get(SOURCE_MAP_LAST_KEY);
        String expectedValue = SOURCE_MAP_LAST_KEY + "_" + remoteMapSequence[threadIndex];
        assertEquals(expectedValue, value);
        map.clear();
    }

    private void verifyQueryLocalMapJob(int threadIndex) {
        Map<Integer, String> map = jet.getMap(SINK_QUERY_LOCAL_MAP + threadIndex);
        assertEquals(EXPECTED_SIZE_AFTER_PREDICATE, map.size());
        String value = map.get(SOURCE_MAP_LAST_KEY);
        String expectedValue = SOURCE_MAP_LAST_KEY + "_" + queryLocalMapSequence[threadIndex];
        assertEquals(expectedValue, value);
        map.clear();
    }

    private void verifyQueryRemoteMapJob(int threadIndex) {
        Map<Integer, String> map = jet.getMap(SINK_QUERY_REMOTE_MAP + threadIndex);
        assertEquals(EXPECTED_SIZE_AFTER_PREDICATE, map.size());
        String value = map.get(SOURCE_MAP_LAST_KEY);
        String expectedValue = SOURCE_MAP_LAST_KEY + "_" + queryRemoteMapSequence[threadIndex];
        assertEquals(expectedValue, value);
        map.clear();
    }

    private void verifyLocalCacheJob(int threadIndex) {
        Map<Integer, String> map = jet.getMap(SINK_LOCAL_CACHE + threadIndex);
        assertEquals(SOURCE_CACHE_ITEMS, map.size());
        String value = map.get(SOURCE_CACHE_LAST_KEY);
        String expectedValue = SOURCE_CACHE_LAST_KEY + "_" + localCacheSequence[threadIndex];
        assertEquals(expectedValue, value);
        map.clear();
    }

    private void verifyRemoteCacheJob(int threadIndex) {
        Map<Integer, String> map = jet.getMap(SINK_REMOTE_CACHE + threadIndex);
        assertEquals(SOURCE_CACHE_ITEMS, map.size());
        String value = map.get(SOURCE_CACHE_LAST_KEY);
        String expectedValue = SOURCE_CACHE_LAST_KEY + "_" + remoteCacheSequence[threadIndex];
        assertEquals(expectedValue, value);
        map.clear();
    }

    private void awaitExecutorServiceTermination(List<ExecutorService> executorServices) throws InterruptedException {
        for (ExecutorService executorService : executorServices) {
            executorService.awaitTermination(durationInMillis + MINUTES.toMillis(1), MILLISECONDS);
        }
    }

    private void initializeSourceMap() {
        Map<Integer, String> map = jet.getMap(SOURCE_MAP);
        for (int i = 0; i < SOURCE_MAP_ITEMS; i++) {
            map.put(i, Integer.toString(i));
        }
    }

    private ClientConfig wrappedRemoteClusterClientConfig() {
        return Util.uncheckCall(() -> remoteClusterClientConfig());
    }

    private void initializeSourceCache() {
        Cache<Integer, String> cache = jet.getCacheManager().getCache(SOURCE_CACHE);
        logger.info("Populating cache " + cache.getName() + "...");
        Map<Integer, String> tmpMap = new HashMap<>();
        for (int i = 0; i < SOURCE_MAP_ITEMS; i++) {
            tmpMap.put(i, Integer.toString(i));
        }
        cache.putAll(tmpMap);
        logger.info("Cache " + cache.getName() + " populated.");
    }

}
