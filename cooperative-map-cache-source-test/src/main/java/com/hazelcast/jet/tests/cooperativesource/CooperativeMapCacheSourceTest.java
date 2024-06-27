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

package com.hazelcast.jet.tests.cooperativesource;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.Predicates;

import javax.cache.Cache;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class CooperativeMapCacheSourceTest extends AbstractJetSoakTest {

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

    private static final int PAUSE_BETWEEN_JOBS = 2_000;

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
    public void init(HazelcastInstance client) {
        threadCount = propertyInt("cooperative_map_cache_thread_count", DEFAULT_THREAD_COUNT);
        localMapSequence = new int[threadCount];
        remoteMapSequence = new int[threadCount];
        queryLocalMapSequence = new int[threadCount];
        queryRemoteMapSequence = new int[threadCount];
        localCacheSequence = new int[threadCount];
        remoteCacheSequence = new int[threadCount];

        initializeSourceMap(client);
        initializeSourceCache(client);
    }

    @Override
    public void test(HazelcastInstance client, String name) throws Exception {
        List<ExecutorService> executorServices = new ArrayList<>();
        executorServices.add(runTestInExecutorService(
                threadIndex -> executeLocalMapJob(client, threadIndex),
                threadIndex -> verifyLocalMapJob(client, threadIndex),
                localMapSequence
        ));
        executorServices.add(runTestInExecutorService(
                threadIndex -> executeRemoteMapJob(client, threadIndex),
                threadIndex -> verifyRemoteMapJob(client, threadIndex),
                remoteMapSequence
        ));
        executorServices.add(runTestInExecutorService(
                threadIndex -> executeQueryLocalMapJob(client, threadIndex),
                threadIndex -> verifyQueryLocalMapJob(client, threadIndex),
                queryLocalMapSequence
        ));
        executorServices.add(runTestInExecutorService(
                threadIndex -> executeQueryRemoteMapJob(client, threadIndex),
                threadIndex -> verifyQueryRemoteMapJob(client, threadIndex),
                queryRemoteMapSequence
        ));
        executorServices.add(runTestInExecutorService(
                threadIndex -> executeLocalCacheJob(client, threadIndex),
                threadIndex -> verifyLocalCacheJob(client, threadIndex),
                localCacheSequence
        ));
        executorServices.add(runTestInExecutorService(
                threadIndex -> executeRemoteCacheJob(client, threadIndex),
                threadIndex -> verifyRemoteCacheJob(client, threadIndex),
                remoteCacheSequence
        ));

        awaitExecutorServiceTermination(executorServices);

        if (exception != null) {
            throw exception;
        }
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
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
                        sleepMillis(PAUSE_BETWEEN_JOBS);
                    } catch (Throwable e) {
                        exception = new Exception(e);
                    }
                }
            });
        }
        executorService.shutdown();
        return executorService;
    }

    private void executeLocalMapJob(HazelcastInstance client, int threadIndex) {
        Pipeline pipeline = Pipeline.create();
        int sequence = localMapSequence[threadIndex];
        pipeline.readFrom(Sources.map(SOURCE_MAP))
                .map((t) -> {
                    t.setValue(t.getValue() + "_" + sequence);
                    return t;
                })
                .writeTo(Sinks.map(SINK_LOCAL_MAP + threadIndex));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Cooperative Source - Local Map [thread " + threadIndex + "]");
        client.getJet().newJob(pipeline, jobConfig).join();
    }

    private void executeRemoteMapJob(HazelcastInstance client, int threadIndex) {
        Pipeline pipeline = Pipeline.create();
        int sequence = remoteMapSequence[threadIndex];
        pipeline.readFrom(Sources.remoteMap(SOURCE_MAP, wrappedRemoteClusterClientConfig()))
                .map((t) -> {
                    t.setValue(t.getValue() + "_" + sequence);
                    return t;
                })
                .writeTo(Sinks.map(SINK_REMOTE_MAP + threadIndex));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Cooperative Source - Remote Map [thread " + threadIndex + "]");
        client.getJet().newJob(pipeline, jobConfig).join();
    }

    private void executeQueryLocalMapJob(HazelcastInstance client, int threadIndex) {
        Pipeline pipeline = Pipeline.create();
        BatchSource<Map.Entry<Integer, String>> source
                = Sources.<Map.Entry<Integer, String>, Integer, String>map(
                SOURCE_MAP,
                Predicates.greaterEqual("__key", PREDICATE_FROM),
                Projections.identity());
        int sequence = queryLocalMapSequence[threadIndex];
        pipeline.readFrom(source)
                .map((t) -> {
                    t.setValue(t.getValue() + "_" + sequence);
                    return t;
                })
                .writeTo(Sinks.map(SINK_QUERY_LOCAL_MAP + threadIndex));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Cooperative Source - Query Local Map [thread " + threadIndex + "]");
        client.getJet().newJob(pipeline, jobConfig).join();
    }

    private void executeQueryRemoteMapJob(HazelcastInstance client, int threadIndex) {
        Pipeline pipeline = Pipeline.create();
        BatchSource<Map.Entry<Integer, String>> source
                = Sources.<Map.Entry<Integer, String>, Integer, String>remoteMap(
                SOURCE_MAP,
                wrappedRemoteClusterClientConfig(),
                Predicates.greaterEqual("__key", PREDICATE_FROM),
                Projections.identity());
        int sequence = queryRemoteMapSequence[threadIndex];
        pipeline.readFrom(source)
                .map((t) -> {
                    t.setValue(t.getValue() + "_" + sequence);
                    return t;
                })
                .writeTo(Sinks.map(SINK_QUERY_REMOTE_MAP + threadIndex));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Cooperative Source - Query Remote Map [thread " + threadIndex + "]");
        client.getJet().newJob(pipeline, jobConfig).join();
    }

    private void executeLocalCacheJob(HazelcastInstance client, int threadIndex) {
        Pipeline pipeline = Pipeline.create();
        int sequence = localCacheSequence[threadIndex];
        pipeline.readFrom(Sources.cache(SOURCE_CACHE))
                .map((t) -> {
                    t.setValue(t.getValue() + "_" + sequence);
                    return t;
                })
                .writeTo(Sinks.map(SINK_LOCAL_CACHE + threadIndex));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Cooperative Source - Local Cache [thread " + threadIndex + "]");
        client.getJet().newJob(pipeline, jobConfig).join();
    }

    private void executeRemoteCacheJob(HazelcastInstance client, int threadIndex) {
        Pipeline pipeline = Pipeline.create();
        int sequence = remoteCacheSequence[threadIndex];
        pipeline.readFrom(Sources.remoteCache(SOURCE_CACHE, wrappedRemoteClusterClientConfig()))
                .map((t) -> {
                    t.setValue(t.getValue() + "_" + sequence);
                    return t;
                })
                .writeTo(Sinks.map(SINK_REMOTE_CACHE + threadIndex));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Cooperative Source - Remote Cache [thread " + threadIndex + "]");
        client.getJet().newJob(pipeline, jobConfig).join();
    }

    private void verifyLocalMapJob(HazelcastInstance client, int threadIndex) {
        Map<Integer, String> map = client.getMap(SINK_LOCAL_MAP + threadIndex);
        assertEquals(SOURCE_MAP_ITEMS, map.size());
        String value = map.get(SOURCE_MAP_LAST_KEY);
        String expectedValue = SOURCE_MAP_LAST_KEY + "_" + localMapSequence[threadIndex];
        assertEquals(expectedValue, value);
        map.clear();
    }

    private void verifyRemoteMapJob(HazelcastInstance client, int threadIndex) {
        Map<Integer, String> map = client.getMap(SINK_REMOTE_MAP + threadIndex);
        assertEquals(SOURCE_MAP_ITEMS, map.size());
        String value = map.get(SOURCE_MAP_LAST_KEY);
        String expectedValue = SOURCE_MAP_LAST_KEY + "_" + remoteMapSequence[threadIndex];
        assertEquals(expectedValue, value);
        map.clear();
    }

    private void verifyQueryLocalMapJob(HazelcastInstance client, int threadIndex) {
        Map<Integer, String> map = client.getMap(SINK_QUERY_LOCAL_MAP + threadIndex);
        assertEquals(EXPECTED_SIZE_AFTER_PREDICATE, map.size());
        String value = map.get(SOURCE_MAP_LAST_KEY);
        String expectedValue = SOURCE_MAP_LAST_KEY + "_" + queryLocalMapSequence[threadIndex];
        assertEquals(expectedValue, value);
        map.clear();
    }

    private void verifyQueryRemoteMapJob(HazelcastInstance client, int threadIndex) {
        Map<Integer, String> map = client.getMap(SINK_QUERY_REMOTE_MAP + threadIndex);
        assertEquals(EXPECTED_SIZE_AFTER_PREDICATE, map.size());
        String value = map.get(SOURCE_MAP_LAST_KEY);
        String expectedValue = SOURCE_MAP_LAST_KEY + "_" + queryRemoteMapSequence[threadIndex];
        assertEquals(expectedValue, value);
        map.clear();
    }

    private void verifyLocalCacheJob(HazelcastInstance client, int threadIndex) {
        Map<Integer, String> map = client.getMap(SINK_LOCAL_CACHE + threadIndex);
        assertEquals(SOURCE_CACHE_ITEMS, map.size());
        String value = map.get(SOURCE_CACHE_LAST_KEY);
        String expectedValue = SOURCE_CACHE_LAST_KEY + "_" + localCacheSequence[threadIndex];
        assertEquals(expectedValue, value);
        map.clear();
    }

    private void verifyRemoteCacheJob(HazelcastInstance client, int threadIndex) {
        Map<Integer, String> map = client.getMap(SINK_REMOTE_CACHE + threadIndex);
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

    private void initializeSourceMap(HazelcastInstance client) {
        Map<Integer, String> map = client.getMap(SOURCE_MAP);
        for (int i = 0; i < SOURCE_MAP_ITEMS; i++) {
            map.put(i, Integer.toString(i));
        }
    }

    private ClientConfig wrappedRemoteClusterClientConfig() {
        return Util.uncheckCall(this::remoteClusterClientConfig);
    }

    private void initializeSourceCache(HazelcastInstance client) {
        Cache<Integer, String> cache = client.getCacheManager().getCache(SOURCE_CACHE);
        logger.info("Populating cache " + cache.getName() + "...");
        Map<Integer, String> tmpMap = new HashMap<>();
        for (int i = 0; i < SOURCE_MAP_ITEMS; i++) {
            tmpMap.put(i, Integer.toString(i));
        }
        cache.putAll(tmpMap);
        logger.info("Cache " + cache.getName() + " populated.");
    }

}
