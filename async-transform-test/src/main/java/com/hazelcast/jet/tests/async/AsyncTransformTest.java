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

package com.hazelcast.jet.tests.async;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamStageWithKey;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.jet.tests.common.BasicEventJournalProducer;
import com.hazelcast.jet.tests.eventjournal.EventJournalConsumer;
import com.hazelcast.logging.ILogger;

import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.LongStream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.Util.toCompletableFuture;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.tests.common.Util.getJobStatus;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static java.util.stream.Collectors.toMap;

public class AsyncTransformTest extends AbstractSoakTest {

    private static final String SOURCE = AsyncTransformTest.class.getSimpleName();
    private static final String ORDERED_SINK = SOURCE + "-orderedSink";
    private static final String UNORDERED_SINK = SOURCE + "-unorderedSink";
    private static final String JOIN_MAP = SOURCE + "-joinMap";
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;
    private static final int EVENT_JOURNAL_CAPACITY = 600_000;
    private static final int JOIN_MAP_SIZE = 100;

    private long snapshotIntervalMs;
    private long durationInMillis;

    private transient ClientConfig remoteClusterClientConfig;
    private transient JetInstance remoteClient;
    private transient BasicEventJournalProducer producer;
    private transient IMap<Long, Long> joinMap;

    public static void main(String[] args) throws Exception {
        new AsyncTransformTest().run(args);
    }

    @Override
    protected void init() throws Exception {
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);
        durationInMillis = durationInMillis();
        remoteClusterClientConfig = remoteClusterClientConfig();

        remoteClient = Jet.newJetClient(remoteClusterClientConfig);

        Config config = remoteClient.getHazelcastInstance().getConfig();
        config.addEventJournalConfig(
                new EventJournalConfig().setMapName(ORDERED_SINK).setCapacity(EVENT_JOURNAL_CAPACITY)
        );
        config.addEventJournalConfig(
                new EventJournalConfig().setMapName(UNORDERED_SINK).setCapacity(EVENT_JOURNAL_CAPACITY)
        );

        producer = new BasicEventJournalProducer(remoteClient, SOURCE, EVENT_JOURNAL_CAPACITY);
        producer.start();

        joinMap = jet.getMap(JOIN_MAP);
        LongStream.range(0, JOIN_MAP_SIZE).forEach(i -> joinMap.set(i, -i));
    }

    @Override
    protected void test() {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(AsyncTransformTest.class.getSimpleName());
        jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        Job job = jet.newJob(pipeline(), jobConfig);


        Map<Long, Long> localJoinMap = joinMap.entrySet().stream().collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

        Verifier orderedVerifier = new Verifier(remoteClient, ORDERED_SINK, localJoinMap);
        Verifier unorderedVerifier = new Verifier(remoteClient, UNORDERED_SINK, localJoinMap);

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            assertNotEquals(getJobStatus(job), FAILED);
            assertTrue(orderedVerifier.isRunning());
            assertTrue(unorderedVerifier.isRunning());
            sleepMinutes(1);
        }
    }

    private Pipeline pipeline() {
        Pipeline p = Pipeline.create();

        StreamStageWithKey<Long, Long> sourceStage = p.drawFrom(Sources.<Long, Long, Long>remoteMapJournal(SOURCE,
                remoteClusterClientConfig, mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
                                                      .withoutTimestamps().setName("Stream from map(" + SOURCE + ")")
                                                      .groupingKey(value -> value % JOIN_MAP_SIZE);

        ContextFactory<IMapJet<Long, Long>> orderedContextFactory = ContextFactory
                .withCreateFn(jet -> jet.<Long, Long>getMap(JOIN_MAP))
                .withLocalSharing();
        sourceStage.mapUsingContextAsync(orderedContextFactory, (map, key, value) ->
                toCompletableFuture(map.getAsync(key % JOIN_MAP_SIZE)).thenApply(v -> entry(key, value + v)))
                   .drainTo(Sinks.remoteMap(ORDERED_SINK, remoteClusterClientConfig));

        ContextFactory<IMapJet<Long, Long>> unorderedContextFactory = orderedContextFactory
                .withUnorderedAsyncResponses();
        sourceStage.mapUsingContextAsync(unorderedContextFactory, (map, key, value) ->
                toCompletableFuture(map.getAsync(key % JOIN_MAP_SIZE)).thenApply(v -> entry(key, value + v)))
                   .drainTo(Sinks.remoteMap(UNORDERED_SINK, remoteClusterClientConfig));
        return p;
    }

    @Override
    protected void teardown() throws Exception {
        if (producer != null) {
            producer.stop();
        }
        if (remoteClient != null) {
            remoteClient.shutdown();
        }
    }

    public static class Verifier {

        private static final int QUEUE_SIZE_LIMIT = 10_000;
        private static final int LOG_COUNTER = 10_000;

        private final EventJournalConsumer<Long, Long> consumer;
        private final Thread thread;
        private final Map<Long, Long> localJoinMap;
        private final PriorityQueue<Long> queue;
        private final ILogger logger;

        private volatile boolean running = true;

        Verifier(JetInstance client, String mapName, Map<Long, Long> localJoinMap) {
            this.localJoinMap = localJoinMap;
            queue = new PriorityQueue<>();
            HazelcastInstance hazelcastInstance = client.getHazelcastInstance();
            logger = hazelcastInstance.getLoggingService().getLogger(Verifier.class.getSimpleName() + "-" + mapName);
            int partitionCount = hazelcastInstance.getPartitionService().getPartitions().size();
            consumer = new EventJournalConsumer<>(hazelcastInstance.getMap(mapName), partitionCount);
            thread = new Thread(this::run);
            thread.start();
        }

        private void run() {
            long counter = 0;
            while (running) {
                try {
                    consumer.drain(e -> {
                        long key = e.getKey();
                        long joinValue = localJoinMap.get(key);
                        long value = e.getNewValue() - joinValue;
                        assertEquals(key, value % JOIN_MAP_SIZE);
                        queue.offer(value);
                    });
                    while (true) {
                        Long peeked = queue.peek();
                        if (peeked == null || peeked != counter) {
                            break;
                        } else {
                            queue.poll();
                            counter++;
                        }
                        if (counter % LOG_COUNTER == 0) {
                            logger.info("counter: " + counter);
                        }
                    }
                    if (queue.size() == QUEUE_SIZE_LIMIT) {
                        logger.severe(String.format("Queue size reached limit(%d), size: %d",
                                QUEUE_SIZE_LIMIT, queue.size()));
                        running = false;
                    }
                } catch (Exception e) {
                    logger.severe("Exception during verification", e);
                    running = false;
                }
            }
        }

        boolean isRunning() {
            return running;
        }

    }
}
