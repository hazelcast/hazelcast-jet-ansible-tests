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
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.jet.tests.common.BasicEventJournalProducer;
import com.hazelcast.jet.tests.eventjournal.EventJournalConsumer;
import com.hazelcast.logging.ILogger;

import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.tests.common.Util.getJobStatus;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class AsyncTransformTest extends AbstractSoakTest {

    private static final String SOURCE = AsyncTransformTest.class.getSimpleName();
    private static final String ORDERED_SINK = SOURCE + "-orderedSink";
    private static final String UNORDERED_SINK = SOURCE + "-unorderedSink";
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;
    private static final int DEFAULT_MAX_SCHEDULER_DELAY = 500;
    private static final int EVENT_JOURNAL_CAPACITY = 600_000;
    private static final int SCHEDULER_CORE_SIZE = 5;

    private long snapshotIntervalMs;
    private long maxSchedulerDelayMillis;
    private long durationInMillis;

    private transient ClientConfig remoteClusterClientConfig;
    private transient JetInstance remoteClient;
    private transient BasicEventJournalProducer producer;

    public static void main(String[] args) throws Exception {
        new AsyncTransformTest().run(args);
    }

    @Override
    protected void init() throws Exception {
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);
        maxSchedulerDelayMillis = propertyInt("maxSchedulerDelayMillis", DEFAULT_MAX_SCHEDULER_DELAY);
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
    }

    @Override
    protected void test() throws InterruptedException {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(AsyncTransformTest.class.getSimpleName());
        jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        Job job = jet.newJob(pipeline(), jobConfig);

        Verifier orderedVerifier = new Verifier(remoteClient, ORDERED_SINK);
        Verifier unorderedVerifier = new Verifier(remoteClient, UNORDERED_SINK);

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            assertNotEquals(getJobStatus(job), FAILED);
            assertTrue(orderedVerifier.isRunning());
            assertTrue(unorderedVerifier.isRunning());
            sleepMinutes(1);
        }

        job.cancel();
        orderedVerifier.stop();
        unorderedVerifier.stop();
    }

    private Pipeline pipeline() {
        Pipeline p = Pipeline.create();

        StreamStage<Long> sourceStage = p.drawFrom(Sources.<Long, Long, Long>remoteMapJournal(SOURCE,
                remoteClusterClientConfig, mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
                                         .withoutTimestamps().setName("Stream from map(" + SOURCE + ")");

        ContextFactory<Scheduler> orderedContextFactory = ContextFactory
                .withCreateFn(x -> new Scheduler(SCHEDULER_CORE_SIZE, maxSchedulerDelayMillis))
                .withLocalSharing();
        sourceStage.mapUsingContextAsync(orderedContextFactory, Scheduler::schedule)
                   .drainTo(Sinks.remoteMap(ORDERED_SINK, remoteClusterClientConfig));

        ContextFactory<Scheduler> unorderedContextFactory = orderedContextFactory
                .withUnorderedAsyncResponses();
        sourceStage.mapUsingContextAsync(unorderedContextFactory, Scheduler::schedule)
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

    public static class Scheduler {

        private final ScheduledThreadPoolExecutor scheduledExecutor;
        private final long maxSchedulerDelayMillis;

        Scheduler(int coreSize, long maxSchedulerDelayMillis) {
            this.scheduledExecutor = new ScheduledThreadPoolExecutor(coreSize);
            this.maxSchedulerDelayMillis = maxSchedulerDelayMillis;
        }

        CompletableFuture<Map.Entry<Long, Long>> schedule(long value) {
            CompletableFuture<Map.Entry<Long, Long>> future = new CompletableFuture<>();
            scheduledExecutor.schedule(() -> future.complete(entry(value, -value)),
                    ThreadLocalRandom.current().nextLong(maxSchedulerDelayMillis), MILLISECONDS);
            return future;
        }
    }

    public static class Verifier {

        private static final int QUEUE_SIZE_LIMIT = 10_000;
        private static final int LOG_COUNTER = 10_000;

        private final EventJournalConsumer<Long, Long> consumer;
        private final Thread thread;
        private final PriorityQueue<Long> queue;
        private final IMap<Long, Long> map;
        private final ILogger logger;

        private volatile boolean running = true;

        Verifier(JetInstance client, String mapName) {
            queue = new PriorityQueue<>();
            HazelcastInstance hazelcastInstance = client.getHazelcastInstance();
            logger = hazelcastInstance.getLoggingService().getLogger(Verifier.class.getSimpleName() + "-" + mapName);
            int partitionCount = hazelcastInstance.getPartitionService().getPartitions().size();
            map = hazelcastInstance.getMap(mapName);
            consumer = new EventJournalConsumer<>(map, partitionCount);
            thread = new Thread(this::run);
            thread.start();
        }

        private void run() {
            long counter = 0;
            while (running) {
                try {
                    consumer.drain(e -> {
                        long key = e.getKey();
                        assertEquals(key, -1 * e.getNewValue());
                        queue.offer(key);
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
                            map.clear();
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

        void stop() throws InterruptedException {
            running = false;
            thread.join();
        }
    }
}
