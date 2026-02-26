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

package com.hazelcast.jet.tests.async;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.jet.tests.common.BasicEventJournalProducer;
import com.hazelcast.jet.tests.eventjournal.EventJournalConsumer;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;

import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.ServiceFactories.sharedService;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class AsyncTransformTest extends AbstractJetSoakTest {

    private static final String SOURCE = AsyncTransformTest.class.getSimpleName();
    private static final String ORDERED_SINK = SOURCE + "-orderedSink";
    private static final String UNORDERED_SINK = SOURCE + "-unorderedSink";
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;
    private static final int DEFAULT_MAX_SCHEDULER_DELAY = 500;
    private static final int EVENT_JOURNAL_CAPACITY = 600_000;
    private static final int SCHEDULER_CORE_SIZE = 5;
    private static final int MAX_CONCURRENT_OPS = 256;
    private static final boolean UNORDERED = false;

    private long snapshotIntervalMs;
    private long maxSchedulerDelayMillis;

    private transient ClientConfig remoteClusterClientConfig;
    private transient HazelcastInstance remoteClient;
    private transient BasicEventJournalProducer producer;

    public static void main(String[] args) throws Exception {
        new AsyncTransformTest().run(args);
    }

    @Override
    protected void init(HazelcastInstance client) throws Exception {
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);
        maxSchedulerDelayMillis = propertyInt("maxSchedulerDelayMillis", DEFAULT_MAX_SCHEDULER_DELAY);

        remoteClusterClientConfig = remoteClusterClientConfig();
        remoteClient = HazelcastClient.newHazelcastClient(remoteClusterClientConfig);

        Config config = remoteClient.getConfig();
        MapConfig mapConfig = new MapConfig(ORDERED_SINK);
        mapConfig.getEventJournalConfig()
                 .setCapacity(EVENT_JOURNAL_CAPACITY)
                 .setEnabled(true);
        config.addMapConfig(mapConfig);
        MapConfig mapConfig2 = new MapConfig(UNORDERED_SINK);
        mapConfig2.getEventJournalConfig()
                  .setCapacity(EVENT_JOURNAL_CAPACITY)
                  .setEnabled(true);
        config.addMapConfig(mapConfig2);
        producer = new BasicEventJournalProducer(remoteClient, SOURCE, EVENT_JOURNAL_CAPACITY);
        producer.start();
    }

    @Override
    protected void test(HazelcastInstance client, String name) throws InterruptedException {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(name);
        jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        Job job = client.getJet().newJob(pipeline(), jobConfig);

        Verifier orderedVerifier = new Verifier(remoteClient, ORDERED_SINK);
        Verifier unorderedVerifier = new Verifier(remoteClient, UNORDERED_SINK);

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            if (getJobStatusWithRetry(job) == FAILED) {
                job.join();
            }
            orderedVerifier.checkStatus();
            unorderedVerifier.checkStatus();
            sleepMinutes(1);
        }

        job.cancel();
        orderedVerifier.stop();
        unorderedVerifier.stop();
    }

    private Pipeline pipeline() {
        Pipeline p = Pipeline.create();

        StreamStage<Long> sourceStage = p.readFrom(Sources.<Long, Long, Long>remoteMapJournal(SOURCE,
                remoteClusterClientConfig, START_FROM_OLDEST, mapEventNewValue(), mapPutEvents()))
                                         .withoutTimestamps().setName("Stream from map(" + SOURCE + ")");

        long maxSchedulerDelayMillisLocal = maxSchedulerDelayMillis;
        ServiceFactory<?, Scheduler> contextFactory = sharedService(
                ctx -> new Scheduler(SCHEDULER_CORE_SIZE, maxSchedulerDelayMillisLocal),
                Scheduler::shutdown
        );
        sourceStage.mapUsingServiceAsync(contextFactory, Scheduler::schedule)
                   .writeTo(Sinks.remoteMap(ORDERED_SINK, remoteClusterClientConfig));

        sourceStage.mapUsingServiceAsync(contextFactory, MAX_CONCURRENT_OPS, UNORDERED, Scheduler::schedule)
                   .writeTo(Sinks.remoteMap(UNORDERED_SINK, remoteClusterClientConfig));
        return p;
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
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

        void shutdown() throws InterruptedException {
            scheduledExecutor.shutdown();
            scheduledExecutor.awaitTermination(1, MINUTES);
        }
    }

    public static class Verifier {

        private static final int QUEUE_SIZE_LIMIT = 50_000;
        private static final int LOG_COUNTER = 10_000;
        private static final int PARK_MS = 10;

        private final EventJournalConsumer<Long, Long> consumer;
        private final Thread thread;
        private final IMap<Long, Long> map;
        private final ILogger logger;

        private volatile boolean running = true;
        private volatile Throwable error;

        Verifier(HazelcastInstance client, String mapName) {
            logger = client.getLoggingService().getLogger(Verifier.class.getSimpleName() + "-" + mapName);
            int partitionCount = client.getPartitionService().getPartitions().size();
            map = client.getMap(mapName);
            consumer = new EventJournalConsumer<>(map, mapPutEvents(), partitionCount);
            thread = new Thread(this::run);
            thread.start();
        }

        private void run() {
            try {
                long counter = 0;
                long cyclesWithoutUpdate = 0;
                // PriorityQueue returns the lowest enqueued item first
                PriorityQueue<Long> queue = new PriorityQueue<>();
                while (running) {
                    LockSupport.parkNanos(MILLISECONDS.toNanos(PARK_MS));
                    consumer.drain(e -> {
                        long key = e.getKey();
                        assertEquals(key, -1 * e.getNewValue());
                        queue.offer(key);
                    });
                    for (Long peeked; (peeked = queue.peek()) != null; ) {
                        if (peeked < counter) {
                            // duplicate key
                            queue.remove();
                            continue;
                        } else if (peeked > counter) {
                            // the item might arrive later
                            break;
                        } else if (peeked == counter) {
                            queue.remove();
                            counter++;
                            cyclesWithoutUpdate = 0;
                        }
                        if (counter % LOG_COUNTER == 0) {
                            logger.info("counter: " + counter);
                            // clear the map from time to time, we only need the journal
                            map.clear();
                        }
                    }
                    cyclesWithoutUpdate++;
                    if (cyclesWithoutUpdate > 2_000) {
                        logger.info("Not proccessed for longer time for counter: " + counter);
                        cyclesWithoutUpdate = 0;
                    }
                    if (queue.size() >= QUEUE_SIZE_LIMIT) {
                        throw new AssertionError(String.format("Queue size exceeded while waiting for the next item. "
                                        + "Limit=%d, expected next=%d, next in queue: %d, %d, %d, %d, ...",
                                QUEUE_SIZE_LIMIT, counter, queue.poll(), queue.poll(), queue.poll(), queue.poll()));
                    }
                }
            } catch (Throwable e) {
                logger.severe("Exception thrown for verifier using " + map.getName(), e);
                error = e;
            } finally {
                running = false;
            }
        }

        void checkStatus() {
            if (!running) {
                if (error != null) {
                    throw new RuntimeException(error);
                }
                throw new RuntimeException("verifier not running");
            }
        }

        void stop() throws InterruptedException {
            running = false;
            thread.join();
            if (error != null) {
                throw new RuntimeException(error);
            }
        }
    }
}
