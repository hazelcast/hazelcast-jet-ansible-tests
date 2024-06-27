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

package com.hazelcast.jet.tests.eventjournal;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.jet.tests.common.QueueVerifier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.map.EventJournalMapEvent;
import com.hazelcast.map.IMap;

import java.io.IOException;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static java.util.concurrent.TimeUnit.SECONDS;

public class EventJournalTest extends AbstractJetSoakTest {

    private static final int DEFAULT_LAG = 1500;
    private static final int DEFAULT_TIMESTAMP_PER_SECOND = 50;
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;
    private static final int DEFAULT_WINDOW_SIZE = 20;
    private static final int DEFAULT_SLIDE_BY = 10;
    private static final int DEFAULT_COUNTER_PER_TICKER = 100;
    private static final int EVENT_JOURNAL_CAPACITY = 1_500_000;
    private static final int RESULTS_EVENT_JOURNAL_CAPACITY = 40_000;

    private static final String MAP_NAME = EventJournalTest.class.getSimpleName();
    private static final String RESULTS_MAP_NAME = MAP_NAME + "-RESULTS";

    private int countPerTicker;
    private int snapshotIntervalMs;
    private int lagMs;
    private int timestampPerSecond;
    private int windowSize;
    private int slideBy;
    private int partitionCount;
    private int memberSize;

    private transient ClientConfig remoteClusterClientConfig;
    private transient EventJournalTradeProducer tradeProducer;
    private transient HazelcastInstance remoteClient;

    public static void main(String[] args) throws Exception {
        new EventJournalTest().run(args);
    }

    @Override
    public void init(HazelcastInstance client) throws Exception {
        lagMs = propertyInt("lagMs", DEFAULT_LAG);
        timestampPerSecond = propertyInt("timestampPerSecond", DEFAULT_TIMESTAMP_PER_SECOND);
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);
        windowSize = propertyInt("windowSize", DEFAULT_WINDOW_SIZE);
        slideBy = propertyInt("slideBy", DEFAULT_SLIDE_BY);
        countPerTicker = propertyInt("countPerTicker", DEFAULT_COUNTER_PER_TICKER);

        remoteClusterClientConfig = remoteClusterClientConfig();

        configureTradeProducer();
    }

    @Override
    public void test(HazelcastInstance client, String name) throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(name);
        jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        Job job = client.getJet().newJob(pipeline(), jobConfig);
        tradeProducer.start();

        int windowCount = windowSize / slideBy;
        LoggingService loggingService = client.getLoggingService();
        QueueVerifier queueVerifier = new QueueVerifier(loggingService,
                "Verifier[" + RESULTS_MAP_NAME + "]", windowCount).startVerification();

        IMap<Long, Long> resultMap = remoteClient.getMap(RESULTS_MAP_NAME);
        EventJournalConsumer<Long, Long> consumer = new EventJournalConsumer<>(resultMap, mapPutEvents(), partitionCount);

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            boolean isEmpty = consumer.drain(e -> {
                assertEquals("EXACTLY_ONCE -> Unexpected count for " + e.getKey(),
                        countPerTicker, (long) e.getNewValue());
                queueVerifier.offer(e.getKey());
            });
            if (isEmpty) {
                SECONDS.sleep(1);
            }
            if (getJobStatusWithRetry(job) == FAILED) {
                job.join();
            }
        }
        if (getJobStatusWithRetry(job) == FAILED) {
            job.join();
        }
        System.out.println("Cancelling jobs..");
        queueVerifier.close();
        job.cancel();
    }

    protected void teardown(Throwable t) throws Exception {
        if (tradeProducer != null) {
            tradeProducer.close();
        }
        if (remoteClient != null) {
            remoteClient.shutdown();
        }
    }

    private Pipeline pipeline() {
        Pipeline pipeline = Pipeline.create();

        pipeline.readFrom(Sources.<Long, Long, Long>remoteMapJournal(MAP_NAME, remoteClusterClientConfig,
                START_FROM_OLDEST, EventJournalMapEvent::getNewValue, e -> e.getType() == EntryEventType.ADDED))
                .withTimestamps(t -> t, lagMs).setName("Read from map(" + MAP_NAME + ")")
                .setLocalParallelism(partitionCount / memberSize)
                .window(sliding(windowSize, slideBy))
                .groupingKey(wholeItem())
                .aggregate(AggregateOperations.counting()).setName("Aggregate(count)")
                .writeTo(Sinks.remoteMap(RESULTS_MAP_NAME, remoteClusterClientConfig))
                .setName("Write to map(" + RESULTS_MAP_NAME + ")");
        return pipeline;
    }

    private void configureTradeProducer() throws IOException {
        remoteClient = HazelcastClient.newHazelcastClient(remoteClusterClientConfig);

        memberSize = remoteClient.getCluster().getMembers().size();
        partitionCount = remoteClient.getPartitionService().getPartitions().size();
        Config config = remoteClient.getConfig();
        MapConfig mapConfig = new MapConfig(MAP_NAME);
        mapConfig.getEventJournalConfig()
                 .setCapacity(EVENT_JOURNAL_CAPACITY)
                 .setEnabled(true);
        config.addMapConfig(mapConfig);
        MapConfig mapConfig2 = new MapConfig(RESULTS_MAP_NAME);
        mapConfig2.getEventJournalConfig()
                  .setCapacity(RESULTS_EVENT_JOURNAL_CAPACITY)
                  .setEnabled(true);
        config.addMapConfig(mapConfig2);

        ILogger producerLogger = getLogger(EventJournalTradeProducer.class);
        IMap<Long, Long> map = remoteClient.getMap(MAP_NAME);
        tradeProducer = new EventJournalTradeProducer(countPerTicker, map, timestampPerSecond, producerLogger);

    }

}
