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

package com.hazelcast.jet.tests.eventjournal;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.jet.tests.snapshot.QueueVerifier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.map.journal.EventJournalMapEvent;

import java.io.IOException;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.tests.common.Util.getJobStatus;
import static java.util.concurrent.TimeUnit.SECONDS;

public class EventJournalTest extends AbstractSoakTest {

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

    private long durationInMillis;
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
    private transient JetInstance remoteClient;

    public static void main(String[] args) throws Exception {
        new EventJournalTest().run(args);
    }

    public void init() throws Exception {
        lagMs = propertyInt("lagMs", DEFAULT_LAG);
        timestampPerSecond = propertyInt("timestampPerSecond", DEFAULT_TIMESTAMP_PER_SECOND);
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);
        windowSize = propertyInt("windowSize", DEFAULT_WINDOW_SIZE);
        slideBy = propertyInt("slideBy", DEFAULT_SLIDE_BY);
        countPerTicker = propertyInt("countPerTicker", DEFAULT_COUNTER_PER_TICKER);
        durationInMillis = durationInMillis();

        remoteClusterClientConfig = remoteClusterClientConfig();

        configureTradeProducer();
    }

    public void test() throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("EventJournalTest");
        jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        Job job = jet.newJob(pipeline(), jobConfig);
        tradeProducer.start();

        int windowCount = windowSize / slideBy;
        LoggingService loggingService = jet.getHazelcastInstance().getLoggingService();
        QueueVerifier queueVerifier = new QueueVerifier(loggingService,
                "Verifier[" + RESULTS_MAP_NAME + "]", windowCount).startVerification();

        IMap<Long, Long> resultMap = remoteClient.getHazelcastInstance().getMap(RESULTS_MAP_NAME);
        EventJournalConsumer<Long, Long> consumer = new EventJournalConsumer<>(resultMap, partitionCount);

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
            assertNotEquals(getJobStatus(job), FAILED);
        }
        assertNotEquals(getJobStatus(job), FAILED);
        System.out.println("Cancelling jobs..");
        queueVerifier.close();
        job.cancel();
    }

    public void teardown() throws Exception {
        if (tradeProducer != null) {
            tradeProducer.close();
        }
        if (remoteClient != null) {
            remoteClient.shutdown();
        }
    }

    private Pipeline pipeline() {
        Pipeline pipeline = Pipeline.create();

        pipeline.drawFrom(Sources.<Long, Long, Long>remoteMapJournal(MAP_NAME, remoteClusterClientConfig,
                e -> e.getType() == EntryEventType.ADDED, EventJournalMapEvent::getNewValue, START_FROM_OLDEST))
                .withTimestamps(t -> t, lagMs).setName("Read from map(" + MAP_NAME + ")")
                .setLocalParallelism(partitionCount / memberSize)
                .window(sliding(windowSize, slideBy))
                .groupingKey(wholeItem())
                .aggregate(AggregateOperations.counting()).setName("Aggregate(count)")
                .drainTo(Sinks.remoteMap(RESULTS_MAP_NAME, remoteClusterClientConfig))
                .setName("Write to map(" + RESULTS_MAP_NAME + ")");
        return pipeline;
    }

    private void configureTradeProducer() throws IOException {
        remoteClient = Jet.newJetClient(remoteClusterClientConfig);
        HazelcastInstance hazelcastInstance = remoteClient.getHazelcastInstance();

        memberSize = hazelcastInstance.getCluster().getMembers().size();
        partitionCount = hazelcastInstance.getPartitionService().getPartitions().size();
        Config config = hazelcastInstance.getConfig();
        config.addEventJournalConfig(
                new EventJournalConfig().setMapName(MAP_NAME).setCapacity(EVENT_JOURNAL_CAPACITY)
        );
        config.addEventJournalConfig(
                new EventJournalConfig().setMapName(RESULTS_MAP_NAME).setCapacity(RESULTS_EVENT_JOURNAL_CAPACITY)
        );

        ILogger producerLogger = getLogger(EventJournalTradeProducer.class);
        IMapJet<Long, Long> map = remoteClient.getMap(MAP_NAME);
        tradeProducer = new EventJournalTradeProducer(countPerTicker, map, timestampPerSecond, producerLogger);

    }

}
