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

package com.hazelcast.jet.tests.largesnapshotchunk;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;

import java.io.IOException;
import java.util.Map.Entry;

import static com.hazelcast.jet.aggregate.AggregateOperations.mapping;
import static com.hazelcast.jet.aggregate.AggregateOperations.toList;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.Sinks.fromProcessor;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;

public class LargeSnapshotChunkTest extends AbstractJetSoakTest {

    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;
    private static final int EVENT_JOURNAL_CAPACITY = 600_000;
    // We need a 1+ lag because the map-journal source doesn't coalesce partitions correctly.
    // It needs to cover for expected restart time. Time advances by 1 per second.
    // 10 is good for development.
    private static final int DEFAULT_LAG = 10;

    private static final String SOURCE = LargeSnapshotChunkTest.class.getSimpleName();
    private static final int WINDOW_SIZE = 8;

    private LargeSnapshotChunkProducer producer;
    private long snapshotIntervalMs;
    private int lagMs;

    private transient ClientConfig remoteClientConfig;
    private transient HazelcastInstance remoteClient;

    public static void main(String[] args) throws Exception {
        new LargeSnapshotChunkTest().run(args);
    }

    @Override
    public void init(HazelcastInstance client) throws Exception {
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);
        lagMs = propertyInt("lagMs", DEFAULT_LAG);

        configureProducer();
    }

    @Override
    public void test(HazelcastInstance client, String name) {
        Pipeline p = Pipeline.create();

        p.readFrom(Sources.<String, int[]>remoteMapJournal(SOURCE, remoteClientConfig,
                START_FROM_OLDEST))
         .withTimestamps(e -> e.getValue()[0], lagMs)
         .setName("Stream from map(" + SOURCE + ")")
         .groupingKey(Entry::getKey)
         .window(WindowDefinition.tumbling(WINDOW_SIZE))
         .aggregate(mapping(Entry::getValue, toList()))
         .setName("WindowAggregate(toList)")
         .writeTo(fromProcessor("VerificationSink", VerificationProcessor.supplier(WINDOW_SIZE)));

        JobConfig jobConfig = new JobConfig()
                .setName(name)
                .setSnapshotIntervalMillis(snapshotIntervalMs)
                .setProcessingGuarantee(EXACTLY_ONCE);

        Job job = client.getJet().newJob(p, jobConfig);

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            if (getJobStatusWithRetry(job) == FAILED) {
                job.join();
            }
            sleepMinutes(1);
        }

        logger.info("Cancelling job...");
        job.cancel();
    }

    protected void teardown(Throwable t) throws Exception {
        if (producer != null) {
            producer.stop();
        }
        if (remoteClient != null) {
            remoteClient.shutdown();
        }
    }

    private void configureProducer() throws IOException {
        remoteClientConfig = remoteClusterClientConfig();
        remoteClient = HazelcastClient.newHazelcastClient(remoteClientConfig);

        Config config = remoteClient.getConfig();
        MapConfig mapConfig = new MapConfig(SOURCE);
        mapConfig.getEventJournalConfig()
                 .setCapacity(EVENT_JOURNAL_CAPACITY)
                 .setEnabled(true);
        config.addMapConfig(mapConfig);
        remoteClient.getMap(SOURCE).clear();
        producer = new LargeSnapshotChunkProducer(logger, remoteClient, WINDOW_SIZE, remoteClient.getMap(SOURCE));
        producer.start();
    }
}
