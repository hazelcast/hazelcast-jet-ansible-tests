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

package com.hazelcast.jet.tests.rolling;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.jet.tests.common.BasicEventJournalProducer;

import static com.hazelcast.function.ComparatorEx.comparing;
import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.aggregate.AggregateOperations.maxBy;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.Sinks.fromProcessor;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;

public class RollingAggregateTest extends AbstractJetSoakTest {

    private static final String SOURCE = RollingAggregateTest.class.getSimpleName();
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;
    private static final int EVENT_JOURNAL_CAPACITY = 600_000;

    private long snapshotIntervalMs;

    private transient ClientConfig remoteClusterClientConfig;
    private transient HazelcastInstance remoteClient;
    private transient BasicEventJournalProducer producer;

    public static void main(String[] args) throws Exception {
        new RollingAggregateTest().run(args);
    }

    @Override
    public void init(HazelcastInstance client) throws Exception {
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);
        remoteClusterClientConfig = remoteClusterClientConfig();
        remoteClient = HazelcastClient.newHazelcastClient(remoteClusterClientConfig);

        producer = new BasicEventJournalProducer(remoteClient, SOURCE, EVENT_JOURNAL_CAPACITY);
        producer.start();
    }

    @Override
    public void test(HazelcastInstance client, String name) {
        Pipeline p = Pipeline.create();

        p.readFrom(Sources.<Long, Long, Long>remoteMapJournal(SOURCE, remoteClusterClientConfig,
                START_FROM_OLDEST, mapEventNewValue(), mapPutEvents()))
         .withoutTimestamps().setName("Stream from map(" + SOURCE + ")")
         .rollingAggregate(maxBy(comparing(val -> val))).setName("RollingAggregate(max)")
         .writeTo(fromProcessor("VerificationSink", VerificationProcessor.supplier()));

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

}
