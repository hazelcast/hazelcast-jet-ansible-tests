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

package com.hazelcast.jet.tests.rolling;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.logging.ILogger;

import java.io.IOException;

import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.aggregate.AggregateOperations.maxBy;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.function.DistributedComparator.comparing;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.Sinks.fromProcessor;
import static com.hazelcast.jet.tests.common.Util.getJobStatus;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;

public class RollingAggregateTest extends AbstractSoakTest {

    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;
    private static final int EVENT_JOURNAL_CAPACITY = 300_000;
    private static final int MAP_CLEAR_THRESHOLD = 5000;

    private static final String SOURCE = RollingAggregateTest.class.getSimpleName();

    private Producer producer;
    private long durationInMillis;
    private long snapshotIntervalMs;

    private transient ClientConfig remoteClientConfig;
    private transient JetInstance remoteClient;

    public static void main(String[] args) throws Exception {
        new RollingAggregateTest().run(args);
    }

    public void init() throws Exception {
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);
        durationInMillis = durationInMillis();
        String remoteClusterXml = property("remoteClusterXml", null);

        configureProducer(remoteClusterXml);
    }

    public void test() throws Exception {
        Pipeline p = Pipeline.create();

        p.drawFrom(Sources.<Long, Long, Long>remoteMapJournal(SOURCE, remoteClientConfig,
                mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
         .withoutTimestamps().setName("Stream from map(" + SOURCE + ")")
         .rollingAggregate(maxBy(comparing(val -> val))).setName("RollingAggregate(max)")
         .drainTo(fromProcessor("VerificationSink", VerificationProcessor.supplier()));

        JobConfig jobConfig = new JobConfig()
                .setName("RollingAggregateTest")
                .setSnapshotIntervalMillis(snapshotIntervalMs)
                .setProcessingGuarantee(EXACTLY_ONCE);

        Job job = jet.newJob(p, jobConfig);

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            assertNotEquals(FAILED, getJobStatus(job));
            sleepMinutes(1);
        }

        logger.info("Cancelling job...");
        job.cancel();
    }

    public void teardown() throws Exception {
        if (producer != null) {
            producer.stop();
        }
        if (jet != null) {
            jet.shutdown();
        }
        if (remoteClient != null) {
            remoteClient.shutdown();
        }
    }

    private void configureProducer(String remoteClusterXml) throws IOException {
        if (remoteClusterXml == null) {
            throw new IllegalArgumentException("Remote cluster xml should be set");
        }
        remoteClientConfig = new XmlClientConfigBuilder(remoteClusterXml).build();
        remoteClient = Jet.newJetClient(remoteClientConfig);

        Config config = remoteClient.getHazelcastInstance().getConfig();
        config.addEventJournalConfig(
                new EventJournalConfig().setMapName(SOURCE).setCapacity(EVENT_JOURNAL_CAPACITY)
        );
        remoteClient.getMap(SOURCE).destroy();
        producer = new Producer(logger, remoteClient.getMap(SOURCE));
        producer.start();
    }

    static class Producer {

        private final ILogger logger;
        private final IMapJet<Long, Long> map;
        private final Thread thread;

        private volatile boolean producing = true;

        Producer(ILogger logger, IMapJet<Long, Long> map) {
            this.logger = logger;
            this.map = map;
            this.thread = new Thread(this::run);
        }

        void run() {
            long counter = 0;
            while (producing) {
                try {
                    map.set(counter, counter);
                } catch (Exception e) {
                    logger.severe("Exception during producing, counter: " + counter, e);
                    sleepSeconds(1);
                    continue;
                }
                counter++;
                if (counter % MAP_CLEAR_THRESHOLD == 0) {
                    map.clear();
                }
                sleepMillis(1);
            }
        }

        void start() {
            thread.start();
        }

        void stop() throws InterruptedException {
            producing = false;
            thread.join();
        }
    }

}
