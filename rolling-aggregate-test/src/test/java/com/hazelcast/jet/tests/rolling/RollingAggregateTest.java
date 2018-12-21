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

import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.server.JetBootstrap;
import com.hazelcast.logging.ILogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import tests.rolling.VerificationProcessor;

import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.aggregate.AggregateOperations.maxBy;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.function.DistributedComparator.comparing;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.Sinks.fromProcessor;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

@RunWith(JUnit4.class)
public class RollingAggregateTest {

    private static final String SOURCE = RollingAggregateTest.class.getSimpleName();

    private JetInstance jet;
    private Producer producer;
    private ILogger logger;
    private long durationInMillis;
    private long snapshotIntervalMs;

    public static void main(String[] args) {
        JUnitCore.main(RollingAggregateTest.class.getName());
    }

    @Before
    public void setUp() {
        System.setProperty("hazelcast.logging.type", "log4j");
        String isolatedClientConfig = System.getProperty("isolatedClientConfig");
        if (isolatedClientConfig != null) {
            System.setProperty("hazelcast.client.config", isolatedClientConfig);
        }
        durationInMillis = MINUTES.toMillis(Integer.parseInt(System.getProperty("durationInMinutes", "30")));
        snapshotIntervalMs = Integer.parseInt(System.getProperty("snapshotIntervalMs", "5000"));
        jet = JetBootstrap.getInstance();
        HazelcastInstance hazelcastInstance = jet.getHazelcastInstance();
        logger = hazelcastInstance.getLoggingService().getLogger(RollingAggregateTest.class);
        Config config = hazelcastInstance.getConfig();
        config.addEventJournalConfig(
                new EventJournalConfig().setMapName(SOURCE).setCapacity(300_000)
        );
        jet.getMap(SOURCE).destroy();
        producer = new Producer(logger, jet.getMap(SOURCE));
        producer.start();
    }

    @After
    public void teardown() throws InterruptedException {
        if (producer != null) {
            producer.stop();
        }
        if (jet != null) {
            jet.shutdown();
        }
    }

    @Test
    public void test() throws Exception {
        Pipeline p = Pipeline.create();

        p.drawFrom(Sources.<Long, Long, Long>mapJournal(SOURCE, mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
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
            assertJobStatus(job);
            SECONDS.sleep(30);
        }

        job.cancel();
        JobStatus status = getJobStatus(job);
        while (status != COMPLETED && status != FAILED) {
            SECONDS.sleep(1);
            status = getJobStatus(job);
        }
    }

    private void assertJobStatus(Job job) {
        if (FAILED.equals(getJobStatus(job))) {
            logger.severe("Job is failed");
            job.join();
        }
    }

    private JobStatus getJobStatus(Job job) {
        try {
            return job.getStatus();
        } catch (Exception e) {
            logger.warning("Exception during status check", e);
            return null;
        }
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
                    parkNanos(SECONDS.toNanos(1));
                    continue;
                }
                counter++;
                if (counter % 5000 == 0) {
                    map.clear();
                }
                parkNanos(500_000);
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
