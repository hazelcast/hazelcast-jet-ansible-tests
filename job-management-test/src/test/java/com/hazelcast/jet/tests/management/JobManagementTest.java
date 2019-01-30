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

package com.hazelcast.jet.tests.management;

import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import tests.management.VerificationProcessor;

import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.tests.common.Util.runTestWithArguments;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static com.hazelcast.jet.tests.common.Util.waitForJobStatus;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

@RunWith(JUnit4.class)
public class JobManagementTest extends AbstractSoakTest {

    private static final String SOURCE = JobManagementTest.class.getSimpleName();

    private Producer producer;
    private int snapshotIntervalMs;
    private long durationInMillis;


    public static void main(String[] args) {
        runTestWithArguments(JobManagementTest.class.getName(), args, 2);
    }

    @Before
    public void setup() {
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", 5000);
        durationInMillis = durationInMillis();

        Config config = jet.getHazelcastInstance().getConfig();
        config.addEventJournalConfig(
                new EventJournalConfig().setMapName(SOURCE).setCapacity(300_000)
        );
        producer = new Producer(jet.getMap(SOURCE));
        producer.start();
    }

    @After
    public void cleanup() throws InterruptedException {
        if (producer != null) {
            producer.stop();
        }
        if (jet != null) {
            jet.shutdown();
        }
    }

    @Test
    public void test() throws InterruptedException {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Long, Long, Long>mapJournal(SOURCE, mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
         .withoutTimestamps()
         .groupingKey(l -> 0L)
         .mapUsingContext(ContextFactory.withCreateFn(jet -> null), (c, k, v) -> v)
         .drainTo(Sinks.fromProcessor("sink", VerificationProcessor.supplier()));

        JobConfig jobConfig = new JobConfig()
                .setName("JobManagementTest")
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(snapshotIntervalMs)
                .setAutoScaling(false);

        Job job = jet.newJob(p, jobConfig);
        waitForJobStatus(job, RUNNING);

        sleepSeconds(20);


        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            System.out.println("Suspend the job");
            job.suspend();
            waitForJobStatus(job, SUSPENDED);
            sleepSeconds(40);

            System.out.println("Resume the job");
            job.resume();
            waitForJobStatus(job, RUNNING);
            sleepSeconds(40);

            System.out.println("Restart the job");
            job.restart();
            waitForJobStatus(job, RUNNING);
            sleepSeconds(40);
        }

        job.cancel();
    }

    static class Producer {

        private final IMapJet<Long, Long> map;
        private final Thread thread;

        private volatile boolean producing = true;

        Producer(IMapJet<Long, Long> map) {
            this.map = map;
            this.thread = new Thread(this::run);
        }

        void run() {
            long counter = 0;
            while (producing) {
                try {
                    map.set(counter, counter);
                } catch (Exception e) {
                    e.printStackTrace();
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
