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

package com.hazelcast.jet.tests.management;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.jet.tests.common.BasicEventJournalProducer;

import java.util.concurrent.CancellationException;

import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.SUSPENDED;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static com.hazelcast.jet.tests.common.Util.waitForJobStatus;

public class JobManagementTest extends AbstractJetSoakTest {

    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;
    private static final int EVENT_JOURNAL_CAPACITY = 1_500_000;

    private static final String SOURCE = JobManagementTest.class.getSimpleName();

    private BasicEventJournalProducer producer;
    private int snapshotIntervalMs;

    public static void main(String[] args) throws Exception {
        new JobManagementTest().run(args);
    }

    @Override
    public void init(HazelcastInstance client) {
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);
        producer = new BasicEventJournalProducer(client, SOURCE, EVENT_JOURNAL_CAPACITY);
        producer.start();
    }

    @Override
    public void test(HazelcastInstance client, String name) {
        // Submit the job without initial snapshot
        Job job = client.getJet().newJob(pipeline(), jobConfig(name, null));
        waitForJobStatus(job, RUNNING);
        sleepMinutes(1);

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            logger.info("Suspend the job");
            job.suspend();
            waitForJobStatus(job, SUSPENDED);
            sleepMinutes(1);

            logger.info("Resume the job");
            job.resume();
            waitForJobStatus(job, RUNNING);
            sleepMinutes(1);

            logger.info("Restart the job");
            job.restart();
            waitForJobStatus(job, RUNNING);
            sleepMinutes(1);

            logger.info("Cancel job and export snapshot");
            JobStateSnapshot exportedSnapshot = job.cancelAndExportSnapshot("JobManagementTestSnapshot");
            try {
                job.join();
            } catch (CancellationException e) { /*ignored*/ }
            sleepMinutes(1);


            logger.info("New job with exported snapshot");
            job = client.getJet().newJob(pipeline(), jobConfig(name, exportedSnapshot.name()));
            waitForJobStatus(job, RUNNING);
            sleepMinutes(1);

            // Destroy the initial snapshot, by now job should produce new
            // snapshots for restart so it is safe.
            exportedSnapshot.destroy();
        }

        job.cancel();
    }

    protected void teardown(Throwable t) throws Exception {
        if (producer != null) {
            producer.stop();
        }
    }

    private JobConfig jobConfig(String name, String initialSnapshot) {
        return new JobConfig()
                .setName(name)
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(snapshotIntervalMs)
                .setInitialSnapshotName(initialSnapshot)
                .setAutoScaling(false);
    }

    private static Pipeline pipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<Long, Long, Long>mapJournal(SOURCE, START_FROM_OLDEST, mapEventNewValue(), mapPutEvents()))
         .withoutTimestamps()
         .writeTo(VerificationProcessor.sink());
        return p;
    }
}
