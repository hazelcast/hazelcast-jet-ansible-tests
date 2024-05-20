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

package com.hazelcast.jet.tests.isolated;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.tests.common.AbstractSoakTest;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static com.hazelcast.jet.tests.common.Util.waitForJobStatus;
import static java.util.stream.Collectors.joining;

/**
 * This test requires Enterprise License with Advanced Compute Feature.
 * To run this test locally, you have to use EE version (it can be forced by adding dependency to hazelcast-enterprise
 * in the pom.xml) and license must be set (it can be done in {@link AbstractSoakTest#run(String[])})
 */
public class IsolatedJobsStreamTest extends AbstractSoakTest {
    private static final int DEFAULT_LOG_VERIFICATION_COUNT_THRESHOLD = 5;
    private static final int DEFAULT_SLEEP_BETWEEN_VALIDATIONS_IN_MINUTES = 2;
    private static final int DEFAULT_JOB_WINDOW_TUMBLE_IN_MINUTES = 1;
    private static final int DEFAULT_SNAPSHOT_INTERVAL_MS = 1000;
    private static final int DEFAULT_SLEEP_BETWEEN_STREAM_RECORDS_MS = 1000 * 30;
    private int snapshotIntervalMs;
    private int logVerificationCountThreshold;
    private int sleepBeforeValidationInMinutes;
    private int jobWindowTumbleInMinutes;
    private int sleepBetweenStreamRecordsMs;

    public static void main(final String[] args) throws Exception {
        new IsolatedJobsStreamTest().run(args);
    }

    @Override
    protected boolean runOnBothClusters() {
        return true;
    }

    @Override
    protected void init(HazelcastInstance client) throws Exception {
        snapshotIntervalMs = propertyInt("snapshotIntervalMs",
                DEFAULT_SNAPSHOT_INTERVAL_MS);
        logVerificationCountThreshold = propertyInt("logVerificationCountThreshold",
                DEFAULT_LOG_VERIFICATION_COUNT_THRESHOLD);
        sleepBeforeValidationInMinutes = propertyInt("sleepBeforeValidationInMinutes",
                DEFAULT_SLEEP_BETWEEN_VALIDATIONS_IN_MINUTES);
        jobWindowTumbleInMinutes = propertyInt("jobWindowTumbleInMinutes",
                DEFAULT_JOB_WINDOW_TUMBLE_IN_MINUTES);
        sleepBetweenStreamRecordsMs = propertyInt("sleepBetweenStreamRecordsMs",
                DEFAULT_SLEEP_BETWEEN_STREAM_RECORDS_MS);
    }

    @Override
    protected void test(HazelcastInstance client, String name) throws Throwable {
        long begin = System.currentTimeMillis();
        String logPrefix = "[" + name + "] ";

        UUID excludedMember = client.getCluster().getMembers().iterator().next().getUuid();
        Job streamJob = createStreamJob(client, excludedMember, name);
        waitForJobStatus(streamJob, RUNNING);

        long validationCount = 0;
        while (System.currentTimeMillis() - begin < durationInMillis) {
            if (getJobStatusWithRetry(streamJob) == JobStatus.FAILED) {
                throw new RuntimeException(logPrefix + "Job failed: " + streamJob.getName());
            }

            sleepMinutes(sleepBeforeValidationInMinutes);

            assertStreamSink(client, excludedMember, name);
            clearSink(client, name);

            if (validationCount % logVerificationCountThreshold == 0) {
                logger.info(logPrefix + "Validations count: " + validationCount);
            }
            validationCount++;
        }
        logger.info(logPrefix + "Final validations count: " + validationCount);
    }

    @Override
    protected void teardown(Throwable t) throws Exception {

    }

    private void clearSink(HazelcastInstance client, String jobName) {
        client.getList(jobName).clear();
    }

    private void assertStreamSink(HazelcastInstance client, UUID excludedMember, String jobName) {
        IList<List<UUID>> list = client.getList(jobName);
        assertFalse("There was no records in the List from job " + jobName, list.isEmpty());
        list.forEach(uuids -> {
            boolean result = uuids.stream()
                    .noneMatch(uuid -> uuid.equals(excludedMember));
            if (!result) {
                throw new AssertionError("Job (" + jobName + ") was executed on excluded member ("
                        + excludedMember + ")." +
                        "List of members from execution: " + uuids.stream()
                        .map(UUID::toString)
                        .collect(joining(","))
                );
            }
        });
    }

    private Job createStreamJob(HazelcastInstance client, UUID excludedMember, String jobName) {
        Pipeline p = Pipeline.create();
        p.readFrom(createStreamSource())
                .withIngestionTimestamps()
                .window(WindowDefinition.tumbling(TimeUnit.MINUTES.toMillis(jobWindowTumbleInMinutes)))
                .aggregate(AggregateOperations.toList())
                .map(WindowResult::result)
                .writeTo(Sinks.list(jobName));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        if (jobName.startsWith(STABLE_CLUSTER)) {
            jobConfig.addClass(IsolatedJobsStreamTest.class);
        } else {
            jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
            jobConfig.setProcessingGuarantee(AT_LEAST_ONCE);
        }

        return client.getJet().newJobBuilder(p)
                .withMemberSelector(member -> !member.getUuid().equals(excludedMember))
                .withConfig(new JobConfig().setName(jobName))
                .start();

    }

    private StreamSource<UUID> createStreamSource() {
        int localSleepBetweenStreamRecordsMs1 = sleepBetweenStreamRecordsMs;
        return SourceBuilder.stream("IsolatedJobsStreamSource",
                        context -> context.hazelcastInstance().getCluster().getLocalMember().getUuid())
                .<UUID>fillBufferFn((ctx, buf) -> {
                    buf.add(ctx);
                    sleepMillis(localSleepBetweenStreamRecordsMs1);
                })
                .distributed(1)
                .createSnapshotFn(ctx -> ctx)
                .restoreSnapshotFn((ctx, state) -> ctx = state.get(0))
                .build();
    }
}
