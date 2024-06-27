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

package com.hazelcast.jet.tests.isolatedjobs;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;

import java.util.Collection;
import java.util.Map;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static com.hazelcast.jet.tests.common.Util.waitForJobStatus;
import static com.hazelcast.jet.tests.isolatedjobs.JetMemberSelectorUtil.excludeMemberWithAddress;

/**
 * This test requires Enterprise License with Advanced Compute Feature.
 * To run this test locally, you have to use EE version (it can be forced by adding dependency to hazelcast-enterprise
 * in the pom.xml) and license must be set (it can be done in {@link AbstractJetSoakTest#run(String[])})
 */
public class IsolatedJobsStreamTest extends AbstractJetSoakTest {
    private static final int DEFAULT_LOG_VERIFICATION_COUNT_THRESHOLD = 5;
    private static final int DEFAULT_SLEEP_BETWEEN_VALIDATIONS_IN_MINUTES = 2;
    private static final int DEFAULT_SNAPSHOT_INTERVAL_MS = 1000;
    private static final int DEFAULT_SLEEP_BETWEEN_STREAM_RECORDS_MS = 500;
    private int snapshotIntervalMs;
    private int logVerificationCountThreshold;
    private int sleepBeforeValidationInMinutes;
    private int sleepBetweenStreamRecordsMs;

    private transient ClientConfig remoteClusterClientConfig;
    private transient HazelcastInstance remoteClient;

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
        sleepBetweenStreamRecordsMs = propertyInt("sleepBetweenStreamRecordsMs",
                DEFAULT_SLEEP_BETWEEN_STREAM_RECORDS_MS);
        remoteClusterClientConfig = remoteClusterClientConfig();
        remoteClient = HazelcastClient.newHazelcastClient(remoteClusterClientConfig);
    }

    @Override
    protected void test(HazelcastInstance client, String name) throws Throwable {
        long begin = System.currentTimeMillis();
        String logPrefix = "[" + name + "] ";

        Member excludedMember = client.getCluster().getMembers().iterator().next();
        String excludedMemberAddress = excludedMember.getSocketAddress().toString();

        Job streamJob = JobDefinition.createStreamJob(sleepBetweenStreamRecordsMs,
                snapshotIntervalMs, client, excludedMemberAddress, name, remoteClusterClientConfig);
        waitForJobStatus(streamJob, RUNNING);
        //For Dynamic cluster tests assertions and clear actions needs to be done on map stored in stable cluster
        //In this case Pipeline writes data to this map with usage of Sinks#remoteMap
        HazelcastInstance outputClient = name.startsWith(DYNAMIC_CLUSTER) ? remoteClient : client;

        long validationCount = 0;
        while (System.currentTimeMillis() - begin < durationInMillis) {
            if (getJobStatusWithRetry(streamJob) == JobStatus.FAILED) {
                throw new RuntimeException(logPrefix + "Job failed: " + streamJob.getName());
            }

            sleepMinutes(sleepBeforeValidationInMinutes);

            assertStreamSinkMap(outputClient, excludedMemberAddress, name);
            clearSink(outputClient, name);

            validationCount++;
            if (validationCount % logVerificationCountThreshold == 0) {
                logger.info(logPrefix + "Validations count: " + validationCount);
            }
        }
        logger.info(logPrefix + "Final validations count: " + validationCount);
    }

    @Override
    protected void teardown(Throwable t) throws Exception {

    }

    private void clearSink(HazelcastInstance client, String jobName) {
        client.getMap(jobName).clear();
    }

    private void assertStreamSinkMap(HazelcastInstance client, String excludedMemberAddress, String jobName) {
        Map<String, String> map = client.getMap(jobName);
        assertFalse("There was no entries in the Map from job " + jobName, map.isEmpty());
        Collection<String> mapValues = map.values();
        mapValues.forEach(address -> {
            if (address.equals(excludedMemberAddress)) {
                throw new AssertionError("Job (" + jobName + ") was executed on excluded member ("
                        + excludedMemberAddress + ")." +
                        "List of members from execution: " + String.join(",", mapValues));
            }
        });
    }

    public static class JobDefinition {
        public static Job createStreamJob(int sleepBetweenStreamRecordsMs,
                                          int snapshotIntervalMs, HazelcastInstance client,
                                          String excludedMember, String jobName, ClientConfig remoteClientConfig) {
            Sink<String> sink;

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(jobName);
            if (jobName.startsWith(STABLE_CLUSTER)) {
                jobConfig.addClass(IsolatedJobsStreamTest.class);
                //Use local map for stable cluster
                sink = Sinks.map(jobName, FunctionEx.identity(), FunctionEx.identity());
            } else {
                jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
                jobConfig.setProcessingGuarantee(AT_LEAST_ONCE);
                // Use remote map from stable cluster for dynamic cluster test to prevent issues with cleared map during
                // restarts of a dynamic cluster
                sink = Sinks.remoteMap(jobName, remoteClientConfig, FunctionEx.identity(), FunctionEx.identity());
            }
            jobConfig.addClass(Sources.class)
                    .addClass(JetMemberSelectorUtil.class)
                    .addClass(JobDefinition.class);

            Pipeline p = Pipeline.create();

            p.readFrom(Sources.createStreamSource(sleepBetweenStreamRecordsMs))
                    .withIngestionTimestamps()
                    .writeTo(sink);

            return client.getJet().newJobBuilder(p)
                    .withMemberSelector(excludeMemberWithAddress(excludedMember))
                    .withConfig(jobConfig)
                    .start();

        }
    }
}
