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

import com.hazelcast.cluster.Member;
import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.isolatedjobs.JetMemberSelectorUtil.excludeMemberWithUuid;
import static java.util.stream.Collectors.joining;

/**
 * This test requires Enterprise License with Advanced Compute Feature.
 * To run this test locally, you have to use EE version (it can be forced by adding dependency to hazelcast-enterprise
 * in the pom.xml) and license must be set (it can be done in {@link AbstractJetSoakTest#run(String[])})
 */
public class IsolatedJobsBatchTest extends AbstractJetSoakTest {
    public static final int DEFAULT_SLEEP_BETWEEN_NEW_JOBS_MS = 5_000;
    public static final String BATCH_SINK_PREFIX = "IsolatedJobsTestBatchSink";
    private static final int LOG_JOB_COUNT_THRESHOLD = 60;
    private int sleepBetweenNewJobsMs;

    public static void main(final String[] args) throws Exception {
        new IsolatedJobsBatchTest().run(args);
    }

    @Override
    protected void init(HazelcastInstance client) throws Exception {
        sleepBetweenNewJobsMs = propertyInt("sleepBetweenNewJobsMs",
                DEFAULT_SLEEP_BETWEEN_NEW_JOBS_MS);
    }

    @Override
    protected void test(HazelcastInstance client, String name) throws Throwable {
        long begin = System.currentTimeMillis();
        long jobCount = 0;
        List<UUID> membersUuids = client.getCluster().getMembers().stream()
                .map(Member::getUuid)
                .collect(Collectors.toList());

        while (System.currentTimeMillis() - begin < durationInMillis) {
            UUID excludedMember = getRandomMemberUUID(membersUuids);
            createBatchJob(client, excludedMember, jobCount)
                    .join();
            assertBatchSink(client, jobCount, membersUuids, excludedMember);
            clearSink(client, jobCount);

            jobCount++;
            if (jobCount % LOG_JOB_COUNT_THRESHOLD == 0) {
                logger.info("Job count: " + jobCount);
            }
            sleepMillis(sleepBetweenNewJobsMs);
        }
        logger.info("Final job count: " + jobCount);
    }

    private void clearSink(HazelcastInstance client, long jobCount) {
        client.getList(BATCH_SINK_PREFIX + jobCount).clear();
    }

    private void assertBatchSink(HazelcastInstance client, long jobCount, List<UUID> membersUuids, UUID excludedMember) {
        List<UUID> expectedUuids = membersUuids
                .stream()
                .filter(uuid -> !uuid.equals(excludedMember))
                .collect(Collectors.toList());

        IList<UUID> list = client.getList(BATCH_SINK_PREFIX + jobCount);
        //Check if job was not processed on excluded member
        if (list.contains(excludedMember)) {
            throw new AssertionError("Job was executed on excluded member ( " + excludedMember + ")."
                    + "List of members from execution: " + list.stream()
                    .map(UUID::toString)
                    .collect(joining(",")));
        }

        //Check if job was processed on all expected members
        expectedUuids.forEach(expectedMemberUuid -> {
            if (!list.contains(expectedMemberUuid)) {
                throw new AssertionError("Job was not executed on expected member ( " + expectedMemberUuid + ")."
                        + "List of members from execution: " + list.stream()
                        .map(UUID::toString)
                        .collect(joining(",")));
            }
        });
    }


    private UUID getRandomMemberUUID(List<UUID> uuids) {
        int randomIndex = ThreadLocalRandom.current().nextInt(uuids.size());
        return uuids.get(randomIndex);
    }

    @Override
    protected void teardown(Throwable t) throws Exception {

    }

    public static Job createBatchJob(HazelcastInstance client, UUID excludedMember, long index) {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.createBatchSource(index))
                .writeTo(Sinks.list(BATCH_SINK_PREFIX + index));

        return client.getJet()
                .newJobBuilder(p)
                .withConfig(new JobConfig()
                        .setName("IsolatedJobsTestBatchJob" + index)
                        .addClass(Sources.class)
                        .addClass(JetMemberSelectorUtil.class))
                .withMemberSelector(excludeMemberWithUuid(excludedMember))
                .start();
    }

}
