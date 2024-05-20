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

import com.hazelcast.cluster.Member;
import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.tests.common.AbstractSoakTest;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static java.util.stream.Collectors.joining;

/**
 * This test requires Enterprise License with Advanced Compute Feature.
 * To run this test locally, you have to use EE version (it can be forced by adding dependency to hazelcast-enterprise
 * in the pom.xml) and license must be set (it can be done in {@link AbstractSoakTest#run(String[])})
 */
public class IsolatedJobsBatchTest extends AbstractSoakTest {
    public static final int DEFAULT_SLEEP_BETWEEN_NEW_JOBS_IN_MINUTES = 1;
    public static final String BATCH_SINK_PREFIX = "IsolatedJobsTestBatchSink";
    private static final int LOG_JOB_COUNT_THRESHOLD = 15;
    private int sleepBetweenNewJobsInMinutes;

    public static void main(final String[] args) throws Exception {
        new IsolatedJobsBatchTest().run(args);
    }

    @Override
    protected void init(HazelcastInstance client) throws Exception {
        sleepBetweenNewJobsInMinutes = propertyInt("sleepBetweenNewJobsInMinutes",
                DEFAULT_SLEEP_BETWEEN_NEW_JOBS_IN_MINUTES);
    }

    @Override
    protected void test(HazelcastInstance client, String name) throws Throwable {
        long begin = System.currentTimeMillis();
        long jobCount = 0;
        while (System.currentTimeMillis() - begin < durationInMillis) {
            UUID excludedMember = getRandomMemberUUID(client);
            createBatchJob(client, excludedMember, jobCount)
                    .join();
            assertBatchSink(client, jobCount, excludedMember);
            clearSink(client, jobCount);

            if (jobCount % LOG_JOB_COUNT_THRESHOLD == 0) {
                logger.info("Job count: " + jobCount);
            }
            jobCount++;
            sleepMinutes(sleepBetweenNewJobsInMinutes);
        }
        logger.info("Final job count: " + jobCount);
    }

    private void clearSink(HazelcastInstance client, long jobCount) {
        client.getList(BATCH_SINK_PREFIX + jobCount).clear();
    }

    private void assertBatchSink(HazelcastInstance client, long jobCount, UUID excludedMember) {
        List<UUID> expectedUuids = client.getCluster().getMembers().stream()
                .map(Member::getUuid)
                .filter(uuid -> !uuid.equals(excludedMember))
                .collect(Collectors.toList());

        IList<UUID> list = client.getList(BATCH_SINK_PREFIX + jobCount);
        assertEquals(expectedUuids.size(), list.size());

        list.forEach(uuid -> {
            if (!expectedUuids.contains(uuid)) {
                throw new AssertionError("Job was executed on excluded member ( " + uuid + ")."
                        + "List of members from execution: " + list.stream()
                        .map(UUID::toString)
                        .collect(joining(",")));
            }
        });
    }


    private UUID getRandomMemberUUID(HazelcastInstance client) {
        Member[] members = client.getCluster().getMembers().toArray(Member[]::new);
        int randomIndex = ThreadLocalRandom.current().nextInt(members.length);
        return members[randomIndex].getUuid();
    }

    @Override
    protected void teardown(Throwable t) throws Exception {

    }

    public static Job createBatchJob(HazelcastInstance client, UUID excludedMember, long index) {
        Pipeline p = Pipeline.create();
        p.readFrom(createBatchSource(index))
                .writeTo(Sinks.list(BATCH_SINK_PREFIX + index));

        return client.getJet().newJobBuilder(p)
                .withMemberSelector(member -> !member.getUuid().equals(excludedMember))
                .withConfig(new JobConfig().setName("IsolatedJobsTestBatchJob" + index))
                .start();
    }

    private static BatchSource<UUID> createBatchSource(long index) {
        return SourceBuilder.batch("IsolatedJobsTestBatchSource" + index, context ->
                        context.hazelcastInstance().getCluster().getLocalMember().getUuid())
                .<UUID>fillBufferFn((uuid, buffer) -> {
                    buffer.add(uuid);
                    buffer.close();
                })
                .distributed(1)
                .build();
    }

}
