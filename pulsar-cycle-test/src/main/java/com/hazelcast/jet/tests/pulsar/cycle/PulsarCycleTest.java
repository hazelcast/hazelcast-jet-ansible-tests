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

package com.hazelcast.jet.tests.pulsar.cycle;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.pulsar.PulsarDataConnection;
import com.hazelcast.jet.pulsar.PulsarSchema;
import com.hazelcast.jet.pulsar.PulsarSinks;
import com.hazelcast.jet.pulsar.PulsarSources;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static java.util.stream.Collectors.toList;

/**
 * Repeated-cycle soak test for the new Pulsar processor-based connector, directly
 * analogous to {@code MongoTest}: every cycle recreates a fresh Pulsar topic, starts a
 * streaming read pipeline into a list sink, runs a bounded "batch" write pipeline into
 * the topic, runs a "batch" read-back pipeline from the topic into another list sink,
 * asserts both sinks contain exactly the expected content, then cancels the streaming
 * jobs and cleans up before starting the next cycle.
 * <p>
 * Unlike Mongo, Pulsar's Jet connector (see {@code PulsarSources}) only exposes
 * {@code StreamSource}s (the Consumer API and the Reader API) - there is no
 * {@code PulsarSources.batch(...)} analogous to {@code MongoSources.batch(...)}. The
 * "batch read-back" phase below is therefore adapted: a Reader-API {@code StreamSource}
 * is started against the topic (a fresh Reader always begins at {@code MessageId.earliest}
 * so it replays everything written to the fresh topic) and verification is driven by a
 * bounded polling loop that waits for the expected item count instead of waiting for the
 * (nonexistent) completion of a terminating batch source; the streaming job is then
 * cancelled explicitly once the expected count has been observed.
 */
public class PulsarCycleTest extends AbstractJetSoakTest {
    private static final int DEFAULT_ITEM_COUNT = 5_000;
    private static final int LOG_JOB_COUNT_THRESHOLD = 50;
    private static final int SLEEP_BETWEEN_READS_SECONDS = 2;
    private static final int JOB_STATUS_ASSERTION_ATTEMPTS = 1200;
    private static final int JOB_STATUS_ASSERTION_SLEEP_MS = 100;
    private static final int SINK_ASSERTION_ATTEMPTS = 120;
    private static final int SINK_ASSERTION_SLEEP_MS = 1000;

    private static final String DATA_CONNECTION_NAME = "pulsarCycleTestConnection";
    private static final String TOPIC_NAMESPACE = "persistent://public/default/";
    private static final String TOPIC_PREFIX = PulsarCycleTest.class.getSimpleName() + "_topic_";
    private static final String DOC_PREFIX = "pulsar-message-from-topic-";
    private static final String DOC_COUNTER_PREFIX = "-counter-";
    private static final String STREAM_READ_FROM_PREFIX = PulsarCycleTest.class.getSimpleName() + "_streamReadFrom_";
    private static final String WRITE_PREFIX = PulsarCycleTest.class.getSimpleName() + "_write_";
    private static final String BATCH_READ_FROM_PREFIX = PulsarCycleTest.class.getSimpleName() + "_batchReadFrom_";
    private static final String BATCH_SINK_LIST_NAME = PulsarCycleTest.class.getSimpleName() + "_listSinkBatch";
    private static final String STREAM_SINK_LIST_NAME = PulsarCycleTest.class.getSimpleName() + "_listSinkStream";

    private String brokerUrl;
    private String httpServiceUrl;
    private int itemCount;
    private List<Integer> inputItems;
    private transient PulsarAdmin pulsarAdmin;
    private transient HazelcastInstance remoteClient;

    public static void main(final String[] args) throws Exception {
        new PulsarCycleTest().run(args);
    }

    @Override
    public void init(final HazelcastInstance client) throws Exception {
        brokerUrl = "pulsar://" + property("pulsarIp", "127.0.0.1") + ":6650";
        httpServiceUrl = "http://" + property("pulsarIp", "127.0.0.1") + ":8080";
        itemCount = propertyInt("itemCount", DEFAULT_ITEM_COUNT);
        inputItems = IntStream.range(0, itemCount).boxed().collect(toList());
        pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(httpServiceUrl).build();
    }

    @Override
    public void test(final HazelcastInstance client, final String name) throws Exception {
        remoteClient = HazelcastClient.newHazelcastClient(remoteClusterClientConfig());
        remoteClient.getConfig().addDataConnectionConfig(PulsarDataConnection.pulsarDataConnectionConf(
                DATA_CONNECTION_NAME, brokerUrl, httpServiceUrl, true));
        final DataConnectionRef dataConnectionRef = DataConnectionRef.dataConnectionRef(DATA_CONNECTION_NAME);

        int jobCounter = 0;
        final long begin = System.currentTimeMillis();
        try {
            clearSinks(remoteClient);
            while (System.currentTimeMillis() - begin < durationInMillis) {
                final String topicName = TOPIC_PREFIX + jobCounter;
                deleteTopicAndCreateNewOne(topicName);
                clearSinks(remoteClient);

                final Job streamReadJob = startStreamReadFromPulsarPipeline(remoteClient, dataConnectionRef,
                        topicName, jobCounter);
                executeWriteToPulsarPipeline(remoteClient, dataConnectionRef, topicName, jobCounter);
                final Job batchReadJob = startBatchReadFromPulsarPipeline(remoteClient, dataConnectionRef,
                        topicName, jobCounter);

                assertBatchResults(remoteClient, jobCounter);
                assertStreamResults(remoteClient, jobCounter);

                batchReadJob.cancel();
                streamReadJob.cancel();
                clearSinks(remoteClient);
                deleteTopic(topicName);

                if (jobCounter % LOG_JOB_COUNT_THRESHOLD == 0) {
                    logger.info("Job count: " + jobCounter);
                }

                jobCounter++;
                sleepSeconds(SLEEP_BETWEEN_READS_SECONDS);
            }
        } finally {
            logger.info("Test finished with job count: " + jobCounter);
        }
    }

    @Override
    protected void teardown(final Throwable t) {
        if (pulsarAdmin != null) {
            pulsarAdmin.close();
        }
        if (remoteClient != null) {
            remoteClient.shutdown();
        }
    }

    private static String docId(final int topicCounter, final int docCounter) {
        return DOC_PREFIX + topicCounter + DOC_COUNTER_PREFIX + docCounter;
    }

    private static void clearSinks(final HazelcastInstance client) {
        client.getList(BATCH_SINK_LIST_NAME).clear();
        client.getList(STREAM_SINK_LIST_NAME).clear();
    }

    private static void assertJobStatusEventually(final Job job) {
        for (int i = 0; i < JOB_STATUS_ASSERTION_ATTEMPTS; i++) {
            if (job.getStatus().equals(RUNNING)) {
                return;
            } else {
                sleepMillis(JOB_STATUS_ASSERTION_SLEEP_MS);
            }
        }
        throw new AssertionError("Job " + job.getName() + " does not have expected status: " + RUNNING
                + ". Job status: " + job.getStatus());
    }

    private void deleteTopicAndCreateNewOne(final String topicName) throws PulsarAdminException {
        final String fullTopicName = TOPIC_NAMESPACE + topicName;
        deleteTopic(fullTopicName);
        pulsarAdmin.topics().createNonPartitionedTopic(fullTopicName);
    }

    private void deleteTopic(final String topicName) {
        final String fullTopicName = TOPIC_NAMESPACE + topicName;
        try {
            pulsarAdmin.topics().delete(fullTopicName, true);
        } catch (final PulsarAdminException e) {
            logger.info("Topic " + fullTopicName + " could not be deleted, ignoring: " + e.getMessage());
        }
    }

    private Job startStreamReadFromPulsarPipeline(final HazelcastInstance client,
                                                  final DataConnectionRef dataConnectionRef,
                                                  final String topicName, final int topicCounter) {
        final StreamSource<String> pulsarSource = PulsarSources
                .pulsarConsumerBuilder(PulsarSchema.string(), Message::getValue)
                .topic(topicName)
                .subscriberName(STREAM_READ_FROM_PREFIX + topicCounter)
                .dataConnectionRef(dataConnectionRef)
                .build();

        final Pipeline fromPulsar = Pipeline.create();
        fromPulsar.readFrom(pulsarSource)
                .withNativeTimestamps(0)
                .writeTo(Sinks.list(STREAM_SINK_LIST_NAME));

        final JobConfig jobConfig = new JobConfig();
        jobConfig.setName(STREAM_READ_FROM_PREFIX + topicCounter);
        final Job job = client.getJet().newJob(fromPulsar, jobConfig);
        assertJobStatusEventually(job);
        return job;
    }

    private void executeWriteToPulsarPipeline(final HazelcastInstance client,
                                              final DataConnectionRef dataConnectionRef,
                                              final String topicName, final int topicCounter) {
        final Pipeline toPulsar = Pipeline.create();
        toPulsar.readFrom(TestSources.items(inputItems))
                .map(docIndex -> docId(topicCounter, docIndex))
                .rebalance()
                .writeTo(PulsarSinks.builder(PulsarSchema.string())
                        .topic(topicName)
                        .dataConnectionRef(dataConnectionRef)
                        .build());

        final JobConfig jobConfig = new JobConfig();
        jobConfig.setName(WRITE_PREFIX + topicCounter);
        client.getJet().newJob(toPulsar, jobConfig).join();
    }

    /**
     * There is no {@code PulsarSources.batch(...)} equivalent of {@code MongoSources.batch(...)}, so the
     * "batch read-back" is adapted from a Reader-API {@code StreamSource}: a freshly created Reader always
     * starts at {@code MessageId.earliest}, so on the fresh, just-written topic it replays every message
     * that was written by {@link #executeWriteToPulsarPipeline}. The caller is responsible for cancelling
     * the returned, never-completing job once {@link #assertBatchResults} observes the expected item count.
     */
    private Job startBatchReadFromPulsarPipeline(final HazelcastInstance client,
                                                 final DataConnectionRef dataConnectionRef,
                                                 final String topicName, final int topicCounter) {
        final Map<String, Object> readerConfig = new HashMap<>();
        readerConfig.put("readerName", BATCH_READ_FROM_PREFIX + topicCounter);

        final StreamSource<String> pulsarSource = PulsarSources
                .pulsarReaderBuilder(PulsarSchema.string())
                .topic(topicName)
                .dataConnectionRef(dataConnectionRef)
                .readerConfig(readerConfig)
                .projectionFn(Message::getValue)
                .build();

        final Pipeline fromPulsar = Pipeline.create();
        fromPulsar.readFrom(pulsarSource)
                .withNativeTimestamps(0)
                .writeTo(Sinks.list(BATCH_SINK_LIST_NAME));

        final JobConfig jobConfig = new JobConfig();
        jobConfig.setName(BATCH_READ_FROM_PREFIX + topicCounter);
        final Job job = client.getJet().newJob(fromPulsar, jobConfig);
        assertJobStatusEventually(job);
        return job;
    }

    /**
     * Waits (bounded) for the Reader-API "batch read-back" pipeline to have replayed every message
     * written to the topic. See {@link #startBatchReadFromPulsarPipeline} for why a bounded poll is
     * used here instead of joining a genuinely terminating batch job.
     */
    private void assertBatchResults(final HazelcastInstance client, final int topicCounter) {
        for (int i = 0; i < SINK_ASSERTION_ATTEMPTS; i++) {
            final long actualTotalCount = client.getList(BATCH_SINK_LIST_NAME).size();
            if (itemCount == actualTotalCount) {
                break;
            }
            sleepMillis(SINK_ASSERTION_SLEEP_MS);
        }

        assertResults(client.getList(BATCH_SINK_LIST_NAME), topicCounter, "batch");
    }

    private void assertStreamResults(final HazelcastInstance client, final int topicCounter) {
        for (int i = 0; i < SINK_ASSERTION_ATTEMPTS; i++) {
            final long actualTotalCount = client.getList(STREAM_SINK_LIST_NAME).size();
            if (itemCount == actualTotalCount) {
                break;
            }
            sleepMillis(SINK_ASSERTION_SLEEP_MS);
        }

        assertResults(client.getList(STREAM_SINK_LIST_NAME), topicCounter, "stream");
    }

    private void assertResults(final IList<String> list, final int topicCounter, final String type) {
        final Set<String> set = new HashSet<>();
        final String expected = DOC_PREFIX + topicCounter + DOC_COUNTER_PREFIX;
        for (final String item : list) {
            assertTrue(type + " does not contain expected part: " + item, item.contains(expected));
            set.add(item);
        }

        final int lastElementNumber = itemCount - 1;
        try {
            assertEquals(itemCount, list.size());
            assertEquals(itemCount, set.size());
            assertTrue(set.contains(docId(topicCounter, 0)));
            assertTrue(set.contains(docId(topicCounter, lastElementNumber)));
        } catch (final Throwable ex) {
            logger.info("Printing content of incorrect list for " + type + ":");
            for (final String item : list) {
                logger.info(item);
            }
            throw ex;
        }
    }

}
