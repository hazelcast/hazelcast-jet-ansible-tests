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

package com.hazelcast.jet.tests.pulsar.stream;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pulsar.PulsarConsumerBuilder;
import com.hazelcast.jet.pulsar.PulsarDataConnection;
import com.hazelcast.jet.pulsar.PulsarSinkBuilder;
import com.hazelcast.jet.pulsar.PulsarSinks;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.jet.tests.common.VerificationProcessor;
import com.hazelcast.logging.ILogger;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.pulsar.PulsarSchema.json;
import static com.hazelcast.jet.pulsar.PulsarSources.pulsarConsumerBuilder;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;

public class PulsarLongStreamTest extends AbstractJetSoakTest {
    private static final String TOPIC_PREFIX = PulsarLongStreamTest.class.getSimpleName();
    private static final String CONSUMED_MESSAGES_MAP_NAME = TOPIC_PREFIX + "_latestCounter";
    private static final int ASSERTION_RETRY_COUNT = 60;
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;
    private static final int DEFAULT_TIMEOUT_FOR_NO_DATA_PROCESSED_MIN = 5;
    private static final int ASSERTION_ATTEMPTS = 1200;
    private static final int ASSERTION_SLEEP_MS = 100;
    // Must match the data-connection name templated into hazelcast.yaml.j2 for the jet cluster.
    private static final String DATA_CONNECTION_NAME = "pulsarDataConnection";

    private String brokerUrl;
    private String httpServiceUrl;
    private int snapshotIntervalMs;
    private int timeoutForNoDataProcessedMin;
    private PulsarAdmin pulsarAdmin;

    public static void main(final String[] args) throws Exception {
        new PulsarLongStreamTest().run(args);
    }

    @Override
    public void init(final HazelcastInstance client) throws Exception {
        brokerUrl = "pulsar://" + property("pulsarIp", "127.0.0.1") + ":6650";
        httpServiceUrl = "http://" + property("pulsarIp", "127.0.0.1") + ":8080";
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);
        timeoutForNoDataProcessedMin = propertyInt("timeoutForNoProcessedDataMin",
                DEFAULT_TIMEOUT_FOR_NO_DATA_PROCESSED_MIN);
        pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(httpServiceUrl).build();
        // EXACTLY_ONCE refuses to run unless broker-side deduplication is enabled
        pulsarAdmin.namespaces().setDeduplicationStatus("public/default", true);
    }

    @Override
    protected boolean runOnBothClusters() {
        return true;
    }

    @Override
    public void test(final HazelcastInstance client, final String clusterName) throws Exception {
        final long begin = System.currentTimeMillis();
        final boolean dynamic = clusterName.startsWith(DYNAMIC_CLUSTER);

        final String inputTopic = TOPIC_PREFIX + "_input_" + clusterName;
        final String middleTopic = TOPIC_PREFIX + "_middle_" + clusterName;
        final String deadLetterTopic = TOPIC_PREFIX + "_dlq_" + clusterName;
        deleteTopicAndCreateNewOne(inputTopic);
        deleteTopicAndCreateNewOne(middleTopic);
        deleteTopicAndCreateNewOne(deadLetterTopic);

        final DataConnectionRef dataConnectionRef = DataConnectionRef.dataConnectionRef(DATA_CONNECTION_NAME);
        if (isRunLocal()) {
            client.getConfig().addDataConnectionConfig(PulsarDataConnection.pulsarDataConnectionConf(
                    DATA_CONNECTION_NAME, brokerUrl, httpServiceUrl, true));
        }

        final DeadLetterPolicy deadLetterPolicy = DeadLetterPolicy.builder()
                .deadLetterTopic(deadLetterTopic)
                .maxRedeliverCount(1)
                .build();

        final PulsarConsumerBuilder<GreetingWithSeq, GreetingWithSeq> inputSourceBuilder =
                pulsarConsumerBuilder(json(GreetingWithSeq.class), Message::getValue)
                        .topic(inputTopic)
                        .subscriberName(clusterName + "-input-subscription")
                        .consumerCustomizer(consumer -> consumer.deadLetterPolicy(deadLetterPolicy))
                        .dataConnectionRef(dataConnectionRef);
        final PulsarSinkBuilder<GreetingWithStatAndSeq, GreetingWithStatAndSeq> middleSinkBuilder =
                PulsarSinks.builder(json(GreetingWithStatAndSeq.class))
                        .topic(middleTopic)
                        .extractKeyFn(GreetingWithStatAndSeq::favouriteMeal)
                        .dataConnectionRef(dataConnectionRef);

        final Pipeline toMiddleTopic = Pipeline.create();
        toMiddleTopic.readFrom(inputSourceBuilder.build())
                .withNativeTimestamps(0)
                .groupingKey(GreetingWithSeq::favouriteMeal)
                .mapStateful(AtomicInteger::new,
                        (counter, meal, item) -> new GreetingWithStatAndSeq(item.name(), item.favouriteMeal(),
                                counter.incrementAndGet(), item.seq()))
                .writeTo(middleSinkBuilder.build());

        final PulsarConsumerBuilder<GreetingWithStatAndSeq, GreetingWithStatAndSeq> middleSourceBuilder =
                pulsarConsumerBuilder(json(GreetingWithStatAndSeq.class), Message::getValue)
                        .topic(middleTopic)
                        .subscriberName(clusterName + "-middle-subscription")
                        .consumerCustomizer(consumer -> consumer.deadLetterPolicy(deadLetterPolicy))
                        .dataConnectionRef(dataConnectionRef);

        final Pipeline toVerification = Pipeline.create();
        toVerification.readFrom(middleSourceBuilder.build())
                .withNativeTimestamps(0)
                .map(GreetingWithStatAndSeq::seq)
                .writeTo(VerificationProcessor.sink(CONSUMED_MESSAGES_MAP_NAME, clusterName));

        final JobConfig toMiddleTopicJobConfig = new JobConfig();
        final JobConfig toVerificationJobConfig = new JobConfig();
        if (dynamic) {
            toMiddleTopicJobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
            toMiddleTopicJobConfig.setProcessingGuarantee(EXACTLY_ONCE);
            toVerificationJobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
            toVerificationJobConfig.setProcessingGuarantee(EXACTLY_ONCE);
        } else {
            toMiddleTopicJobConfig.addClass(PulsarLongStreamTest.class, GreetingWithSeq.class,
                    GreetingWithStatAndSeq.class, PulsarMessageProducer.class, VerificationProcessor.class);
            toVerificationJobConfig.addClass(PulsarLongStreamTest.class, GreetingWithSeq.class,
                    GreetingWithStatAndSeq.class, PulsarMessageProducer.class, VerificationProcessor.class);
        }
        toMiddleTopicJobConfig.setName(clusterName + "_" + TOPIC_PREFIX + "_toMiddleTopic");
        toVerificationJobConfig.setName(clusterName + "_" + TOPIC_PREFIX + "_toVerification");

        final Job toMiddleTopicJob = client.getJet().newJob(toMiddleTopic, toMiddleTopicJobConfig);
        final Job toVerificationJob = client.getJet().newJob(toVerification, toVerificationJobConfig);
        assertJobStatusEventually(toMiddleTopicJob);
        assertJobStatusEventually(toVerificationJob);

        final PulsarMessageProducer producer = new PulsarMessageProducer(brokerUrl, inputTopic, logger);
        final PulsarDeadLetterQueueTracker deadLetterQueueTracker =
                new PulsarDeadLetterQueueTracker(brokerUrl, deadLetterTopic, logger);
        producer.start();

        final long expectedTotalCount;
        long lastlyProcessed = -1;
        int noNewMessagesCounter = 0;
        try {
            while (System.currentTimeMillis() - begin < durationInMillis) {
                if (getJobStatusWithRetry(toMiddleTopicJob) == FAILED) {
                    toMiddleTopicJob.join();
                } else if (getJobStatusWithRetry(toVerificationJob) == FAILED) {
                    toVerificationJob.join();
                } else {
                    final long processedMessages = getNumberOfProcessedMessages(client, clusterName);

                    if (processedMessages == lastlyProcessed) {
                        noNewMessagesCounter++;
                        log(logger, "Nothing was processed in last minute, current counter:"
                                + processedMessages, clusterName);
                        if (noNewMessagesCounter > timeoutForNoDataProcessedMin) {
                            throw new AssertionError("Failed. Exceeded timeout for no data processed");
                        }
                    } else {
                        noNewMessagesCounter = 0;
                        lastlyProcessed = processedMessages;
                    }
                }
                sleepMinutes(1);
            }
        } finally {
            expectedTotalCount = producer.stop();
        }

        log(logger, "Producer stopped, expectedTotalCount: " + expectedTotalCount, clusterName);
        assertCountEventually(client, expectedTotalCount, clusterName);
        deadLetterQueueTracker.assertDeadLetterQueueIsEmpty();
        deadLetterQueueTracker.close();
        toMiddleTopicJob.cancel();
        toVerificationJob.cancel();
        log(logger, "Job completed", clusterName);
    }

    private static void assertJobStatusEventually(final Job job) {
        for (int i = 0; i < ASSERTION_ATTEMPTS; i++) {
            final JobStatus status = getJobStatusWithRetry(job);
            if (status == RUNNING) {
                return;
            }
            if (status == FAILED) {
                job.join();
            }
            sleepMillis(ASSERTION_SLEEP_MS);
        }
        throw new AssertionError("Job " + job.getName() + " does not have expected status: " + RUNNING
                + ". Job status: " + job.getStatus());
    }

    private static long getNumberOfProcessedMessages(final HazelcastInstance client, final String clusterName) {
        final Map<String, Long> latestCounterMap = client.getMap(CONSUMED_MESSAGES_MAP_NAME);
        return Optional.ofNullable(latestCounterMap.get(clusterName)).orElse(0L);
    }

    private static void assertCountEventually(final HazelcastInstance client, final long expectedTotalCount,
                                              final String clusterName) {
        final Map<String, Long> latestCounterMap = client.getMap(CONSUMED_MESSAGES_MAP_NAME);
        for (int i = 0; i < ASSERTION_RETRY_COUNT; i++) {
            final long actualTotalCount = latestCounterMap.get(clusterName);
            if (expectedTotalCount == actualTotalCount) {
                return;
            }
            sleepSeconds(1);
        }
        final long actualTotalCount = latestCounterMap.get(clusterName);
        assertEquals(expectedTotalCount, actualTotalCount);
    }

    private static void log(final ILogger logger, final String message, final String clusterName) {
        logger.info("Cluster" + clusterName + "\t\t" + message);
    }

    private void deleteTopicAndCreateNewOne(final String topicName) throws PulsarAdminException {
        final String fullTopicName = "persistent://public/default/" + topicName;
        try {
            pulsarAdmin.topics().delete(fullTopicName, true);
        } catch (PulsarAdminException e) {
            logger.info("Topic " + fullTopicName + " did not exist yet, nothing to delete");
        }
        pulsarAdmin.topics().createNonPartitionedTopic(fullTopicName);
    }

    @Override
    protected void teardown(final Throwable t) {
        if (pulsarAdmin != null) {
            pulsarAdmin.close();
        }
    }

}
