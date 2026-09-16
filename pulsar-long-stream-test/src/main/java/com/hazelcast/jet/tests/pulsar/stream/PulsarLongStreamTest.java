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

/**
 * Long-running exactly-once soak test for the Pulsar source/sink connectors introduced in
 * "Rework consumers to processors" [CTT-1423][CTT-1366][CTT-1362]. Mirrors {@code
 * MongoLongStreamTest}'s dual-cluster/restart architecture: a background producer keeps
 * publishing an ever-increasing global sequence of messages to a per-cluster input topic while a
 * two-hop Jet pipeline (ported from {@code pulsar-test}: a {@code favouriteMeal}-grouped {@code
 * mapStateful} counting stage plus a transactional, dead-letter-aware {@link
 * PulsarDataConnection}) streams them through a middle topic into a memory-bounded {@link
 * VerificationProcessor} sink that checks the original global sequence id - carried through both
 * hops untouched - arrives exactly once, without ever holding the whole run's history in memory.
 * The pipeline runs identically, and concurrently, on the "dynamic" cluster (whose members this
 * framework periodically restarts - the actual restart/snapshot-recovery coverage) and a separate
 * "stable" cluster - both use the same transactional data connection, since both run against a
 * genuine HazelcastClient.
 */
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

        // Both the "dynamic" and "stable" branches run against a genuine HazelcastClient, so both
        // can use a real transactional PulsarDataConnection. hz-cli submit -f <clientYaml> (which
        // is how the dynamic branch's "client" comes to life) builds one via
        // HazelcastClient.newHazelcastClient(...) - see HazelcastCommandLine#getClientConfig /
        // hzClientFn in hazelcast-mono - so client.getConfig() there is a live
        // ClientDynamicClusterConfig exactly like the manually-created stable client, not a
        // member's frozen startup-time Config. There is no need to fall back to a plain
        // connectionSupplier for either branch.
        //
        // Against a real ansible-deployed cluster, DATA_CONNECTION_NAME is expected to already be
        // configured on the cluster's members via the "data-connection" section templated into
        // hazelcast.yaml.j2 - it exists from cluster startup, so it survives restarts and is
        // shared by every soak test run against that cluster, rather than being pushed at runtime.
        // Runtime registration is only needed - and only attempted - for a local run, where there
        // is no ansible-managed member config to bake it into.
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
            // getJobStatusWithRetry (not a bare job.getStatus()) so a transient JobNotFoundException
            // right after submission - e.g. before the job is visible on every member/coordinator -
            // is retried instead of instantly failing this thread.
            final JobStatus status = getJobStatusWithRetry(job);
            if (status == RUNNING) {
                return;
            }
            if (status == FAILED) {
                // job.join() rethrows the job's actual failure cause instead of the generic
                // "does not have expected status" message below, which otherwise swallows the real
                // reason the job failed and makes this unnecessarily hard to diagnose from logs.
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
