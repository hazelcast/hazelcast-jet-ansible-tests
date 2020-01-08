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

package com.hazelcast.jet.tests.twopc;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.kafka.KafkaSinks;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import java.util.List;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.STARTING;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class KafkaToKafkaTest extends AbstractSoakTest {

    private static final int DEFAULT_LAG = 1800;
    private static final int DEFAULT_BULK_SIZE = 10;
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;
    private static final int DEFAULT_WINDOW_SIZE = 50;
    private static final int DEFAULT_BULKS_PER_SECOND = 5;
    // Some acceptable threshold for last emitted and not processed windows.
    // It can be useful if test finish during period when isolated cluster is down.
    private static final int DEFAULT_DELAY_THRESHOLD_SECOND = 360;
    private static final int DELAY_BETWEEN_STATUS_CHECKS = 60;
    private static final int DELAY_AFTER_TEST_FINISHED_FACTOR = 2;

    private static final String TOPIC = KafkaToKafkaTest.class.getSimpleName();
    private static final String RESULTS_TOPIC = TOPIC + "-RESULTS";
    private static final String VERIFICATION_MAP_NAME = KafkaToKafkaTest.class.getSimpleName() + "_verificationMap";

    private ClientConfig stableClusterClientConfig;
    private JetInstance stableClusterClient;

    private int windowSize;
    private int bulkSize;
    private int bulksPerSecond;
    private int snapshotIntervalMs;
    private int delayThreshold;
    private String brokerUri;
    private String offsetReset;
    private int lagMs;

    private transient ExecutorService producerExecutorService;
    private transient Future<?> producerFuture;

    public static void main(String[] args) throws Exception {
        new KafkaToKafkaTest().run(args);
    }

    @Override
    public void init() throws Exception {
        producerExecutorService = Executors.newSingleThreadExecutor();

        brokerUri = property("brokerUri", "localhost:9092");
        offsetReset = property("offsetReset", "earliest");
        lagMs = propertyInt("lagMs", DEFAULT_LAG);
        windowSize = propertyInt("windowSize", DEFAULT_WINDOW_SIZE);
        bulksPerSecond = propertyInt("bulksPerSecond", DEFAULT_BULKS_PER_SECOND);
        bulkSize = propertyInt("bulkSize", DEFAULT_BULK_SIZE);
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);
        delayThreshold = propertyInt("delayThreshold", DEFAULT_DELAY_THRESHOLD_SECOND);

        stableClusterClientConfig = remoteClusterClientConfig();
        stableClusterClient = Jet.newJetClient(stableClusterClientConfig);

        producerFuture = producerExecutorService.submit(() -> {
            try (TradeProducer tradeProducer = new TradeProducer(brokerUri, bulksPerSecond)) {
                tradeProducer.produce(TOPIC, bulkSize);
            } catch (Throwable e) {
                logger.info("Exception in tradeProducer.produce");
                e.printStackTrace();
            }
        });
    }

    @Override
    public void test() throws Throwable {
        Throwable[] exceptions = new Throwable[2];
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(() -> {
            try {
                testInternal(jet, "Dynamic");
            } catch (Throwable t) {
                logger.severe("Exception in Dynamic cluster test", t);
                exceptions[0] = t;
            }
        });
        executorService.execute(() -> {
            try {
                testInternal(stableClusterClient, "Stable");
            } catch (Throwable t) {
                logger.severe("Exception in Stable cluster test", t);
                exceptions[1] = t;
            }
        });
        executorService.shutdown();
        long extraDuration = DELAY_AFTER_TEST_FINISHED_FACTOR * (SECONDS.toMillis(DELAY_BETWEEN_STATUS_CHECKS));
        executorService.awaitTermination(durationInMillis + extraDuration, MILLISECONDS);

        if (exceptions[0] != null) {
            logger.severe("Exception in Dynamic cluster test", exceptions[0]);
        }
        if (exceptions[1] != null) {
            logger.severe("Exception in Stable cluster test", exceptions[1]);
        }
        if (exceptions[0] != null) {
            throw exceptions[0];
        }
        if (exceptions[1] != null) {
            throw exceptions[1];
        }
    }

    public void testInternal(JetInstance client, String name) throws Exception {
        logger.info("[" + name + "] Executing job..");
        JobConfig jobConfig = new JobConfig()
                .setName("[" + name + "] Kafka to Kafka 2pc Test")
                .setSnapshotIntervalMillis(snapshotIntervalMs)
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);

        Job testJob = client.newJob(pipeline(name), jobConfig);

        logger.info("[" + name + "] Executing verification job..");
        JobConfig verificationJobConfig = new JobConfig()
                .setName("[" + name + "] Kafka to Kafka 2pc Verification");
        Job verificationJob = stableClusterClient.newJob(verificationPipeline(name), verificationJobConfig);

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            SECONDS.sleep(DELAY_BETWEEN_STATUS_CHECKS);
            try {
                assertFalse(producerFuture.isDone());
            } catch (AssertionError e) {
                if (TradeProducer.isFinished()) {
                    logger.info("TradeProducer already finished.");
                    break;
                } else {
                    throw e;
                }
            }
            JobStatus status = getJobStatusWithRetry(verificationJob);
            if (status != STARTING && status != RUNNING) {
                throw new AssertionError("[" + name + "] Job is failed, jobStatus: " + status);
            }
        }
        TradeProducer.finish();

        logger.info("[" + name + "] Cancelling jobs..");
        testJob.cancel();
        verificationJob.cancel();

        logger.info("[" + name + "] Final verification..");
        assertAllExpectedWindowsWereProcessed(name);

    }

    @Override
    protected void teardown(Throwable t) throws Exception {
        if (producerExecutorService != null) {
            producerExecutorService.shutdown();
        }
    }

    private Pipeline pipeline(String name) {
        Pipeline pipeline = Pipeline.create();

        Properties kafkaProps = kafkaPropertiesForSource(brokerUri, offsetReset);
        Properties propsForResult = kafkaPropertiesForSink(brokerUri, offsetReset);

        pipeline.readFrom(KafkaSources.<Long, Long>kafka(kafkaProps, TOPIC))
                .withTimestamps(Map.Entry::getValue, lagMs)
                .window(tumbling(windowSize))
                .aggregate(counting())
                .writeTo(KafkaSinks.kafka(propsForResult, RESULTS_TOPIC + name, t -> t.start(), t -> t.result()));

        return pipeline;
    }

    private Pipeline verificationPipeline(String name) {
        Pipeline pipeline = Pipeline.create();

        Properties properties = kafkaPropertiesForVerification(brokerUri, offsetReset);
        StreamStage<Map.Entry<Long, Long>> streamStage
                = pipeline.readFrom(KafkaSources.<Long, Long>kafka(properties, RESULTS_TOPIC + name))
                        .withoutTimestamps();
        streamStage.writeTo(buildVerificationSink(name));
        streamStage.writeTo(Sinks.map(VERIFICATION_MAP_NAME + name));

        return pipeline;
    }

    private Sink<Map.Entry<Long, Long>> buildVerificationSink(String name) {
        final int expectedCount = bulkSize * windowSize;
        return SinkBuilder.sinkBuilder("verification-sink" + name, ignored -> ignored)
                .<Map.Entry<Long, Long>>receiveFn((ignored, entry) -> {
                    assertEquals("[" + name + "] KafkaToKafkaTest - Unexpected count for " + entry.getKey(),
                            expectedCount, (long) entry.getValue());
                })
                .build();
    }

    private void assertAllExpectedWindowsWereProcessed(String name) {
        // Checks that any window is not missing in Kafka sink.
        Map<Long, Long> map = stableClusterClient.getMap(VERIFICATION_MAP_NAME + name);
        List<Long> startTimes = map.keySet().stream().sorted().collect(Collectors.toList());
        // log size before any validation
        logger.info("[" + name + "] Processed windows: " + startTimes.size());
        for (int i = 0; i < startTimes.size(); i++) {
            assertEquals(i * windowSize, (long) startTimes.get(i));
        }

        // Checks that expected minimal amount of windows was emitted.
        // It serves mainly for asserting that Pipeline did not stop processing items during the test.
        long minimalWindowCount
                = (((MILLISECONDS.toSeconds(durationInMillis) - delayThreshold) * bulksPerSecond) - lagMs) / windowSize;
        assertTrue("[" + name + "] It was expected that at least " + minimalWindowCount + " windows, but was "
                + startTimes.size(),
                startTimes.size() >= minimalWindowCount);
    }

    private static Properties kafkaPropertiesForSource(String brokerUrl, String offsetReset) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUrl);
        props.setProperty("group.id", UuidUtil.newUnsecureUuidString());
        props.setProperty("key.deserializer", LongDeserializer.class.getName());
        props.setProperty("value.deserializer", LongDeserializer.class.getName());
        props.setProperty("auto.offset.reset", offsetReset);
        props.setProperty("max.poll.records", "32768");
        return props;
    }

    private static Properties kafkaPropertiesForSink(String brokerUrl, String offsetReset) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUrl);
        props.setProperty("group.id", UuidUtil.newUnsecureUuidString());
        props.setProperty("key.deserializer", LongDeserializer.class.getName());
        props.setProperty("value.deserializer", LongDeserializer.class.getName());
        props.setProperty("key.serializer", LongSerializer.class.getName());
        props.setProperty("value.serializer", LongSerializer.class.getName());
        props.setProperty("auto.offset.reset", offsetReset);
        props.setProperty("max.poll.records", "32768");
        return props;
    }

    private static Properties kafkaPropertiesForVerification(String brokerUrl, String offsetReset) {
        Properties props = kafkaPropertiesForSink(brokerUrl, offsetReset);
        props.setProperty("isolation.level", "read_committed");
        return props;
    }
}
