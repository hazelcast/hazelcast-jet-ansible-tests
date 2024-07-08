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

package com.hazelcast.jet.tests.kafka;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.UuidUtil;
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
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.map.IMap;

import java.time.Duration;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.STARTING;
import static com.hazelcast.jet.pipeline.WindowDefinition.session;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static java.util.Map.entry;
import static java.util.concurrent.TimeUnit.MINUTES;

public class KafkaSessionWindowTest extends AbstractJetSoakTest {

    private static final int DEFAULT_LAG = 1500;
    private static final int DEFAULT_COUNTER_PER_TICKER = 20;
    private static final int DEFAULT_SESSION_TIMEOUT = 100;
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;

    private static final String TOPIC = KafkaSessionWindowTest.class.getSimpleName();
    private static final String RESULTS_TOPIC = TOPIC + "-RESULTS";

    private static final String ITEM_PROCESSED_MAP = "KafkaSessionWindowTest_itemProcessedMap";
    private static final String ITEM_PROCESSED_MAP_KEY = "KafkaSessionWindowTest_itemProcessedMapKey";

    private int sessionTimeout;
    private int snapshotIntervalMs;
    private String brokerUri;
    private String offsetReset;
    private int lagMs;
    private int countPerTicker;

    private transient ExecutorService producerExecutorService;
    private transient Future<?> producerFuture;

    public static void main(String[] args) throws Exception {
        new KafkaSessionWindowTest().run(args);
    }

    @Override
    public void init(HazelcastInstance client) {
        producerExecutorService = Executors.newSingleThreadExecutor();

        brokerUri = property("brokerUri", "localhost:9092");
        offsetReset = property("offsetReset", "earliest");
        lagMs = propertyInt("lagMs", DEFAULT_LAG);
        countPerTicker = propertyInt("countPerTicker", DEFAULT_COUNTER_PER_TICKER);
        sessionTimeout = propertyInt("sessionTimeout", DEFAULT_SESSION_TIMEOUT);
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);

        producerFuture = producerExecutorService.submit(() -> {
            try (TradeProducer tradeProducer = new TradeProducer(brokerUri)) {
                tradeProducer.produce(TOPIC, countPerTicker);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void test(HazelcastInstance client, String name) throws Exception {
        System.out.println("Executing job..");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(name);
        jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);

        Job testJob = client.getJet().newJob(pipeline(), jobConfig);

        System.out.println("Executing verification job..");
        JobConfig verificationJobConfig = new JobConfig().setName("Kafka Session Window Verification");
        Job verificationJob = client.getJet().newJob(verificationPipeline(), verificationJobConfig);

        long begin = System.currentTimeMillis();
        String previousProcessedUuid = "initialValue";
        IMap<String, String> itemProcessedMap = client.getMap(ITEM_PROCESSED_MAP);
        itemProcessedMap.put(ITEM_PROCESSED_MAP_KEY, previousProcessedUuid);
        try {
            while (System.currentTimeMillis() - begin < durationInMillis) {
                MINUTES.sleep(1);
                assertFalse(producerFuture.isDone());
                JobStatus status = getJobStatusWithRetry(verificationJob);
                if (status != STARTING && status != RUNNING) {
                    throw new AssertionError("Job is failed, jobStatus: " + status);
                }

                // check whether verification pipeline actually processed some items
                // start to check this after 5minutes
                if (System.currentTimeMillis() - begin > Duration.ofMinutes(5).toMillis()) {
                    String currentLastProcessedUuid = itemProcessedMap.get(ITEM_PROCESSED_MAP_KEY);
                    assertNotNull(currentLastProcessedUuid);
                    assertNotEquals(previousProcessedUuid, currentLastProcessedUuid);
                    previousProcessedUuid = currentLastProcessedUuid;
                }
            }
        } finally {
            System.out.println("Cancelling jobs..");
            try {
                testJob.cancel();
            } catch (Throwable t) {
                System.out.println("Cancelling test job failed... " + t.getMessage());
                t.printStackTrace();
            }
            try {
                verificationJob.cancel();
            } catch (Throwable t) {
                System.out.println("Cancelling verification job failed... " + t.getMessage());
                t.printStackTrace();
            }
        }

    }

    protected void teardown(Throwable t) throws Exception {
        if (producerExecutorService != null) {
            producerExecutorService.shutdown();
        }
    }

    private Pipeline pipeline() {
        Pipeline pipeline = Pipeline.create();

        Properties kafkaProps = kafkaPropertiesForTrades(brokerUri, offsetReset);
        Properties propsForResult = kafkaPropertiesForResults(brokerUri, offsetReset);

        pipeline.readFrom(KafkaSources.<String, Long>kafka(kafkaProps, TOPIC))
                .withTimestamps(Map.Entry::getValue, lagMs)
                .window(session(sessionTimeout))
                .groupingKey(Map.Entry::getKey)
                .aggregate(counting())
                .writeTo(KafkaSinks.kafka(propsForResult, RESULTS_TOPIC));

        return pipeline;
    }

    private Pipeline verificationPipeline() {
        Pipeline pipeline = Pipeline.create();

        Properties properties = kafkaPropertiesForResults(brokerUri, offsetReset);
        StreamStage<Map.Entry<String, Long>> beforeSink = pipeline
                .readFrom(KafkaSources.<String, Long>kafka(properties, RESULTS_TOPIC))
                .withoutTimestamps();
        beforeSink
                .writeTo(buildVerificationSink());
        // for checking that we actually receive some new items
        beforeSink
                .map(t -> entry(ITEM_PROCESSED_MAP_KEY, UuidUtil.newUnsecureUuidString()))
                .writeTo(Sinks.map(ITEM_PROCESSED_MAP));

        return pipeline;
    }

    private Sink<Map.Entry<String, Long>> buildVerificationSink() {
        final int expectedCount = countPerTicker;
        return SinkBuilder.sinkBuilder("verification-sink", ignored -> ignored)
                .<Map.Entry<String, Long>>receiveFn((ignored, entry) ->
                        assertEquals("Unexpected count for " + entry.getKey(), expectedCount, (long) entry.getValue()))
                .build();
    }

    private static Properties kafkaPropertiesForTrades(String brokerUrl, String offsetReset) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUrl);
        props.setProperty("group.id", UuidUtil.newUnsecureUuidString());
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", LongDeserializer.class.getName());
        props.setProperty("auto.offset.reset", offsetReset);
        props.setProperty("max.poll.records", "32768");
        return props;
    }

    private static Properties kafkaPropertiesForResults(String brokerUrl, String offsetReset) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUrl);
        props.setProperty("group.id", UuidUtil.newUnsecureUuidString());
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", LongDeserializer.class.getName());
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", LongSerializer.class.getName());
        props.setProperty("auto.offset.reset", offsetReset);
        props.setProperty("max.poll.records", "32768");
        return props;
    }


}
