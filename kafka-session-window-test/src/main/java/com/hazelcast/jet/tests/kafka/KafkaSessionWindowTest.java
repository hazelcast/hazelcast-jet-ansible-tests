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

package com.hazelcast.jet.tests.kafka;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.kafka.KafkaSinks;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.util.UuidUtil;
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
import static java.util.concurrent.TimeUnit.MINUTES;

public class KafkaSessionWindowTest extends AbstractSoakTest {

    private static final int DEFAULT_LAG = 1500;
    private static final int DEFAULT_COUNTER_PER_TICKER = 20;
    private static final int DEFAULT_SESSION_TIMEOUT = 100;
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;

    private static final String TOPIC = KafkaSessionWindowTest.class.getSimpleName();
    private static final String RESULTS_TOPIC = TOPIC + "-RESULTS";

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

    public void init() {
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

    public void test() throws Exception {
        System.out.println("Executing job..");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Kafka Session Window Test");
        jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);

        Job testJob = jet.newJob(pipeline(), jobConfig);

        System.out.println("Executing verification job..");
        JobConfig verificationJobConfig = new JobConfig().setName("Kafka Session Window Verification");
        Job verificationJob = jet.newJob(verificationPipeline(), verificationJobConfig);

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            MINUTES.sleep(1);
            assertFalse(producerFuture.isDone());
            JobStatus status = getJobStatusWithRetry(verificationJob);
            if (status != STARTING && status != RUNNING) {
                throw new AssertionError("Job is failed, jobStatus: " + status);
            }
        }
        System.out.println("Cancelling jobs..");

        testJob.cancel();
        verificationJob.cancel();
    }

    public void teardown() {
        if (producerExecutorService != null) {
            producerExecutorService.shutdown();
        }
    }

    private Pipeline pipeline() {
        Pipeline pipeline = Pipeline.create();

        Properties kafkaProps = kafkaPropertiesForTrades(brokerUri, offsetReset);
        Properties propsForResult = kafkaPropertiesForResults(brokerUri, offsetReset);

        pipeline.drawFrom(KafkaSources.<String, Long>kafka(kafkaProps, TOPIC))
                .withTimestamps(Map.Entry::getValue, lagMs)
                .window(session(sessionTimeout))
                .groupingKey(Map.Entry::getKey)
                .aggregate(counting())
                .drainTo(KafkaSinks.kafka(propsForResult, RESULTS_TOPIC));

        return pipeline;
    }

    private Pipeline verificationPipeline() {
        Pipeline pipeline = Pipeline.create();

        Properties properties = kafkaPropertiesForResults(brokerUri, offsetReset);
        pipeline.drawFrom(KafkaSources.<String, Long>kafka(properties, RESULTS_TOPIC))
                .withoutTimestamps()
                .drainTo(buildVerificationSink());

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
