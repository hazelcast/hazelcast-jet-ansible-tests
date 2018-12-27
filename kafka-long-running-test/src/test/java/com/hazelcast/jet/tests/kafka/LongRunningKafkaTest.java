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
import com.hazelcast.jet.server.JetBootstrap;
import com.hazelcast.util.UuidUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import tests.kafka.LongRunningTradeProducer;
import tests.kafka.Trade;
import tests.kafka.TradeDeserializer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.core.JobStatus.STARTING;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class LongRunningKafkaTest {

    private static final String TOPIC = LongRunningKafkaTest.class.getSimpleName();
    private static final String RESULTS_TOPIC = TOPIC + "-RESULTS";

    private JetInstance jet;
    private String brokerUri;
    private String offsetReset;
    private long durationInMillis;
    private int countPerTicker;
    private int lagMs;
    private int windowSize;
    private int slideBy;
    private Properties kafkaProps;
    private ExecutorService producerExecutorService;

    public static void main(String[] args) {
        JUnitCore.main(LongRunningKafkaTest.class.getName());
    }

    @Before
    public void setUp() {
        System.setProperty("hazelcast.logging.type", "log4j");
        producerExecutorService = Executors.newSingleThreadExecutor();
        brokerUri = System.getProperty("brokerUri", "localhost:9092");
        offsetReset = System.getProperty("offsetReset", "earliest");
        lagMs = Integer.parseInt(System.getProperty("lagMs", "1"));
        windowSize = Integer.parseInt(System.getProperty("windowSize", "20"));
        slideBy = Integer.parseInt(System.getProperty("slideBy", "10"));
        countPerTicker = Integer.parseInt(System.getProperty("countPerTicker", "100"));
        durationInMillis = MINUTES.toMillis(Integer.parseInt(System.getProperty("durationInMinutes", "1")));
        kafkaProps = kafkaPropertiesForTrades(brokerUri, offsetReset);
        jet = JetBootstrap.getInstance();

        producerExecutorService.submit(() -> {
            try (LongRunningTradeProducer tradeProducer = new LongRunningTradeProducer(brokerUri)) {
                tradeProducer.produce(TOPIC, countPerTicker);
            }
        });
    }

    @Test
    public void kafkaTest() throws InterruptedException {
        System.out.println("Executing test job..");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Kafka WordCount Test");
        jobConfig.setSnapshotIntervalMillis(5000);
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
        Job testJob = jet.newJob(pipeline(), jobConfig);

        System.out.println("Executing verification job..");
        JobConfig verificationJobConfig = new JobConfig().setName("Kafka WordCount Test Verification");
        Job verificationJob = jet.newJob(verificationPipeline(), verificationJobConfig);

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            MINUTES.sleep(1);
            JobStatus status = verificationJob.getStatus();
            if (status != STARTING && status != RUNNING) {
                throw new AssertionError("Job is failed, jobStatus: " + status);
            }
        }
        System.out.println("Cancelling jobs..");
        testJob.cancel();
        verificationJob.cancel();
    }

    @After
    public void tearDown() {
        jet.shutdown();
        producerExecutorService.shutdown();
    }

    private Pipeline pipeline() {
        Pipeline pipeline = Pipeline.create();

        Properties propsForResult = kafkaPropertiesForResults(brokerUri, offsetReset);

        pipeline.drawFrom(KafkaSources.kafka(kafkaProps, ConsumerRecord<String, Trade>::value, TOPIC))
                .addTimestamps(Trade::getTime, lagMs)
                .window(sliding(windowSize, slideBy))
                .groupingKey(Trade::getTicker)
                .aggregate(counting())
                .drainTo(KafkaSinks.kafka(propsForResult, RESULTS_TOPIC));

        return pipeline;
    }

    private Pipeline verificationPipeline() {
        Pipeline pipeline = Pipeline.create();

        Properties properties = kafkaPropertiesForResults(brokerUri, offsetReset);
        pipeline.drawFrom(KafkaSources.<String, Long>kafka(properties, RESULTS_TOPIC))
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
        props.setProperty("value.deserializer", TradeDeserializer.class.getName());
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
