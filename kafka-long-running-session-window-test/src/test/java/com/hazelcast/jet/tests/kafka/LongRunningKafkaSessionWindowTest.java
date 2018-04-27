/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.pipeline.Sinks;
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
import tests.kafka.Trade;
import tests.kafka.TradeDeserializer;
import tests.kafka.TradeProducer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RESTARTING;
import static com.hazelcast.jet.pipeline.WindowDefinition.session;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class LongRunningKafkaSessionWindowTest {

    private int sessionTimeout;
    private String brokerUri;
    private String topic;
    private String offsetReset;
    private int lagMs;
    private int countPerTicker;
    private Properties kafkaProps;
    private JetInstance jet;
    private long durationInMillis;
    private ExecutorService producerExecutorService;

    public static void main(String[] args) {
        JUnitCore.main(LongRunningKafkaSessionWindowTest.class.getName());
    }

    @Before
    public void setUp() {
        producerExecutorService = Executors.newSingleThreadExecutor();
        brokerUri = System.getProperty("brokerUri", "localhost:9092");
        topic = System.getProperty("topic", String.format("%s-%d", "trades-session", System.currentTimeMillis()));
        offsetReset = System.getProperty("offsetReset", "earliest");
        lagMs = Integer.parseInt(System.getProperty("lagMs", "50"));
        countPerTicker = Integer.parseInt(System.getProperty("countPerTicker", "20"));
        sessionTimeout = Integer.parseInt(System.getProperty("sessionTimeout", "100"));
        durationInMillis = MINUTES.toMillis(Integer.parseInt(System.getProperty("durationInMinutes", "3")));
        kafkaProps = kafkaPropertiesForTrades(brokerUri, offsetReset);
        jet = JetBootstrap.getInstance();

        producerExecutorService.submit(() -> {
            try (TradeProducer tradeProducer = new TradeProducer(brokerUri)) {
                tradeProducer.produce(topic, countPerTicker);
            }
        });
    }

    @Test
    public void kafkaTest() throws InterruptedException {
        System.out.println("Executing job..");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setSnapshotIntervalMillis(5000);
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);

        Job testJob = jet.newJob(pipeline(), jobConfig);

        System.out.println("Executing verification job..");
        Job verificationJob = jet.newJob(verificationPipeline());

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            MINUTES.sleep(1);
            JobStatus status = verificationJob.getStatus();
            if (status == RESTARTING || status == FAILED || status == COMPLETED) {
                throw new AssertionError("Job is failed, jobStatus: " + status);
            }
        }
        System.out.println("Cancelling jobs..");

        testJob.cancel();
        verificationJob.cancel();

        while (testJob.getStatus() != COMPLETED ||
                verificationJob.getStatus() != COMPLETED) {
            SECONDS.sleep(1);
        }
    }

    @After
    public void tearDown() {
        jet.shutdown();
        producerExecutorService.shutdown();
    }

    private Pipeline pipeline() {
        Pipeline pipeline = Pipeline.create();

        Properties propsForResult = kafkaPropertiesForResults(brokerUri, offsetReset);

        pipeline.drawFrom(KafkaSources.kafka(kafkaProps, ConsumerRecord<String, Trade>::value, topic))
                .addTimestamps(Trade::getTime, lagMs)
                .window(session(sessionTimeout))
                .groupingKey(Trade::getTicker)
                .aggregate(counting())
                .drainTo(KafkaSinks.kafka(propsForResult, topic + "-results"));

        return pipeline;
    }

    private Pipeline verificationPipeline() {
        Pipeline pipeline = Pipeline.create();

        Properties properties = kafkaPropertiesForResults(brokerUri, offsetReset);
        pipeline.drawFrom(KafkaSources.<String, Long>kafka(properties, topic + "-results"))
                .drainTo(buildVerificationSink());

        return pipeline;
    }

    private Sink<Map.Entry<String, Long>> buildVerificationSink() {
        final int expectedCount = countPerTicker;
        return Sinks.<Object, Map.Entry<String, Long>>builder(ignored -> ignored)
                .onReceiveFn((ignored, entry) ->
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
