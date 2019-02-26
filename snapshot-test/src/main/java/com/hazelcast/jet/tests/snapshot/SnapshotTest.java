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

package com.hazelcast.jet.tests.snapshot;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.kafka.KafkaSinks;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.util.UuidUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;

public class SnapshotTest extends AbstractSoakTest {

    private static final int DEFAULT_LAG = 3000;
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 1000;
    private static final int DEFAULT_WINDOW_SIZE = 20;
    private static final int DEFAULT_SLIDE_BY = 10;
    private static final int DEFAULT_COUNTER_PER_TICKER = 1000;

    private static final String TOPIC = SnapshotTest.class.getSimpleName();
    private static final String RESULTS_TOPIC = TOPIC + "-RESULTS";
    private static final int POLL_TIMEOUT = 1000;

    private String brokerUri;
    private String offsetReset;
    private long durationInMillis;
    private int countPerTicker;
    private int snapshotIntervalMs;
    private int lagMs;
    private int windowSize;
    private int slideBy;
    private int jobCount;
    private ExecutorService producerExecutorService;

    public static void main(String[] args) throws Exception {
        new SnapshotTest().run(args);
    }

    public void init() {
        producerExecutorService = Executors.newSingleThreadExecutor();
        brokerUri = property("brokerUri", "localhost:9092");
        offsetReset = property("offsetReset", "earliest");
        lagMs = propertyInt("lagMs", DEFAULT_LAG);
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);
        windowSize = propertyInt("windowSize", DEFAULT_WINDOW_SIZE);
        slideBy = propertyInt("slideBy", DEFAULT_SLIDE_BY);
        jobCount = propertyInt("jobCount", 2);
        countPerTicker = propertyInt("countPerTicker", DEFAULT_COUNTER_PER_TICKER);
        durationInMillis = durationInMillis();

        producerExecutorService.submit(() -> {
            try (SnapshotTradeProducer tradeProducer = new SnapshotTradeProducer(brokerUri)) {
                tradeProducer.produce(TOPIC, countPerTicker);
            }
        });
    }

    public void test() throws Exception {
        logger.info("SnapshotTest jobCount: " + jobCount);
        Job[] atLeastOnceJobs = submitJobs(AT_LEAST_ONCE);
        Job[] exactlyOnceJobs = submitJobs(EXACTLY_ONCE);

        int windowCount = windowSize / slideBy;
        LoggingService loggingService = jet.getHazelcastInstance().getLoggingService();
        QueueVerifier atLeastOnceVerifier = new QueueVerifier(loggingService,
                "Verifier[" + AT_LEAST_ONCE + "]", windowCount * jobCount);
        QueueVerifier exactlyOnceVerifier = new QueueVerifier(loggingService,
                "Verifier[" + EXACTLY_ONCE + "]", windowCount * jobCount);
        atLeastOnceVerifier.start();
        exactlyOnceVerifier.start();

        KafkaConsumer<Long, Long> consumer = new KafkaConsumer<>(kafkaPropsForResults(brokerUri, offsetReset));
        List<String> topicList = new ArrayList<>();
        for (int i = 0; i < jobCount; i++) {
            topicList.add(resultsTopicName(AT_LEAST_ONCE, i));
            topicList.add(resultsTopicName(EXACTLY_ONCE, i));
        }
        consumer.subscribe(topicList);

        try {
            long begin = System.currentTimeMillis();
            while (System.currentTimeMillis() - begin < durationInMillis) {
                ConsumerRecords<Long, Long> records = consumer.poll(POLL_TIMEOUT);
                records.iterator().forEachRemaining(r -> {
                            String topic = r.topic();
                            if (topic.contains(AT_LEAST_ONCE.name())) {
                                assertTrue(topic + " -> Unexpected count for " + r.key() + ", count: " +
                                        r.value(), r.value() >= countPerTicker);
                                atLeastOnceVerifier.offer(r.key());
                            } else {
                                assertEquals(topic + " -> Unexpected count for " + r.key(),
                                        countPerTicker, (long) r.value());
                                exactlyOnceVerifier.offer(r.key());
                            }
                        }
                );
            }
        } finally {
            logger.info("Cancelling jobs...");
            consumer.close();
            atLeastOnceVerifier.close();
            exactlyOnceVerifier.close();

            for (Job job : atLeastOnceJobs) {
                job.cancel();
            }
            for (Job job : exactlyOnceJobs) {
                job.cancel();
            }
        }
    }

    public void teardown() {
        if (producerExecutorService != null) {
            producerExecutorService.shutdown();
        }
    }

    private Pipeline pipeline(ProcessingGuarantee guarantee, int jobIndex) {
        String resultTopic = resultsTopicName(guarantee, jobIndex);
        Pipeline pipeline = Pipeline.create();

        Properties propsForTrades = kafkaPropsForTrades(brokerUri, offsetReset);
        Properties propsForResult = kafkaPropsForResults(brokerUri, offsetReset);

        pipeline.drawFrom(KafkaSources.kafka(propsForTrades, ConsumerRecord<Long, Long>::value, TOPIC))
                .withTimestamps(t -> t, lagMs)
                .setName(String.format("ReadKafka(%s-%s-%d)", TOPIC, guarantee, jobIndex))
                .window(sliding(windowSize, slideBy))
                .groupingKey(wholeItem())
                .aggregate(counting()).setName(String.format("AggregateCount(%s-%s-%d)", TOPIC, guarantee, jobIndex))
                .drainTo(KafkaSinks.kafka(propsForResult, resultTopic))
                .setName(String.format("WriteKafka(%s)", resultTopic));
        return pipeline;
    }

    private Job[] submitJobs(ProcessingGuarantee guarantee) {
        Job[] jobs = new Job[jobCount];
        for (int i = 0; i < jobCount; i++) {
            System.out.println(String.format("Executing %s test[%d] job..", guarantee.name(), i));
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(String.format("SnapshotTest(%s[%d])", guarantee.name(), i));
            jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
            jobConfig.setProcessingGuarantee(guarantee);

            jobs[i] = jet.newJob(pipeline(guarantee, i), jobConfig);
        }
        return jobs;
    }

    private static String resultsTopicName(ProcessingGuarantee guarantee, int jobIndex) {
        return RESULTS_TOPIC + "-" + guarantee.name() + "-" + jobIndex;
    }

    private static Properties kafkaPropsForTrades(String brokerUrl, String offsetReset) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUrl);
        props.setProperty("group.id", UuidUtil.newUnsecureUuidString());
        props.setProperty("key.deserializer", LongDeserializer.class.getName());
        props.setProperty("value.deserializer", LongDeserializer.class.getName());
        props.setProperty("auto.offset.reset", offsetReset);
        props.setProperty("max.poll.records", "32768");
        return props;
    }

    private static Properties kafkaPropsForResults(String brokerUrl, String offsetReset) {
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
}
