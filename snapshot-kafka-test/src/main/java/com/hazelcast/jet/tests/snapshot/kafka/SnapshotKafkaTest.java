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

package com.hazelcast.jet.tests.snapshot.kafka;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.kafka.KafkaSinks;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.jet.tests.common.QueueVerifier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
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
import java.util.concurrent.Future;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SnapshotKafkaTest extends AbstractSoakTest {

    private static final int DEFAULT_LAG = 3000;
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 1000;
    private static final int DEFAULT_WINDOW_SIZE = 20;
    private static final int DEFAULT_SLIDE_BY = 10;
    private static final int DEFAULT_COUNTER_PER_TICKER = 1000;

    private static final String TOPIC = SnapshotKafkaTest.class.getSimpleName();
    private static final String RESULTS_TOPIC = TOPIC + "-RESULTS";
    private static final int POLL_TIMEOUT = 1000;
    private static final int DELAY_AFTER_TEST_FINISHED_FACTOR = 60;

    private ClientConfig stableClusterClientConfig;
    private JetInstance stableClusterClient;

    private String brokerUri;
    private String offsetReset;
    private int countPerTicker;
    private int snapshotIntervalMs;
    private int lagMs;
    private int windowSize;
    private int slideBy;
    private int jobCount;

    private transient ExecutorService producerExecutorService;
    private transient Future<?> producerFuture;

    public static void main(String[] args) throws Exception {
        new SnapshotKafkaTest().run(args);
    }

    @Override
    public void init() throws Exception {
        producerExecutorService = Executors.newSingleThreadExecutor();
        brokerUri = property("brokerUri", "localhost:9092");
        offsetReset = property("offsetReset", "earliest");
        lagMs = propertyInt("lagMs", DEFAULT_LAG);
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);
        windowSize = propertyInt("windowSize", DEFAULT_WINDOW_SIZE);
        slideBy = propertyInt("slideBy", DEFAULT_SLIDE_BY);
        jobCount = propertyInt("jobCount", 2);
        countPerTicker = propertyInt("countPerTicker", DEFAULT_COUNTER_PER_TICKER);

        stableClusterClientConfig = remoteClusterClientConfig();
        stableClusterClient = Jet.newJetClient(stableClusterClientConfig);

        ILogger producerLogger = getLogger(SnapshotTradeProducer.class);
        producerFuture = producerExecutorService.submit(() -> {
            try (SnapshotTradeProducer tradeProducer = new SnapshotTradeProducer(brokerUri, producerLogger)) {
                tradeProducer.produce(TOPIC, countPerTicker);
            } catch (Exception e) {
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
        long extraDuration = DELAY_AFTER_TEST_FINISHED_FACTOR * (SECONDS.toMillis(POLL_TIMEOUT));
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
        logger.info("[" + name + "] SnapshotTest jobCount: " + jobCount);
        Job[] atLeastOnceJobs = submitJobs(client, name, AT_LEAST_ONCE);
        Job[] exactlyOnceJobs = submitJobs(client, name, EXACTLY_ONCE);

        int windowCount = windowSize / slideBy;
        LoggingService loggingService = jet.getHazelcastInstance().getLoggingService();
        QueueVerifier atLeastOnceVerifier = new QueueVerifier(loggingService,
                "Verifier[" + name + ", " + AT_LEAST_ONCE + "]", windowCount * jobCount);
        QueueVerifier exactlyOnceVerifier = new QueueVerifier(loggingService,
                "Verifier[" + name + ", " + EXACTLY_ONCE + "]", windowCount * jobCount);
        atLeastOnceVerifier.start();
        exactlyOnceVerifier.start();

        KafkaConsumer<Long, Long> consumer = new KafkaConsumer<>(kafkaPropsForVerifier(brokerUri, offsetReset));
        List<String> topicList = new ArrayList<>();
        for (int i = 0; i < jobCount; i++) {
            topicList.add(resultsTopicName(name, AT_LEAST_ONCE, i));
            topicList.add(resultsTopicName(name, EXACTLY_ONCE, i));
        }
        consumer.subscribe(topicList);

        try {
            long begin = System.currentTimeMillis();
            while (System.currentTimeMillis() - begin < durationInMillis) {
                ConsumerRecords<Long, Long> records = consumer.poll(POLL_TIMEOUT);
                records.iterator().forEachRemaining(r -> {
                            String topic = r.topic();
                            if (topic.contains(AT_LEAST_ONCE.name())) {
                                assertTrue("[" + name + "] " + topic + " -> Unexpected count for " + r.key() + ", "
                                        + "count: " + r.value(),
                                        r.value() >= countPerTicker);
                                atLeastOnceVerifier.offer(r.key());
                            } else {
                                assertEquals("[" + name + "] " + topic + " -> Unexpected count for " + r.key() + ", "
                                        + "count: " + r.value(),
                                        countPerTicker, (long) r.value());
                                exactlyOnceVerifier.offer(r.key());
                            }
                        }
                );
                assertFalse(producerFuture.isDone());
            }
        } finally {
            logger.info("[" + name + "] Cancelling jobs...");
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

    protected void teardown(Throwable t) throws Exception {
        if (producerExecutorService != null) {
            producerExecutorService.shutdown();
        }
    }

    private Pipeline pipeline(String name, ProcessingGuarantee guarantee, int jobIndex) {
        String resultTopic = resultsTopicName(name, guarantee, jobIndex);
        Pipeline pipeline = Pipeline.create();

        Properties propsForTrades = kafkaPropsForTrades(brokerUri, offsetReset);
        Properties propsForResult = kafkaPropsForResults(brokerUri, offsetReset);

        pipeline.readFrom(KafkaSources.kafka(propsForTrades, ConsumerRecord<Long, Long>::value, TOPIC))
                .withTimestamps(t -> t, lagMs)
                .setName(String.format("ReadKafka(%s-%s-%d)", TOPIC, guarantee, jobIndex))
                .window(sliding(windowSize, slideBy))
                .groupingKey(wholeItem())
                .aggregate(counting()).setName(String.format("AggregateCount(%s-%s-%d)", TOPIC, guarantee, jobIndex))
                .writeTo(KafkaSinks.kafka(propsForResult, resultTopic))
                .setName(String.format("WriteKafka(%s)", resultTopic));
        return pipeline;
    }

    private Job[] submitJobs(JetInstance client, String name, ProcessingGuarantee guarantee) {
        Job[] jobs = new Job[jobCount];
        for (int i = 0; i < jobCount; i++) {
            System.out.println(String.format("[%s] Executing %s test[%d] job..", name, guarantee.name(), i));
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(String.format("SnapshotTest(%s[%d])", guarantee.name(), i));
            jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
            jobConfig.setProcessingGuarantee(guarantee);

            jobs[i] = client.newJob(pipeline(name, guarantee, i), jobConfig);
        }
        return jobs;
    }

    private static String resultsTopicName(String name, ProcessingGuarantee guarantee, int jobIndex) {
        return RESULTS_TOPIC + "-" + name + "-" + guarantee.name() + "-" + jobIndex;
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

    private static Properties kafkaPropsForVerifier(String brokerUrl, String offsetReset) {
        Properties props = kafkaPropsForResults(brokerUrl, offsetReset);
        props.setProperty("isolation.level", "read_committed");
        return props;
    }
}
