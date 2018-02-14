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

package com.hazelcast.jet.tests.snapshot;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.core.WindowDefinition;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.server.JetBootstrap;
import com.hazelcast.util.UuidUtil;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import tests.snapshot.QueueVerifier;
import tests.snapshot.SnapshotTradeProducer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.Partitioner.HASH_CODE;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.emitByFrame;
import static com.hazelcast.jet.core.WatermarkGenerationParams.wmGenParams;
import static com.hazelcast.jet.core.WatermarkPolicies.withFixedLag;
import static com.hazelcast.jet.core.WindowDefinition.slidingWindowDef;
import static com.hazelcast.jet.core.processor.KafkaProcessors.streamKafkaP;
import static com.hazelcast.jet.core.processor.KafkaProcessors.writeKafkaP;
import static com.hazelcast.jet.core.processor.Processors.accumulateByFrameP;
import static com.hazelcast.jet.core.processor.Processors.combineToSlidingWindowP;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class SnapshotTest {

    private static final int POLL_TIMEOUT = 1000;

    private JetInstance jet;
    private String brokerUri;
    private String offsetReset;
    private String topic;
    private long durationInMillis;
    private int countPerTicker;
    private int snapshotIntervalMs;
    private int lagMs;
    private int windowSize;
    private int slideBy;
    private ExecutorService producerExecutorService;

    public static void main(String[] args) {
        JUnitCore.main(SnapshotTest.class.getName());
    }

    @Before
    public void setUp() throws Exception {
        String isolatedClientConfig = System.getProperty("isolatedClientConfig");
        if (isolatedClientConfig != null) {
            System.setProperty("hazelcast.client.config", isolatedClientConfig);
        }
        producerExecutorService = Executors.newSingleThreadExecutor();
        brokerUri = System.getProperty("brokerUri", "localhost:9092");
        topic = System.getProperty("topic", String.format("%s-%d", "snapshot", System.currentTimeMillis()));
        offsetReset = System.getProperty("offsetReset", "earliest");
        lagMs = Integer.parseInt(System.getProperty("lagMs", "3000"));
        snapshotIntervalMs = Integer.parseInt(System.getProperty("snapshotIntervalMs", "1000"));
        windowSize = Integer.parseInt(System.getProperty("windowSize", "20"));
        slideBy = Integer.parseInt(System.getProperty("slideBy", "10"));
        countPerTicker = Integer.parseInt(System.getProperty("countPerTicker", "1000"));
        durationInMillis = MINUTES.toMillis(Integer.parseInt(System.getProperty("durationInMinutes", "10")));
        jet = JetBootstrap.getInstance();

        producerExecutorService.submit(() -> {
            try (SnapshotTradeProducer tradeProducer = new SnapshotTradeProducer(brokerUri)) {
                tradeProducer.produce(topic, countPerTicker);
            }
        });
    }

    @Test
    public void snapshotTest() throws Exception {
        String atLeastOnceTopic = resultsTopicName(AT_LEAST_ONCE);
        String exactlyOnceTopic = resultsTopicName(EXACTLY_ONCE);

        Job atLeastOnceJob = submitJob(AT_LEAST_ONCE);
        Job exactlyOnceJob = submitJob(EXACTLY_ONCE);

        int windowCount = windowSize / slideBy;
        QueueVerifier atLeastOnceVerifier = new QueueVerifier(AT_LEAST_ONCE.name(), windowCount);
        QueueVerifier exactlyOnceVerifier = new QueueVerifier(EXACTLY_ONCE.name(), windowCount);
        atLeastOnceVerifier.start();
        exactlyOnceVerifier.start();

        KafkaConsumer<Long, Long> consumer = new KafkaConsumer<>(kafkaPropsForResults(brokerUri, offsetReset));
        List<String> topicList = new ArrayList<>();
        topicList.add(atLeastOnceTopic);
        topicList.add(exactlyOnceTopic);
        consumer.subscribe(topicList);

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            ConsumerRecords<Long, Long> records = consumer.poll(POLL_TIMEOUT);
            records.iterator().forEachRemaining(r -> {
                        if (r.topic().equals(atLeastOnceTopic)) {
                            assertTrue("AT_LEAST_ONCE -> Unexpected count for " + r.key() + ", count: " +
                                    r.value(), r.value() >= countPerTicker);
                            atLeastOnceVerifier.offer(r.key());
                        } else {
                            assertEquals("EXACTLY_ONCE -> Unexpected count for " + r.key(),
                                    countPerTicker, (long) r.value());
                            exactlyOnceVerifier.offer(r.key());
                        }
                    }
            );
        }
        System.out.println("Cancelling jobs..");
        consumer.close();
        atLeastOnceVerifier.close();
        exactlyOnceVerifier.close();

        atLeastOnceJob.cancel();
        exactlyOnceJob.cancel();
        while (atLeastOnceJob.getStatus() != COMPLETED || exactlyOnceJob.getStatus() != COMPLETED) {
            SECONDS.sleep(1);
        }
    }

    @After
    public void tearDown() throws Exception {
        jet.shutdown();
        producerExecutorService.shutdown();
    }

    private DAG testDAG(String resultTopic) {
        WindowDefinition windowDef = slidingWindowDef(windowSize, slideBy);
        AggregateOperation1<Object, LongAccumulator, Long> counting = AggregateOperations.counting();

        DAG dag = new DAG();
        Properties properties = kafkaPropsForTrades(brokerUri, offsetReset);
        WatermarkGenerationParams<Long> wmGenParams = wmGenParams((Long t) -> t, withFixedLag(lagMs),
                emitByFrame(windowDef), 10_000);
        Vertex readKafka = dag.newVertex("read-kafka", streamKafkaP(properties, (key, value) -> (long) value,
                wmGenParams, topic));
        Vertex accumulateByF = dag.newVertex("accumulate-by-frame", accumulateByFrameP(
                wholeItem(), (Long t) -> t, TimestampKind.EVENT, windowDef, counting)
        );
        Vertex slidingW = dag.newVertex("sliding-window", combineToSlidingWindowP(windowDef, counting));
        Vertex formatOutput = dag.newVertex("format-output",
                mapP((TimestampedEntry entry) -> {
                    long timeMs = currentTimeMillis();
                    long latencyMs = timeMs - entry.getTimestamp();
                    return String.format("%d,%s,%s,%d,%d", entry.getTimestamp(), entry.getKey(), entry.getValue(),
                            timeMs, latencyMs);
                }));
        Vertex sink = dag.newVertex("write-kafka", writeKafkaP(kafkaPropsForResults(brokerUri, offsetReset),
                resultTopic,
                (String s) -> Long.valueOf(s.split(",")[1]),
                (String s) -> Long.valueOf(s.split(",")[2])
        ));

        dag
                .edge(between(readKafka, accumulateByF).partitioned(wholeItem(), HASH_CODE))
                .edge(between(accumulateByF, slidingW).partitioned(entryKey())
                                                      .distributed())
                .edge(between(slidingW, formatOutput).isolated())
                .edge(between(formatOutput, sink));
        return dag;
    }

    private String resultsTopicName(ProcessingGuarantee guarantee) {
        return topic + "-results-" + guarantee.name();
    }

    private Job submitJob(ProcessingGuarantee guarantee) {
        System.out.println(String.format("Executing %s test job..", guarantee.name()));
        JobConfig jobConfig = new JobConfig();
        jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
        jobConfig.setProcessingGuarantee(guarantee);
        String resultTopic = resultsTopicName(guarantee);
        return jet.newJob(testDAG(resultTopic), jobConfig);
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
