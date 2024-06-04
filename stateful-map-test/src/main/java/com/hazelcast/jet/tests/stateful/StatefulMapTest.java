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

package com.hazelcast.jet.tests.stateful;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.kafka.KafkaSinks;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.tests.common.Util.cancelJobAndJoin;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Pipeline reads messages from kafka, transform them to TransactionEvents and
 * use mapStateful for processing them. Results are written to kafka. Job uses
 * exactly once processing guarantee.
 *
 * We have two kafka sinks:
 *   - for events with positive IDs.
 *   - for events with negative IDs.
 *
 * Each item with positive ID is expected to have start and end event. Once
 * when both of them are processed by mapStateful it emits item to sink for
 * positive IDs.
 *
 * Each item with negative ID is expected to have only start event (they serve
 * for testing eviction).
 *
 * Verification:
 *   - for positive IDs we check that every item was correctly processed by
 *     pipeline (i.e. that it has expected start and end event).
 *   - for negative IDs we check that there was exactly
 *     "emitted items count"/generatorBatchCount. This is checked at the end
 *     of the test.
 */
public class StatefulMapTest extends AbstractJetSoakTest {

    static final long TIMED_OUT_CODE = -1;
    static final int WAIT_TX_TIMEOUT_FACTOR = 4;

    private static final String TOPIC_PREFIX = StatefulMapTest.class.getSimpleName();
    private static final String TX_TOPIC_PREFIX = TOPIC_PREFIX + "tx";
    private static final String TX_TIMEOUT_TOPIC_PREFIX = TOPIC_PREFIX + "txTimeout";
    private static final int DEFAULT_TX_TIMEOUT = 5000;
    private static final int DEFAULT_GENERATOR_BATCH_COUNT = 100;
    private static final int DEFAULT_TX_PER_SECOND = 1000;
    private static final int DEFAULT_SNAPSHOT_INTERVAL_MILLIS = 5000;
    private static final int ASSERTION_RETRY_COUNT = 60;
    private static final int POLL_TIMEOUT = 1000;

    private String brokerUri;
    private int txTimeout;
    private int txPerSecond;
    private int generatorBatchCount;
    private int snapshotIntervalMillis;

    public static void main(String[] args) throws Exception {
        new StatefulMapTest().run(args);
    }

    @Override
    protected void init(HazelcastInstance client) throws Exception {
        brokerUri = property("brokerUri", "localhost:9092");
        txTimeout = propertyInt("txTimeout", DEFAULT_TX_TIMEOUT);
        txPerSecond = propertyInt("txPerSecond", DEFAULT_TX_PER_SECOND);
        generatorBatchCount = propertyInt("generatorBatchCount", DEFAULT_GENERATOR_BATCH_COUNT);
        snapshotIntervalMillis = propertyInt("snapshotIntervalMillis", DEFAULT_SNAPSHOT_INTERVAL_MILLIS);
    }

    @Override
    protected boolean runOnBothClusters() {
        return true;
    }

    @Override
    public void test(HazelcastInstance client, String name) throws Exception {
        KafkaTradeProducer producer = new KafkaTradeProducer(
                logger, brokerUri, TOPIC_PREFIX + name, txPerSecond, generatorBatchCount, txTimeout);
        producer.start();

        KafkaSinkVerifier verifier = new KafkaSinkVerifier(name, brokerUri, TX_TOPIC_PREFIX + name, logger);
        verifier.start();

        KafkaConsumer<Long, Long> txTimeoutConsumer = prepareTxTimeoutConsumer(name);
        long txTimoutCount = 0;

        JobConfig jobConfig = new JobConfig()
                .setName(name)
                .setProcessingGuarantee(EXACTLY_ONCE)
                .setSnapshotIntervalMillis(snapshotIntervalMillis);

        Job job = client.getJet().newJob(buildPipeline(name), jobConfig);

        long begin = System.currentTimeMillis();
        try {
            while (System.currentTimeMillis() - begin < durationInMillis) {
                if (getJobStatusWithRetry(job) == FAILED) {
                    job.join();
                }
                verifier.checkStatus();
                producer.checkStatus();
                txTimoutCount += processTimeoutItems(name, txTimeoutConsumer);
                sleepMinutes(1);
            }
        } catch (Throwable ex) {
            producer.finish();
            throw ex;
        }
        verifier.checkStatus();
        txTimoutCount += processTimeoutItems(name, txTimeoutConsumer);

        logger.info("[" + name + "] Stop message generation");
        long emittedItemsCount = producer.finish();
        logger.info("[" + name + "] Emitted items: " + emittedItemsCount);

        sleepMillis(WAIT_TX_TIMEOUT_FACTOR * txTimeout);

        assertCountEventually(name, emittedItemsCount, verifier);
        verifier.finish();

        verifyTimeoutTx(name, emittedItemsCount, txTimoutCount, txTimeoutConsumer);
        txTimeoutConsumer.close();

        cancelJobAndJoin(client, job);
    }

    private KafkaConsumer<Long, Long> prepareTxTimeoutConsumer(String name) {
        KafkaConsumer<Long, Long> txTimeoutConsumer = new KafkaConsumer<>(kafkaPropertiesForTimeoutVerifier());
        List<String> topicList = new ArrayList<>();
        topicList.add(TX_TIMEOUT_TOPIC_PREFIX + name);
        txTimeoutConsumer.subscribe(topicList);
        return txTimeoutConsumer;
    }

    private long processTimeoutItems(String name, KafkaConsumer<Long, Long> txTimeoutConsumer) {
        ConsumerRecords<Long, Long> records = txTimeoutConsumer.poll(Duration.ofMillis(POLL_TIMEOUT));
        for (ConsumerRecord<Long, Long> record : records) {
            if (record.value() != -1) {
                throw new AssertionError(
                        String.format("[%s] Unexpected value for timeout item with id %s. expected: 0, but was: %s",
                                name, record.key(), record.value()));
            }
        }
        return records.count();
    }

    private static void assertCountEventually(String name, long expectedCount, KafkaSinkVerifier verifier)
            throws Exception {
        for (int i = 0; i < ASSERTION_RETRY_COUNT; i++) {
            long processedCount = verifier.getProcessedCount();
            if (expectedCount == processedCount) {
                return;
            }
            SECONDS.sleep(1);
        }
        long actualTotalCount = verifier.getProcessedCount();
        assertEquals(String.format("[%s] expected: %d, but was: %d", name, expectedCount, actualTotalCount),
                expectedCount, actualTotalCount);
    }

    private void verifyTimeoutTx(String name, long emittedItemsCount, long txTimoutCount,
            KafkaConsumer<Long, Long> txTimeoutConsumer) throws Exception {
        long count = txTimoutCount;
        long expectedCount = emittedItemsCount / generatorBatchCount;
        for (int i = 0; i < ASSERTION_RETRY_COUNT; i++) {
            count += processTimeoutItems(name, txTimeoutConsumer);
            if (expectedCount == count) {
                return;
            }
            SECONDS.sleep(1);
        }
        assertEquals(expectedCount, count);
    }

    private Pipeline buildPipeline(String clusterName) {
        Pipeline p = Pipeline.create();
        StreamSource<TransactionEvent> source = KafkaSources.<String, Long, TransactionEvent>kafka(
                kafkaPropertiesForSource(),
                t -> {
                    String key = t.key();
                    String[] split = key.split(",");
                    return new TransactionEvent(
                            Integer.parseInt(split[0]), Long.parseLong(split[1]), t.value());
                },
                TOPIC_PREFIX + clusterName);

        StreamStage<Map.Entry<Long, Long>> streamStage =
                p.readFrom(source)
                 .withTimestamps(TransactionEvent::timestamp, 0)
                 .groupingKey(TransactionEvent::transactionId)
                 .mapStateful(
                         txTimeout,
                         () -> new TransactionEvent[2],
                         (startEnd, transactionId, transactionEvent) -> {
                             if (transactionId == Long.MAX_VALUE) {
                                 // ignore events with txId=Long.MAX_VALUE, these are
                                 // for advancing the wm.
                                 return null;
                             }
                             switch (transactionEvent.type()) {
                                 case 0:
                                     startEnd[0] = transactionEvent;
                                     break;
                                 case 1:
                                     startEnd[1] = transactionEvent;
                                     break;
                                 default:
                                     System.out.println("Wrong event in the stream: " + transactionEvent.type());
                             }
                             TransactionEvent startEvent = startEnd[0];
                             TransactionEvent endEvent = startEnd[1];
                             return (startEvent != null && endEvent != null) ?
                                     entry(transactionId, endEvent.timestamp() - startEvent.timestamp()) : null;
                         },
                         (startEnd, transactionId, wm) -> {
                             if (startEnd[0] != null && startEnd[1] == null) {
                                 if (transactionId > 0) {
                                     System.out.println("StatefulMapTest Timeout for txId: " + transactionId);
                                 }
                                 return entry(transactionId, TIMED_OUT_CODE);
                             }
                             return null;
                         }
                 );

        streamStage
                .filter(e -> e.getKey() < 0)
                .writeTo(KafkaSinks.kafka(kafkaPropertiesForSink(), TX_TIMEOUT_TOPIC_PREFIX + clusterName));
        streamStage
                .filter(e -> e.getKey() >= 0)
                .writeTo(KafkaSinks.kafka(kafkaPropertiesForSink(), TX_TOPIC_PREFIX + clusterName));
        return p;
    }

    private Properties kafkaPropertiesForSource() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUri);
        props.setProperty("group.id", UuidUtil.newUnsecureUuidString());
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", LongDeserializer.class.getName());
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("max.poll.records", "32768");
        return props;
    }

    private Properties kafkaPropertiesForSink() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUri);
        props.setProperty("group.id", UuidUtil.newUnsecureUuidString());
        props.setProperty("key.serializer", LongSerializer.class.getName());
        props.setProperty("value.serializer", LongSerializer.class.getName());
        return props;
    }

    private Properties kafkaPropertiesForTimeoutVerifier() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUri);
        props.setProperty("group.id", UuidUtil.newUnsecureUuidString());
        props.setProperty("key.deserializer", LongDeserializer.class.getName());
        props.setProperty("value.deserializer", LongDeserializer.class.getName());
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("max.poll.records", "32768");
        props.setProperty("isolation.level", "read_committed");
        return props;
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
    }
}
