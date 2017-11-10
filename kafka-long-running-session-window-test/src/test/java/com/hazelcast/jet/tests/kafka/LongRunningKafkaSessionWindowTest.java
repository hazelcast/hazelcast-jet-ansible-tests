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
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.WatermarkPolicies;
import com.hazelcast.jet.datamodel.Session;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.server.JetBootstrap;
import com.hazelcast.util.UuidUtil;
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
import tests.kafka.VerificationSink;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Edge.from;
import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RESTARTING;
import static com.hazelcast.jet.core.Partitioner.HASH_CODE;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.emitByMinStep;
import static com.hazelcast.jet.core.processor.KafkaProcessors.streamKafkaP;
import static com.hazelcast.jet.core.processor.KafkaProcessors.writeKafkaP;
import static com.hazelcast.jet.core.processor.Processors.aggregateToSessionWindowP;
import static com.hazelcast.jet.core.processor.Processors.insertWatermarksP;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

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
    public void setUp() throws Exception {
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
    public void kafkaTest() throws IOException, ExecutionException, InterruptedException {
        System.out.println("Executing job..");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setSnapshotIntervalMillis(5000);
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);

        Job testJob = jet.newJob(testDAG(), jobConfig);

        System.out.println("Executing verification job..");
        Job verificationJob = jet.newJob(verificationDAG());

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            MINUTES.sleep(1);
            JobStatus status = verificationJob.getJobStatus();
            if (status == RESTARTING || status == FAILED || status == COMPLETED) {
                throw new AssertionError("Job is failed, jobStatus: " + status);
            }
        }
        System.out.println("Cancelling jobs..");

        testJob.getFuture().cancel(true);
        verificationJob.getFuture().cancel(true);

        while (testJob.getJobStatus() != COMPLETED ||
                verificationJob.getJobStatus() != COMPLETED) {
            SECONDS.sleep(1);
        }
    }

    private DAG testDAG() {
        DAG dag = new DAG();

        Vertex readKafka = dag.newVertex("read-kafka", streamKafkaP(kafkaProps, (key, value) -> value, topic));

        Vertex insertWm = dag.newVertex("insert-watermark",
                insertWatermarksP(Trade::getTime, WatermarkPolicies.withFixedLag(lagMs),
                        emitByMinStep(lagMs)));

        Vertex aggregateSessionWindow = dag.newVertex("session-window",
                aggregateToSessionWindowP(sessionTimeout, Trade::getTime,
                        Trade::getTicker,
                        counting()));

        Vertex writeResultsToKafka = dag.newVertex("write-results-kafka",
                writeKafkaP(kafkaPropertiesForResults(brokerUri, offsetReset), topic + "-result",
                        (DistributedFunction<Session<String, Long>, String>) Session::getKey,
                        (DistributedFunction<Session<String, Long>, Long>) Session::getResult));

        dag
                .edge(between(readKafka, insertWm).isolated())
                .edge(between(insertWm, aggregateSessionWindow)
                        .partitioned(Trade::getTicker, HASH_CODE)
                        .distributed()
                )
                .edge(from(aggregateSessionWindow).to(writeResultsToKafka));

        return dag;
    }


    private DAG verificationDAG() {
        DAG dag = new DAG();

        Vertex readVerificationRecords = dag.newVertex("read-verification-kafka",
                streamKafkaP(kafkaPropertiesForResults(brokerUri, offsetReset), topic + "-result"));

        long countCopy = countPerTicker;
        Vertex verifyRecords = dag.newVertex("verification", () -> new VerificationSink(countCopy));


        dag.edge(from(readVerificationRecords).to(verifyRecords));
        return dag;
    }

    @After
    public void tearDown() throws Exception {
        jet.shutdown();
        producerExecutorService.shutdown();
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
