package com.hazelcast.jet.tests.kafka;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.WatermarkPolicies;
import com.hazelcast.jet.core.processor.DiagnosticProcessors;
import com.hazelcast.jet.server.JetBootstrap;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import test.kafka.HashJoinP;
import test.kafka.Trade;
import test.kafka.TradeDeserializer;
import test.kafka.TradeProducer;

import static com.hazelcast.jet.aggregate.AggregateOperations.summingDouble;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Edge.from;
import static com.hazelcast.jet.core.Partitioner.HASH_CODE;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.emitByMinStep;
import static com.hazelcast.jet.core.processor.KafkaProcessors.streamKafkaP;
import static com.hazelcast.jet.core.processor.Processors.aggregateToSessionWindowP;
import static com.hazelcast.jet.core.processor.Processors.insertWatermarksP;
import static com.hazelcast.jet.core.processor.Processors.noopP;

@RunWith(JUnit4.class)
public class LongRunningKafkaSessionWindowTest {

    private String brokerUri;
    private String topic;
    private String offsetReset;
    private int lagMs;
    private int countPerTicker;
    private Properties kafkaProps;
    private JetInstance jet;
    private int durationInMinutes;
    private ScheduledExecutorService scheduledExecutorService;
    private ExecutorService producerExecutorService;

    @Before
    public void setUp() throws Exception {
        scheduledExecutorService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
        producerExecutorService = Executors.newSingleThreadExecutor();
        brokerUri = System.getProperty("brokerUri", "localhost:9092");
        topic = System.getProperty("topic", String.format("%s-%d", "trades", System.currentTimeMillis()));
        offsetReset = System.getProperty("offsetReset", "earliest");
        lagMs = Integer.parseInt(System.getProperty("lagMs", "100"));
        countPerTicker = Integer.parseInt(System.getProperty("countPerTicker", "20"));
        durationInMinutes = Integer.parseInt(System.getProperty("durationInMinutes", "3"));
        kafkaProps = getKafkaProperties(brokerUri, offsetReset);
        jet = JetBootstrap.getInstance();

        producerExecutorService.submit(() -> {
            try (TradeProducer tradeProducer = new TradeProducer(brokerUri)) {
                tradeProducer.produce(topic, countPerTicker);
            }
        });
    }

    @Test
    public void kafkaTest() throws IOException, ExecutionException, InterruptedException {
        DAG dag = new DAG();
        Vertex readKafka = dag.newVertex("read-kafka", streamKafkaP(kafkaProps, (key, value) -> value, topic));
        Vertex insertPunctuation = dag.newVertex("insert-punctuation",
                insertWatermarksP(Trade::getTime, WatermarkPolicies.withFixedLag(lagMs), emitByMinStep(lagMs)));

        Vertex aggregateSessionWindow = dag.newVertex("session-window",
                aggregateToSessionWindowP(100, Trade::getTime, Trade::getTicker, summingDouble(Trade::getPrice)));

        Vertex readVerificationRecords = dag.newVertex("read-verification-kafka", streamKafkaP(kafkaProps, (key, value) -> value, topic + "-result"));

        Vertex joinResultsandVerify = dag.newVertex("verification", HashJoinP.getSupplier());
        Vertex logger = dag.newVertex("writer", DiagnosticProcessors.peekInputP(noopP()));


        dag
                .edge(between(readKafka, insertPunctuation).isolated())
                .edge(between(insertPunctuation, aggregateSessionWindow)
                        .partitioned(Trade::getTicker, HASH_CODE)
                        .distributed()
                )
                .edge(from(aggregateSessionWindow).to(joinResultsandVerify, 1))
                .edge(from(readVerificationRecords).broadcast().distributed().to(joinResultsandVerify, 0))
                .edge(between(joinResultsandVerify, logger));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setSnapshotIntervalMillis(5000);
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
        System.out.println("Executing job..");
        Future<Void> execute = jet.newJob(dag, jobConfig).getFuture();

        CountDownLatch latch = new CountDownLatch(1);
        scheduledExecutorService.schedule(() -> {
            try {
                System.out.println("Cancelling job...");
                execute.cancel(true);
                execute.get();
            } catch (Exception ignored) {
            }
            latch.countDown();
        }, durationInMinutes, TimeUnit.MINUTES);
        latch.await();
    }

    @After
    public void tearDown() throws Exception {
        jet.shutdown();
        producerExecutorService.shutdown();
        scheduledExecutorService.shutdown();
    }

    private static Properties getKafkaProperties(String brokerUrl, String offsetReset) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUrl);
        props.setProperty("group.id", UUID.randomUUID().toString());
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", TradeDeserializer.class.getName());
        props.setProperty("auto.offset.reset", offsetReset);
        props.setProperty("max.poll.records", "32768");
        return props;
    }


}
