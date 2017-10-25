package com.hazelcast.jet.tests.kafka;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.WatermarkPolicies;
import com.hazelcast.jet.core.WindowDefinition;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.server.JetBootstrap;
import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testpackage.VisibleAssertions;
import test.kafka.Trade;
import test.kafka.TradeDeserializer;
import test.kafka.TradeProducer;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Partitioner.HASH_CODE;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.emitByFrame;
import static com.hazelcast.jet.core.WindowDefinition.slidingWindowDef;
import static com.hazelcast.jet.core.processor.KafkaProcessors.streamKafkaP;
import static com.hazelcast.jet.core.processor.Processors.accumulateByFrameP;
import static com.hazelcast.jet.core.processor.Processors.combineToSlidingWindowP;
import static com.hazelcast.jet.core.processor.Processors.insertWatermarksP;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeListP;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;

@RunWith(JUnit4.class)
public class LongRunningKafkaTest {

    private String brokerUri;
    private String topic;
    private String offsetReset;
    private int lagMs;
    private int windowSize;
    private int slideBy;
    private String outputList;
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
        lagMs = Integer.parseInt(System.getProperty("lagMs", "10"));
        windowSize = Integer.parseInt(System.getProperty("windowSize", "20"));
        slideBy = Integer.parseInt(System.getProperty("slideBy", "10"));
        outputList = System.getProperty("outputList", "jet-output-" + System.currentTimeMillis());
        countPerTicker = Integer.parseInt(System.getProperty("countPerTicker", "100"));
        durationInMinutes = Integer.parseInt(System.getProperty("durationInMinutes", "1"));
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
        WindowDefinition windowDef = slidingWindowDef(windowSize, slideBy);
        AggregateOperation1<Object, LongAccumulator, Long> counting = AggregateOperations.counting();

        DAG dag = new DAG();
        Vertex readKafka = dag.newVertex("read-kafka", streamKafkaP(kafkaProps, (key, value) -> value, topic));
        Vertex insertPunctuation = dag.newVertex("insert-punctuation",
                insertWatermarksP(Trade::getTime, WatermarkPolicies.withFixedLag(lagMs), emitByFrame(windowDef)));
        Vertex accumulateByF = dag.newVertex("accumulate-by-frame",
                accumulateByFrameP(Trade::getTicker, Trade::getTime, TimestampKind.EVENT, windowDef, counting));
        Vertex slidingW = dag.newVertex("sliding-window", combineToSlidingWindowP(windowDef, counting));
        Vertex formatOutput = dag.newVertex("format-output",
                mapP((TimestampedEntry entry) -> {
                    long timeMs = currentTimeMillis();
                    long latencyMs = timeMs - entry.getTimestamp();
                    return String.format("%d,%s,%s,%d,%d", entry.getTimestamp(), entry.getKey(), entry.getValue(),
                            timeMs, latencyMs);
                }));
        Vertex listSink = dag.newVertex("write-list", writeListP(outputList));

        dag
                .edge(between(readKafka, insertPunctuation).isolated())
                .edge(between(insertPunctuation, accumulateByF).partitioned(Trade::getTicker, HASH_CODE))
                .edge(between(accumulateByF, slidingW).partitioned(entryKey())
                                                      .distributed())
                .edge(between(slidingW, formatOutput).isolated())
                .edge(between(formatOutput, listSink));

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
        ArrayList<String> localList = new ArrayList<>(jet.getList(outputList));
        localList.forEach(System.out::println);
        boolean result = localList.stream()
                                  .collect(Collectors.groupingBy(
                                          l -> l.split(",")[0], Collectors.mapping(
                                                  l -> {
                                                      String[] split = l.split(",");
                                                      return new SimpleImmutableEntry<>(split[1], split[2]);
                                                  }, Collectors.<Entry>toSet()
                                          )
                                          )
                                  )
                                  .entrySet()
                                  .stream()
                                  .filter(windowSet -> windowSet.getValue().size() == windowSize)
                                  .findFirst()
                                  .get()
                                  .getValue()
                                  .stream()
                                  .allMatch(countedTicker -> countedTicker.getValue().equals(valueOf(countPerTicker)));
        VisibleAssertions.assertTrue("tick count per window matches", result);
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
