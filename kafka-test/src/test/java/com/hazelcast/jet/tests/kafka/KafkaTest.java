package com.hazelcast.jet.tests.kafka;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.TimestampedEntry;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.WatermarkPolicies;
import com.hazelcast.jet.core.WindowDefinition;
import com.hazelcast.jet.server.JetBootstrap;
import com.hazelcast.jet.stream.IStreamList;
import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.LongDeserializer;
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
import static com.hazelcast.jet.function.DistributedFunctions.entryValue;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;

@RunWith(JUnit4.class)
public class KafkaTest {

    private String brokerUri;
    private String topic;
    private String offsetReset;
    private int lagMs;
    private int windowSize;
    private int slideBy;
    private String outputPath;
    private int tickerCount;
    private int countPerTicker;
    private Properties kafkaProps;

    @Before
    public void setUp() throws Exception {
        brokerUri = System.getProperty("brokerUri", "localhost:9092");
        topic = System.getProperty("topic", String.format("%s-%d", "trades", System.currentTimeMillis()));
        offsetReset = System.getProperty("offsetReset", "earliest");
        lagMs = Integer.parseInt(System.getProperty("lagMs", "1000"));
        windowSize = Integer.parseInt(System.getProperty("windowSize", "5000"));
        slideBy = Integer.parseInt(System.getProperty("slideBy", "1000"));
        outputPath = System.getProperty("outputPath", System.getProperty("user.dir") + "/jet-output");
        tickerCount = 20;
        countPerTicker = 100;
        kafkaProps = getKafkaProperties(brokerUri, offsetReset);

        try (TradeProducer tradeProducer = new TradeProducer(brokerUri)) {
            tradeProducer.produce(topic, tickerCount, countPerTicker);
        }
    }

    @Test
    public void kafkaTest() throws IOException, ExecutionException, InterruptedException {
        WindowDefinition windowDef = slidingWindowDef(windowSize, slideBy);
        AggregateOperation1<Object, LongAccumulator, Long> counting = AggregateOperations.counting();

        DAG dag = new DAG();
        Vertex readKafka = dag.newVertex("read-kafka", streamKafkaP(kafkaProps, topic))
                              .localParallelism(1);
        Vertex extractTrade = dag.newVertex("extract-trade", mapP(entryValue()));
        Vertex insertPunctuation = dag.newVertex("insert-punctuation",
                insertWatermarksP(Trade::getTime, WatermarkPolicies.limitingLagAndLull(lagMs, lagMs), emitByFrame(windowDef)));
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
        Vertex fileSink = dag.newVertex("write-file", writeListP(outputPath)).localParallelism(1);

        dag
                .edge(between(readKafka, extractTrade).isolated())
                .edge(between(extractTrade, insertPunctuation).isolated())
                .edge(between(insertPunctuation, accumulateByF).partitioned(Trade::getTicker, HASH_CODE))
                .edge(between(accumulateByF, slidingW).partitioned(entryKey())
                                                      .distributed())
                .edge(between(slidingW, formatOutput).isolated())
                .edge(between(formatOutput, fileSink));


        JetInstance jet = JetBootstrap.getInstance();
        System.out.println("Executing job..");
        Future<Void> execute = jet.newJob(dag).getFuture();

        try {
            Thread.sleep(5000);
            System.out.println("Cancelling job...");
            execute.cancel(true);
            execute.get();

            IStreamList<String> list = jet.getList(outputPath);
            boolean result = list.stream()
                                 .collect(Collectors.groupingBy(
                                         l -> l.split(",")[0], Collectors.mapping(
                                                 l -> {
                                                     String[] split = l.split(",");
                                                     return new SimpleImmutableEntry<>(split[1], split[2]);
                                                 }, Collectors.toSet()
                                         )
                                         )
                                 )
                                 .entrySet()
                                 .stream()
                                 .filter(windowSet -> windowSet.getValue().size() == tickerCount)
                                 .findFirst()
                                 .get()
                                 .getValue()
                                 .stream()
                                 .allMatch(countedTicker -> countedTicker.getValue().equals(valueOf(countPerTicker)));
            VisibleAssertions.assertTrue("tick count per window matches", result);
        } catch (Exception ignored) {
        } finally {
            jet.shutdown();
        }
    }


    private static Properties getKafkaProperties(String brokerUrl, String offsetReset) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUrl);
        props.setProperty("group.id", UUID.randomUUID().toString());
        props.setProperty("key.deserializer", LongDeserializer.class.getName());
        props.setProperty("value.deserializer", TradeDeserializer.class.getName());
        props.setProperty("auto.offset.reset", offsetReset);
        props.setProperty("max.poll.records", "32768");
        return props;
    }


}
