package com.hazelcast.jet.tests.kafka;

import com.hazelcast.jet.AggregateOperation;
import com.hazelcast.jet.AggregateOperations;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.TimestampKind;
import com.hazelcast.jet.TimestampedEntry;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.WatermarkPolicies;
import com.hazelcast.jet.WindowDefinition;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.server.JetBootstrap;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.WatermarkEmissionPolicy.emitByFrame;
import static com.hazelcast.jet.WindowDefinition.slidingWindowDef;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.function.DistributedFunctions.entryValue;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.processor.KafkaProcessors.streamKafka;
import static com.hazelcast.jet.processor.Processors.accumulateByFrame;
import static com.hazelcast.jet.processor.Processors.combineToSlidingWindow;
import static com.hazelcast.jet.processor.Processors.insertWatermarks;
import static com.hazelcast.jet.processor.Processors.map;
import static com.hazelcast.jet.processor.Sinks.writeFile;
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
        outputPath = System.getProperty("outputPath", "jet-output");
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
        AggregateOperation<Object, LongAccumulator, Long> counting = AggregateOperations.counting();

        DAG dag = new DAG();
        Vertex readKafka = dag.newVertex("read-kafka", streamKafka(kafkaProps, topic))
                              .localParallelism(1);
        Vertex extractTrade = dag.newVertex("extract-trade", map(entryValue()));
        Vertex insertPunctuation = dag.newVertex("insert-punctuation",
                insertWatermarks(Trade::getTime, WatermarkPolicies.limitingLagAndLull(lagMs, lagMs), emitByFrame(windowDef)));
        Vertex accumulateByF = dag.newVertex("accumulate-by-frame",
                accumulateByFrame(Trade::getTicker, Trade::getTime, TimestampKind.EVENT, windowDef, counting));
        Vertex slidingW = dag.newVertex("sliding-window", combineToSlidingWindow(windowDef, counting));
        Vertex formatOutput = dag.newVertex("format-output",
                map((TimestampedEntry entry) -> {
                    long timeMs = currentTimeMillis();
                    long latencyMs = timeMs - entry.getTimestamp();
                    return String.format("%d,%s,%s,%d,%d", entry.getTimestamp(), entry.getKey(), entry.getValue(),
                            timeMs, latencyMs);
                }));
        Vertex fileSink = dag.newVertex("write-file", writeFile(outputPath)).localParallelism(1);

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
        Future<Void> execute = jet.newJob(dag).execute();

        try {
            Thread.sleep(5000);
            System.out.println("Cancelling job...");
            execute.cancel(true);
            execute.get();
        } catch (Exception ignored) {
        } finally {
            jet.shutdown();
        }


        boolean result = Files.list(Paths.get(outputPath))
                              .flatMap(f -> uncheckCall(() -> Files.lines(f)))
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