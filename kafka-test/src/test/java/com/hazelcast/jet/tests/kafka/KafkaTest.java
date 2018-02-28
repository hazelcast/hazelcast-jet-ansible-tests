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
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.pipeline.SlidingWindowDef;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.server.JetBootstrap;
import com.hazelcast.util.UuidUtil;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import tests.kafka.Trade;
import tests.kafka.TradeDeserializer;
import tests.kafka.TradeProducer;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Partitioner.HASH_CODE;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.emitByFrame;
import static com.hazelcast.jet.core.WatermarkGenerationParams.wmGenParams;
import static com.hazelcast.jet.core.WatermarkPolicies.limitingLag;
import static com.hazelcast.jet.core.processor.KafkaProcessors.streamKafkaP;
import static com.hazelcast.jet.core.processor.Processors.accumulateByFrameP;
import static com.hazelcast.jet.core.processor.Processors.combineToSlidingWindowP;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeListP;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static java.lang.System.currentTimeMillis;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class KafkaTest {

    private String brokerUri;
    private String topic;
    private String offsetReset;
    private int lagMs;
    private int windowSize;
    private int slideBy;
    private String outputList;
    private int tickerCount;
    private int countPerTicker;
    private Properties kafkaProps;
    private JetInstance jet;

    public static void main(String[] args) {
        JUnitCore.main(KafkaTest.class.getName());
    }

    @Before
    public void setUp() {
        brokerUri = System.getProperty("brokerUri", "localhost:9092");
        topic = System.getProperty("topic", String.format("%s-%d", "trades", System.currentTimeMillis()));
        offsetReset = System.getProperty("offsetReset", "earliest");
        lagMs = Integer.parseInt(System.getProperty("lagMs", "10"));
        windowSize = Integer.parseInt(System.getProperty("windowSize", "50"));
        slideBy = Integer.parseInt(System.getProperty("slideBy", "10"));
        outputList = System.getProperty("outputList", "jet-output-" + System.currentTimeMillis());
        tickerCount = Integer.parseInt(System.getProperty("tickerCount", "500"));
        countPerTicker = Integer.parseInt(System.getProperty("countPerTicker", "7"));
        kafkaProps = getKafkaProperties(brokerUri, offsetReset);
        jet = JetBootstrap.getInstance();

        try (TradeProducer tradeProducer = new TradeProducer(brokerUri)) {
            tradeProducer.produce(topic, tickerCount, countPerTicker);
        }
    }

    @Test
    public void kafkaTest() throws InterruptedException {
        SlidingWindowDef windowDef = WindowDefinition.sliding(windowSize, slideBy);
        AggregateOperation1<Object, LongAccumulator, Long> counting = AggregateOperations.counting();

        DAG dag = new DAG();
        WatermarkGenerationParams<Trade> wmGenParams = wmGenParams(Trade::getTime, limitingLag(lagMs),
                emitByFrame(windowDef.toSlidingWindowPolicy()), 10_000);
        Vertex readKafka = dag.newVertex("read-kafka", streamKafkaP(kafkaProps,
                consumerRecord -> (Trade) consumerRecord.value(), wmGenParams, topic));

        DistributedFunction<Trade, String> keyFn = Trade::getTicker;
        DistributedToLongFunction<Trade> timestampFn = Trade::getTime;
        Vertex accumulateByF = dag.newVertex("accumulate-by-frame",
                accumulateByFrameP(singletonList(keyFn), singletonList(timestampFn), TimestampKind.EVENT,
                        windowDef.toSlidingWindowPolicy(), counting));
        Vertex slidingW = dag.newVertex("sliding-window",
                combineToSlidingWindowP(windowDef.toSlidingWindowPolicy(), counting, TimestampedEntry::new)
        );
        Vertex formatOutput = dag.newVertex("format-output",
                mapP((TimestampedEntry entry) -> {
                    long timeMs = currentTimeMillis();
                    long latencyMs = timeMs - entry.getTimestamp();
                    return String.format("%d,%s,%s,%d,%d", entry.getTimestamp(), entry.getKey(), entry.getValue(),
                            timeMs, latencyMs);
                })
        );
        Vertex listSink = dag.newVertex("write-list", writeListP(outputList));

        dag
                .edge(between(readKafka, accumulateByF).partitioned(Trade::getTicker, HASH_CODE))
                .edge(between(accumulateByF, slidingW).partitioned(entryKey())
                                                      .distributed())
                .edge(between(slidingW, formatOutput).isolated())
                .edge(between(formatOutput, listSink));


        System.out.println("Executing job..");
        Future<Void> execute = jet.newJob(dag).getFuture();

        try {
            Thread.sleep(MINUTES.toMillis(1));
            System.out.println("Cancelling job...");
            execute.cancel(true);
            execute.get();
        } catch (Exception ignored) {
        }

        Thread.sleep(MINUTES.toMillis(2));

        ArrayList<String> localList = new ArrayList<>(jet.getList(outputList));
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
                                  .map(Entry::getValue)
                                  .flatMap(Collection::stream)
                                  .allMatch(countedTicker -> countedTicker
                                          .getValue().equals(String.valueOf(countPerTicker)));
        assertTrue("tick count per window matches", result);
    }

    @After
    public void tearDown() {
        jet.shutdown();
    }

    private static Properties getKafkaProperties(String brokerUrl, String offsetReset) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUrl);
        props.setProperty("group.id", UuidUtil.newUnsecureUUID().toString());
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", TradeDeserializer.class.getName());
        props.setProperty("auto.offset.reset", offsetReset);
        props.setProperty("max.poll.records", "32768");
        return props;
    }

}
