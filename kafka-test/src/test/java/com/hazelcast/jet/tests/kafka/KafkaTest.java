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
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.server.JetBootstrap;
import com.hazelcast.util.UuidUtil;
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

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class KafkaTest {

    private String topic;
    private int lagMs;
    private int windowSize;
    private int slideBy;
    private String outputList;
    private int countPerTicker;
    private Properties kafkaProps;
    private JetInstance jet;

    public static void main(String[] args) {
        JUnitCore.main(KafkaTest.class.getName());
    }

    @Before
    public void setUp() {
        String brokerUri = System.getProperty("brokerUri", "localhost:9092");
        topic = System.getProperty("topic", String.format("%s-%d", "trades", System.currentTimeMillis()));
        String offsetReset = System.getProperty("offsetReset", "earliest");
        lagMs = Integer.parseInt(System.getProperty("lagMs", "10"));
        windowSize = Integer.parseInt(System.getProperty("windowSize", "50"));
        slideBy = Integer.parseInt(System.getProperty("slideBy", "10"));
        outputList = System.getProperty("outputList", "jet-output-" + System.currentTimeMillis());
        int tickerCount = Integer.parseInt(System.getProperty("tickerCount", "500"));
        countPerTicker = Integer.parseInt(System.getProperty("countPerTicker", "7"));
        kafkaProps = getKafkaProperties(brokerUri, offsetReset);
        jet = JetBootstrap.getInstance();

        try (TradeProducer tradeProducer = new TradeProducer(brokerUri)) {
            tradeProducer.produce(topic, tickerCount, countPerTicker);
        }
    }

    @Test
    public void kafkaTest() throws IOException, ExecutionException, InterruptedException {
        System.out.println("Executing job..");
        Future<Void> execute = jet.newJob(pipeline()).getFuture();

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

    private Pipeline pipeline() {
        Pipeline pipeline = Pipeline.create();

        pipeline.drawFrom(KafkaSources.<String, Trade>kafka(kafkaProps, topic))
                .map(Entry::getValue)
                .addTimestamps(Trade::getTime, lagMs)
                .window(sliding(windowSize, slideBy))
                .groupingKey(Trade::getTicker)
                .aggregate(counting(), (winStart, timestamp, key, value) -> {
                    long timeMs = currentTimeMillis();
                    long latencyMs = timeMs - timestamp;
                    return String.format("%d,%s,%s,%d,%d", timestamp, key, value, timeMs, latencyMs);
                })
                .drainTo(Sinks.list(outputList));


        return pipeline;
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
