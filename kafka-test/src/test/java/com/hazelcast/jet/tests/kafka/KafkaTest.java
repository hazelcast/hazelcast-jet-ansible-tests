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

package com.hazelcast.jet.tests.kafka;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.server.JetBootstrap;
import com.hazelcast.util.UuidUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class KafkaTest {

    private static final String TOPIC = KafkaTest.class.getSimpleName();

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
        System.setProperty("hazelcast.logging.type", "log4j");
        String brokerUri = System.getProperty("brokerUri", "localhost:9092");
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
            tradeProducer.produce(TOPIC, tickerCount, countPerTicker);
        }
    }

    @Test
    public void kafkaTest() throws InterruptedException {
        System.out.println("Executing job..");
        Job job = jet.newJob(pipeline(), new JobConfig().setName("Kafka Short Word Count Test"));

        Thread.sleep(MINUTES.toMillis(1));
        System.out.println("Cancelling job...");
        job.cancel();
        waitForJobStatus(job, JobStatus.COMPLETED);

        Thread.sleep(MINUTES.toMillis(1));

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

        pipeline.drawFrom(KafkaSources.kafka(kafkaProps, ConsumerRecord<String, Trade>::value, TOPIC))
                .withTimestamps(Trade::getTime, lagMs)
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

    private static void waitForJobStatus(Job job, JobStatus expectedStatus) throws InterruptedException {
        while (true) {
            JobStatus currentStatus = job.getStatus();
            assertNotEquals(FAILED, currentStatus);
            if (currentStatus.equals(expectedStatus)) {
                return;
            }
            SECONDS.sleep(1);
        }
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
