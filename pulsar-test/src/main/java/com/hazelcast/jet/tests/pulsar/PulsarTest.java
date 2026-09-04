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

package com.hazelcast.jet.tests.pulsar;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pulsar.PulsarDataConnection;
import com.hazelcast.jet.pulsar.PulsarSinks;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.jet.pulsar.PulsarSchema.json;
import static com.hazelcast.jet.pulsar.PulsarSchema.string;
import static com.hazelcast.jet.pulsar.PulsarSources.pulsarConsumerBuilder;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;

public class PulsarTest extends AbstractJetSoakTest {
    public static final String INPUT_TOPIC = "incomingGreetings";
    public static final String MIDDLE_TOPIC = "middleTopic";
    public static final String END_TOPIC = "endTopic";

    private String brokerUrl;
    private String httpServiceUrl;
    private PulsarClient pulsarClient;

    public static void main(final String[] args) throws Exception {
        new PulsarTest().run(args);
    }

    @Override
    public void init(final HazelcastInstance client) {
        brokerUrl = "pulsar://" + property("pulsarIp", "127.0.0.1") + "6650";
        httpServiceUrl = "http://" + property("pulsarIp", "127.0.0.1") + "8080";
        try {
           pulsarClient = PulsarClient.builder()
                    .serviceUrl(brokerUrl)
                    .build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }

        client.getConfig().addDataConnectionConfig(PulsarDataConnection.pulsarDataConnectionConf("pulsarInstance",
                brokerUrl, httpServiceUrl, true));
    }

    /**
     * Message is Name + favourite meal (just randomly selected).
     * It goes through one pipeline to middle topic, then to end topic.
     */
    @Override
    public void test(final HazelcastInstance client, final String name) {
        final long begin = System.currentTimeMillis();

        DeadLetterPolicy deadLetterPolicy = DeadLetterPolicy.builder()
                                                            .deadLetterTopic("errors")
                                                            .build();
        Pipeline p1 = Pipeline.create();
        DataConnectionRef dataConnectionRef = DataConnectionRef.dataConnectionRef("pulsarInstance");
        p1.readFrom(pulsarConsumerBuilder(json(Greeting.class))
                  .projectionFn(Message::getValue)
                  .topic(INPUT_TOPIC)
                  .dataConnectionRef(dataConnectionRef)
                  .consumerCustomizer(consumer -> {
                      consumer.deadLetterPolicy(deadLetterPolicy);
                  })
                  .build()
          )
          .withNativeTimestamps(0)
          .groupingKey(Greeting::favouriteMeal)
          .mapStateful(() -> new AtomicInteger(0),
                  (counter, meal, item) -> {
                      int value = counter.incrementAndGet();
                      return new GreetingWithStat(item.name(), item.favouriteMeal(), value);
                  })
          .writeTo(PulsarSinks.builder(json(GreetingWithStat.class))
                              .topic(MIDDLE_TOPIC)
                              .extractKeyFn(GreetingWithStat::favouriteMeal)
                              .dataConnectionRef(dataConnectionRef)
                              .build());

        Pipeline p2 = Pipeline.create();
        p2.readFrom(pulsarConsumerBuilder(json(GreetingWithStat.class))
                  .projectionFn(Message::getValue)
                  .topic(MIDDLE_TOPIC)
                  .dataConnectionRef(dataConnectionRef)
                  .consumerCustomizer(consumer -> {
                      consumer.deadLetterPolicy(deadLetterPolicy);
                  })
                  .build()
          )
          .withNativeTimestamps(0)
          .mapStateful(() -> new AtomicLong(0),
                  (counter, item) -> {
                      long value = counter.incrementAndGet();
                      return value + "Hello " + item.name() + " it you are " + item.howMany()
                              + " to like " + item.favouriteMeal();
                  })
          .writeTo(PulsarSinks.builder(string())
                              .topic(END_TOPIC)
                              .dataConnectionRef(dataConnectionRef)
                              .build());


        JobConfig jobConfig1 = new JobConfig()
                .setName("Messages to middle topic")
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        JobConfig jobConfig2 = new JobConfig()
                .setName("Messages to end topic")
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);

        client.getJet().newJob(p1, jobConfig1);
        client.getJet().newJob(p2, jobConfig2);

        MessageProducer producer = new MessageProducer(brokerUrl);
        MessageConsumer consumer = new MessageConsumer(brokerUrl);

        while (System.currentTimeMillis() - begin < durationInMillis) {
            producer.sendGreeting();
            sleepMillis(300);
        }

        consumer.verifyLast(producer.totalSent());;
    }

    @Override
    protected void teardown(final Throwable t) {
        try {
            pulsarClient.close();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

}
