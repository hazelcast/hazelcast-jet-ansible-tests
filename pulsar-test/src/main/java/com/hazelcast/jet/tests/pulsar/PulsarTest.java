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

import com.hazelcast.client.HazelcastClient;
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

/**
 * locally to run you will need pulsar prepared and running in a way:
 * docker run -d \
 *   --name pulsar \
 *   -p 6650:6650 \
 *   -p 8080:8080 \
 *   apachepulsar/pulsar:4.1.2 \
 *   sh -c 'sed -i "s/^transactionCoordinatorEnabled=false/transactionCoordinatorEnabled=true/" \n
 *   conf/standalone.conf && bin/pulsar standalone'
 * -
 *   wait for it to run and add topics:
 *      docker exec pulsar bin/pulsar-admin topics create persistent://public/default/incomingGreetings
 *      docker exec pulsar bin/pulsar-admin topics create persistent://public/default/middleTopic
 *      docker exec pulsar bin/pulsar-admin topics create persistent://public/default/endTopic
 * -
 *   maybe you will also need to add dead letter queue topic -> but should be fine
 */

public class PulsarTest extends AbstractJetSoakTest {
    public static final String INPUT_TOPIC = "incomingGreetings";
    public static final String MIDDLE_TOPIC = "middleTopic";
    public static final String END_TOPIC = "endTopic";
    public static final String DEAD_LETTER_TOPIC = "errors";

    private String brokerUrl;
    private String httpServiceUrl;
    private PulsarClient pulsarClient;
    private transient HazelcastInstance remoteClient;

    public static void main(final String[] args) throws Exception {
        new PulsarTest().run(args);
    }

    @Override
    public void init(final HazelcastInstance client) {
        brokerUrl = "pulsar://" + property("pulsarIp", "127.0.0.1") + ":6650";
        httpServiceUrl = "http://" + property("pulsarIp", "127.0.0.1") + ":8080";
        try {
           pulsarClient = PulsarClient.builder()
                    .serviceUrl(brokerUrl)
                    .build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Message is Name + favourite meal (just randomly selected).
     * It goes through one pipeline to middle topic, then to end topic.
     */
    @Override
    public void test(final HazelcastInstance client, final String name) throws Exception {
        final long begin = System.currentTimeMillis();

        remoteClient = HazelcastClient.newHazelcastClient(remoteClusterClientConfig());
        remoteClient.getConfig().addDataConnectionConfig(PulsarDataConnection.pulsarDataConnectionConf("pulsarInstance",
                brokerUrl, httpServiceUrl, true));

        DeadLetterPolicy deadLetterPolicy = DeadLetterPolicy.builder()
                                                            .deadLetterTopic(DEAD_LETTER_TOPIC)
                                                            .maxRedeliverCount(1)
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

        remoteClient.getJet().newJob(p1, jobConfig1);
        remoteClient.getJet().newJob(p2, jobConfig2);

        MessageProducer producer = new MessageProducer(brokerUrl);
        MessageConsumer consumer = new MessageConsumer(brokerUrl, remoteClient.getLoggingService());

        while (System.currentTimeMillis() - begin < durationInMillis) {
            producer.sendGreeting();
            sleepMillis(300);
        }

        int totalSent = producer.totalSent();
        if (!consumer.awaitDrain(totalSent)) {
            logger.warning("Timed out waiting for the pipeline to catch up with the producer, "
                    + "verification below will report exactly what is missing/duplicated");
        }
        consumer.verifyExactlyOnce(totalSent);
        consumer.verifyDeadLetterQueueIsEmpty();
    }

    @Override
    protected void teardown(final Throwable t) {
        try {
            pulsarClient.close();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        } finally {
            if (remoteClient != null) {
                remoteClient.shutdown();
            }
        }
    }

}
