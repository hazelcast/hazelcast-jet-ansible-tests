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

package com.hazelcast.jet.tests.pulsar.stream;

import com.hazelcast.logging.ILogger;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import java.util.List;
import java.util.Random;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;

/**
 * Publishes an ever-increasing global sequence of {@link GreetingWithSeq} messages to the input
 * topic for the whole duration of the test. The {@code seq} field is the same globally-monotonic
 * counter the single-hop version of this test used to send as a bare {@code Long}; {@code name}
 * and {@code favouriteMeal} are additional payload fields feeding the grouped {@code
 * mapStateful} stage ported from {@code pulsar-test}.
 */
public class PulsarMessageProducer {
    private static final int PRINT_LOG_SENT_ITEMS = 5_000;

    private static final List<String> NAMES = List.of(
            "Tomasz", "Anna", "Jan", "Maria", "Piotr",
            "Katarzyna", "Krzysztof", "Andrzej", "Agnieszka", "Pawel"
    );
    private static final List<String> MEALS = List.of(
            "Pierogi", "Pizza", "Spaghetti Carbonara", "Cheeseburger",
            "Grilled Salmon", "Caesar Salad", "Sushi Roll", "Tacos al Pastor"
    );

    private final String brokerUrl;
    private final String topicName;
    private final ILogger logger;
    private final Thread producerThread;
    private final Random random = new Random();
    private volatile boolean running = true;
    private volatile long producedItems;

    public PulsarMessageProducer(final String brokerUrl, final String topicName, final ILogger logger) {
        this.brokerUrl = brokerUrl;
        this.topicName = topicName;
        this.logger = logger;
        this.producerThread = new Thread(() -> uncheckRun(this::run));
    }

    private void run() throws PulsarClientException {
        try (PulsarClient client = PulsarClient.builder().serviceUrl(brokerUrl).build();
             Producer<GreetingWithSeq> producer = client.newProducer(Schema.JSON(GreetingWithSeq.class))
                     .topic(topicName)
                     .create()) {
            long seq = 0;
            while (running) {
                String name = NAMES.get(random.nextInt(NAMES.size()));
                String meal = MEALS.get(random.nextInt(MEALS.size()));
                producer.newMessage().value(new GreetingWithSeq(name, meal, seq)).send();
                seq++;
                producedItems = seq;

                if (seq % PRINT_LOG_SENT_ITEMS == 0) {
                    logger.info(String.format("Sent %d messages into %s topic", seq, topicName));
                }
                sleepMillis(150);
            }
        } finally {
            logger.info(String.format("Total number of sent messages into %s topic is %d", topicName, producedItems));
        }
    }

    public void start() {
        producerThread.start();
    }

    public long stop() throws InterruptedException {
        running = false;
        producerThread.join();
        return producedItems;
    }

}
