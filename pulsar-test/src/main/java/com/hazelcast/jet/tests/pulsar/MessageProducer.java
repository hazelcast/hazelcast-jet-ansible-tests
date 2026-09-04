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

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.tests.pulsar.PulsarTest.INPUT_TOPIC;

class MessageProducer {

    private static final List<String> NAMES = List.of(
            "Tomasz", "Anna", "Jan", "Maria", "Piotr",
            "Katarzyna", "Krzysztof", "Małgorzata", "Andrzej", "Agnieszka",
            "Paweł", "Barbara", "Marcin", "Krystyna", "Jakub",
            "Ewa", "Łukasz", "Elżbieta", "Michał", "Zofia",
            "Teresa", "Dawid", "Danuta", "Mateusz", "Helena",
            "Stanisław", "Jadwiga", "Bartosz", "Karolina", "Grzegorz",
            "Beata", "Kamil", "Marta", "Adam", "Dorota",
            "Maciej", "Iwona", "Robert", "Jolanta", "Sebastian"
    );
    private static final List<String> MEALS = List.of(
            "Pierogi", "Pizza", "Spaghetti Carbonara", "Cheeseburger",
            "Grilled Salmon", "Caesar Salad", "Chicken Tikka Masala",
            "Sushi Roll", "Tacos al Pastor", "Beef Lasagna",
            "Pad Thai", "Ribeye Steak", "Fish and Chips",
            "Butter Chicken", "Greek Salad", "Ramen Noodles",
            "Mushroom Risotto", "Falafel Wrap", "Eggs Benedict", "Roast Beef"
    );

    private final PulsarClient client;
    private final Producer<Greeting> producer;
    private final Random random = new Random();
    private AtomicInteger sent = new AtomicInteger();

    MessageProducer(String brokerUrl) {
        try {
            client = PulsarClient.builder().serviceUrl(brokerUrl).build();
            producer = client.newProducer(Schema.JSON(Greeting.class)).topic(INPUT_TOPIC).create();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    void sendGreeting() {
        int nextPerson = random.nextInt(NAMES.size());
        int nextMeal = random.nextInt(MEALS.size());
        try {
            producer.newMessage()
                    .value(new Greeting(NAMES.get(nextPerson), MEALS.get(nextMeal)))
                    .send();
            sent.incrementAndGet();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    public int totalSent() {
        return sent.get();
    }
}
