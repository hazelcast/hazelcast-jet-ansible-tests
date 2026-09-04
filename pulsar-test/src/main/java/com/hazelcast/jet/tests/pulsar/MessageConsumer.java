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

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;

import java.util.concurrent.ConcurrentLinkedDeque;

import static com.hazelcast.jet.tests.pulsar.PulsarTest.END_TOPIC;

class MessageConsumer {

    private final PulsarClient client;
    private final Reader<String> reader;

    private final ConcurrentLinkedDeque<String> items = new  ConcurrentLinkedDeque<>();

    MessageConsumer(String brokerUrl) {
        try {
            client = PulsarClient.builder().serviceUrl(brokerUrl).build();
            reader = client.newReader(Schema.STRING)
                           .topic(END_TOPIC)
                           .readerListener((ignored, item) -> {
                               items.add(item.getValue());
                           })
                           .create();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    void verifyLast(int expectedValue) {
        String last = items.getLast();
        int count = Integer.parseInt(last.substring(0, last.indexOf("-")));
        if (count != expectedValue) {
            throw new AssertionError("Expected " + expectedValue + " but got " + count);
        }
    }

}
