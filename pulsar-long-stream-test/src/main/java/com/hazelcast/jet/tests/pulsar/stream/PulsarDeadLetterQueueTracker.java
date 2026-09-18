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
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Watches a single cluster's dead letter topic for the whole lifetime of {@code test(...)},
 * simplified from {@code pulsar-test}'s {@code MessageConsumer}: this test already has its own
 * exactly-once mechanism via {@link com.hazelcast.jet.tests.common.VerificationProcessor}, so only
 * the dead-letter-queue-emptiness half of that class is ported here, parameterized per cluster
 * name so the concurrently-running dynamic and stable clusters each track their own dead letter
 * topic independently.
 */
class PulsarDeadLetterQueueTracker {

    private final PulsarClient client;
    private final Reader<byte[]> deadLetterReader;
    private final String deadLetterTopic;

    /**
     * Message ids of every message observed on the dead letter topic. Should stay empty for the
     * whole test - anything landing here means a message was rejected/failed by one of the jobs.
     */
    private final ConcurrentLinkedQueue<String> deadLetteredMessageIds = new ConcurrentLinkedQueue<>();

    PulsarDeadLetterQueueTracker(final String brokerUrl, final String deadLetterTopic, final ILogger logger) {
        this.deadLetterTopic = deadLetterTopic;
        try {
            client = PulsarClient.builder().serviceUrl(brokerUrl).build();
            deadLetterReader = client.newReader(Schema.BYTES)
                    .topic(deadLetterTopic)
                    .startMessageId(MessageId.earliest)
                    .readerListener((ignored, item) -> {
                        deadLetteredMessageIds.add(String.valueOf(item.getMessageId()));
                        logger.warning("Dead-lettered message observed on topic " + deadLetterTopic
                                + ", id: " + item.getMessageId());
                    })
                    .create();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Verifies that no message ever landed on the dead letter topic, i.e. both jobs processed
     * every message they read without exhausting their retries.
     */
    void assertDeadLetterQueueIsEmpty() {
        if (!deadLetteredMessageIds.isEmpty()) {
            throw new AssertionError("Expected dead letter topic '" + deadLetterTopic + "' to be empty, but found "
                    + deadLetteredMessageIds.size() + " dead-lettered message(s) with ids: " + deadLetteredMessageIds);
        }
    }

    void close() {
        try {
            deadLetterReader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                client.close();
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
