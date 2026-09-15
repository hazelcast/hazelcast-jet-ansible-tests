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

import com.hazelcast.jet.tests.common.QueueVerifier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.pulsar.PulsarTest.DEAD_LETTER_TOPIC;
import static com.hazelcast.jet.tests.pulsar.PulsarTest.END_TOPIC;

/**
 * Reads every message produced to {@link PulsarTest#END_TOPIC} and feeds its sequence number to
 * the shared {@link QueueVerifier} from {@code soak-tests-common} (already used by
 * {@code EventJournalTest}, {@code SnapshotKafkaTest} and others). It runs on its own thread for
 * the whole lifetime of the test, checking continuously - not just at the end - that sequence
 * numbers arrive in order and that none goes missing for longer than its own timeout allows,
 * without ever holding the full history of the run in memory.
 *
 * <p>{@link QueueVerifier} treats any sequence number lower than the one it currently expects as
 * a harmless duplicate and silently drops it, so on its own it doesn't actually enforce
 * "exactly once" - just "in order, nothing missing for too long". {@link #receivedCount} is a
 * single counter kept alongside it purely to catch duplicates: since a correct run has exactly
 * one arrival per sequence number, that raw count must land exactly on the producer's total, and
 * {@link #verifyExactlyOnce} checks exactly that. This also watches the pipeline's
 * {@link PulsarTest#DEAD_LETTER_TOPIC}, checked once at the end since nothing is ever expected to
 * land there.
 */
class MessageConsumer {

    /**
     * How long {@link #awaitDrain} is willing to wait for the last in-flight messages to catch up
     * with the producer before giving up and letting the verification run (and fail with a
     * precise diagnosis) anyway.
     */
    private static final long DRAIN_TIMEOUT_MILLIS = 60_000;
    private static final long DRAIN_POLL_INTERVAL_MILLIS = 200;

    private final PulsarClient client;
    private final Reader<String> reader;
    private final Reader<byte[]> deadLetterReader;
    private final QueueVerifier queueVerifier;

    /**
     * Raw count of every message observed on the end topic, duplicates included. See the class
     * javadoc: this is what turns {@link QueueVerifier}'s ordering/no-permanent-gap check into a
     * true exactly-once check.
     */
    private final AtomicInteger receivedCount = new AtomicInteger();

    /**
     * Message ids of every message observed on the dead letter topic. Should stay empty for the
     * whole test - anything landing here means a message was rejected/failed by one of the jobs.
     */
    private final ConcurrentLinkedQueue<String> deadLetteredMessageIds = new ConcurrentLinkedQueue<>();

    MessageConsumer(String brokerUrl, LoggingService loggingService) {
        // each sequence number is expected exactly once, hence a window count of 1
        queueVerifier = new QueueVerifier(loggingService, "PulsarEndTopicVerifier", 1).startVerification();
        try {
            client = PulsarClient.builder().serviceUrl(brokerUrl).build();
            ILogger logger = loggingService.getLogger("PulsarClient");
            reader = client.newReader(Schema.STRING)
                           .topic(END_TOPIC)
                            .startMessageId(MessageId.earliest)
                           .readerListener((ignored, item) -> {
                               int sequenceNumber = extractSequenceNumber(item.getValue());
                               receivedCount.incrementAndGet();
                               logger.info("Received sequence number: " + sequenceNumber
                                       + ", receivedCount: " + receivedCount.get());
                               // QueueVerifier's keys start at 0, our sequence numbers start at 1
                               queueVerifier.offer(sequenceNumber - 1L);
                           })
                           .create();
            deadLetterReader = client.newReader(Schema.BYTES)
                                     .topic(DEAD_LETTER_TOPIC)
                    .startMessageId(MessageId.earliest)
                                     .readerListener((ignored, item) -> {
                                         deadLetteredMessageIds.add(String.valueOf(item.getMessageId()));
                                     })
                                     .create();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    private static int extractSequenceNumber(String message) {
        int i = 0;
        while (i < message.length() && Character.isDigit(message.charAt(i))) {
            i++;
        }
        if (i == 0) {
            throw new IllegalStateException("Message does not start with a numeric sequence number: " + message);
        }
        return Integer.parseInt(message.substring(0, i));
    }

    /**
     * Waits, up to {@link #DRAIN_TIMEOUT_MILLIS}, for the pipeline to finish delivering the
     * messages the producer already sent, or for the verifier to give up on a stuck message,
     * whichever comes first. Calling {@link #verifyExactlyOnce} right after the producer stops -
     * without this wait - is prone to spurious failures caused by nothing more than the last few
     * messages still being in flight.
     *
     * @return {@code true} if {@code expectedValue} messages were observed before the timeout
     *         elapsed, {@code false} if the timeout was reached (or the verifier already gave up)
     *         first
     */
    boolean awaitDrain(int expectedValue) {
        long deadline = System.currentTimeMillis() + DRAIN_TIMEOUT_MILLIS;
        while (receivedCount.get() < expectedValue) {
            if (!queueVerifier.isRunning() || System.currentTimeMillis() >= deadline) {
                return false;
            }
            sleepMillis(DRAIN_POLL_INTERVAL_MILLIS);
        }
        return true;
    }

    /**
     * Verifies that every message the producer sent was consumed exactly once, then stops the
     * verifier's background thread.
     */
    void verifyExactlyOnce(int expectedValue) throws Exception {
        boolean wasRunning = queueVerifier.isRunning();
        boolean processedAnything = queueVerifier.processedAnything();
        int observed = receivedCount.get();
        queueVerifier.close();

        if (!wasRunning) {
            throw new AssertionError("Pulsar end topic verifier stopped before verification finished - "
                    + "a sequence number went missing, or arrived out of order for longer than its timeout "
                    + "allows");
        }
        if (!processedAnything) {
            throw new AssertionError("Pulsar end topic verifier did not confirm any messages");
        }
        if (observed != expectedValue) {
            throw new AssertionError("Expected exactly " + expectedValue + " messages on the end topic "
                    + "(one per produced item) but observed " + observed + " - a difference indicates "
                    + "duplicate delivery (more) or lost messages (fewer)");
        }
    }

    /**
     * Verifies that no message ever landed on the dead letter topic, i.e. both jobs processed
     * every message they read without exhausting their retries.
     */
    void verifyDeadLetterQueueIsEmpty() {
        if (!deadLetteredMessageIds.isEmpty()) {
            throw new AssertionError("Expected dead letter topic '" + DEAD_LETTER_TOPIC + "' to be empty, but found "
                    + deadLetteredMessageIds.size() + " dead-lettered message(s) with ids: " + deadLetteredMessageIds);
        }
    }

}
