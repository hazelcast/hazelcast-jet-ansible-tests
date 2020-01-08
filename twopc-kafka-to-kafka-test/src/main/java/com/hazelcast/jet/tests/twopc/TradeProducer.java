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

package com.hazelcast.jet.tests.twopc;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import static java.util.concurrent.TimeUnit.SECONDS;

public class TradeProducer implements AutoCloseable {

    private static volatile boolean running = true;
    private final KafkaProducer<Long, Long> producer;
    private final long produceThreshold;
    private long timestamp;
    private long lastEmit = System.currentTimeMillis();

    TradeProducer(String broker, int bulksPerSec) {
        this.produceThreshold = SECONDS.toMillis(1) / bulksPerSec;
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", broker);
        props.setProperty("key.serializer", LongSerializer.class.getName());
        props.setProperty("value.serializer", LongSerializer.class.getName());
        producer = new KafkaProducer<>(props);
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    void produce(String topic, int bulkSize) {
        while (running) {
            long now = System.currentTimeMillis();
            if (now - lastEmit > produceThreshold) {
                for (int i = 0; i < bulkSize; i++) {
                    producer.send(new ProducerRecord<>(topic, timestamp, timestamp));
                }
                timestamp++;
                lastEmit = now;
            }
        }
    }

    public static void finish() {
        running = false;
    }

    public static boolean isFinished() {
        return !running;
    }

}
