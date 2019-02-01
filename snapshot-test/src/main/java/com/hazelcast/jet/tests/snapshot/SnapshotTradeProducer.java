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

package com.hazelcast.jet.tests.snapshot;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.Properties;

public class SnapshotTradeProducer implements AutoCloseable {

    private static final int SLEEPY_MILLIS = 10;

    private KafkaProducer<Long, Long> producer;

    public SnapshotTradeProducer(String broker) {
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

    public void produce(String topic, int countPerTicker) {
        for (long i = 0; i < Long.MAX_VALUE; i++) {
            try {
                Thread.sleep(SLEEPY_MILLIS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for (int j = 0; j < countPerTicker; j++) {
                producer.send(new ProducerRecord<>(topic, i, i));
            }
        }
    }
}
