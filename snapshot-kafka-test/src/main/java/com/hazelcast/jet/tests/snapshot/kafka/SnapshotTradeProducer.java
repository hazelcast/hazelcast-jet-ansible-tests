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

package com.hazelcast.jet.tests.snapshot.kafka;

import com.hazelcast.logging.ILogger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

public class SnapshotTradeProducer implements AutoCloseable {

    private static final int SLEEPY_MILLIS = 10;

    private final KafkaProducer<Long, Long> producer;
    private final ILogger logger;
    private final List<Future<RecordMetadata>> futureList = new ArrayList<>();

    public SnapshotTradeProducer(String broker, ILogger logger) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", broker);
        props.setProperty("key.serializer", LongSerializer.class.getName());
        props.setProperty("value.serializer", LongSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
        this.logger = logger;
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    public void produce(String topic, int countPerTicker) {
        for (long item = 0; item < Long.MAX_VALUE; item++) {
            int remainingItems = countPerTicker;
            do {
                remainingItems -= sendItems(topic, item, remainingItems);
            } while (remainingItems > 0);
            try {
                Thread.sleep(SLEEPY_MILLIS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private long sendItems(String topic, long item, int count) {
        futureList.clear();
        for (int i = 0; i < count; i++) {
            futureList.add(producer.send(new ProducerRecord<>(topic, item, item)));
        }
        return futureList.stream().filter(f -> {
            try {
                f.get();
                return true;
            } catch (Exception e) {
                logger.warning("Exception while publishing " + item, e);
                return false;
            }
        }).count();
    }
}
