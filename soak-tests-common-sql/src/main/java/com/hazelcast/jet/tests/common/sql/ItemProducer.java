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

package com.hazelcast.jet.tests.common.sql;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.kafka.impl.HazelcastJsonValueSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.function.Function;

public class ItemProducer implements AutoCloseable {

    private final KafkaProducer<Integer, HazelcastJsonValue> producer;

    public ItemProducer(String brokerUrl) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUrl);
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.setProperty("value.serializer", HazelcastJsonValueSerializer.class.getName());
        producer = new KafkaProducer<>(props);
    }

    public void produceItems(String topic, long startItem, long itemCount, int timeInterval,
                             Function<Number, String> createValue) {
        if (itemCount <= 0 || timeInterval <= 0) {
            throw new IllegalArgumentException("itemCount and timeInterval must be greater than 0!");
        }

        long nextItem = startItem;
        for (long i = startItem; i < startItem + itemCount; i++) {
            producer.send(new ProducerRecord<>(topic, null,
                    new HazelcastJsonValue(createValue.apply(nextItem))));
            nextItem += timeInterval;
        }
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}
