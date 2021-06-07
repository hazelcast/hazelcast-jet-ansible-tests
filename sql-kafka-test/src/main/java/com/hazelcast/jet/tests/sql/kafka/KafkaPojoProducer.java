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

package com.hazelcast.jet.tests.sql.kafka;

import com.hazelcast.jet.tests.sql.pojo.Key;
import com.hazelcast.jet.tests.sql.pojo.Pojo;
import com.hazelcast.logging.ILogger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

public class KafkaPojoProducer extends Thread {

    private static final int PRODUCE_WAIT_TIMEOUT_MILLIS = 10_000;

    private final KafkaProducer<Key, Pojo> producer;
    private final String topic;
    private final long nanosBetweenEvents;
    private final int batchCount;
    private ILogger logger;
    private long begin;
    private long durationInMillis;

    private long txId;

    public KafkaPojoProducer(
            ILogger logger, String broker, String topic, int txPerSeconds,
            int batchCount, long begin, long durationInMillis
    ) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", broker);
        props.setProperty("key.serializer", "com.hazelcast.jet.tests.sql.serializer.KeySerializer");
        props.setProperty("key.deserializer", "com.hazelcast.jet.tests.sql.serializer.KeyDeserializer");
        props.setProperty("value.serializer", "com.hazelcast.jet.tests.sql.serializer.PojoSerializer");
        props.setProperty("value.deserializer", "com.hazelcast.jet.tests.sql.serializer.PojoDeserializer");
        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.batchCount = batchCount;
        this.nanosBetweenEvents = SECONDS.toNanos(1) / txPerSeconds;
        this.logger = logger;
        this.begin = begin;
        this.durationInMillis = durationInMillis;
    }

    @Override
    public void run() {
        List<Future<RecordMetadata>> futureList = new ArrayList<>(batchCount);
        while (System.currentTimeMillis() - begin < durationInMillis) {
            for (int i = 0; i < batchCount; i++) {
                long id = txId + i;
                futureList.add(produce(new Key(id), new Pojo(id)));
                parkNanos(nanosBetweenEvents);
            }
            waitForCompleteAndClearList(futureList);
        }
    }

    private Future<RecordMetadata> produce(Key key, Pojo pojo) {
        logger.fine("Produce: " + key.getKey());
        return producer.send(new ProducerRecord<>(topic, key, pojo));
    }

    private static void waitForCompleteAndClearList(List<Future<RecordMetadata>> futureList) {
        futureList.forEach(f -> uncheckCall(() -> f.get(PRODUCE_WAIT_TIMEOUT_MILLIS, MILLISECONDS)));
        futureList.clear();
    }

}
