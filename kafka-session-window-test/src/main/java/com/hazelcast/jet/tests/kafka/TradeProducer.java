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

package com.hazelcast.jet.tests.kafka;

import com.google.common.collect.Iterables;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Properties;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

public class TradeProducer implements AutoCloseable {

    private static final int SLEEP_BETWEEN_PRODUCE = 200;
    private KafkaProducer<String, Long> producer;
    private List<String> tickers;

    TradeProducer(String broker) {
        loadTickers();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", broker);
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", LongSerializer.class.getName());
        producer = new KafkaProducer<>(props);
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    void produce(String topic, int countPerTicker) {
        final long[] timeStamp = {0};
        Iterables.cycle(tickers).forEach(ticker -> {
            uncheckRun(() -> MILLISECONDS.sleep(SLEEP_BETWEEN_PRODUCE));
            for (int i = 0; i < countPerTicker; i++) {
                producer.send(new ProducerRecord<>(topic, ticker, timeStamp[0]++));
            }
        });
    }

    private void loadTickers() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                TradeProducer.class.getResourceAsStream("/nasdaqlisted.txt")))) {
            tickers = reader.lines().skip(1).map(l -> l.split("\\|")[0]).collect(toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
