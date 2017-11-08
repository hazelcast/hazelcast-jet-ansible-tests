/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package tests.kafka;

import com.google.common.collect.Iterables;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class TradeProducer implements AutoCloseable {

    private static final int PRICE_UPPER_BOUND = 20000;
    private static final int SLEEP_BETWEEN_PRODUCE = 100;
    private KafkaProducer<String, Trade> producer;
    private Map<String, Integer> tickersToPrice = new HashMap<>();
    private final Random random;

    public TradeProducer(String broker) {
        random = new Random();
        loadTickers();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", broker);
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", TradeSerializer.class.getName());
        producer = new KafkaProducer<>(props);
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    public void produce(String topic, int countPerTicker) {
        final long[] timeStamp = {0};
        Iterables.cycle(tickersToPrice.entrySet()).forEach((entry) -> {
            String ticker = entry.getKey();
            Integer price = entry.getValue();
            try {
                Thread.sleep(SLEEP_BETWEEN_PRODUCE);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for (int i = 0; i < countPerTicker; i++) {
                Trade trade = new Trade(timeStamp[0]++, ticker, 1, price);
                producer.send(new ProducerRecord<>(topic, ticker, trade));
            }
        });
    }

    private void loadTickers() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                TradeProducer.class.getResourceAsStream("/nasdaqlisted.txt")))) {
            reader.lines().skip(1).map(l -> l.split("\\|")[0]).forEach(t ->
                    tickersToPrice.put(t, random.nextInt(PRICE_UPPER_BOUND)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
