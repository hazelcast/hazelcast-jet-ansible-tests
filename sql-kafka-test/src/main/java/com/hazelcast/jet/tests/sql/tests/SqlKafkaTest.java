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

package com.hazelcast.jet.tests.sql.tests;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.jet.tests.common.Util;
import com.hazelcast.jet.tests.sql.kafka.KafkaPojoProducer;
import com.hazelcast.jet.tests.sql.kafka.KafkaSqlReader;

import java.util.Random;
import java.util.concurrent.FutureTask;

public class SqlKafkaTest extends AbstractSoakTest {

    private static final String TOPIC_NAME = "topic" + new Random().nextInt(9999);

    private static final int READ_FROM_KAFKA_THRESHOLD = 10000;
    private static final int DEFAULT_GENERATOR_BATCH_COUNT = 100;
    private static final int DEFAULT_TX_PER_SECOND = 50;

    private String brokerUri;
    private int txPerSecond;
    private int generatorBatchCount;
    private long readFromKafkaThreshold;
    private long begin;

    public static void main(String[] args) throws Exception {
        new SqlKafkaTest().run(args);
    }

    @Override
    protected void init(HazelcastInstance client) {
        brokerUri = property("brokerUri", "127.0.0.1:9092");
        txPerSecond = propertyInt("txPerSecond", DEFAULT_TX_PER_SECOND);
        generatorBatchCount = propertyInt("generatorBatchCount", DEFAULT_GENERATOR_BATCH_COUNT);
        readFromKafkaThreshold = propertyInt("readFromKafkaThreshold", READ_FROM_KAFKA_THRESHOLD);
        begin = System.currentTimeMillis();
    }

    @Override
    protected void test(HazelcastInstance client, String name) throws Exception {
        createKafkaSqlMapping(client);

        Thread producer = new KafkaPojoProducer(
                logger, brokerUri, TOPIC_NAME, txPerSecond, generatorBatchCount, begin, durationInMillis);

        FutureTask<Integer> readerFuture = new FutureTask<>(
                new KafkaSqlReader(logger, client, TOPIC_NAME, begin, durationInMillis, readFromKafkaThreshold));
        Thread reader = new Thread(readerFuture);

        producer.start();
        reader.start();

        producer.join();
        reader.join();

        int queriesRun = readerFuture.get();

        logger.info(String.format(
                "Test completed successfully. Executed %d queries in: %s", queriesRun, Util.getTimeElapsed(begin)));
    }

    @Override
    protected void teardown(Throwable t) {
    }

    private void createKafkaSqlMapping(HazelcastInstance client) {
        client.getSql().execute("CREATE MAPPING " + TOPIC_NAME + "(" +
                " booleanVal BOOLEAN," +
                " tinyIntVal TINYINT," +
                " smallIntVal SMALLINT," +
                " intVal INT," +
                " bigIntVal BIGINT," +
                " realVal REAL," +
                " doubleVal DOUBLE," +
                " timestampVal BIGINT," +
                " decimalVal DECIMAL," +
                " varcharVal VARCHAR)" +
                " TYPE Kafka" +
                " OPTIONS (" +
                " 'keyFormat' = 'java'," +
                " 'keyJavaClass' = 'com.hazelcast.jet.tests.sql.pojo.Key'," +
                " 'key.serializer' = 'com.hazelcast.jet.tests.sql.serializer.KeySerializer'," +
                " 'key.deserializer' = 'com.hazelcast.jet.tests.sql.serializer.KeyDeserializer'," +
                " 'valueFormat' = 'java'," +
                " 'valueJavaClass' = 'com.hazelcast.jet.tests.sql.pojo.Pojo'," +
                " 'value.serializer' = 'com.hazelcast.jet.tests.sql.serializer.PojoSerializer'," +
                " 'value.deserializer' = 'com.hazelcast.jet.tests.sql.serializer.PojoDeserializer'," +
                " 'bootstrap.servers' = '127.0.0.1:9092'" +
                ")");
    }
}
