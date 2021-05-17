package com.hazelcast.jet.tests.sql.tests;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.jet.tests.common.Util;
import com.hazelcast.jet.tests.sql.kafka.KafkaPojoProducer;
import com.hazelcast.jet.tests.sql.kafka.KafkaSqlReader;

import java.util.Random;
import java.util.concurrent.FutureTask;

public class SqlKafkaTest extends AbstractSoakTest {

    private static final String TOPIC_NAME = "topic" + new Random().nextInt(9999);
    private static final int DEFAULT_TX_TIMEOUT = 5000;
    private static final int DEFAULT_GENERATOR_BATCH_COUNT = 100;
    private static final int DEFAULT_TX_PER_SECOND = 50;

    private String brokerUri;
    private int txTimeout;
    private int txPerSecond;
    private int generatorBatchCount;
    private long begin;

    public static void main(String[] args) throws Exception {
        new SqlKafkaTest().run(args);
    }

    @Override
    protected void init(JetInstance client) {
        brokerUri = property("brokerUri", "127.0.0.1:9092");
        txTimeout = propertyInt("txTimeout", DEFAULT_TX_TIMEOUT);
        txPerSecond = propertyInt("txPerSecond", DEFAULT_TX_PER_SECOND);
        generatorBatchCount = propertyInt("generatorBatchCount", DEFAULT_GENERATOR_BATCH_COUNT);
        begin = System.currentTimeMillis();
    }

    @Override
    protected void test(JetInstance client, String name) throws Exception{
        createKafkaSqlMapping(client);

        Thread producer = new KafkaPojoProducer(
                logger, brokerUri, TOPIC_NAME, txPerSecond, generatorBatchCount, txTimeout, begin, durationInMillis);

        FutureTask<Integer> readerTask =
                new FutureTask<>(new KafkaSqlReader(logger, client, TOPIC_NAME, begin, durationInMillis));
        Thread reader = new Thread(readerTask);

        producer.start();
        reader.start();

        producer.join();
        reader.join();

        int queriesRun = readerTask.get();

        logger.info(String.format(
                "Test completed successfully. Executed %d queries in: %s", queriesRun, Util.getTimeElapsed(begin)));
    }

    @Override
    protected void teardown(Throwable t) {
    }

    private void createKafkaSqlMapping(JetInstance client) {
        client.getSql().execute("CREATE MAPPING " + TOPIC_NAME + "(" +
                " booleanVal BOOLEAN," +
                " tinyIntVal TINYINT," +
                " smallIntVal SMALLINT," +
                " intVal INT," +
                " bigIntVal BIGINT," +
                " realVal REAL," +
                " doubleVal DOUBLE," +
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
