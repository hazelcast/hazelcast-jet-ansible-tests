package com.hazelcast.jet.tests.sql.tests;

import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.kafka.KafkaSinks;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.jet.tests.sql.pojo.Pojo;
import com.hazelcast.jet.tests.stateful.KafkaSinkVerifier;
import com.hazelcast.sql.SqlResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;

public class SqlKafkaTest extends AbstractSoakTest {

    public static final String TOPIC_NAME = "topic";
    private static final int DEFAULT_LAG = 3000;
    private static final int DEFAULT_TX_TIMEOUT = 5000;
    private static final int DEFAULT_GENERATOR_BATCH_COUNT = 100;
    private static final int DEFAULT_TX_PER_SECOND = 1000;
    private static final int POLL_TIMEOUT = 1000;
    private static final int DEFAULT_COUNTER_PER_TICKER = 1000;
    private static final int DEFAULT_WINDOW_SIZE = 20;
    private static final int DEFAULT_SLIDE_BY = 10;

    private String brokerUri;
    private String offsetReset;
    private int txTimeout;
    private int txPerSecond;
    private int generatorBatchCount;
    private int countPerTicker;
    private int windowSize;
    private int slideBy;
    private int lagMs;

    private transient ExecutorService producerExecutorService;

    public SqlKafkaTest() {

    }

    public static void main(String[] args) throws Exception {
        new SqlKafkaTest().run(args);
    }

    @Override
    protected void init(JetInstance client) {
        producerExecutorService = Executors.newSingleThreadExecutor();
        brokerUri = property("brokerUri", "localhost:9092");
        offsetReset = property("offsetReset", "earliest");
        txTimeout = propertyInt("txTimeout", DEFAULT_TX_TIMEOUT);
        txPerSecond = propertyInt("txPerSecond", DEFAULT_TX_PER_SECOND);
        generatorBatchCount = propertyInt("generatorBatchCount", DEFAULT_GENERATOR_BATCH_COUNT);
        countPerTicker = propertyInt("countPerTicker", DEFAULT_COUNTER_PER_TICKER);
        lagMs = propertyInt("lagMs", DEFAULT_LAG);
        windowSize = propertyInt("windowSize", DEFAULT_WINDOW_SIZE);
        slideBy = propertyInt("slideBy", DEFAULT_SLIDE_BY);

    }

    @Override
    protected void test(JetInstance client, String name) {
        SqlResult sqlResult = client.getSql().execute("SELECT * FROM kafka");


        KafkaPojoProducer producer = new KafkaPojoProducer(
                logger, brokerUri, TOPIC_NAME, txPerSecond, generatorBatchCount, txTimeout);
        producer.start();

        KafkaSinkVerifier verifier = new KafkaSinkVerifier(name, brokerUri, TOPIC_NAME, logger);
        verifier.start();

        KafkaConsumer<Long, Pojo> consumer = new KafkaConsumer<>(kafkaProps(brokerUri, offsetReset));
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        JobConfig cfg = new JobConfig().setName("kafka-sql-pojo");
        client.newJob(pipeline("kafka_pipeline", AT_LEAST_ONCE, 0), cfg);

        String tableName = "kafka";

        client.getSql().execute("CREATE MAPPING kafka (" +
                " booleanVal BOOLEAN," +
                " tinyIntVal TINYINT," +
                " smallIntVal SMALLINT," +
                " intVal INT," +
                " bigIntVal BIGINT," +
                " realVal REAL," +
                " doubleVal DOUBLE," +
                " decimalBigIntegerVal BIGINT," +
                " decimalVal DECIMAL," +
                " varcharVal VARCHAR," +
                " timeVal TIME," +
                " dateVal DATE," +
                " timestampVal TIMESTAMP)" +
                " TYPE Kafka" +
                " OPTIONS (" +
                " 'valueFormat' = 'json'," +
                " 'bootstrap.servers' = 'kafka:9092'" +
                ")");

        while(true) {
            SqlResult sqlResult1 = client.getSql().execute("SELECT * FROM kafka");
        }

//        client.getJob("kafka_pipeline").cancel();

    }

    @Override
    protected void teardown(Throwable t) {
    }

    private Pipeline pipeline(String name, ProcessingGuarantee guarantee, int jobIndex) {
        Pipeline pipeline = Pipeline.create();

        Properties propsForTrades = kafkaPropsForTrades(brokerUri, offsetReset);
        Properties propsForResult = kafkaPropsForResults(brokerUri, offsetReset);

        pipeline.readFrom(KafkaSources.kafka(propsForTrades, ConsumerRecord<Long, Long>::value, TOPIC_NAME))
                .withTimestamps(t -> t, lagMs)
                .setName(String.format("ReadKafka(%s-%s-%d)", TOPIC_NAME, guarantee, jobIndex))
                .window(sliding(windowSize, slideBy))
                .groupingKey(wholeItem())
                .aggregate(counting()).setName(String.format("AggregateCount(%s-%s-%d)", TOPIC_NAME, guarantee, jobIndex))
                .writeTo(KafkaSinks.kafka(propsForResult, TOPIC_NAME))
                .setName(String.format("WriteKafka(%s)", TOPIC_NAME));
        return pipeline;
    }

    private static Properties kafkaPropsForTrades(String brokerUrl, String offsetReset) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUrl);
        props.setProperty("group.id", UuidUtil.newUnsecureUuidString());
        props.setProperty("key.deserializer", LongDeserializer.class.getName());
        props.setProperty("value.deserializer", LongDeserializer.class.getName());
        props.setProperty("auto.offset.reset", offsetReset);
        props.setProperty("max.poll.records", "32768");
        return props;
    }

    private static Properties kafkaPropsForResults(String brokerUrl, String offsetReset) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUrl);
        props.setProperty("group.id", UuidUtil.newUnsecureUuidString());
        props.setProperty("key.deserializer", LongDeserializer.class.getName());
        props.setProperty("value.deserializer", LongDeserializer.class.getName());
        props.setProperty("key.serializer", LongSerializer.class.getName());
        props.setProperty("value.serializer", LongSerializer.class.getName());
        props.setProperty("auto.offset.reset", offsetReset);
        props.setProperty("max.poll.records", "32768");
        return props;
    }

    private static Properties kafkaProps(String brokerUrl, String offsetReset) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUrl);
        props.setProperty("group.id", UuidUtil.newUnsecureUuidString());
        props.setProperty("key.deserializer", LongDeserializer.class.getName());
        props.setProperty("value.deserializer", LongDeserializer.class.getName());
        props.setProperty("key.serializer", LongSerializer.class.getName());
        props.setProperty("value.serializer", LongSerializer.class.getName());
        props.setProperty("auto.offset.reset", offsetReset);
        props.setProperty("max.poll.records", "32768");
        return props;
    }
}
