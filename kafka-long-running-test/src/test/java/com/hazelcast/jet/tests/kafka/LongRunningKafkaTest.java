package com.hazelcast.jet.tests.kafka;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.server.JetBootstrap;
import com.hazelcast.jet.stream.IStreamMap;
import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testpackage.VisibleAssertions;
import test.kafka.IntProducer;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.KafkaProcessors.streamKafkaP;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeMapP;

@RunWith(JUnit4.class)
public class LongRunningKafkaTest {

    private String brokerUri;
    private String topic;
    private String offsetReset;
    private String outputMap;
    private Properties kafkaProps;
    private JetInstance jet;
    private int distinctInts;
    private int durationInMinutes;
    private ScheduledExecutorService executorService;

    @Before
    public void setUp() throws Exception {
        executorService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
        brokerUri = System.getProperty("brokerUri", "localhost:9092");
        topic = System.getProperty("topic", String.format("%s-%d", "numbers", System.currentTimeMillis()));
        offsetReset = System.getProperty("offsetReset", "earliest");
        outputMap = System.getProperty("outputMap", "jet-output-" + System.currentTimeMillis());
        distinctInts = Integer.parseInt(System.getProperty("distinctInts", "10000"));
        durationInMinutes = Integer.parseInt(System.getProperty("durationInMinutes", "1"));
        kafkaProps = getKafkaProperties(brokerUri, offsetReset);
        jet = JetBootstrap.getInstance();

        Runnable runnable = () -> {
            try (IntProducer intProducer = new IntProducer(brokerUri)) {
                intProducer.produce(topic, distinctInts);
            }
        };
        executorService.schedule(runnable, 1, TimeUnit.NANOSECONDS);
    }

    @Test
    public void kafkaTest() throws IOException, ExecutionException, InterruptedException {
        DAG dag = new DAG();
        Vertex readKafka = dag.newVertex("read-kafka", streamKafkaP(kafkaProps, (key, value) -> value, topic));
        Vertex map = dag.newVertex("map", mapP(o -> new SimpleImmutableEntry<>(o, o)));
        Vertex mapSink = dag.newVertex("write-map", writeMapP(outputMap));

        dag
                .edge(between(readKafka, map))
                .edge(between(map, mapSink));

        System.out.println("Executing job..");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setSnapshotIntervalMillis(5000);
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
        Future<Void> execute = jet.newJob(dag).getFuture();

        CountDownLatch latch = new CountDownLatch(1);
        executorService.schedule(() -> {
            try {
                System.out.println("Cancelling job...");
                execute.cancel(true);
                execute.get();
            } catch (Exception ignored) {
            }
            latch.countDown();
        }, durationInMinutes, TimeUnit.MINUTES);
        latch.await();
    }

    @After
    public void tearDown() throws Exception {
        IStreamMap<Integer, Integer> results = jet.getMap(outputMap);
        int count = ((int) results.keySet().stream().distinct().count());
        VisibleAssertions.assertEquals("Distinct integer count should match", distinctInts, count);
        jet.shutdown();
    }

    private static Properties getKafkaProperties(String brokerUrl, String offsetReset) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerUrl);
        props.setProperty("group.id", UUID.randomUUID().toString());
        props.setProperty("key.deserializer", IntegerDeserializer.class.getName());
        props.setProperty("value.deserializer", IntegerDeserializer.class.getName());
        props.setProperty("auto.offset.reset", offsetReset);
        props.setProperty("max.poll.records", "32768");
        return props;
    }

}
