package test.kafka;

import java.util.Properties;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

public class IntProducer implements AutoCloseable {

    private KafkaProducer<Integer, Integer> producer;

    public IntProducer(String broker) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", broker);
        props.setProperty("key.serializer", IntegerSerializer.class.getName());
        props.setProperty("value.serializer", IntegerSerializer.class.getName());
        producer = new KafkaProducer<>(props);
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    public void produce(String topic, int distinctInts) {
        IntStream intStream = IntStream.iterate(1, i -> (i + 1) % distinctInts);
        intStream.forEach(number -> {
            LockSupport.parkNanos(1000);
            producer.send(new ProducerRecord<>(topic, number, number));
        });
    }


}
