package test.kafka;

import com.google.common.collect.Iterables;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class TradeProducer implements AutoCloseable {

    private KafkaProducer<String, Trade> producer;
    private Map<String, Integer> tickersToPrice = new HashMap<>();

    public TradeProducer(String broker) {
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
        Iterables.cycle(tickersToPrice.keySet()).forEach(ticker -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for (int i = 0; i < countPerTicker; i++) {
                Trade trade = new Trade(timeStamp[0], ticker, 100, 400);
                producer.send(new ProducerRecord<>(topic, ticker, trade));
            }
            timeStamp[0]++;
        });
    }

    private void loadTickers() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(TradeProducer.class.getResourceAsStream
                ("/nasdaqlisted.txt")))) {
            reader.lines().skip(1).map(l -> l.split("\\|")[0]).forEach(t -> tickersToPrice.put(t, 10000));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
