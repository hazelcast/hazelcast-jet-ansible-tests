package test.kafka;

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
            Integer sum = entry.getValue();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            producer.send(new ProducerRecord<>(topic + "-result", ticker, new Trade(-1, ticker, -1, sum)));
            double[] trades = getRandDistArray(countPerTicker, sum);
            for (int i = 0; i < countPerTicker; i++) {
                Trade trade = new Trade(timeStamp[0]++, ticker, 1, trades[i]);
                producer.send(new ProducerRecord<>(topic, ticker, trade));
            }
        });
    }

    private static double[] getRandDistArray(int n, double m) {
        double randArray[] = new double[n];
        double sum = 0;

        // Generate n random numbers
        for (int i = 0; i < randArray.length; i++) {
            randArray[i] = Math.random();
            sum += randArray[i];
        }

        // Normalize sum to m
        for (int i = 0; i < randArray.length; i++) {
            randArray[i] /= sum;
            randArray[i] *= m;
        }
        return randArray;
    }


    private void loadTickers() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(TradeProducer.class.getResourceAsStream
                ("/nasdaqlisted.txt")))) {
            reader.lines().skip(1).map(l -> l.split("\\|")[0]).forEach(t -> tickersToPrice.put(t, random.nextInt(20000)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
