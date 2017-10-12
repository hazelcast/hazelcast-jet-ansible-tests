package test.kafka;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

public class TradeProducer implements AutoCloseable {

    private static long SEC_TO_NANOS = TimeUnit.SECONDS.toNanos(1);

    private KafkaProducer<Long, Trade> producer;
    private Map<String, Integer> tickersToPrice = new HashMap<>();
    private String[] tickers;
    private int tickerIndex;
    private long lag;

    public TradeProducer(String broker) {
        loadTickers();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", broker);
        props.setProperty("key.serializer", LongSerializer.class.getName());
        props.setProperty("value.serializer", TradeSerializer.class.getName());
        producer = new KafkaProducer<>(props);
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    public void produce(String topic, int tickerCount, int countPerTicker) {
        tickersToPrice.keySet().stream().limit(tickerCount).forEach(ticker -> {
            long timeStamp = System.currentTimeMillis();
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(timeStamp);
            for (int i = 0; i < countPerTicker; i++) {
                calendar.add(Calendar.MILLISECOND, 5);
                Trade trade = new Trade(calendar.getTimeInMillis(), ticker, 100, 400);
                producer.send(new ProducerRecord<>(topic, trade));
            }
        });
    }

    private void loadTickers() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(TradeProducer.class.getResourceAsStream
                ("/nasdaqlisted.txt")))) {
            reader.lines().skip(1).map(l -> l.split("\\|")[0]).forEach(t -> tickersToPrice.put(t, 10000));
            tickers = tickersToPrice.keySet().toArray(new String[0]);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
