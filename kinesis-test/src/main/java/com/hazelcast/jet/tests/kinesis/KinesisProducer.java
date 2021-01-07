package com.hazelcast.jet.tests.kinesis;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.CreateStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.hazelcast.jet.kinesis.impl.AwsConfig;
import com.hazelcast.jet.tests.common.Util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;

public class KinesisProducer {

    private final AmazonKinesisAsync client;
    private final Thread thread;
    private final String stream;
    private final int batchCount;
    private final int sleepBetweenBatches;
    private int counter;

    private volatile boolean running = true;
    private volatile int totalProduced;

    public KinesisProducer(AwsConfig awsConfig, String stream, int shard, int batchCount, int sleepBetweenBatches) {
        this.stream = stream;
        this.batchCount = batchCount;
        this.sleepBetweenBatches = sleepBetweenBatches;
        client = awsConfig.buildClient();
        client.createStream(stream, shard);

        thread = new Thread(() -> uncheckRun(this::produce));
    }

    private void produce() throws ExecutionException, InterruptedException {
        List<PutRecordsRequestEntry> entryList = new ArrayList<>();
        while (running) {
            entryList.clear();
            for (int i = 0; i < batchCount; i++) {
                entryList.add(new PutRecordsRequestEntry().withData(data()).withPartitionKey(key()));
                counter++;
            }
            client.putRecordsAsync(new PutRecordsRequest().withRecords(entryList).withStreamName(stream)).get();
            sleepMillis(sleepBetweenBatches);
        }
        totalProduced = counter;
    }

    private ByteBuffer data() {
        return ByteBuffer.wrap(("item-" + counter).getBytes());
    }

    private String key() {
        return "key-" + counter;
    }

    public void start() {
        thread.start();
    }

    public int stop() throws InterruptedException {
        running = false;
        thread.join();
        client.shutdown();
        return totalProduced;
    }

}
