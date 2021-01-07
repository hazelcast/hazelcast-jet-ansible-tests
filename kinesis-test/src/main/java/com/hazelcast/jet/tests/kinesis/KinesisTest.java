package com.hazelcast.jet.tests.kinesis;

import com.amazonaws.regions.Regions;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.kinesis.impl.AwsConfig;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.jet.tests.common.Util;

public class KinesisTest extends AbstractSoakTest {

    private final String stream = KinesisTest.class.getSimpleName();
    private AwsConfig awsConfig;
    private KinesisProducer producer;

    public static void main(String[] args) throws Exception {
        new KinesisTest().run(args);
    }

    @Override
    protected boolean runOnBothClusters() {
        return false;
    }

    @Override
    protected void init(JetInstance client) throws Exception {
        String endpoint = property("endpoint", "http://localhost:4566");
        String region = property("region", Regions.US_EAST_1.getName());
        String accessKey = property("accessKey", "accessKey");
        String secretKey = property("secretKey", "secretKey");

        int batchCount = propertyInt("batchCount", 100);
        int shardCount = propertyInt("shardCount", 1);
        int sleepBetweenBatches = propertyInt("sleepBetweenBatches", 100);

        awsConfig = new AwsConfig().withEndpoint(endpoint).withRegion(region).withCredentials(accessKey, secretKey);
        producer = new KinesisProducer(awsConfig, "stream", shardCount, batchCount, sleepBetweenBatches);
        producer.start();
    }

    @Override
    protected void test(JetInstance client, String name) throws Throwable {
        Util.sleepSeconds(10);
        System.out.println(producer.stop());
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
    }
}
