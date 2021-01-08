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

package com.hazelcast.jet.tests.kinesis;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.kinesis.KinesisSinks;
import com.hazelcast.jet.kinesis.KinesisSources;
import com.hazelcast.jet.kinesis.impl.AwsConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.tests.common.AbstractSoakTest;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.tests.common.Util.entry;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static com.hazelcast.jet.tests.common.Util.waitForJobStatus;

public class KinesisTest extends AbstractSoakTest {

    private static final int DEFAULT_SHARD_COUNT = 1;
    private static final int DEFAULT_PARTITION_KEYS = 10_000;
    private static final int DEFAULT_SLEEP_MS_BETWEEN_ITEM = 1;
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;
    private static final int ASSERTION_RETRY_COUNT = 60;

    private AwsConfig awsConfig;
    private Helper helper;

    private String streamName;
    private int partitionKeys;
    private long sleepMsBetweenItem;
    private long snapshotIntervalMs;

    public static void main(String[] args) throws Exception {
        new KinesisTest().run(args);
    }

    @Override
    protected void init(JetInstance client) {
        awsConfig = new AwsConfig()
                .withEndpoint(property("endpoint", null))
                .withRegion(property("region", null))
                .withCredentials(property("accessKey", null), property("secretKey", null));

        streamName = property("streamName", KinesisTest.class.getSimpleName());
        int shardCount = propertyInt("shardCount", DEFAULT_SHARD_COUNT);

        partitionKeys = propertyInt("partitionKeys", DEFAULT_PARTITION_KEYS);

        sleepMsBetweenItem = propertyInt("sleepMsBetweenItem", DEFAULT_SLEEP_MS_BETWEEN_ITEM);
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);

        helper = new Helper(awsConfig.buildClient(), streamName, logger);
        helper.createStream(shardCount);
    }

    @Override
    protected void teardown(Throwable t) {
        helper.deleteStream();
    }

    @Override
    protected boolean runOnBothClusters() {
        return true;
    }

    @Override
    protected void test(JetInstance client, String cluster) throws Throwable {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(cluster);
        if (cluster.startsWith(STABLE_CLUSTER)) {
            jobConfig.addClass(KinesisTest.class, Helper.class, VerificationProcessor.class);
        } else {
            jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
            jobConfig.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
        }

        Job readJob = client.newJob(readPipeline(streamName, awsConfig, cluster), jobConfig);
        waitForJobStatus(readJob, RUNNING);

        Job writeJob = client.newJob(writePipeline(streamName, awsConfig), jobConfig);
        waitForJobStatus(writeJob, RUNNING);

        int cycles = 0;
        int latestCheckedCount = 0;
        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            if (getJobStatusWithRetry(readJob) == FAILED) {
                writeJob.cancel();
                writeJob.join();
                readJob.join();
            }
            if (getJobStatusWithRetry(writeJob) == FAILED) {
                readJob.cancel();
                readJob.join();
                writeJob.join();
            }

            cycles++;
            if (cycles % 20 == 0) {
                latestCheckedCount = checkNewDataProcessed(client, cluster, latestCheckedCount);
                log("Check for insert changes succeeded, latestCheckedCount: " + latestCheckedCount, cluster);
            }
            sleepMinutes(1);
        }
        log("Job completed", cluster);
    }

    private Pipeline writePipeline(String stream, AwsConfig awsConfig) {
        StreamSource<Map.Entry<String, byte[]>> generatorSource = SourceBuilder
                .stream("dataSource", procCtx -> new long[1])
                .<Map.Entry<String, byte[]>>fillBufferFn((ctx, buf) -> {
                    long messageId = ctx[0];
                    String partitionKey = Long.toString(messageId % partitionKeys);
                    byte[] data = Long.toString(messageId).getBytes(StandardCharsets.UTF_8);
                    buf.add(entry(partitionKey, data));
                    ctx[0] += 1;
                    sleepMillis(sleepMsBetweenItem);
                })
                .createSnapshotFn(ctx -> ctx[0])
                .restoreSnapshotFn((ctx, state) -> ctx[0] = state.get(0))
                .build();


        Sink<Map.Entry<String, byte[]>> kinesisSink = KinesisSinks.kinesis(stream)
                .withEndpoint(awsConfig.getEndpoint())
                .withRegion(awsConfig.getRegion())
                .withCredentials(awsConfig.getAccessKey(), awsConfig.getSecretKey())
                .build();

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(generatorSource)
                .withoutTimestamps()
                .rebalance()
                .writeTo(kinesisSink);
        return pipeline;
    }

    private Pipeline readPipeline(String stream, AwsConfig awsConfig, String cluster) {
        StreamSource<Map.Entry<String, byte[]>> kinesisSource = KinesisSources.kinesis(stream)
                .withEndpoint(awsConfig.getEndpoint())
                .withRegion(awsConfig.getRegion())
                .withCredentials(awsConfig.getAccessKey(), awsConfig.getSecretKey())
                .build();

        Sink<String> verificationSink = VerificationProcessor.sink("KinesisVerificationSink", cluster);

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(kinesisSource)
                .withoutTimestamps()
                .map(Map.Entry::getValue)
                .map(String::new)
                .writeTo(verificationSink);
        return pipeline;
    }

    private int checkNewDataProcessed(JetInstance client, String cluster, int latestChecked) {
        Map<String, Integer> latestCounterMap = client.getMap(VerificationProcessor.CONSUMED_MESSAGES_MAP_NAME);
        int latest = 0;
        for (int i = 0; i < ASSERTION_RETRY_COUNT; i++) {
            Integer latestInteger = latestCounterMap.get(cluster);
            if (latestInteger != null) {
                latest = latestInteger;
                break;
            }
            sleepSeconds(1);
        }
        assertTrue("LatestChecked was: " + latestChecked + ", but after 20 minutes latest is: " + latest,
                latest > latestChecked);
        return latest;
    }

    private void log(String message, String clusterName) {
        logger.info("Cluster" + clusterName + "\t\t" + message);
    }
}
