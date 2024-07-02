/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.amazonaws.SDKGlobalConfiguration;
import com.hazelcast.core.HazelcastInstance;
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
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;

import com.hazelcast.shaded.com.amazonaws.regions.Regions;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.tests.common.Util.entry;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static com.hazelcast.jet.tests.common.Util.waitForJobStatus;

public class KinesisTest extends AbstractJetSoakTest {

    private static final int DEFAULT_SHARD_COUNT = 1;
    private static final int DEFAULT_PARTITION_KEYS = 10_000;
    private static final int DEFAULT_SLEEP_MS_BETWEEN_ITEM = 20;
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;
    private static final int NEW_DATA_PROCESSED_CHECK_CYCLE = 10;
    private static final int ASSERTION_RETRY_COUNT = 60;

    private final List<String> streamNames = new ArrayList<>(2);

    private AwsConfig awsConfig;
    private Helper helper;

    private int partitionKeys;
    private int shardCount;
    private long sleepMsBetweenItem;
    private long snapshotIntervalMs;
    private String runIdSuffix;

    private boolean testFailed;

    public static void main(String[] args) throws Exception {
        System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true");
        new KinesisTest().run(args);
    }

    @Override
    protected void init(HazelcastInstance client) {
        awsConfig = new AwsConfig()
                .withEndpoint(property("endpoint", "http://localhost:4566"))
                .withRegion(Regions.US_EAST_1.getName())
                .withCredentials(property("accessKey", "accessKey"), property("secretKey", "secretKey"));

        shardCount = propertyInt("shardCount", DEFAULT_SHARD_COUNT);
        partitionKeys = propertyInt("partitionKeys", DEFAULT_PARTITION_KEYS);

        sleepMsBetweenItem = propertyInt("sleepMsBetweenItem", DEFAULT_SLEEP_MS_BETWEEN_ITEM);
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);

        runIdSuffix = property("runIdSuffix", "defaultStreamSuffix");

        helper = new Helper(awsConfig.buildClient(), logger);
    }

    @Override
    protected void teardown(Throwable t) {
        if (!testFailed) {
            boolean failedToDeleteStream = false;
            for (String streamName : streamNames) {
                try {
                    helper.deleteStream(streamName);
                } catch (Exception e) {
                    logger.severe("Exception when deleting stream: " + streamName, e);
                    failedToDeleteStream = true;
                }
            }
            if (t == null && failedToDeleteStream) {
                logger.severe("Failed to delete some streams, see above for the stacktrace");
                System.exit(1);
            }
        } else {
            logger.severe("KINESIS STREAMS WERE NOT DELETED BECAUSE TESTS FAILED. DELETE THEM AFTER INVESTIGATION");
        }
    }

    @Override
    protected boolean runOnBothClusters() {
        return true;
    }

    @Override
    protected void test(HazelcastInstance client, String cluster) throws Throwable {
        String name = cluster + runIdSuffix;
        streamNames.add(name);
        helper.createStream(name, shardCount);

        JobConfig readJobConfig = jobConfig(name + "_read");
        Job readJob = client.getJet().newJob(readPipeline(name, awsConfig, name), readJobConfig);
        waitForJobStatus(readJob, RUNNING);

        JobConfig writeJobConfig = jobConfig(name + "write");
        Job writeJob = client.getJet().newJob(writePipeline(name, awsConfig), writeJobConfig);
        waitForJobStatus(writeJob, RUNNING);

        int cycles = 0;
        int latestCycle = 0;
        long latestCheckedCount = 0;
        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            if (getJobStatusWithRetry(readJob) == FAILED) {
                testFailed = true;
                writeJob.cancel();
                readJob.join();
            }
            if (getJobStatusWithRetry(writeJob) == FAILED) {
                testFailed = true;
                readJob.cancel();
                writeJob.join();
            }

            cycles++;
            if (cycles % NEW_DATA_PROCESSED_CHECK_CYCLE == 0) {
                latestCycle = cycles;
                latestCheckedCount
                        = checkNewDataProcessed(client, name, latestCheckedCount, NEW_DATA_PROCESSED_CHECK_CYCLE);
                log("Check for insert changes succeeded, latestCheckedCount: " + latestCheckedCount, name);
            }
            sleepMinutes(1);
        }
        if (latestCycle != cycles) {
            latestCheckedCount = checkNewDataProcessed(client, name, latestCheckedCount, cycles - latestCycle);
            log("Check for insert changes succeeded, latestCheckedCount: " + latestCheckedCount, name);
        }
        log("Job completed", name);
    }

    private JobConfig jobConfig(String name) {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(name);
        if (name.contains(STABLE_CLUSTER)) {
            jobConfig.addClass(KinesisTest.class, Helper.class, KinesisVerificationP.class);
        } else {
            jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
            jobConfig.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
        }
        return jobConfig;
    }

    private Pipeline writePipeline(String stream, AwsConfig awsConfig) {
        int localPartitionKeys = partitionKeys;
        long localSleepMsBetweenItem = sleepMsBetweenItem;
        StreamSource<Map.Entry<String, byte[]>> generatorSource = SourceBuilder
                .stream("KinesisDataSource", procCtx -> new long[1])
                .<Map.Entry<String, byte[]>>fillBufferFn((ctx, buf) -> {
                    long messageId = ctx[0];
                    String partitionKey = Long.toString(messageId % localPartitionKeys);
                    byte[] data = Long.toString(messageId).getBytes(StandardCharsets.UTF_8);
                    buf.add(entry(partitionKey, data));
                    ctx[0] += 1;
                    sleepMillis(localSleepMsBetweenItem);
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

        Sink<String> verificationSink = KinesisVerificationP.sink(cluster);

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(kinesisSource)
                .withoutTimestamps()
                .map(KinesisTest::valueAsString)
                .writeTo(verificationSink);
        return pipeline;
    }

    private long checkNewDataProcessed(HazelcastInstance client, String cluster, long latestChecked, int elapsedCycle) {
        Map<String, Long> latestCounterMap = client.getMap(KinesisVerificationP.CONSUMED_MESSAGES_MAP_NAME);
        long latest = 0;
        for (int i = 0; i < ASSERTION_RETRY_COUNT; i++) {
            Long latestLong = latestCounterMap.get(cluster);
            if (latestLong != null) {
                latest = latestLong;
                break;
            }
            sleepSeconds(1);
        }
        assertTrue(String.format("LatestChecked was: %d, but after %d minutes latest is %d",
                latestChecked, elapsedCycle, latest), latest > latestChecked);
        return latest;
    }

    private static String valueAsString(Map.Entry<?, byte[]> entry) {
        return new String(entry.getValue());
    }

    private void log(String message, String clusterName) {
        logger.info("Cluster" + clusterName + "\t\t" + message);
    }
}
