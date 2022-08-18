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

package com.hazelcast.jet.tests.cdc.sink;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.cdc.CdcSinks;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.impl.ChangeRecordImpl;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static com.hazelcast.jet.cdc.Operation.DELETE;
import static com.hazelcast.jet.cdc.Operation.INSERT;
import static com.hazelcast.jet.cdc.Operation.SYNC;
import static com.hazelcast.jet.cdc.Operation.UPDATE;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static com.hazelcast.jet.tests.common.Util.waitForJobStatus;

public class CdcSinkTest extends AbstractSoakTest {

    public static final String SINK_MAP_NAME = "CdcSinkTestSinkMap";
    public static final String EXPECTED_VALUE_PATTERN = "name_%d_1_1";

    private static final int DEFAULT_SLEEP_MS_BETWEEN_ITEM = 1;
    private static final int DEFAULT_PRESERVE_ITEM_MOD = 20_000;
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;
    private static final int ASSERTION_RETRY_COUNT = 60;
    private static final String KEY_PATTERN = "{\"id\":%d}";
    private static final String VALUE_PATTERN = "{\"id\":%d,\"name\":\"name_%d_%d_%d\"," +
            "\"__op\":\"%s\",\"__ts_ms\":1588927306264,\"__deleted\":\"%b\"}";

    private int sleepMsBetweenItem;
    private int preserveItemMod;
    private long snapshotIntervalMs;

    public static void main(String[] args) throws Exception {
        new CdcSinkTest().run(args);
    }

    @Override
    public void init(HazelcastInstance client) throws Exception {
        sleepMsBetweenItem = propertyInt("sleepMsBetweenItem", DEFAULT_SLEEP_MS_BETWEEN_ITEM);
        preserveItemMod = propertyInt("preserveItemMod", DEFAULT_PRESERVE_ITEM_MOD);
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);
    }

    @Override
    protected boolean runOnBothClusters() {
        return true;
    }

    @Override
    public void test(HazelcastInstance client, String name) throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(name);
        if (name.startsWith(STABLE_CLUSTER)) {
            jobConfig.addClass(CdcSinkTest.class, CdcSinkVerifier.class);
        } else {
            jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
            jobConfig.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
        }
        Job job = client.getJet().newJob(pipeline(name), jobConfig);

        waitForJobStatus(job, RUNNING);

        CdcSinkVerifier verifier = new CdcSinkVerifier(stableClusterClient, name, preserveItemMod, logger);
        verifier.start();

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            if (getJobStatusWithRetry(job) == FAILED) {
                job.join();
            }
            verifier.checkStatus();
            sleepMinutes(1);
        }
        log("Going to do final checks.", name);
        verifier.checkStatus();
        int lastPreservedItem = verifier.finish();
        log("Verifier stopped. Checked items: " + lastPreservedItem, name);
        assertTrue(lastPreservedItem > 0);
        job.cancel();
        assertMap(name, lastPreservedItem);
    }

    private Pipeline pipeline(String clusterName) {
        int sleep = sleepMsBetweenItem;
        int preserveItem = preserveItemMod;

        Pipeline pipeline = Pipeline.create();

        StreamSource<ChangeRecord> source = SourceBuilder
                .stream("srcForCdcSink", procCtx -> new int[1])
                .<ChangeRecord>fillBufferFn((ctx, buf) -> {
                    int messageId = ctx[0];
                    int entryId = messageId / 3;
                    String key = String.format(KEY_PATTERN, entryId);
                    buf.add(insert(key, entryId, messageId));
                    buf.add(update(key, entryId, messageId + 1));
                    buf.add(delete(key, entryId, messageId + 2, preserveItem));
                    ctx[0] += 3;
                    sleepMillis(sleep);
                })
                .createSnapshotFn(ctx -> ctx[0])
                .restoreSnapshotFn((ctx, state) -> ctx[0] = state.get(0))
                .build();

        Sink<ChangeRecord> sink;
        if (clusterName.contains(DYNAMIC_CLUSTER)) {
            sink = CdcSinks.remoteMap(SINK_MAP_NAME + clusterName,
                    stableClusterClientConfig,
                    r -> (Integer) r.key().toMap().get("id"),
                    r -> (String) r.value().toMap().get("name"));
        } else {
            sink = CdcSinks.map(SINK_MAP_NAME + clusterName,
                    r -> (Integer) r.key().toMap().get("id"),
                    r -> (String) r.value().toMap().get("name"));
        }

        pipeline.readFrom(source)
                .withoutTimestamps()
                .rebalance()
                .writeTo(sink);

        return pipeline;
    }

    private void assertMap(String clusterName, int lastPreservedItem) {
        IMap<Integer, String> map = stableClusterClient.getMap(SINK_MAP_NAME + clusterName);
        for (int i = 0; i < ASSERTION_RETRY_COUNT; i++) {
            log("Asserting items with retry[" + i + "], lastPreservedItem: " + lastPreservedItem, clusterName);
            if (assertItems(clusterName, lastPreservedItem, map)) {
                return;
            }
            sleepSeconds(1);
        }
        throw new AssertionError();
    }

    private boolean assertItems(String clusterName, int lastPreservedItem, IMap<Integer, String> map) {
        for (int itemId = 0; itemId <= lastPreservedItem; itemId += preserveItemMod) {
            String expectedValue = prepareExpectedValue(itemId);
            String value = map.get(itemId);
            if (!expectedValue.equals(value)) {
                log("expected value:" + expectedValue + ", actual value: " + value, clusterName);
                return false;
            }
        }
        Predicate<Integer, String> predicate = Predicates.lessEqual("__key", lastPreservedItem);
        int expectedSize = lastPreservedItem / preserveItemMod + 1;
        Set<Map.Entry<Integer, String>> entries = map.entrySet(predicate);
        int actualSize = entries.size();
        log("expected size:" + expectedSize + ", actual size: " + actualSize, clusterName);
        if (expectedSize != actualSize) {
            for (Map.Entry<Integer, String> entry : entries) {
                if (entry.getKey() % preserveItemMod != 0) {
                    log("unexpected entry in map : " + entry.getKey() + " - " + entry.getValue(), clusterName);
                }
            }
            return false;
        }
        return true;
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
    }

    private void log(String message, String clusterName) {
        logger.info("Cluster" + clusterName + "\t\t" + message);
    }

    /**
     * key   = {"id":entryId}
     * value = {"id":entryId, "name":"name_entryId_0_0", "__op":"c", "__ts_ms":1588927306264, "__deleted":false}
     */
    private static ChangeRecord insert(String key, int entryId, int messageId) {
        return record(messageId, INSERT, key, String.format(VALUE_PATTERN, entryId, entryId, 0, 0, "c", false));
    }

    /**
     * key   = {"id":entryId}
     * value = {"id":entryId, "name":"name_entryId_1_0", "__op":"u", "__ts_ms":1588927306264, "__deleted":false}
     */
    private static ChangeRecord update(String key, int entryId, int messageId) {
        return record(messageId, UPDATE, key, String.format(VALUE_PATTERN, entryId, entryId, 1, 0, "u", false));
    }

    /**
     * key   = {"id":entryId}
     * preserved-value = {"id":entryId, "name":"name_entryId_1_1", "__op":"u", "__ts_ms":1588927306264, "__deleted":false}
     * deleted-value = {"id":entryId, "name":"name_entryId_1_0", "__op":"d", "__ts_ms":1588927306264, "__deleted":true}
     */
    private static ChangeRecord delete(String key, int entryId, int messageId, int preserveItem) {
        if (entryId % preserveItem == 0) {
            return record(messageId, UPDATE, key, String.format(VALUE_PATTERN, entryId, entryId, 1, 1, "u", false));
        }
        return record(messageId, DELETE, key, String.format(VALUE_PATTERN, entryId, entryId, entryId, 0, "d", true));
    }

    private static ChangeRecord record(int messageId, Operation op, String key, String value) {
        Supplier<String> oldValue = op == INSERT || op == SYNC
                ? null
                : () -> value;
        Supplier<String> newValue = op == DELETE
                ? null
                : () -> value;
        return new ChangeRecordImpl(
                0,
                0,
                messageId,
                op,
                key,
                oldValue,
                newValue,
                "table",
                "schema",
                "database"
        );
    }

    private static String prepareExpectedValue(int entryId) {
        return String.format(EXPECTED_VALUE_PATTERN, entryId);
    }

}
