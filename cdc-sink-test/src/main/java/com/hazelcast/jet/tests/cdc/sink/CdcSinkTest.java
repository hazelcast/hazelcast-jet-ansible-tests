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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.cdc.CdcSinks;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.impl.ChangeRecordImpl;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.map.IMap;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static com.hazelcast.jet.tests.common.Util.waitForJobStatus;
import static org.junit.Assert.assertNotNull;

public class CdcSinkTest extends AbstractSoakTest {

    public static final String SINK_MAP_NAME = "CdcSinkTestSinkMap";

    private static final int DEFAULT_SLEEP_MS_BETWEEN_ITEM = 1;
    private static final int DEFAULT_PRESERVE_ITEM_MOD = 20_000;
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;
    private static final int ASSERTION_RETRY_COUNT = 60;

    private int sleepMsBetweenItem;
    private int preserveItemMod;
    private long snapshotIntervalMs;

    public static void main(String[] args) throws Exception {
        new CdcSinkTest().run(args);
    }

    @Override
    public void init(JetInstance client) throws Exception {
        sleepMsBetweenItem = propertyInt("sleepMsBetweenItem", DEFAULT_SLEEP_MS_BETWEEN_ITEM);
        preserveItemMod = propertyInt("preserveItemMod", DEFAULT_PRESERVE_ITEM_MOD);
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);
    }

    @Override
    protected boolean runOnBothClusters() {
        return true;
    }

    @Override
    public void test(JetInstance client, String name) throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(name);
        if (name.startsWith(STABLE_CLUSTER)) {
            jobConfig.addClass(CdcSinkTest.class, CdcSinkVerifier.class);
        } else {
            jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
            jobConfig.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
        }
        Job job = client.newJob(pipeline(name), jobConfig);

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
        int checkedItems = verifier.finish();
        log("Verifier stopped. Checked items: " + checkedItems, name);
        assertTrue(checkedItems > 0);
        assertMap(name, checkedItems);
        job.cancel();
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
                    int messageType = messageId % 3;
                    ChangeRecord changeRecord;
                    if (messageType == 0) {
                        // insert
                        changeRecord = new ChangeRecordImpl(0, messageId, "{\"id\":" + entryId + "}",
                                "{\"id\":" + entryId + ",\"name\":\"name" + entryId + "\","
                                + "\"__op\":\"c\",\"__ts_ms\":1588927306264,\"__deleted\":\"false\"}");
                    } else {
                        if (messageType == 1) {
                            // update
                            changeRecord = new ChangeRecordImpl(0, messageId, "{\"id\":" + entryId + "}",
                                    "{\"id\":" + entryId + ",\"name\":\"name" + entryId + "_" + entryId + "\","
                                    + "\"__op\":\"u\",\"__ts_ms\":1588927306264,\"__deleted\":\"false\"}");
                        } else {
                            if (entryId % preserveItem == 0) {
                                // For every "preserveItemMod" item do not remove it from the map.
                                // This should happen ~ once per minute when default params are used.
                                changeRecord = new ChangeRecordImpl(0, messageId, "{\"id\":" + entryId + "}",
                                        "{\"id\":" + entryId + ","
                                        + "\"name\":\"name" + entryId + "_" + entryId + "_" + entryId + "\","
                                        + "\"__op\":\"u\",\"__ts_ms\":1588927306264,\"__deleted\":\"false\"}");
                            } else {
                                // delete
                                changeRecord = new ChangeRecordImpl(0, messageId, "{\"id\":" + entryId + "}",
                                        "{\"id\":" + entryId + ",\"name\":\"name" + entryId + "_" + entryId + "\","
                                        + "\"__op\":\"d\",\"__ts_ms\":1588927306264,\"__deleted\":\"true\"}");
                            }
                        }
                    }
                    buf.add(changeRecord);
                    ctx[0]++;
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

    private void assertMap(String clusterName, int checkedItems) {
        IMap<Integer, String> map = stableClusterClient.getMap(SINK_MAP_NAME + clusterName);
        // We want to check only items to latest verified
        for (int i = 0; i < ASSERTION_RETRY_COUNT; i++) {
            Map<Integer, String> mapForCheck = map.entrySet().stream()
                    .filter((entry) -> (entry.getKey() < checkedItems * preserveItemMod))
                    .collect(Collectors.toMap(t -> t.getKey(), t -> t.getValue()));
            if (mapForCheck.size() == checkedItems) {
                boolean checkFailed = false;
                for (int j = 0; j < checkedItems; j++) {
                    int checkId = j * preserveItemMod;
                    String value = mapForCheck.get(checkId);
                    if (value == null || !value.equals(prepareExpectedValue(checkId))) {
                        checkFailed = true;
                        break;
                    }
                }
                if (!checkFailed) {
                    return;
                }
            }
            sleepSeconds(1);
        }

        Map<Integer, String> mapForCheck = map.entrySet().stream()
                .filter((entry) -> (entry.getKey() < checkedItems * preserveItemMod))
                .collect(Collectors.toMap(t -> t.getKey(), t -> t.getValue()));
        // log it, test will fail in following assert
        if (checkedItems != mapForCheck.size()) {
            int printLimit = 10_000;
            int printLimitCounter = 0;
            log("There are " + mapForCheck.size() + " entries in checked Map.", clusterName);
            for (Map.Entry<Integer, String> entry : mapForCheck.entrySet()) {
                log(entry.getKey() + ":" + entry.getValue(), clusterName);
                if (printLimitCounter > printLimit) {
                    log("There are more entries in map. Truncated.", clusterName);
                    break;
                }
                printLimitCounter++;
            }
        }
        assertEquals(checkedItems, mapForCheck.size());

        for (int i = 0; i < checkedItems; i++) {
            int checkId = i * preserveItemMod;
            String value = mapForCheck.get(checkId);
            assertNotNull("Null value for key " + checkId, value);
            assertEquals(prepareExpectedValue(checkId), value);
        }
    }

    public static String prepareExpectedValue(int entryId) {
        return "name" + entryId + "_" + entryId + "_" + entryId;
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
    }

    private void log(String message, String clusterName) {
        logger.info("Cluster" + clusterName + "\t\t" + message);
    }

}
