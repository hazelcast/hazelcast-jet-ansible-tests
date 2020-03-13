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

package com.hazelcast.jet.tests.stateful;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;

import java.util.Map;
import java.util.Set;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.tests.common.Util.cancelJobAndJoin;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static com.hazelcast.jet.tests.stateful.TransactionGenerator.transactionEventSource;
import static com.hazelcast.jet.tests.stateful.VerificationEntryProcessor.predicate;
import static java.util.concurrent.TimeUnit.MINUTES;

public class StatefulMapTest extends AbstractSoakTest {

    static final String MESSAGING_MAP = "messagingMap";
    static final String TOTAL_KEY_COUNT = "total-key-count";
    static final String STOP_GENERATION_MESSAGE = "stop-generation";
    static final String CURRENT_TX_ID = "current-tx-id";
    static final long TIMED_OUT_CODE = -1;

    private static final String TX_MAP_STABLE = "total-key-count-map-stable";
    private static final String TIMEOUT_TX_MAP_STABLE = "timeout-key-count-map-stable";
    private static final String TX_MAP_DYNAMIC = "total-key-count-map-dynamic";
    private static final String TIMEOUT_TX_MAP_DYNAMIC = "timeout-key-count-map-dynamic";
    private static final int DEFAULT_TX_TIMEOUT = 5000;
    private static final int DEFAULT_GENERATOR_BATCH_COUNT = 100;
    private static final int DEFAULT_TX_PER_SECOND = 1000;
    private static final int DELAY_BETWEEN_STATUS_CHECKS = 30;
    private static final int DEFAULT_VERIFICATION_GAP_MINUTES = 1;
    private static final int DEFAULT_SNAPSHOT_INTERVAL_MILLIS = 5000;
    private static final int WAIT_TX_TIMEOUT_FACTOR = 4;

    private int txTimeout;
    private int txPerSecond;
    private int generatorBatchCount;
    private int snapshotIntervalMillis;
    private long estimatedTxIdGap;

    public static void main(String[] args) throws Exception {
        new StatefulMapTest().run(args);
    }

    @Override
    protected void init(JetInstance client) throws Exception {
        txTimeout = propertyInt("txTimeout", DEFAULT_TX_TIMEOUT);
        txPerSecond = propertyInt("txPerSecond", DEFAULT_TX_PER_SECOND);
        generatorBatchCount = propertyInt("generatorBatchCount", DEFAULT_GENERATOR_BATCH_COUNT);
        snapshotIntervalMillis = propertyInt("snapshotIntervalMillis", DEFAULT_SNAPSHOT_INTERVAL_MILLIS);
        int verificationGapMinutes = propertyInt("verificationGapMinutes", DEFAULT_VERIFICATION_GAP_MINUTES);
        estimatedTxIdGap = MINUTES.toSeconds(verificationGapMinutes) * txPerSecond / 2;
    }

    @Override
    protected boolean runOnBothClusters() {
        return true;
    }

//    @Override
//    protected void test(Jetin) throws Throwable {
//        Throwable[] exceptions = new Throwable[2];
//        ExecutorService executorService = Executors.newFixedThreadPool(2);
//        executorService.execute(() -> {
//            try {
//                testInternal(jet, "Dynamic-StatefulMapTest");
//            } catch (Throwable t) {
//                logger.severe("Exception in Dynamic cluster test", t);
//                exceptions[0] = t;
//            }
//        });
//        executorService.execute(() -> {
//            try {
//                testInternal(stableClusterClient, "Stable-StatefulMapTest");
//            } catch (Throwable t) {
//                logger.severe("Exception in Stable cluster test", t);
//                exceptions[1] = t;
//            }
//        });
//        executorService.shutdown();
//        long extraDuration = WAIT_TX_TIMEOUT_FACTOR * (SECONDS.toMillis(DELAY_BETWEEN_STATUS_CHECKS) + txTimeout);
//        executorService.awaitTermination(durationInMillis + extraDuration, MILLISECONDS);
//
//        if (exceptions[0] != null) {
//            logger.severe("Exception in Dynamic cluster test", exceptions[0]);
//            throw exceptions[0];
//        }
//        if (exceptions[1] != null) {
//            logger.severe("Exception in Stable cluster test", exceptions[1]);
//            throw exceptions[1];
//        }
//    }

    public void test(JetInstance client, String name) {
        IMap<String, Long> messagingMap = client.getMap(MESSAGING_MAP);
        String txMapName = name.startsWith("Stable") ? TX_MAP_STABLE : TX_MAP_DYNAMIC;
        String timeoutMapName = name.startsWith("Stable") ? TIMEOUT_TX_MAP_STABLE : TIMEOUT_TX_MAP_DYNAMIC;
        IMap<Long, Long> txMap = stableClusterClient.getMap(txMapName);
        IMap<Long, Long> timeoutTxMap = stableClusterClient.getMap(timeoutMapName);

        ILogger logger = client.getHazelcastInstance().getLoggingService().getLogger(name);

        JobConfig jobConfig = new JobConfig()
                .setName(name)
                .setProcessingGuarantee(name.startsWith("Dynamic") ? EXACTLY_ONCE : NONE)
                .setSnapshotIntervalMillis(snapshotIntervalMillis);

        Job job = client.newJob(buildPipeline(txMapName, timeoutMapName), jobConfig);

        long totalTxNumber = 0;
        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            JobStatus jobStatus = getJobStatusWithRetry(job);
            assertNotEquals(name, FAILED, jobStatus);
            sleepSeconds(DELAY_BETWEEN_STATUS_CHECKS);

            Long currentTxId = messagingMap.get(CURRENT_TX_ID);
            if (currentTxId == null) {
                continue;
            }
            totalTxNumber += completedTxCount(txMap, logger, currentTxId);
        }
        logger.info("Stop message generation");
        messagingMap.put(STOP_GENERATION_MESSAGE, 1L);
        sleepMillis(WAIT_TX_TIMEOUT_FACTOR * txTimeout);
        cancelJobAndJoin(client, job);
        sleepSeconds(DELAY_BETWEEN_STATUS_CHECKS);

        long expectedTotalKeyCount = messagingMap.remove(TOTAL_KEY_COUNT);
        totalTxNumber += completedTxCount(txMap, logger, Long.MAX_VALUE);
        assertWithClusterName(name, expectedTotalKeyCount, totalTxNumber);

        Set<Long> keySet = timeoutTxMap.keySet(mapEntry -> mapEntry.getValue() != TIMED_OUT_CODE);
        assertWithClusterName(name, 0, keySet.size());
        assertWithClusterName(name, expectedTotalKeyCount / generatorBatchCount, timeoutTxMap.size());
    }

    private void assertWithClusterName(String clusterName, long expected, long actual) {
        assertEquals(String.format("%s -> expected: %d, actual: %d", clusterName, expected, actual), expected, actual);
    }

    private long completedTxCount(IMap<Long, Long> txMap, ILogger logger, long currentTxId) {
        Map<Long, Integer> verifiedTxs = txMap.executeOnEntries(
                new VerificationEntryProcessor(),
                predicate(currentTxId - estimatedTxIdGap)
        );
        long timeoutTxs = verifiedTxs
                .entrySet().stream()
                .filter(e -> e.getValue().equals(0))
                .peek(e -> logger.severe("Timeout for txId: " + e.getKey()))
                .count();

        long completedTxs = verifiedTxs.size() - timeoutTxs;

        logger.info(String.format("CurrentTxId: %d, currentMapSize: %d, timeoutCount: %d, completedCount: %d",
                currentTxId, txMap.size(), timeoutTxs, completedTxs));

        assertEquals(0, timeoutTxs);

        return completedTxs;
    }

    private Pipeline buildPipeline(String txMapName, String timeoutMapName) {
        Pipeline p = Pipeline.create();
        StreamSource<TransactionEvent> source = transactionEventSource(txPerSecond, generatorBatchCount);

        StreamStage<Map.Entry<Long, Long>> streamStage =
                p.readFrom(source).withTimestamps(TransactionEvent::timestamp, 0)
                 .groupingKey(TransactionEvent::transactionId)
                 .mapStateful(
                         txTimeout,
                         () -> new TransactionEvent[2],
                         (startEnd, transactionId, transactionEvent) -> {
                             if (transactionId == Long.MAX_VALUE) {
                                 // ignore events with txId=Long.MAX_VALUE, these are
                                 // for advancing the wm.
                                 return null;
                             }
                             switch (transactionEvent.type()) {
                                 case START:
                                     startEnd[0] = transactionEvent;
                                     break;
                                 case END:
                                     startEnd[1] = transactionEvent;
                                     break;
                                 default:
                                     System.out.println("Wrong event in the stream: " + transactionEvent.type());
                             }
                             TransactionEvent startEvent = startEnd[0];
                             TransactionEvent endEvent = startEnd[1];
                             return (startEvent != null && endEvent != null) ?
                                     entry(transactionId, endEvent.timestamp() - startEvent.timestamp()) : null;
                         },
                         (startEnd, transactionId, wm) -> {
                             if (startEnd[0] != null && startEnd[1] == null) {
                                 if (transactionId > 0) {
                                     System.out.println("StatefulMapTest Timeout for txId: " + transactionId);
                                 }
                                 return entry(transactionId, TIMED_OUT_CODE);
                             }
                             return null;
                         }
                 );
        streamStage
                .filter(e -> e.getKey() < 0)
                .writeTo(Sinks.remoteMapWithMerging(
                        timeoutMapName,
                        stableClusterClientConfig,
                        (oldValue, newValue) -> {
                            if (oldValue != null && !oldValue.equals(newValue)) {
                                System.out.println("StatefulMapTest timeoutMap duplicate old: "
                                        + oldValue + ", new: " + newValue);
                            }
                            return newValue;
                        }));

        streamStage
                .filter(e -> e.getKey() >= 0)
                .writeTo(Sinks.remoteMapWithMerging(
                        txMapName,
                        stableClusterClientConfig,
                        (oldValue, newValue) -> {
                            if (oldValue != null && !oldValue.equals(newValue)) {
                                System.out.println("StatefulMapTest txMap duplicate old: "
                                        + oldValue + ", new: " + newValue);
                            }
                            return newValue;
                        }));
        return p;
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
    }
}
