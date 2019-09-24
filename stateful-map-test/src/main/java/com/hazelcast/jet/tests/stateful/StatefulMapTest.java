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

import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Jet;
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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.tests.common.Util.cancelJobAndJoin;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static com.hazelcast.jet.tests.stateful.TransactionGenerator.transactionEventSource;
import static com.hazelcast.jet.tests.stateful.VerificationEntryProcessor.predicate;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class StatefulMapTest extends AbstractSoakTest {

    static final String REPLICATED_MAP = "replicatedMap";
    static final String TOTAL_KEY_COUNT = "total-key-count";
    static final String STOP_GENERATION_MESSAGE = "stop-generation";
    static final String CURRENT_TX_ID = "current-tx-id";
    static final long TIMED_OUT_CODE = -2;

    private static final String TX_MAP = "total-key-count-map";
    private static final String TIMEOUT_TX_MAP = "timeout-key-count-verification";
    private static final long PENDING_CODE = -1;
    private static final int DEFAULT_TX_TIMEOUT = 10;
    private static final int DEFAULT_GENERATOR_BATCH_COUNT = 100;
    private static final int DEFAULT_TX_PER_SECOND = 1000;
    private static final int DELAY_BETWEEN_STATUS_CHECKS = 30;
    private static final int DEFAULT_VERIFICATION_GAP_MINUTES = 1;
    private static final int DEFAULT_SNAPSHOT_INTERVAL_MILLIS = 5000;

    private JetInstance stableClusterClient;
    private int txTimeoutSeconds;
    private int txPerSecond;
    private int generatorBatchCount;
    private int snapshotIntervalMillis;
    private long estimatedTxIdGap;


    public static void main(String[] args) throws Exception {
        new StatefulMapTest().run(args);
    }

    @Override
    protected void init() throws Exception {
        txTimeoutSeconds = propertyInt("txTimeoutSeconds", DEFAULT_TX_TIMEOUT);
        txPerSecond = propertyInt("txPerSecond", DEFAULT_TX_PER_SECOND);
        generatorBatchCount = propertyInt("generatorBatchCount", DEFAULT_GENERATOR_BATCH_COUNT);
        snapshotIntervalMillis = propertyInt("snapshotIntervalMillis", DEFAULT_SNAPSHOT_INTERVAL_MILLIS);
        int verificationGapMinutes = propertyInt("verificationGapMinutes", DEFAULT_VERIFICATION_GAP_MINUTES);
        estimatedTxIdGap = MINUTES.toSeconds(verificationGapMinutes) * txPerSecond / 2;

        stableClusterClient = Jet.newJetClient(remoteClusterClientConfig());
    }

    @Override
    protected void test() throws Throwable {
        Throwable[] exceptions = new Throwable[2];
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(() -> {
            try {
                testInternal(jet, "Dynamic-StatefulMapTest");
            } catch (Throwable t) {
                exceptions[0] = t;
            }
        });
        executorService.execute(() -> {
            try {
                testInternal(stableClusterClient, "Stable-StatefulMapTest");
            } catch (Throwable t) {
                exceptions[1] = t;
            }
        });
        executorService.shutdown();
        long extraDuration = 2 * SECONDS.toMillis(txTimeoutSeconds + DELAY_BETWEEN_STATUS_CHECKS);
        executorService.awaitTermination(durationInMillis + extraDuration, MILLISECONDS);

        if (exceptions[0] != null) {
            logger.severe("Exception in Dynamic cluster test", exceptions[0]);
            throw exceptions[0];
        }
        if (exceptions[1] != null) {
            logger.severe("Exception in Stable cluster test", exceptions[1]);
            throw exceptions[1];
        }
    }

    private void testInternal(JetInstance client, String name) {
        ReplicatedMap<String, Long> replicatedMap = client.getReplicatedMap(REPLICATED_MAP);
        IMapJet<Long, Long> txMap = client.getMap(TX_MAP);
        IMapJet<Long, Long> timeoutTxMap = client.getMap(TIMEOUT_TX_MAP);
        ILogger logger = client.getHazelcastInstance().getLoggingService().getLogger(name);

        JobConfig jobConfig = new JobConfig()
                .setName(name)
                .setProcessingGuarantee(name.startsWith("Dynamic") ? EXACTLY_ONCE : NONE)
                .setSnapshotIntervalMillis(snapshotIntervalMillis);

        Job job = client.newJob(buildPipeline(), jobConfig);

        long totalTxNumber = 0;
        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            JobStatus jobStatus = getJobStatusWithRetry(job);
            assertNotEquals(name, FAILED, jobStatus);
            sleepSeconds(DELAY_BETWEEN_STATUS_CHECKS);

            Long currentTxId = replicatedMap.get(CURRENT_TX_ID);
            if (currentTxId == null) {
                continue;
            }
            totalTxNumber += completedTxCount(txMap, logger, currentTxId);
        }
        replicatedMap.put(STOP_GENERATION_MESSAGE, 1L);
        sleepSeconds(2 * txTimeoutSeconds);
        cancelJobAndJoin(job);
        sleepSeconds(DELAY_BETWEEN_STATUS_CHECKS);

        long expectedTotalKeyCount = replicatedMap.remove(TOTAL_KEY_COUNT);
        totalTxNumber += completedTxCount(txMap, logger, Long.MAX_VALUE);
        assertEquals(name, expectedTotalKeyCount, totalTxNumber);

        Set<Long> keySet = timeoutTxMap.keySet(mapEntry -> ((long) mapEntry.getValue()) != TIMED_OUT_CODE);
        assertEquals(name, 0, keySet.size());
        assertEquals(name, expectedTotalKeyCount / generatorBatchCount, timeoutTxMap.size());
    }

    private long completedTxCount(IMapJet<Long, Long> txMap, ILogger logger, long currentTxId) {
        Map<Long, Object> verifiedTxs = txMap.executeOnEntries(
                new VerificationEntryProcessor(),
                predicate(currentTxId - estimatedTxIdGap)
        );
        long pendingTxs = verifiedTxs
                .values().stream()
                .mapToInt(o -> (int) o)
                .filter(i -> i == 0)
                .count();
        long completedTxs = verifiedTxs
                .values().stream()
                .mapToInt(o -> (int) o)
                .filter(i -> i == 1)
                .count();

        logger.info(String.format("CurrentTxId: %d, currentMapSize: %d, pendingCount: %d, completedCount: %d",
                currentTxId, txMap.size(), pendingTxs, completedTxs));

        return completedTxs;
    }

    private Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        StreamSource<TransactionEvent> source = transactionEventSource(txPerSecond, generatorBatchCount);

        StreamStage<Map.Entry<Long, Long>> streamStage =
                p.drawFrom(source).withTimestamps(TransactionEvent::timestamp, 0)
                 .groupingKey(TransactionEvent::transactionId)
                 .mapStateful(
                         SECONDS.toMillis(txTimeoutSeconds),
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
                                     entry(transactionId, endEvent.timestamp() - startEvent.timestamp())
                                     : (startEvent != null) ? entry(transactionId, PENDING_CODE) : null;
                         },
                         (startEnd, transactionId, wm) -> (startEnd[0] != null && startEnd[1] == null) ?
                                 entry(transactionId, TIMED_OUT_CODE) : null
                 );
        streamStage
                .filter(e -> e.getKey() < 0)
                .drainTo(Sinks.mapWithMerging(TIMEOUT_TX_MAP, (oldValue, newValue) ->
                        (newValue == PENDING_CODE && oldValue == TIMED_OUT_CODE) ? oldValue : newValue));

        streamStage
                .filter(e -> e.getKey() >= 0)
                .drainTo(Sinks.mapWithMerging(TX_MAP, (oldValue, newValue) ->
                        (newValue == PENDING_CODE && oldValue >= 0) ? oldValue : newValue));
        return p;
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
        if (stableClusterClient != null) {
            stableClusterClient.shutdown();
        }
    }
}
