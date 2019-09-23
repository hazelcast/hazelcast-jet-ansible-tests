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
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.tests.common.AbstractSoakTest;

import java.util.Map;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.tests.common.Util.cancelJobAndJoin;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static com.hazelcast.jet.tests.stateful.TransactionGenerator.transactionEventSource;
import static java.util.concurrent.TimeUnit.SECONDS;

public class StatefulMapTest extends AbstractSoakTest {

    public static final String REPLICATED_MAP = "replicatedMap";
    public static final String TOTAL_KEY_COUNT = "total-key-count";
    public static final String STOP_GENERATION_MESSAGE = "stop-generation";
    public static final String TOTAL_KEY_COUNT_PREFIX = "total";
    public static final String TIMEOUT_KEY_COUNT_PREFIX = "timeout";

    public static final long PENDING_CODE = -1;
    public static final long TIMED_OUT_CODE = -2;
    public static final int DEFAULT_TX_TIMEOUT = 10;
    public static final int DEFAULT_GENERATOR_BATCH_COUNT = 1000;
    public static final int DEFAULT_TX_PER_SECOND = 1000;
    public static final int DELAY_BETWEEN_STATUS_CHECKS = 5;

    private int txTimeout;
    private int txPerSecond;
    private int generatorBatchCount;

    public static void main(String[] args) throws Exception {
        new StatefulMapTest().run(args);
    }

    @Override
    protected void init() throws Exception {
        txTimeout = propertyInt("txTimeout", DEFAULT_TX_TIMEOUT);
        txPerSecond = propertyInt("txPerSecond", DEFAULT_TX_PER_SECOND);
        generatorBatchCount = propertyInt("generatorBatchCount", DEFAULT_GENERATOR_BATCH_COUNT);
    }

    @Override
    protected void test() throws Exception {
        Job job = jet.newJob(buildPipeline());

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            JobStatus jobStatus = getJobStatusWithRetry(job);
            assertNotEquals(FAILED, jobStatus);
            sleepSeconds(DELAY_BETWEEN_STATUS_CHECKS);
        }
        ReplicatedMap<String, Long> replicatedMap = jet.getHazelcastInstance().getReplicatedMap(REPLICATED_MAP);
        replicatedMap.put(STOP_GENERATION_MESSAGE, 1L);
        sleepSeconds(2 * txTimeout);
        cancelJobAndJoin(job);
        sleepSeconds(DELAY_BETWEEN_STATUS_CHECKS);
        replicatedMap.remove(STOP_GENERATION_MESSAGE);

        long expectedTotalKeyCount = replicatedMap.remove(TOTAL_KEY_COUNT);
        long totalKeyCount = replicatedMap.entrySet()
                                          .stream()
                                          .filter(e -> e.getKey().startsWith(TOTAL_KEY_COUNT_PREFIX))
                                          .mapToLong(Map.Entry::getValue)
                                          .sum();
        long totalTimeoutKeyCount = replicatedMap.entrySet()
                                                 .stream()
                                                 .filter(e -> e.getKey().startsWith(TIMEOUT_KEY_COUNT_PREFIX))
                                                 .mapToLong(Map.Entry::getValue)
                                                 .sum();
        assertEquals(expectedTotalKeyCount, totalKeyCount);
        assertEquals(expectedTotalKeyCount / generatorBatchCount, totalTimeoutKeyCount);

    }

    private Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        StreamSource<TransactionEvent> source = transactionEventSource(txPerSecond, generatorBatchCount);
        p.drawFrom(source).withTimestamps(TransactionEvent::timestamp, 0)
         .groupingKey(TransactionEvent::transactionId)
         .mapStateful(
                 SECONDS.toMillis(txTimeout),
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
         )
         .drainTo(StatefulMapVerifier.verifier());
        return p;
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
    }
}
