/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.tests.eventjournal;

import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.core.WindowDefinition;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.server.JetBootstrap;
import com.hazelcast.map.journal.EventJournalMapEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import tests.eventjournal.EventJournalConsumer;
import tests.eventjournal.EventJournalTradeProducer;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.jet.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Partitioner.HASH_CODE;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.emitByFrame;
import static com.hazelcast.jet.core.WatermarkGenerationParams.wmGenParams;
import static com.hazelcast.jet.core.WatermarkPolicies.withFixedLag;
import static com.hazelcast.jet.core.WindowDefinition.slidingWindowDef;
import static com.hazelcast.jet.core.processor.Processors.accumulateByFrameP;
import static com.hazelcast.jet.core.processor.Processors.combineToSlidingWindowP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamMapP;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class EventJournalTest {

    private JetInstance jet;
    private String offsetReset;
    private String mapName;
    private long durationInMillis;
    private int countPerTicker;
    private int snapshotIntervalMs;
    private int lagMs;
    private int windowSize;
    private int slideBy;
    private ExecutorService producerExecutorService;

    public static void main(String[] args) {
        JUnitCore.main(EventJournalTest.class.getName());
    }

    @Before
    public void setUp() throws Exception {
        String isolatedClientConfig = System.getProperty("isolatedClientConfig");
        if (isolatedClientConfig != null) {
            System.setProperty("hazelcast.client.config", isolatedClientConfig);
        }
        producerExecutorService = Executors.newSingleThreadExecutor();
        mapName = System.getProperty("map", String.format("%s-%d", "event-journal", System.currentTimeMillis()));
        offsetReset = System.getProperty("offsetReset", "earliest");
        lagMs = Integer.parseInt(System.getProperty("lagMs", "3000"));
        snapshotIntervalMs = Integer.parseInt(System.getProperty("snapshotIntervalMs", "500"));
        windowSize = Integer.parseInt(System.getProperty("windowSize", "20"));
        slideBy = Integer.parseInt(System.getProperty("slideBy", "10"));
        countPerTicker = Integer.parseInt(System.getProperty("countPerTicker", "100"));
        durationInMillis = MINUTES.toMillis(Integer.parseInt(System.getProperty("durationInMinutes", "10")));
        jet = JetBootstrap.getInstance();
        Config config = jet.getHazelcastInstance().getConfig();
        EventJournalConfig eventJournalConfig = new EventJournalConfig()
                .setMapName(mapName + "*").setEnabled(true).setCapacity(100_000);
        config.addEventJournalConfig(eventJournalConfig);

    }

    @Test
    public void eventJournalTest() throws Exception {
        JobConfig jobConfig = new JobConfig().addClass(EventJournalTest.class).addClass(EventJournalTradeProducer.class);
        jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        Job job = jet.newJob(testDAG(), jobConfig);
        startProducer();

        ClientMapProxy<Long, Long> resultMap = (ClientMapProxy) jet.getHazelcastInstance().getMap(mapName + "-results");
        int partitionCount = jet.getHazelcastInstance().getPartitionService().getPartitions().size();
        EventJournalConsumer<Long, Long> consumer = new EventJournalConsumer<>(resultMap, partitionCount);

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            List<EventJournalMapEvent<Long, Long>> list = consumer.poll();
            list.forEach(e -> {
                assertEquals("EXACTLY_ONCE -> Unexpected count for " + e.getKey(),
                        countPerTicker, (long) e.getNewValue());
            });
        }

    }

    @After
    public void teardown() throws Exception {
        jet.shutdown();
        producerExecutorService.shutdown();
    }

    private DAG testDAG() {
        WindowDefinition windowDef = slidingWindowDef(windowSize, slideBy);
        AggregateOperation1<Object, LongAccumulator, Long> counting = AggregateOperations.counting();
        DAG dag = new DAG();
        WatermarkGenerationParams<Long> wmGenParams = wmGenParams((Long t) -> t, withFixedLag(lagMs),
                emitByFrame(windowDef), 10_000);

        Vertex journal = dag.newVertex("journal", streamMapP(mapName, e -> e.getType() == ADDED || e.getType() == UPDATED,
                EventJournalMapEvent<Long, Long>::getNewValue, START_FROM_OLDEST, wmGenParams));
        Vertex accumulateByF = dag.newVertex("accumulate-by-frame", accumulateByFrameP(
                wholeItem(), (Long t) -> t, TimestampKind.EVENT, windowDef, counting)
        );
        Vertex slidingW = dag.newVertex("sliding-window", combineToSlidingWindowP(windowDef, counting));
        Vertex writeMap = dag.newVertex("writeMap", SinkProcessors.writeMapP(mapName + "-results"));

        dag
                .edge(between(journal, accumulateByF).partitioned(wholeItem(), HASH_CODE))
                .edge(between(accumulateByF, slidingW).partitioned(entryKey())
                                                      .distributed())
                .edge(between(slidingW, writeMap));
        return dag;
    }

    private void startProducer() {
        producerExecutorService.submit(() -> {
            try (EventJournalTradeProducer tradeProducer = new EventJournalTradeProducer()) {
                tradeProducer.produce(jet.getMap(mapName), countPerTicker);
            }
        });
    }

}
