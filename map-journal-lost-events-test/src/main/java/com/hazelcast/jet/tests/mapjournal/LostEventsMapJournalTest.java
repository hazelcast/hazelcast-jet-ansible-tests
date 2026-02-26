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

package com.hazelcast.jet.tests.mapjournal;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.jet.tests.common.BasicEventJournalProducer;
import com.hazelcast.jet.tests.common.QueueVerifier;
import com.hazelcast.jet.tests.eventjournal.EventJournalConsumer;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.query.Predicates;

import java.util.Map;

import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;

public class LostEventsMapJournalTest extends AbstractJetSoakTest {

    private static final String CLASS_NAME = LostEventsMapJournalTest.class.getSimpleName();
    private static final String SOURCE = CLASS_NAME + "-INPUT";
    private static final String OUTPUT = CLASS_NAME + "-OUTPUT";
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;

    private static final int JOURNAL_CAPACITY_PER_PARTITION = 30;
    private static final int BIG_EVENT_JOURNAL_CAPACITY = 1_500_000;

    private long snapshotIntervalMs;
    private int partitionsSize;

    private transient BasicEventJournalProducer producer;

    public static void main(String[] args) throws Exception {
        new LostEventsMapJournalTest().run(args);
    }

    @Override
    protected void init(HazelcastInstance client) throws Exception {
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);
        partitionsSize = client.getPartitionService().getPartitions().size();
    }

    @Override
    protected void test(HazelcastInstance client, String name) throws Throwable {
        MapConfig mapConfig = new MapConfig(OUTPUT);
        mapConfig.getEventJournalConfig()
                .setCapacity(BIG_EVENT_JOURNAL_CAPACITY)
                .setEnabled(true);
        client.getConfig().addMapConfig(mapConfig);
        producer = new BasicEventJournalProducer(client, SOURCE, JOURNAL_CAPACITY_PER_PARTITION * partitionsSize);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(name);
        jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        Job job = client.getJet().newJob(pipeline(), jobConfig);

        producer.start();

        IMap<Long, Long> mapOutput = client.getMap(OUTPUT);

        LoggingService loggingService = client.getLoggingService();
        QueueVerifier queueVerifier = new QueueVerifier(loggingService,
                "Verifier[" + OUTPUT + "]", 1).startVerification();

        EventJournalConsumer<Long, Long> consumer = new EventJournalConsumer<>(mapOutput, mapPutEvents(), partitionsSize);

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            if (getJobStatusWithRetry(job) == FAILED) {
                job.join();
            }
            consumer.drain(e -> queueVerifier.offer(e.getKey()));

            sleepMinutes(1);
            mapOutput.clear();
        }
        queueVerifier.close();
        job.cancel();
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
        if (producer != null) {
            producer.stop();
        }
    }

    /**
     * based on idea suggested by <a href="https://github.com/shultseva/hazelcast-mono/pull/4/changes#top">PR</a>
     * @return pipeline as shown in PR
     */
    private Pipeline pipeline() {
        Pipeline p = Pipeline.create();

        ServiceFactory<?, PartitionService> partitionService = ServiceFactories.sharedService(
                ctx -> ctx.hazelcastInstance().getPartitionService());
        ServiceFactory<?, IMap<Long, Long>> sourceMapService = ServiceFactories.sharedService(
                ctx -> ctx.hazelcastInstance().getMap(SOURCE));

        var source = Sources.<Long, Long>mapJournalEntries(SOURCE, START_FROM_OLDEST);
        var input = p.readFrom(source);

        //otherwise createFn serialization issues.
        final int poolSize = partitionsSize;

        StreamStage<Map.Entry<Long, Long>> lostEventBranch = input
                .withoutTimestamps()
                .setLocalParallelism(1)
                .mapUsingService(
                        partitionService,
                        (ser, e) -> {
                            int partitionId = ser.getPartition(e.getKey()).getPartitionId();
                            return Tuple2.tuple2(partitionId, e);
                        })
                .setLocalParallelism(1)
                .setName("append-partition-id") // add partition id to the event
                .mapStateful(
                        () -> new long[poolSize],
                        (memory, pair) -> {
                            var event = pair.getValue();
                            int partition = pair.getKey();
                            assert event != null;
                            if (event.isAfterLostEvents()) {
                                //you will see log line:
                                // {X} events lost for partition {Y} due to journal overflow ...
                                // see StreamEventJournalP line 297
                                var lastTimestamp = memory[partition];
                                memory[partition] = event.getValue();
                                return Tuple3.tuple3(event.getKey(), lastTimestamp, event.getValue());
                            } else {
                                memory[partition] = event.getValue();
                                return null;
                            }
                        }
                )
                .setName("lost-event-detector") // save in memory last emitted event and detect lost events
                .flatMapUsingService(
                        sourceMapService,
                        (m, trigger) -> {
                            var tuple = (Tuple3<Long, Long, Long>)  trigger;
                            var sampleKey = tuple.f0();
                            long from = tuple.f1();
                            long to = tuple.f2();
                            var set = m.entrySet(
                                    Predicates.partitionPredicate(
                                            sampleKey,
                                            entry -> entry.getValue() > from  && entry.getValue() < to
                                    )
                            );
                            return Traversers.traverseIterable(set);
                        }
                )
                .setName("lost-events-stream");

        input.withoutTimestamps()
                .map(e -> Map.entry(e.getKey(), e.getValue()))
                .merge(lostEventBranch)
                .addTimestamps(Map.Entry::getValue, 0)
                .writeTo(Sinks.map(OUTPUT));

        return p;
    }
}
