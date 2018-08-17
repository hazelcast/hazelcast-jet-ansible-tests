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

package jet.com.hazelcast.tests.eventjournal;

import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
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
import tests.snapshot.QueueVerifier;

import java.io.Serializable;
import java.util.Objects;

import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.function.DistributedFunctions.alwaysFalse;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(JUnit4.class)
public class EventJournalTest implements Serializable {

    private transient JetInstance jet;
    private String mapName;
    private String resultsMapName;
    private long durationInMillis;
    private int countPerTicker;
    private int snapshotIntervalMs;
    private int lagMs;
    private int windowSize;
    private int slideBy;
    private int partitionCount;
    private EventJournalTradeProducer tradeProducer;

    public static void main(String[] args) {
        JUnitCore.main(EventJournalTest.class.getName());
    }

    @Before
    public void setUp() {
        System.setProperty("hazelcast.logging.type", "log4j");
        String isolatedClientConfig = System.getProperty("isolatedClientConfig");
        if (isolatedClientConfig != null) {
            System.setProperty("hazelcast.client.config", isolatedClientConfig);
        }
        mapName = System.getProperty("map", String.format("%s-%d", "event-journal", System.currentTimeMillis()));
        resultsMapName = mapName + "-results";
        lagMs = Integer.parseInt(System.getProperty("lagMs", "1500"));
        int timestampPerSecond = Integer.parseInt(System.getProperty("timestampPerSecond", "50"));
        snapshotIntervalMs = Integer.parseInt(System.getProperty("snapshotIntervalMs", "5000"));
        windowSize = Integer.parseInt(System.getProperty("windowSize", "20"));
        slideBy = Integer.parseInt(System.getProperty("slideBy", "10"));
        countPerTicker = Integer.parseInt(System.getProperty("countPerTicker", "100"));
        durationInMillis = MINUTES.toMillis(Integer.parseInt(System.getProperty("durationInMinutes", "30")));
        jet = JetBootstrap.getInstance();
        tradeProducer = new EventJournalTradeProducer(countPerTicker, jet.getMap(mapName), timestampPerSecond);
        partitionCount = jet.getHazelcastInstance().getPartitionService().getPartitions().size();
        Config config = jet.getHazelcastInstance().getConfig();
        config.addEventJournalConfig(
                new EventJournalConfig().setMapName(mapName).setCapacity(500_000)
        );
        config.addEventJournalConfig(
                new EventJournalConfig().setMapName(resultsMapName).setCapacity(10_000)
        );

    }

    @Test
    public void eventJournalTest() throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        Job job = jet.newJob(pipeline(), jobConfig);
        tradeProducer.start();

        int windowCount = windowSize / slideBy;
        QueueVerifier queueVerifier = new QueueVerifier(resultsMapName, windowCount).startVerification();

        ClientMapProxy<Long, Long> resultMap = (ClientMapProxy) jet.getHazelcastInstance().getMap(resultsMapName);
        EventJournalConsumer<Long, Long> consumer = new EventJournalConsumer<>(resultMap, partitionCount);

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            boolean isEmpty = consumer.drain(e -> {
                assertEquals("EXACTLY_ONCE -> Unexpected count for " + e.getKey(),
                        countPerTicker, (long) e.getNewValue());
                queueVerifier.offer(e.getKey());
            });
            if (isEmpty) {
                SECONDS.sleep(1);
            }
            assertNotEquals(getJobStatus(job), FAILED);
        }
        System.out.println("Cancelling jobs..");
        queueVerifier.close();
        job.cancel();
        JobStatus status = getJobStatus(job);
        while (status != COMPLETED && status != FAILED) {
            SECONDS.sleep(1);
            status = getJobStatus(job);
        }
    }

    @After
    public void teardown() throws Exception {
        if (tradeProducer != null) {
            tradeProducer.close();
        }
        if (jet != null) {
            jet.shutdown();
        }
    }

    private Pipeline pipeline() {
        Pipeline pipeline = Pipeline.create();

        pipeline.drawFrom(Sources.<Long, Long, Long>mapJournal(mapName, e -> e.getType() == EntryEventType.ADDED,
                EventJournalMapEvent::getNewValue, START_FROM_OLDEST))
                .addTimestamps(t -> t, lagMs)
                .peek(alwaysFalse(), Objects::toString)
                .window(sliding(windowSize, slideBy))
                .groupingKey(wholeItem())
                .aggregate(AggregateOperations.counting())
                .drainTo(Sinks.map(resultsMapName));
        return pipeline;
    }

    private static JobStatus getJobStatus(Job job) {
        try {
            return job.getStatus();
        } catch (Exception e) {
            uncheckRun(() -> MILLISECONDS.sleep(250));
            return getJobStatus(job);
        }
    }

}
