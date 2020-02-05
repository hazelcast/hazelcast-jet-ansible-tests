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

package com.hazelcast.jet.tests.snapshot.jms;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.hazelcast.logging.ILogger;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.jms.ConnectionFactory;
import javax.jms.TextMessage;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.pipeline.ServiceFactories.sharedService;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static com.hazelcast.jet.tests.common.Util.waitForJobStatus;
import static com.hazelcast.jet.tests.snapshot.jms.JmsMessageProducer.MESSAGE_PREFIX;
import static com.hazelcast.jet.tests.snapshot.jms.JmsSourceTest.JmsFactorySupplier.getConnectionFactory;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class JmsSourceTest extends AbstractSoakTest {

    private static final int SNAPSHOT_INTERVAL = 5_000;

    private static final int ASSERTION_RETRY_COUNT = 60;
    private static final double DELAY_AFTER_TEST_FINISHED = 60_000;

    private static final int MESSAGE_PREFIX_LENGHT = MESSAGE_PREFIX.length();

    private static final String STABLE_CLUSTER = "Stable";
    private static final String DYNAMIC_CLUSTER = "Dynamic";

    private static final String SOURCE_QUEUE = "JmsSourceTest_source";

    private JetInstance stableClusterClient;

    private int snapshotIntervalMs;
    private String brokerURL;

    public static void main(String[] args) throws Exception {
        new JmsSourceTest().run(args);
    }

    @Override
    public void init() throws IOException {
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", SNAPSHOT_INTERVAL);
        brokerURL = property("brokerURL", "tcp://localhost:61616");

        stableClusterClient = Jet.newJetClient(remoteClusterClientConfig());
    }

    @Override
    public void test() throws Throwable {
        Throwable[] exceptions = new Throwable[2];
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(() -> {
            ILogger logger = getLogger(stableClusterClient, JmsSourceTest.class);
            try {
                testInternal(stableClusterClient, logger, STABLE_CLUSTER);
            } catch (Throwable t) {
                logger.severe("Exception in Stable cluster test", t);
                exceptions[0] = t;
            }
        });
        executorService.execute(() -> {
            try {
                testInternal(jet, logger, DYNAMIC_CLUSTER);
            } catch (Throwable t) {
                logger.severe("Exception in Dynamic cluster test", t);
                exceptions[1] = t;
            }
        });
        executorService.shutdown();
        executorService.awaitTermination((long) (durationInMillis + DELAY_AFTER_TEST_FINISHED), MILLISECONDS);

        if (exceptions[0] != null) {
            logger.severe("Exception in Stable cluster test", exceptions[0]);
        }
        if (exceptions[1] != null) {
            logger.severe("Exception in Dynamic cluster test", exceptions[1]);
        }
        if (exceptions[0] != null) {
            throw exceptions[0];
        }
        if (exceptions[1] != null) {
            throw exceptions[1];
        }
    }

    public void testInternal(JetInstance client, ILogger logger, String clusterName) throws Exception {
        Pipeline p = Pipeline.create();

        StreamSource<Long> source = Sources.jmsQueueBuilder(getConnectionFactory(brokerURL))
                .maxGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .destinationName(SOURCE_QUEUE + clusterName)
                .build(msg -> Long.parseLong(((TextMessage) msg).getText().substring(MESSAGE_PREFIX_LENGHT)));

        Sink<Long> sink = Sinks.fromProcessor("sinkVerificationProcessor", VerificationProcessor.supplier(clusterName));

        p.readFrom(source)
                .withoutTimestamps()
                .groupingKey(t -> 0L)
                .mapUsingService(sharedService(ctw -> null), (c, k, v) -> v)
                .writeTo(sink);

        JobConfig jobConfig = new JobConfig()
                .setName("JMS transactional source " + clusterName)
                .setSnapshotIntervalMillis(snapshotIntervalMs)
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        if (clusterName.equals(STABLE_CLUSTER)) {
            jobConfig.addClass(JmsSourceTest.class, JmsMessageProducer.class, VerificationProcessor.class,
                    JmsSourceTest.JmsFactorySupplier.class);
        }
        Job job = client.newJob(p, jobConfig);
        waitForJobStatus(job, RUNNING);
        log(logger, "Job started", clusterName);

        JmsMessageProducer producer = new JmsMessageProducer(brokerURL, SOURCE_QUEUE + clusterName);
        producer.start();
        log(logger, "Producer started", clusterName);

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            if (getJobStatusWithRetry(job) == FAILED) {
                job.join();
            }
            sleepMinutes(1);
        }

        long expectedTotalCount = producer.stop();
        log(logger, "Producer stopped, expectedTotalCount: " + expectedTotalCount, clusterName);
        assertCountEventually(client, expectedTotalCount, logger, clusterName);
        job.cancel();
        log(logger, "Job completed", clusterName);
    }

    @Override
    protected void teardown(Throwable t) {
        if (stableClusterClient != null) {
            stableClusterClient.shutdown();
        }
    }

    private static void log(ILogger logger, String message, String clusterName) {
        logger.info("Cluster" + clusterName + "\t\t" + message);
    }

    private static void assertCountEventually(JetInstance client, long expectedTotalCount, ILogger logger,
            String clusterName) throws Exception {
        Map<String, Long> latestCounterMap = client.getMap(VerificationProcessor.CONSUMED_MESSAGES_MAP_NAME);
        for (int i = 0; i < ASSERTION_RETRY_COUNT; i++) {
            long actualTotalCount = latestCounterMap.get(clusterName);
            log(logger, "expected: " + expectedTotalCount + ", actual: " + actualTotalCount, clusterName);
            if (expectedTotalCount == actualTotalCount) {
                return;
            }
            SECONDS.sleep(1);
        }
        long actualTotalCount = latestCounterMap.get(clusterName);
        assertEquals(expectedTotalCount, actualTotalCount);
    }

    static class JmsFactorySupplier {

        static SupplierEx<ConnectionFactory> getConnectionFactory(String brokerURL) {
            return () -> new ActiveMQConnectionFactory(brokerURL);
        }
    }
}
