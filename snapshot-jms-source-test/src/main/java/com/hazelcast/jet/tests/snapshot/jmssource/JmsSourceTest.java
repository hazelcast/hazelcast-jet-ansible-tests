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

package com.hazelcast.jet.tests.snapshot.jmssource;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.jet.tests.jms.JmsMessageProducer;
import com.hazelcast.logging.ILogger;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.util.Map;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static com.hazelcast.jet.tests.common.Util.waitForJobStatus;
import static com.hazelcast.jet.tests.jms.JmsMessageProducer.MESSAGE_PREFIX;
import static com.hazelcast.jet.tests.snapshot.jmssource.JmsSourceTest.JmsFactorySupplier.getConnectionFactory;
import static java.util.concurrent.TimeUnit.SECONDS;

public class JmsSourceTest extends AbstractJetSoakTest {

    private static final int SNAPSHOT_INTERVAL = 5_000;
    private static final int ASSERTION_RETRY_COUNT = 60;
    private static final int MESSAGE_PREFIX_LENGTH = MESSAGE_PREFIX.length();
    private static final String SOURCE_QUEUE = "JmsSourceTest_source";

    private int snapshotIntervalMs;
    private String brokerURL;

    public static void main(String[] args) throws Exception {
        new JmsSourceTest().run(args);
    }

    @Override
    public void init(HazelcastInstance client) {
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", SNAPSHOT_INTERVAL);
        brokerURL = property("brokerURL", "tcp://localhost:61616");
    }

    @Override
    protected boolean runOnBothClusters() {
        return true;
    }

    public void test(HazelcastInstance client, String clusterName) throws Exception {
        ILogger logger = getLogger(client, JmsSourceTest.class);
        Pipeline p = Pipeline.create();

        StreamSource<Long> source = Sources
                .jmsQueueBuilder(getConnectionFactory(brokerURL))
                .maxGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .destinationName(SOURCE_QUEUE + clusterName)
                .build(msg -> Long.parseLong(((TextMessage) msg).getText().substring(MESSAGE_PREFIX_LENGTH)));

        Sink<Long> sink = VerificationProcessor.sink(clusterName);

        p.readFrom(source)
                .withoutTimestamps()
                .writeTo(sink);

        JobConfig jobConfig = new JobConfig()
                .setName("JMS transactional source " + clusterName)
                .setSnapshotIntervalMillis(snapshotIntervalMs)
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        if (clusterName.startsWith(STABLE_CLUSTER)) {
            jobConfig.addClass(JmsSourceTest.class, JmsMessageProducer.class, VerificationProcessor.class,
                    JmsSourceTest.JmsFactorySupplier.class);
        }
        Job job = client.getJet().newJob(p, jobConfig);
        waitForJobStatus(job, RUNNING);
        log(logger, "Job started", clusterName);

        JmsMessageProducer producer = new JmsMessageProducer(brokerURL, SOURCE_QUEUE + clusterName);
        producer.start();
        log(logger, "Producer started", clusterName);

        long begin = System.currentTimeMillis();
        long expectedTotalCount;
        try {
            while (System.currentTimeMillis() - begin < durationInMillis) {
                if (getJobStatusWithRetry(job) == FAILED) {
                    job.join();
                }
                sleepMinutes(1);
            }
        } finally {
            expectedTotalCount = producer.stop();
        }

        log(logger, "Producer stopped, expectedTotalCount: " + expectedTotalCount, clusterName);
        assertCountEventually(client, expectedTotalCount, logger, clusterName);
        job.cancel();
        log(logger, "Job completed", clusterName);
    }

    @Override
    protected void teardown(Throwable t) {
    }

    private static void log(ILogger logger, String message, String clusterName) {
        logger.info("Cluster" + clusterName + "\t\t" + message);
    }

    private static void assertCountEventually(HazelcastInstance client, long expectedTotalCount, ILogger logger,
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
