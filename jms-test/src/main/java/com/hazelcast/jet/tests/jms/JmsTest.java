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

package com.hazelcast.jet.tests.jms;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.logging.ILogger;
import jakarta.jms.ConnectionFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQXAConnectionFactory;

import java.io.IOException;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static com.hazelcast.jet.tests.common.Util.waitForJobStatus;
import static java.util.concurrent.TimeUnit.SECONDS;

public class JmsTest extends AbstractJetSoakTest {

    private static final int ASSERTION_RETRY_COUNT = 100;
    private static final String SOURCE_QUEUE = "source";
    private static final String MIDDLE_QUEUE = "middle";
    private static final String SINK_QUEUE = "sink";


    private String brokerURL;

    public static void main(String[] args) throws Exception {
        new JmsTest().run(args);
    }

    @Override
    public void init(HazelcastInstance client) throws IOException {
        brokerURL = property("brokerURL", "tcp://localhost:61616");
    }

    @Override
    protected boolean runOnBothClusters() {
        return true;
    }

    @Override
    public void test(HazelcastInstance client, String clusterName) throws Exception {
        ILogger logger = getLogger(client, JmsTest.class);

        Pipeline p1 = Pipeline.create();
        p1.readFrom(Sources.jmsQueue(SOURCE_QUEUE + clusterName, connectionFactory()))
          .withoutTimestamps()
          .writeTo(Sinks.jmsQueue(MIDDLE_QUEUE + clusterName, xaConnectionFactory()));

        Pipeline p2 = Pipeline.create();
        p2.readFrom(Sources.jmsQueue(MIDDLE_QUEUE + clusterName, connectionFactory()))
          .withoutTimestamps()
          .writeTo(Sinks.jmsQueue(SINK_QUEUE + clusterName, xaConnectionFactory()));

        JobConfig jobConfig1 = new JobConfig()
                .setName("JMS Test source to middle queue")
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        if (clusterName.startsWith(STABLE_CLUSTER)) {
            jobConfig1.addClass(JmsTest.class, JmsMessageProducer.class, JmsMessageConsumer.class);
        }
        Job job1 = client.getJet().newJob(p1, jobConfig1);
        waitForJobStatus(job1, RUNNING);
        log(logger, "Job1 started", clusterName);

        JobConfig jobConfig2 = new JobConfig()
                .setName("JMS Test middle to sink queue")
                .setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        if (clusterName.startsWith(STABLE_CLUSTER)) {
            jobConfig2.addClass(JmsTest.class, JmsMessageProducer.class, JmsMessageConsumer.class);
        }
        Job job2 = client.getJet().newJob(p2, jobConfig2);
        waitForJobStatus(job2, RUNNING);
        log(logger, "Job2 started", clusterName);

        JmsMessageProducer producer = new JmsMessageProducer(brokerURL, SOURCE_QUEUE + clusterName);
        producer.start();
        log(logger, "Producer started", clusterName);

        JmsMessageConsumer consumer = new JmsMessageConsumer(brokerURL, SINK_QUEUE + clusterName);
        consumer.start();
        log(logger, "Consumer started", clusterName);

        long begin = System.currentTimeMillis();
        long expectedTotalCount;
        try {
            while (System.currentTimeMillis() - begin < durationInMillis) {
                if (getJobStatusWithRetry(job1) == FAILED) {
                    job2.cancel();
                    job1.join();
                }
                if (getJobStatusWithRetry(job2) == FAILED) {
                    job1.cancel();
                    job2.join();
                }
                sleepMinutes(1);
            }
        } finally {
            expectedTotalCount = producer.stop();
        }

        log(logger, "Producer stopped, expectedTotalCount: " + expectedTotalCount, clusterName);
        assertCountEventually(consumer, logger, expectedTotalCount, clusterName);
        consumer.stop();
        log(logger, "Consumer stopped", clusterName);

        job2.cancel();
        log(logger, "Job2 completed", clusterName);

        job1.cancel();
        log(logger, "Job1 completed", clusterName);
    }

    protected void teardown(Throwable t) {
    }

    private SupplierEx<ConnectionFactory> connectionFactory() {
        String localBrokerURL = this.brokerURL;
        return () -> new ActiveMQConnectionFactory(localBrokerURL);
    }

    private SupplierEx<ConnectionFactory> xaConnectionFactory() {
        String localBrokerURL = this.brokerURL;
        return () -> new ActiveMQXAConnectionFactory(localBrokerURL);
    }

    private static void log(ILogger logger, String message, String clusterName) {
        logger.info("Cluster " + clusterName + "\t\t" + message);
    }

    private static void assertCountEventually(
            JmsMessageConsumer consumer, ILogger logger, long expectedTotalCount, String clusterName) throws Exception {
        for (int i = 0; i < ASSERTION_RETRY_COUNT; i++) {
            long actualTotalCount = consumer.getCount();
            log(logger, "expected: " + expectedTotalCount + ", actual: " + actualTotalCount, clusterName);
            if (expectedTotalCount == actualTotalCount) {
                return;
            }
            SECONDS.sleep(1);
        }
        assertEquals(expectedTotalCount, consumer.getCount());
    }

}
