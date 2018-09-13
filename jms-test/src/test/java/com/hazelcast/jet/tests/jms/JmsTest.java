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

package com.hazelcast.jet.tests.jms;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.server.JetBootstrap;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import tests.jms.JmsMessageConsumer;
import tests.jms.JmsMessageProducer;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(JUnit4.class)
public class JmsTest {

    private static final String SOURCE_QUEUE = "source";
    private static final String MIDDLE_QUEUE = "middle";
    private static final String SINK_QUEUE = "sink";

    private JetInstance jet;
    private JmsMessageProducer producer;
    private JmsMessageConsumer consumer;
    private String brokerURL;
    private long durationInMillis;

    public static void main(String[] args) {
        JUnitCore.main(JmsTest.class.getName());
    }

    @Before
    public void setup() {
        brokerURL = System.getProperty("brokerURL", "tcp://localhost:61616");
        durationInMillis = MINUTES.toMillis(Integer.parseInt(System.getProperty("durationInMinutes", "10")));
        jet = JetBootstrap.getInstance();
        producer = new JmsMessageProducer(brokerURL, SOURCE_QUEUE);
        consumer = new JmsMessageConsumer(brokerURL, SINK_QUEUE);
    }

    @After
    public void cleanup() {
        if (jet != null) {
            jet.shutdown();
        }
    }

    @Test
    public void test() throws Exception {
        String localBrokerUrl = brokerURL;

        Pipeline p1 = Pipeline.create();
        p1.drawFrom(Sources.jmsQueue(() -> new ActiveMQConnectionFactory(localBrokerUrl), SOURCE_QUEUE))
          .drainTo(Sinks.jmsQueue(() -> new ActiveMQConnectionFactory(localBrokerUrl), MIDDLE_QUEUE));

        Pipeline p2 = Pipeline.create();
        p2.drawFrom(Sources.jmsQueue(() -> new ActiveMQConnectionFactory(localBrokerUrl), MIDDLE_QUEUE))
          .drainTo(Sinks.jmsQueue(() -> new ActiveMQConnectionFactory(localBrokerUrl), SINK_QUEUE));

        Job job1 = jet.newJob(p1, new JobConfig().setName("JMS Test source to middle queue"));
        waitForJobStatus(job1, RUNNING);
        System.out.println("job1 started");

        Job job2 = jet.newJob(p2, new JobConfig().setName("JMS Test middle to sink queue"));
        waitForJobStatus(job2, RUNNING);
        System.out.println("job2 started");

        producer.start();
        System.out.println("producer started");
        consumer.start();
        System.out.println("consumer started");

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            assertNotEquals(FAILED, job1.getStatus());
            assertNotEquals(FAILED, job2.getStatus());
            SECONDS.sleep(30);
        }

        long expectedTotalCount = producer.stop();
        System.out.println("Producer stopped, expectedTotalCount: " + expectedTotalCount);
        assertCount(expectedTotalCount);
        consumer.stop();
        System.out.println("Consumer stopped");

        job2.cancel();
//        waitForJobStatus(job2, COMPLETED);
        System.out.println("Job2 completed");

        job1.cancel();
//        waitForJobStatus(job1, COMPLETED);
        System.out.println("Job1 completed");

    }

    private void assertCount(long expectedTotalCount) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            long actualTotalCount = consumer.getCount();
            System.out.println("expected: " + expectedTotalCount + ", actual: " + actualTotalCount);
            if (expectedTotalCount == actualTotalCount) {
                return;
            }
            SECONDS.sleep(1);
        }
        assertEquals(expectedTotalCount, consumer.getCount());
    }

    private static void waitForJobStatus(Job job, JobStatus expectedStatus) throws InterruptedException {
        while (true) {
            JobStatus currentStatus = job.getStatus();
            System.out.println("expectedStatus: " + expectedStatus + ", actualStatus: " + currentStatus);
            assertNotEquals(FAILED, currentStatus);
            if (currentStatus.equals(expectedStatus)) {
                return;
            }
            SECONDS.sleep(1);
        }
    }
}
