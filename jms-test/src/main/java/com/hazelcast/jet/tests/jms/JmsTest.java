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

import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import org.apache.activemq.ActiveMQConnectionFactory;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static com.hazelcast.jet.tests.common.Util.waitForJobStatus;
import static java.util.concurrent.TimeUnit.SECONDS;

public class JmsTest extends AbstractSoakTest {

    private static final int ASSERTION_RETRY_COUNT = 100;
    private static final String SOURCE_QUEUE = "source";
    private static final String MIDDLE_QUEUE = "middle";
    private static final String SINK_QUEUE = "sink";

    private JmsMessageProducer producer;
    private JmsMessageConsumer consumer;
    private String brokerURL;

    public static void main(String[] args) throws Exception {
        new JmsTest().run(args);
    }

    public void init() {
        brokerURL = property("brokerURL", "tcp://localhost:61616");

        producer = new JmsMessageProducer(brokerURL, SOURCE_QUEUE);
        consumer = new JmsMessageConsumer(brokerURL, SINK_QUEUE);
    }

    public void test() throws Exception {
        String localBrokerUrl = brokerURL;

        Pipeline p1 = Pipeline.create();
        p1.drawFrom(Sources.jmsQueue(() -> new ActiveMQConnectionFactory(localBrokerUrl), SOURCE_QUEUE))
          .withoutTimestamps()
          .drainTo(Sinks.jmsQueue(() -> new ActiveMQConnectionFactory(localBrokerUrl), MIDDLE_QUEUE));

        Pipeline p2 = Pipeline.create();
        p2.drawFrom(Sources.jmsQueue(() -> new ActiveMQConnectionFactory(localBrokerUrl), MIDDLE_QUEUE))
          .withoutTimestamps()
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
            if (job1.getStatus() == FAILED) {
                job1.join();
            }
            if (job2.getStatus() == FAILED) {
                job2.join();
            }
            sleepMinutes(1);
        }

        long expectedTotalCount = producer.stop();
        System.out.println("Producer stopped, expectedTotalCount: " + expectedTotalCount);
        assertCount(expectedTotalCount);
        consumer.stop();
        System.out.println("Consumer stopped");

        job2.cancel();
        System.out.println("Job2 completed");

        job1.cancel();
        System.out.println("Job1 completed");
    }

    public void teardown() {
    }

    private void assertCount(long expectedTotalCount) throws InterruptedException {
        for (int i = 0; i < ASSERTION_RETRY_COUNT; i++) {
            long actualTotalCount = consumer.getCount();
            System.out.println("expected: " + expectedTotalCount + ", actual: " + actualTotalCount);
            if (expectedTotalCount == actualTotalCount) {
                return;
            }
            SECONDS.sleep(1);
        }
        assertEquals(expectedTotalCount, consumer.getCount());
    }

}
