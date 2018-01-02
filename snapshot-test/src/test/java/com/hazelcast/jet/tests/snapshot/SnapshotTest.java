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

package com.hazelcast.jet.tests.snapshot;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.tests.kafka.LongRunningKafkaTest;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import tests.kafka.Trade;
import tests.kafka.TradeDeserializer;
import tests.kafka.TradeProducer;
import tests.kafka.TradeSerializer;
import tests.kafka.VerificationSink;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.core.JobStatus.COMPLETED;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class SnapshotTest extends LongRunningKafkaTest {

    public static void main(String[] args) {
        JUnitCore.main(SnapshotTest.class.getName());
    }

    @Before
    public void setUp() throws Exception {
        String isolatedClientConfig = System.getProperty("isolatedClientConfig");
        if (isolatedClientConfig != null) {
            System.setProperty("hazelcast.client.config", isolatedClientConfig);
        }
        super.setUp();
        durationInMillis = MINUTES.toMillis(Integer.parseInt(System.getProperty("durationInMinutes", "10")));
    }

    @Test
    public void kafkaTest() throws IOException, ExecutionException, InterruptedException {
        System.out.println("Executing test job..");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setSnapshotIntervalMillis(5000);
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        jobConfig.addClass(Trade.class, TradeDeserializer.class, TradeProducer.class, TradeSerializer.class,
                VerificationSink.class, LongRunningKafkaTest.class);
        Job testJob = jet.newJob(testDAG(), jobConfig);

        KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(kafkaPropertiesForResults(brokerUri, offsetReset));
        consumer.subscribe(Collections.singleton(topic + "-results"));

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            ConsumerRecords<String, Long> records = consumer.poll(1000);
            records.iterator().forEachRemaining(r ->
                    assertEquals("Unexpected count for " + r.key(), countPerTicker, (long) r.value())
            );
            checkJobStatus(testJob);
        }
        System.out.println("Cancelling jobs..");
        consumer.close();

        testJob.getFuture().cancel(true);
        while (testJob.getJobStatus() != COMPLETED) {
            SECONDS.sleep(1);
        }
    }

    private static void checkJobStatus(Job testJob) {
        try {
            JobStatus status = testJob.getJobStatus();
            if (status == FAILED || status == COMPLETED) {
                throw new AssertionError("Job is failed, jobStatus: " + status);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
