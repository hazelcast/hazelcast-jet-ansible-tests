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

package com.hazelcast.jet.tests.kafkaconnectsource;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.kafka.connect.KafkaConnectSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.jet.tests.common.AbstractSoakTest;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.CompletionException;

public class KafkaConnectSourceTest extends AbstractSoakTest {

    private static final String CONNECTOR_URL = "https://repository.hazelcast.com/download"
                                                + "/tests/confluentinc-kafka-connect-datagen-0.6.0.zip";

    public static void main(String[] args) throws Exception {
        new KafkaConnectSourceTest().run(args);
    }

    @Override
    protected void init(HazelcastInstance client) {
    }

    @Override
    protected void test(HazelcastInstance client, String name) throws MalformedURLException {

        final int ITEM_COUNT = 10;

        Properties connectorProperties = new Properties();
        connectorProperties.setProperty("name", "datagen-connector");
        connectorProperties.setProperty("connector.class", "io.confluent.kafka.connect.datagen.DatagenConnector");
        connectorProperties.setProperty("tasks.max", "1");
        connectorProperties.setProperty("iterations", String.valueOf(ITEM_COUNT));
        connectorProperties.setProperty("kafka.topic", "orders");
        connectorProperties.setProperty("quickstart", "orders");


        StreamSource<Order> source = KafkaConnectSources.connect(connectorProperties,Order::new);

        // Throws AssertionCompletedException
        Sink<Order> sink = AssertionSinks.assertCollectedEventually(60,
                list -> assertEquals(ITEM_COUNT, list.size()));

        Pipeline pipeline = Pipeline.create();
        StreamStage<Order> streamStage = pipeline.readFrom(source)
                .withoutTimestamps()
                .setLocalParallelism(1);

        streamStage.writeTo(sink);

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(new URL(CONNECTOR_URL));

        Job job = client.getJet().newJob(pipeline, jobConfig);

        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {
            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: " + e.getCause(),
                    errorMsg.contains(AssertionCompletedException.class.getName()));

        }
    }

    @Override
    protected void teardown(Throwable t) {
    }

}
