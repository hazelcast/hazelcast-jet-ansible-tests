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

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.kafka.connect.KafkaConnectSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.tests.common.AbstractSoakTest;

import java.net.URL;
import java.util.Properties;
import java.util.concurrent.CompletionException;

import static com.hazelcast.jet.tests.common.Util.sleepSeconds;

public class KafkaConnectSourceTest extends AbstractSoakTest {

    private static final String CONNECTOR_URL = "confluentinc-kafka-connect-datagen-0.6.0.zip";

    private static final int SLEEP_BETWEEN_TESTS_SECONDS = 2;

    public static void main(String[] args) throws Exception {
        new KafkaConnectSourceTest().run(args);
    }

    @Override
    protected void init(HazelcastInstance client) {
    }

    @Override
    protected boolean runOnBothClusters() {
        return true;
    }

    @Override
    protected void test(HazelcastInstance client, String name) {
        int jobCounter = 0;
        final long begin = System.currentTimeMillis();
        try {
            while (System.currentTimeMillis() - begin < durationInMillis) {

                logger.info("Starting Job count: " + jobCounter);

                runJob(client);

                jobCounter++;
                logger.info("Sleeping between tests ");
                sleepSeconds(SLEEP_BETWEEN_TESTS_SECONDS);
            }
        } finally {
            logger.info("Test finished with job count: " + jobCounter);
        }
    }

    private void runJob(HazelcastInstance client) {
        final int itemCount = 100;

        Properties connectorProperties = getConnectorProperties(itemCount);

        StreamSource<Order> source = KafkaConnectSources.connect(connectorProperties, Order::new);

        Pipeline pipeline = Pipeline.create();
        StreamStage<Order> streamStage = pipeline.readFrom(source)
                .withoutTimestamps()
                .setLocalParallelism(2);

        String listName = "test_kafka_connect_list";
        streamStage.writeTo(Sinks.list(listName));

        JobConfig jobConfig = new JobConfig();
        URL connectorURL = getConnectorURL();
        jobConfig.addJarsInZip(connectorURL);

        Job job = client.getJet().newJob(pipeline, jobConfig);

        try {
            job.join();
        } catch (CompletionException exception) {
            Throwable cause = exception.getCause();
            boolean stoppingConnector = cause.getMessage().contains("Stopping connector");
            String message = "Job was expected to complete with Stopping connector message but completed with: " +
                             cause;
            assertTrue(message, stoppingConnector);
        }
        // Ensure that the list is not empty. It is not possible to assert the list size since, when one of the
        // connector tasks finishes, the other one may still be running.
        IList<Object> list = client.getList(listName);
        assertNotEmpty(list, "List should not be empty");

        // Clear the list for next iteration
        list.clear();
    }

    private Properties getConnectorProperties(int itemCount) {
        Properties connectorProperties = new Properties();
        connectorProperties.setProperty("name", "datagen-connector");
        connectorProperties.setProperty("connector.class", "io.confluent.kafka.connect.datagen.DatagenConnector");
        connectorProperties.setProperty("tasks.max", "2");
        connectorProperties.setProperty("iterations", String.valueOf(itemCount));
        connectorProperties.setProperty("kafka.topic", "orders");
        connectorProperties.setProperty("quickstart", "orders");
        return connectorProperties;
    }

    private URL getConnectorURL() {
        ClassLoader classLoader = KafkaConnectSourceTest.class.getClassLoader();
        URL resource = classLoader.getResource(KafkaConnectSourceTest.CONNECTOR_URL);
        assert resource != null;
        return resource;
    }

    @Override
    protected void teardown(Throwable t) {
        logger.info("Tearing down with Throwable : " + t);
    }
}
