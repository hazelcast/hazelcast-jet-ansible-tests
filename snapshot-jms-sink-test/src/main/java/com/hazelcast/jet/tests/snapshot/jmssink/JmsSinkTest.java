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

package com.hazelcast.jet.tests.snapshot.jmssink;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import jakarta.jms.ConnectionFactory;
import org.apache.activemq.ActiveMQXAConnectionFactory;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static com.hazelcast.jet.tests.snapshot.jmssink.JmsSinkTest.JmsFactorySupplier.getConnectionFactory;

public class JmsSinkTest extends AbstractJetSoakTest {

    public static final String SINK_QUEUE = "JmsSinkTest_sink";

    private static final int DEFAULT_SLEEP_MS_BETWEEN_ITEM = 50;
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;

    private int sleepMsBetweenItem;
    private long snapshotIntervalMs;
    private String brokerURL;

    public static void main(String[] args) throws Exception {
        new JmsSinkTest().run(args);
    }

    @Override
    public void init(HazelcastInstance client) {
        sleepMsBetweenItem = propertyInt("sleepMsBetweenItem", DEFAULT_SLEEP_MS_BETWEEN_ITEM);
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);
        brokerURL = property("brokerURL", "tcp://localhost:61616");
    }

    @Override
    protected boolean runOnBothClusters() {
        return true;
    }

    @Override
    public void test(HazelcastInstance client, String name) throws InterruptedException {
        JmsSinkVerifier verifier = new JmsSinkVerifier(name, brokerURL, logger);
        verifier.start();

        try {
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(name);
            jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
            jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
            if (name.startsWith(STABLE_CLUSTER)) {
                jobConfig.addClass(JmsSinkTest.class, JmsSinkVerifier.class);
            }
            Job job = client.getJet().newJob(pipeline(name), jobConfig);

            try {
                long begin = System.currentTimeMillis();
                while (System.currentTimeMillis() - begin < durationInMillis) {
                    if (getJobStatusWithRetry(job) == FAILED) {
                        job.join();
                    }
                    verifier.checkStatus();
                    sleepMinutes(1);
                }
            } finally {
                job.cancel();
            }
        } finally {
            verifier.finish();
        }
    }

    @Override
    protected void teardown(Throwable t) {
    }

    private Pipeline pipeline(String clusterName) {
        int sleep = sleepMsBetweenItem;

        Pipeline pipeline = Pipeline.create();

        StreamSource<Long> source = SourceBuilder
                .stream("srcForJmsSink", procCtx -> new long[1])
                .<Long>fillBufferFn((ctx, buf) -> {
                    buf.add(ctx[0]++);
                    sleepMillis(sleep);
                })
                .createSnapshotFn(ctx -> ctx[0])
                .restoreSnapshotFn((ctx, state) -> ctx[0] = state.get(0))
                .build();

        Sink<Object> sink = Sinks.jmsQueueBuilder(getConnectionFactory(brokerURL))
                .destinationName(SINK_QUEUE + clusterName)
                .exactlyOnce(true)
                .build();

        pipeline.readFrom(source)
                .withoutTimestamps()
                .rebalance()
                .writeTo(sink);

        return pipeline;
    }

    static class JmsFactorySupplier {

        static SupplierEx<ConnectionFactory> getConnectionFactory(String brokerURL) {
            return () -> new ActiveMQXAConnectionFactory(brokerURL);
        }
    }
}
