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

package com.hazelcast.jet.tests.snapshot.jdbc;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import com.mysql.cj.jdbc.MysqlXADataSource;
import java.sql.Connection;
import javax.sql.CommonDataSource;
import javax.sql.DataSource;

import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.tests.common.Util.getJobStatusWithRetry;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.common.Util.sleepMinutes;
import static com.hazelcast.jet.tests.snapshot.jdbc.JdbcSinkTest.DataSourceSupplier.getDataSourceSupplier;

public class JdbcSinkTest extends AbstractSoakTest {

    public static final String TABLE_PREFIX = "JdbcSinkTest";
    private static final String DATABASE_NAME = "snapshot-jdbc-test-db";

    private static final String DEFAULT_DATABASE_URL = "jdbc:mysql://localhost";
    private static final int DEFAULT_SLEEP_MS_BETWEEN_ITEM = 50;
    private static final int DEFAULT_SNAPSHOT_INTERVAL = 5000;

    private String connectionUrl;

    private int sleepMsBetweenItem;
    private long snapshotIntervalMs;

    public static void main(String[] args) throws Exception {
        new JdbcSinkTest().run(args);
    }

    @Override
    public void init(JetInstance client) throws Exception {
        connectionUrl = property("connectionUrl", DEFAULT_DATABASE_URL) + "/" + DATABASE_NAME + "?useSSL=false";

        sleepMsBetweenItem = propertyInt("sleepMsBetweenItem", DEFAULT_SLEEP_MS_BETWEEN_ITEM);
        snapshotIntervalMs = propertyInt("snapshotIntervalMs", DEFAULT_SNAPSHOT_INTERVAL);
    }

    @Override
    protected boolean runOnBothClusters() {
        return true;
    }

    @Override
    public void test(JetInstance client, String name) throws Exception {
        String tableName = (TABLE_PREFIX + name).replaceAll("-", "_");
        try (Connection connection = ((DataSource) getDataSourceSupplier(connectionUrl).get()).getConnection()) {
            connection.createStatement().execute("DROP TABLE IF EXISTS " + tableName);
            connection.createStatement().execute("CREATE TABLE " + tableName
                    + "(id int PRIMARY KEY AUTO_INCREMENT, value int)");
        }
        JdbcSinkVerifier verifier = new JdbcSinkVerifier(name, logger, connectionUrl);
        verifier.start();

        try {
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(name);
            jobConfig.setSnapshotIntervalMillis(snapshotIntervalMs);
            jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
            if (name.startsWith(STABLE_CLUSTER)) {
                jobConfig.addClass(JdbcSinkTest.class, JdbcSinkVerifier.class);
            }
            Job job = client.newJob(pipeline(tableName), jobConfig);

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
    protected void teardown(Throwable t) throws Exception {
    }

    private Pipeline pipeline(String tableName) {
        int sleep = sleepMsBetweenItem;

        Pipeline pipeline = Pipeline.create();

        StreamSource<Integer> source = SourceBuilder
                .stream("srcForJmsSink", procCtx -> new int[1])
                .<Integer>fillBufferFn((ctx, buf) -> {
                    buf.add(ctx[0]++);
                    sleepMillis(sleep);
                })
                .createSnapshotFn(ctx -> ctx[0])
                .restoreSnapshotFn((ctx, state) -> ctx[0] = state.get(0))
                .build();

        Sink<Integer> sink = Sinks.<Integer>jdbcBuilder()
                .dataSourceSupplier(getDataSourceSupplier(connectionUrl))
                .updateQuery("INSERT INTO " + tableName + "(value) VALUES(?)")
                .bindFn(
                        (stmt, item) -> {
                            stmt.setInt(1, item);
                        })
                .exactlyOnce(true)
                .build();

        pipeline.readFrom(source)
                .withoutTimestamps()
                .rebalance()
                .writeTo(sink);

        return pipeline;
    }

    static class DataSourceSupplier {

        public static SupplierEx<CommonDataSource> getDataSourceSupplier(String connectionUrl) {
            return () -> {
                MysqlXADataSource ds = new MysqlXADataSource();
                ds.setUrl(connectionUrl);
                ds.setUser("root");
                ds.setPassword("soak-test");
                ds.setDatabaseName(DATABASE_NAME);
                return ds;
            };
        }
    }

}
