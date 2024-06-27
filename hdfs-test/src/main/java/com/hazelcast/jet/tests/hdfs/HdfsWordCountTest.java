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

package com.hazelcast.jet.tests.hdfs;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.hadoop.HadoopSinks;
import com.hazelcast.jet.hadoop.HadoopSources;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.processor.Processors.noopP;
import static com.hazelcast.jet.pipeline.Sinks.fromProcessor;
import static com.hazelcast.jet.pipeline.Sources.batchFromProcessor;
import static com.hazelcast.jet.tests.common.Util.sleepMillis;
import static com.hazelcast.jet.tests.hdfs.WordGenerator.wordGenerator;
import static java.lang.Integer.parseInt;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class HdfsWordCountTest extends AbstractJetSoakTest {

    private static final String TAB_STRING = "\t";
    private static final int DEFAULT_TOTAL = 4800000;
    private static final int DEFAULT_DISTINCT = 500000;
    private static final int PAUSE_BETWEEN_JOBS = 2_000;

    private String hdfsUri;
    private String inputPath;
    private String outputPath;
    private int distinct;
    private int total;
    private int threadCount;
    private volatile Exception exception;

    public static void main(String[] args) throws Exception {
        new HdfsWordCountTest().run(args);
    }

    @Override
    public void init(HazelcastInstance client) {
        long timestamp = System.nanoTime();
        hdfsUri = property("hdfs_name_node", "hdfs://localhost:8020");
        inputPath = property("hdfs_input_path", "hdfs-input-") + timestamp;
        outputPath = property("hdfs_output_path", "hdfs-output-") + timestamp;
        distinct = propertyInt("hdfs_distinct", DEFAULT_DISTINCT);
        total = propertyInt("hdfs_total", DEFAULT_TOTAL);
        threadCount = propertyInt("hdfs_thread_count", 1);

        Pipeline pipeline = Pipeline.create();

        BatchSource<Object> source = batchFromProcessor("generator", wordGenerator(hdfsUri, inputPath, distinct, total));
        Sink<Object> noopSink = fromProcessor("noopSink", ProcessorMetaSupplier.of(noopP()));
        pipeline.readFrom(source).writeTo(noopSink);

        JobConfig jobConfig = new JobConfig();
        jobConfig.addClass(HdfsWordCountTest.class);
        jobConfig.addClass(WordGenerator.class);
        jobConfig.addClass(WordGenerator.MetaSupplier.class);

        client.getJet().newJob(pipeline, jobConfig).join();
    }

    @Override
    public void test(HazelcastInstance client, String name) throws Exception {
        long begin = System.currentTimeMillis();
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            final int threadIndex = i;
            executorService.submit(() -> {
                while ((System.currentTimeMillis() - begin) < durationInMillis && exception == null) {
                    try {
                        executeJob(client, threadIndex);
                        verify(threadIndex);
                        sleepMillis(PAUSE_BETWEEN_JOBS);
                    } catch (Throwable e) {
                        exception = new Exception(e);
                    }
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(durationInMillis + MINUTES.toMillis(1), MILLISECONDS);
        if (exception != null) {
            throw exception;
        }
    }

    protected void teardown(Throwable t) throws Exception {
    }

    private void executeJob(HazelcastInstance client, int threadIndex) {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Hdfs WordCount Test [" + threadIndex + "]");
        client.getJet().newJob(pipeline(threadIndex), jobConfig).join();
    }

    private Pipeline pipeline(int threadIndex) {
        Pipeline pipeline = Pipeline.create();

        JobConf conf = new JobConf();
        conf.set("fs.defaultFS", hdfsUri);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setInputFormat(TextInputFormat.class);
        TextInputFormat.addInputPath(conf, new Path(inputPath));
        TextOutputFormat.setOutputPath(conf, new Path(outputPath + "/" + threadIndex));

        pipeline.readFrom(HadoopSources.inputFormat(conf, (k, v) -> v.toString()))
                .flatMap((String line) -> {
                    StringTokenizer s = new StringTokenizer(line);
                    return () -> s.hasMoreTokens() ? s.nextToken() : null;
                })
                .groupingKey(wholeItem())
                .aggregate(counting())
                .writeTo(HadoopSinks.outputFormat(conf));

        return pipeline;
    }

    private void verify(int threadIndex) throws IOException {
        URI uri = URI.create(hdfsUri);
        String disableCacheName = String.format("fs.%s.impl.disable.cache", uri.getScheme());
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setBoolean(disableCacheName, true);
        try (FileSystem fs = FileSystem.get(uri, conf)) {
            String path = outputPath + "/" + threadIndex;

            Path p = new Path(path);
            RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(p, false);
            int totalCount = 0;
            int wordCount = 0;
            while (iterator.hasNext()) {
                LocatedFileStatus status = iterator.next();
                if (status.getPath().getName().equals("_SUCCESS")) {
                    continue;
                }
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath())))) {
                    String line = reader.readLine();
                    while (line != null) {
                        wordCount++;
                        totalCount += parseInt(line.split(TAB_STRING)[1]);
                        line = reader.readLine();
                    }
                }
            }
            assertEquals(distinct, wordCount);
            assertEquals(total, totalCount);
            fs.delete(p, true);
        }
    }

}
