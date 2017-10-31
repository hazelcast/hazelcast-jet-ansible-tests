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

package com.hazelcast.jet.tests.hdfs;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Pipeline;
import com.hazelcast.jet.Sink;
import com.hazelcast.jet.Sinks;
import com.hazelcast.jet.Source;
import com.hazelcast.jet.Sources;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.server.JetBootstrap;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import tests.hdfs.WordGenerator;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Partitioner.HASH_CODE;
import static com.hazelcast.jet.core.processor.HdfsProcessors.readHdfsP;
import static com.hazelcast.jet.core.processor.HdfsProcessors.writeHdfsP;
import static com.hazelcast.jet.core.processor.Processors.combineByKeyP;
import static com.hazelcast.jet.core.processor.Processors.flatMapP;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.function.DistributedFunctions.entryValue;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static tests.hdfs.WordGenerator.wordGenerator;

@RunWith(JUnit4.class)
public class HdfsWordCountTest {

    private static final String TAB_STRING = "\t";

    private JetInstance jet;
    private String hdfsUri;
    private String inputPath;
    private String outputPath;
    private long distinct;
    private long total;
    private int threadCount;
    private long durationNanos;
    private Throwable error;

    public static void main(String[] args) throws Exception {
        JUnitCore.main(HdfsWordCountTest.class.getName());
    }

    @Before
    public void init() {
        jet = JetBootstrap.getInstance();
        long timestamp = System.nanoTime();
        hdfsUri = System.getProperty("hdfs_name_node", "hdfs://localhost:8020");
        inputPath = System.getProperty("hdfs_input_path", "hdfs-input-") + timestamp;
        outputPath = System.getProperty("hdfs_output_path", "hdfs-output-") + timestamp;
        distinct = parseLong(System.getProperty("hdfs_distinct", "1000000"));
        total = parseLong(System.getProperty("hdfs_total", "10000000"));
        threadCount = parseInt(System.getProperty("hdfs_thread_count", "4"));
        durationNanos = SECONDS.toNanos(parseLong(System.getProperty("hdfs_duration_seconds", "600")));

        Pipeline pipeline = Pipeline.create();

        Source<Object> source = Sources.fromProcessor("generator", wordGenerator(hdfsUri, inputPath, distinct, total));
        Sink<Object> noopSink = Sinks.fromProcessor("noopSink", ProcessorSupplier.of(Processors.noopP()));
        pipeline.drawFrom(source).drainTo(noopSink);

        JobConfig jobConfig = new JobConfig();
        jobConfig.addClass(HdfsWordCountTest.class);
        jobConfig.addClass(WordGenerator.class);
        jobConfig.addClass(WordGenerator.MetaSupplier.class);

        jet.newJob(pipeline, jobConfig).join();
    }

    @Test
    public void test() throws Throwable {
        long begin = System.nanoTime();
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            final int threadIndex = i;
            executorService.submit(() -> {
                while ((System.nanoTime() - begin) < durationNanos && error == null) {
                    try {
                        executeJob(threadIndex);
                        verify(threadIndex);
                    } catch (Throwable t) {
                        error = t;
                    }
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(durationNanos + MINUTES.toNanos(1), TimeUnit.NANOSECONDS);
        if (error != null) {
            throw error;
        }
    }

    private void executeJob(int threadIndex) {
        DAG dag = new DAG();
        JobConf conf = new JobConf();
        conf.set("fs.defaultFS", hdfsUri);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setInputFormat(TextInputFormat.class);
        TextInputFormat.addInputPath(conf, new Path(inputPath));
        TextOutputFormat.setOutputPath(conf, new Path(outputPath + "/" + threadIndex));

        Vertex producer = dag.newVertex("reader", readHdfsP(conf,
                (k, v) -> v.toString())).localParallelism(3);

        Vertex tokenizer = dag.newVertex("tokenizer",
                flatMapP((String line) -> {
                    StringTokenizer s = new StringTokenizer(line);
                    return () -> s.hasMoreTokens() ? s.nextToken() : null;
                })
        );

        // word -> (word, count)
        Vertex accumulate = dag.newVertex("accumulate", Processors.accumulateByKeyP(wholeItem(), counting()));

        // (word, count) -> (word, count)
        Vertex combine = dag.newVertex("combine", combineByKeyP(counting()));
        Vertex consumer = dag.newVertex("writer", writeHdfsP(conf, entryKey(), entryValue())).localParallelism(1);

        dag.edge(between(producer, tokenizer))
           .edge(between(tokenizer, accumulate)
                   .partitioned(wholeItem(), HASH_CODE))
           .edge(between(accumulate, combine)
                   .distributed()
                   .partitioned(entryKey()))
           .edge(between(combine, consumer));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addClass(HdfsWordCountTest.class);

        jet.newJob(dag, jobConfig).join();
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
            long totalCount = 0;
            long wordCount = 0;
            while (iterator.hasNext()) {
                LocatedFileStatus status = iterator.next();
                if (status.getPath().getName().equals("_SUCCESS")) {
                    continue;
                }
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath())))) {
                    String line = reader.readLine();
                    while (line != null) {
                        wordCount++;
                        totalCount += parseLong(line.split(TAB_STRING)[1]);
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
