package com.hazelcast.jet.tests.hdfs;/*
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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Pipeline;
import com.hazelcast.jet.Source;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.server.JetBootstrap;
import com.hazelcast.util.ExceptionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.junit.Before;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.StringTokenizer;

import static com.hazelcast.jet.Sinks.writeList;
import static com.hazelcast.jet.Sources.fromProcessor;
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

public class HdfsWordCountTest {

    private JetInstance jet;
    private String inputPath;
    private String outputPath;
    private long distinct;
    private long total;

    @Before
    public void init() {
        jet = JetBootstrap.getInstance();
        inputPath = System.getProperty("hdfs_input_path", "input");
        outputPath = System.getProperty("hdfs_output_path", "output");
        distinct = Long.parseLong(System.getProperty("hdfs_distinct", "1000000"));
        total = Long.parseLong(System.getProperty("hdfs_total", "10000000"));

        Pipeline pipeline = Pipeline.create();

        Source<Object> source = fromProcessor("generator",
                () -> new WordGenerator(inputPath, distinct, total));
        pipeline.drawFrom(source).drainTo(writeList("resultList"));

        jet.newJob(pipeline).join();

    }


    @Test
    public void test() {

        DAG dag = new DAG();
        JobConf conf = new JobConf();
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setInputFormat(TextInputFormat.class);
        TextInputFormat.addInputPath(conf, new Path(inputPath));
        TextOutputFormat.setOutputPath(conf, new Path(outputPath));

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

        jet.newJob(dag).join();
    }

    public static class WordGenerator extends AbstractProcessor {

        private final String path;
        private final long distinct;
        private final long total;

        public WordGenerator(String path, long distinct, long total) {
            this.path = path;
            this.distinct = distinct;
            this.total = total;
        }

        @Override
        public boolean complete() {
            try {
                FileSystem fs = FileSystem.get(new Configuration());
                DataOutputStream hdfsFile = fs.create(new Path(path));
                try (OutputStreamWriter stream = new OutputStreamWriter(hdfsFile)) {
                    writeToFile(stream, distinct, total);
                }
                return tryEmit("done!");
            } catch (IOException e) {
                throw ExceptionUtil.rethrow(e);
            }
        }

        @Override
        public boolean isCooperative() {
            return false;
        }

        private void writeToFile(OutputStreamWriter stream, long distinctWords, long numWords) throws IOException {
            for (long i = 0; i < numWords; i++) {
                stream.write(i % distinctWords + "");
                if (i % 20 == 0) {
                    stream.write("\n");
                } else {
                    stream.write(" ");
                }
            }
            stream.write("\n");
        }
    }

}
