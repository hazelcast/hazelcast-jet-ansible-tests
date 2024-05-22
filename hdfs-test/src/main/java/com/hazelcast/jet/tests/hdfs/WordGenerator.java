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

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class WordGenerator extends AbstractProcessor {

    private static final int WORDS_PER_LINE = 20;

    private final String hdfsUri;
    private final String path;
    private final long distinct;
    private final long total;

    private WordGenerator(String hdfsUri, String path, long distinct, long total) {
        this.hdfsUri = hdfsUri;
        this.path = path;
        this.distinct = distinct;
        this.total = total;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean complete() {
        URI uri = URI.create(hdfsUri);
        String disableCacheName = String.format("fs.%s.impl.disable.cache", uri.getScheme());
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setBoolean(disableCacheName, true);
        try (FileSystem fs = FileSystem.get(uri, conf)) {
            Path p = new Path(path);
            try (OutputStreamWriter stream = new OutputStreamWriter(fs.create(p))) {
                writeToFile(stream, distinct, total);
            }
            return true;
        } catch (IOException e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private void writeToFile(OutputStreamWriter stream, long distinctWords, long numWords) throws IOException {
        for (long i = 0; i < numWords; i++) {
            stream.write(i % distinctWords + "");
            if (i % WORDS_PER_LINE == 0) {
                stream.write("\n");
            } else {
                stream.write(" ");
            }
        }
        stream.write("\n");
    }

    static ProcessorMetaSupplier wordGenerator(String hdfsUri, String outputPath, long distinct, long total) {
        return new MetaSupplier((path, memberSize) ->
                new WordGenerator(hdfsUri, outputPath + "/" + path, distinct, total / memberSize));
    }

    public static class MetaSupplier implements ProcessorMetaSupplier {

        private final BiFunctionEx<String, Integer, Processor> processorF;

        private transient Map<Address, UUID> addressUuidMap;

        MetaSupplier(BiFunctionEx<String, Integer, Processor> processorF) {
            this.processorF = processorF;
        }

        @Override
        public void init(@Nonnull Context context) {
            addressUuidMap = context.hazelcastInstance().getCluster().getMembers()
                    .stream()
                    .collect(Collectors.toMap(Member::getAddress, Member::getUuid));
        }

        @Nonnull
        @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> processorSupplier(processorF, addressUuidMap.get(address).toString(), addresses.size());
        }

        private static ProcessorSupplier processorSupplier(BiFunction<String, Integer, Processor> processorF,
                                                           String path, int total) {
            return count -> IntStream
                    .range(0, count)
                    .mapToObj(i -> i == 0 ? processorF.apply(path, total) : Processors.noopP().get())
                    .collect(Collectors.toList());
        }
    }
}
