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

package com.hazelcast.jet.tests.file.ingestion;

import com.hazelcast.shaded.com.fasterxml.jackson.databind.SequenceWriter;
import com.hazelcast.shaded.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.hazelcast.shaded.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;

public final class CsvSink<T> {

    private final SequenceWriter writer;

    private CsvSink(Processor.Context context, String path, Class<T> clazz) throws IOException {
        CsvMapper mapper = new CsvMapper();
        CsvSchema schema = mapper.schemaFor(clazz).withHeader();
        Path filePath = Paths.get(path, Integer.toString(context.globalProcessorIndex()));
        Files.createDirectories(filePath.getParent());
        writer = mapper.writer(schema).writeValues(Files.newOutputStream(filePath, CREATE_NEW, WRITE));
    }

    public static <T> Sink<T> sink(String path, Class<T> clazz) {
        return SinkBuilder.sinkBuilder("writeCsv", context -> new CsvSink<>(context, path, clazz))
                .<T>receiveFn(CsvSink::receive)
                .destroyFn(CsvSink::destroy)
                .build();
    }

    private void receive(T item) throws IOException {
        writer.write(item);
    }

    private void destroy() throws IOException {
        writer.close();
    }

}
