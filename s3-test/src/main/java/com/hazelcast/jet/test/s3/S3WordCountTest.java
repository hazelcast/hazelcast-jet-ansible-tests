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

package com.hazelcast.jet.test.s3;

import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.s3.S3Sinks;
import com.hazelcast.jet.s3.S3Sources;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.StringTokenizer;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.function.Functions.wholeItem;
import static com.hazelcast.jet.pipeline.Sources.batchFromProcessor;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static software.amazon.awssdk.regions.Region.US_EAST_1;

public class S3WordCountTest extends AbstractSoakTest {

    private static final String DEFAULT_BUCKET_NAME = "jet-soak-tests-source-bucket";
    private static final String RESULTS_PREFIX = "results/";
    private static final int DEFAULT_TOTAL = 400000;
    private static final int DEFAULT_DISTINCT = 50000;


    private S3Client s3Client;
    private String bucketName;
    private int distinct;
    private int totalWordCount;

    public static void main(String[] args) throws Exception {
        System.setProperty("runLocal", "true");
        new S3WordCountTest().run(args);
    }

    @Override
    protected void init() {
        bucketName = property("bucket_name", DEFAULT_BUCKET_NAME);
        distinct = propertyInt("distinct_words", DEFAULT_DISTINCT);
        totalWordCount = propertyInt("total_word_count", DEFAULT_TOTAL);

        s3Client = clientSupplier().get();
        deleteBucket();
        createBucket();

        Pipeline p = Pipeline.create();
        p.drawFrom(batchFromProcessor("s3-word-generator",
                WordGenerator.metaSupplier(distinct, totalWordCount)))
         .drainTo(S3Sinks.s3(bucketName, clientSupplier()));

        jet.newJob(p).join();
    }

    @Override
    protected void test() {
        long begin = System.currentTimeMillis();
        while ((System.currentTimeMillis() - begin) < durationInMillis) {
            jet.newJob(pipeline()).join();
            verify();
        }
    }

    private Pipeline pipeline() {
        Pipeline pipeline = Pipeline.create();

        pipeline.drawFrom(S3Sources.s3(singletonList(bucketName), null, clientSupplier()))
                .flatMap((String line) -> {
                    StringTokenizer s = new StringTokenizer(line);
                    return () -> s.hasMoreTokens() ? s.nextToken() : null;
                })
                .groupingKey(wholeItem())
                .aggregate(counting())
                .drainTo(S3Sinks.s3(bucketName, RESULTS_PREFIX, UTF_8,
                        clientSupplier(), e -> e.getKey() + " " + e.getValue()));

        return pipeline;
    }

    private void verify() {
        Iterator<S3Object> iterator = s3Client.listObjectsV2Paginator(
                b -> b.bucket(bucketName).prefix(RESULTS_PREFIX)).contents().iterator();

        int wordNumber = 0;
        int totalNumber = 0;
        while (iterator.hasNext()) {
            S3Object s3Object = iterator.next();
            try (ResponseInputStream<GetObjectResponse> response =
                         s3Client.getObject(b -> b.bucket(bucketName).key(s3Object.key()))) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(response));
                String line = reader.readLine();
                while (line != null) {
                    wordNumber++;
                    totalNumber += Integer.parseInt(line.split(" ")[1]);
                    line = reader.readLine();
                }
            } catch (IOException e) {
                throw ExceptionUtil.rethrow(e);
            }
            s3Client.deleteObject(b -> b.bucket(bucketName).key(s3Object.key()));
        }
        assertEquals(distinct, wordNumber);
        assertEquals(totalWordCount, totalNumber);
    }


    @Override
    protected void teardown() {
        deleteBucket();
    }

    private SupplierEx<S3Client> clientSupplier() {
        return () -> S3Client.builder().region(US_EAST_1).build();
    }

    private void createBucket() {
        s3Client.createBucket(b -> b.bucket(bucketName));
    }

    private void deleteBucket() {
        try {
            s3Client.deleteBucket(b -> b.bucket(bucketName));
        } catch (NoSuchBucketException ignored) {
        }
    }
}
