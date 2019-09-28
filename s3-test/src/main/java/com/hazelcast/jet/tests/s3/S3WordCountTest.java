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

package com.hazelcast.jet.tests.s3;

import com.hazelcast.client.UndefinedErrorCodeException;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.s3.S3Sinks;
import com.hazelcast.jet.s3.S3Sources;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.function.Functions.wholeItem;
import static com.hazelcast.jet.pipeline.Sources.batchFromProcessor;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static software.amazon.awssdk.regions.Region.US_EAST_1;

public class S3WordCountTest extends AbstractSoakTest {


    private static final int GET_OBJECT_RETRY_COUNT = 30;
    private static final long GET_OBJECT_RETRY_WAIT_TIME = TimeUnit.SECONDS.toNanos(1);
    private static final int S3_CLIENT_CONNECTION_TIMEOUT_SECONDS = 10;
    private static final int S3_CLIENT_SOCKET_TIMEOUT_MINUTES = 5;

    private static final String DEFAULT_BUCKET_NAME = "jet-soak-tests-bucket";
    private static final String RESULTS_PREFIX = "results/";
    private static final int DEFAULT_TOTAL = 400000;
    private static final int DEFAULT_DISTINCT = 50000;


    private S3Client s3Client;
    private String bucketName;
    private String accessKey;
    private String secretKey;
    private int distinct;
    private int totalWordCount;

    public static void main(String[] args) throws Exception {
        new S3WordCountTest().run(args);
    }

    @Override
    protected void init() {
        bucketName = property("bucketName", DEFAULT_BUCKET_NAME);
        accessKey = property("accessKey", null);
        secretKey = property("secretKey", null);
        distinct = propertyInt("distinctWords", DEFAULT_DISTINCT);
        totalWordCount = propertyInt("totalWordCount", DEFAULT_TOTAL);

        s3Client = clientSupplier().get();
        deleteBucketContents();

        Pipeline p = Pipeline.create();
        p.drawFrom(batchFromProcessor("s3-word-generator",
                WordGenerator.metaSupplier(distinct, totalWordCount)))
         .drainTo(S3Sinks.s3(bucketName, clientSupplier()));

        jet.newJob(p).join();
    }

    @Override
    protected void test() {
        long begin = System.currentTimeMillis();
        int jobNumber = 0;
        int socketTimeoutNumber = 0;
        while ((System.currentTimeMillis() - begin) < durationInMillis) {
            try {
                JobConfig jobConfig = new JobConfig();
                jobConfig.setName("s3-test-" + jobNumber);
                jet.newJob(pipeline(), jobConfig).join();
                verify(jobNumber);
            } catch (Throwable e) {
                if (isSocketTimeoutException(e) || isSocketException(e)) {
                    logger.warning("Socket timeout ", e);
                    socketTimeoutNumber++;
                } else {
                    throw ExceptionUtil.rethrow(e);
                }
            }
            jobNumber++;
        }
        long thresholdForSocketTimeout = TimeUnit.MILLISECONDS.toHours(durationInMillis) + 1;
        assertTrue("Socket timeout number is too big", thresholdForSocketTimeout > socketTimeoutNumber);
        logger.info(String.format("Total number of jobs finished: %d, socketTimeout: %d", jobNumber, socketTimeoutNumber));
    }

    private boolean isSocketTimeoutException(Throwable e) {
        if (e instanceof SocketTimeoutException) {
            return true;
        }
        if (e instanceof UndefinedErrorCodeException) {
            return ((UndefinedErrorCodeException) e).getOriginClassName().equals(SocketTimeoutException.class.getName());
        }
        Throwable cause = e.getCause();
        if (cause != null) {
            return isSocketTimeoutException(cause);
        }
        return false;
    }

    private boolean isSocketException(Throwable e) {
        if (e instanceof SocketException) {
            return true;
        }
        if (e instanceof UndefinedErrorCodeException) {
            return ((UndefinedErrorCodeException) e).getOriginClassName().equals(SocketException.class.getName());
        }
        Throwable cause = e.getCause();
        if (cause != null) {
            return isSocketException(cause);
        }
        return false;
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

    private void verify(int jobNumber) {
        Iterator<S3Object> iterator = s3Client.listObjectsV2Paginator(
                b -> b.bucket(bucketName).prefix(RESULTS_PREFIX)).contents().iterator();

        int wordNumber = 0;
        int totalNumber = 0;
        while (iterator.hasNext()) {
            S3Object s3Object = iterator.next();
            try (ResponseInputStream<GetObjectResponse> response = getObjectWithRetry(jobNumber, s3Object)) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(response));
                String line = reader.readLine();
                while (line != null) {
                    wordNumber++;
                    totalNumber += Integer.parseInt(line.split(" ")[1]);
                    line = reader.readLine();
                }
            } catch (Exception e) {
                logger.severe(String.format("Verification failed for job: %d, object: %s", jobNumber, s3Object.key()), e);
                throw ExceptionUtil.rethrow(e);
            }
        }
        assertEquals(distinct, wordNumber);
        assertEquals(totalWordCount, totalNumber);

        s3Client.listObjectsV2Paginator(b -> b.bucket(bucketName).prefix(RESULTS_PREFIX))
                .contents()
                .forEach(s3Object -> s3Client.deleteObject(b -> b.bucket(bucketName).key(s3Object.key())));
    }

    /**
     * Retries the getObject call due to eventual consistency model of S3
     */
    private ResponseInputStream<GetObjectResponse> getObjectWithRetry(int jobNumber, S3Object s3Object) {
        NoSuchKeyException exception = null;
        for (int i = 0; i < GET_OBJECT_RETRY_COUNT; i++) {
            try {
                return s3Client.getObject(b -> b.bucket(bucketName).key(s3Object.key()));
            } catch (NoSuchKeyException e) {
                exception = e;
                logger.warning(String.format("GetObject failed for job: %d, object: %s", jobNumber, s3Object.key()));
                LockSupport.parkNanos(GET_OBJECT_RETRY_WAIT_TIME);
            }
        }
        throw exception;
    }


    @Override
    protected void teardown(Throwable t) throws Exception {
        if (t != null) {
            deleteBucketContents();
        }
    }

    private SupplierEx<S3Client> clientSupplier() {
        String localAccessKey = accessKey;
        String localSecretKey = secretKey;
        return () -> {
            AwsBasicCredentials credentials = AwsBasicCredentials.create(localAccessKey, localSecretKey);
            return S3Client.builder()
                           .credentialsProvider(StaticCredentialsProvider.create(credentials))
                           .region(US_EAST_1)
                           .httpClientBuilder(
                                   ApacheHttpClient
                                           .builder()
                                           .connectionTimeout(Duration.ofSeconds(S3_CLIENT_CONNECTION_TIMEOUT_SECONDS))
                                           .socketTimeout(Duration.ofMinutes(S3_CLIENT_SOCKET_TIMEOUT_MINUTES))

                           )
                           .build();
        };
    }

    private void deleteBucketContents() {
        try {
            s3Client.listObjectsV2Paginator(b -> b.bucket(bucketName))
                    .contents()
                    .forEach(s3Object -> s3Client.deleteObject(b -> b.bucket(bucketName).key(s3Object.key())));
        } catch (Exception e) {
            logger.warning("Exception while deleting bucket contents", e);
            throw ExceptionUtil.rethrow(e);
        }
    }
}
