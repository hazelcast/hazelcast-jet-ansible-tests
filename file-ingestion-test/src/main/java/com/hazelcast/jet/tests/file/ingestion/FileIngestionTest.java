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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.avro.AvroSinks;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.hadoop.HadoopSinks;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSources;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.jet.tests.file.ingestion.domain.AvroUser;
import com.hazelcast.jet.tests.file.ingestion.domain.JsonUser;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.hazelcast.function.FunctionEx.identity;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static com.hazelcast.jet.tests.file.ingestion.FileIngestionTest.JobType.AVRO;
import static com.hazelcast.jet.tests.file.ingestion.FileIngestionTest.JobType.CSV;
import static com.hazelcast.jet.tests.file.ingestion.FileIngestionTest.JobType.JSON;
import static com.hazelcast.jet.tests.file.ingestion.FileIngestionTest.JobType.LOCAL;
import static com.hazelcast.jet.tests.file.ingestion.FileIngestionTest.JobType.PARQUET;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.mapreduce.Job.getInstance;

public class FileIngestionTest extends AbstractJetSoakTest {


    private static final String LOCAL_DIRECTORY = "/tmp/file-ingestion/";
    private static final String DEFAULT_HDFS_URI = "hdfs://localhost:8020";
    private static final String DEFAULT_BUCKET_NAME = "jet-soak-tests-bucket";
    private static final int S3_CLIENT_CONNECTION_TIMEOUT_SECONDS = 10;
    private static final int S3_CLIENT_SOCKET_TIMEOUT_MINUTES = 5;
    private static final int DEFAULT_SLEEP_SECONDS = 5;
    private static final int ITEM_COUNT = 1000;

    private final List<Throwable> throwableList = new ArrayList<>();
    private String hdfsUri;
    private String hdfsPath;
    private String bucketName;
    private String s3Directory;
    private String accessKey;
    private String secretKey;
    private int sleepSeconds;

    public static void main(String[] args) throws Exception {
        new FileIngestionTest().run(args);
    }

    @Override
    protected void init(HazelcastInstance client) throws Exception {
        hdfsUri = property("hdfsUri", DEFAULT_HDFS_URI);
        hdfsPath = hdfsUri + "/" + getClass().getSimpleName();

        bucketName = property("bucketName", DEFAULT_BUCKET_NAME);
        s3Directory = getClass().getSimpleName() + "/";
        accessKey = property("accessKey", "");
        secretKey = property("secretKey", "");

        sleepSeconds = propertyInt("sleepSecondsBetweenJobs", DEFAULT_SLEEP_SECONDS);

        createSourceFiles(client);
    }

    @Override
    protected void test(HazelcastInstance client, String name) {
        long begin = System.currentTimeMillis();
        int jobCount = 0;

        JobType[] jobTypes = JobType.values();
        Job[] jobs = new Job[jobTypes.length];
        while ((System.currentTimeMillis() - begin) < durationInMillis) {
            for (int i = 0; i < jobTypes.length; i++) {
                JobType jobType = jobTypes[i];
                JobConfig jobConfig = new JobConfig();
                jobConfig.setName(name + "-" + jobType + "-" + jobCount);
                jobs[i] = client.getJet().newJob(pipeline(jobType, jobCount), jobConfig);
            }
            for (int i = 0; i < jobTypes.length; i++) {
                Observable<Long> observable = client.getJet().getObservable(observableName(jobTypes[i], jobCount));
                try {
                    jobs[i].join();
                    verifyObservable(observable);
                } catch (Throwable t) {
                    logger.severe("Failure while verifying job: " + jobs[i].getName(), t);
                    throwableList.add(t);
                } finally {
                    observable.destroy();
                }
            }
            sleepSeconds(sleepSeconds);
            jobCount++;
            if (jobCount % 100 == 0) {
                logger.info("Job count " + jobCount);
            }
        }
        logger.info("Final job count " + jobCount);
        if (!throwableList.isEmpty()) {
            logger.severe("Total failed jobs: " + throwableList.size());
            throwableList.forEach(t -> logger.severe("Failed job", t));
            throw new AssertionError("Total failed jobs: " + throwableList.size());
        }
    }

    private void verifyObservable(Observable<Long> observable) {
        Iterator<Long> iterator = observable.iterator();
        String name = observable.name();
        assertTrue(name + " is empty", iterator.hasNext());
        int actual = iterator.next().intValue();
        assertEquals(name + " -> expected: " + ITEM_COUNT + ", actual: " + actual, ITEM_COUNT, actual);
        assertFalse(name + " should have single item", iterator.hasNext());
    }

    @Override
    protected void teardown(Throwable t) {
    }

    private Pipeline pipeline(JobType jobType, int jobNumber) {
        Pipeline p = Pipeline.create();

        p.readFrom(source(jobType))
                .groupingKey(ignored -> 0)
                .aggregate(counting())
                .map(Map.Entry::getValue)
                .writeTo(Sinks.observable(observableName(jobType, jobNumber)));

        return p;
    }

    private BatchSource<?> source(JobType jobType) {
        BatchSource<?> source;
        switch (jobType) {
            case LOCAL:
                source = FileSources.files(LOCAL_DIRECTORY + LOCAL).build();
                break;
            case AVRO:
                source = FileSources.files(LOCAL_DIRECTORY + AVRO)
                        .format(FileFormat.avro(AvroUser.class))
                        .build();
                break;
            case JSON:
                source = FileSources.files(LOCAL_DIRECTORY + JSON)
                        .format(FileFormat.json(JsonUser.class))
                        .build();
                break;
            case CSV:
                source = FileSources.files(LOCAL_DIRECTORY + CSV)
                        .format(FileFormat.csv(JsonUser.class))
                        .build();
                break;
            case PARQUET:
                source = FileSources.files(LOCAL_DIRECTORY + PARQUET)
                        .useHadoopForLocalFiles(true)
                        .format(FileFormat.parquet())
                        .build();
                break;
            case LOCAL_WITH_HADOOP:
                source = FileSources.files(LOCAL_DIRECTORY + LOCAL)
                        .useHadoopForLocalFiles(true)
                        .build();
                break;
            case AVRO_WITH_HADOOP:
                source = FileSources.files(LOCAL_DIRECTORY + AVRO)
                        .useHadoopForLocalFiles(true)
                        .format(FileFormat.avro(AvroUser.class))
                        .build();
                break;
            case JSON_WITH_HADOOP:
                source = FileSources.files(LOCAL_DIRECTORY + JSON)
                        .useHadoopForLocalFiles(true)
                        .format(FileFormat.json(JsonUser.class))
                        .build();
                break;
            case CSV_WITH_HADOOP:
                source = FileSources.files(LOCAL_DIRECTORY + CSV)
                        .useHadoopForLocalFiles(true)
                        .format(FileFormat.csv(JsonUser.class))
                        .build();
                break;
            case HDFS:
                source = FileSources.files(hdfsPath).build();
                break;
            case S3:
                source = FileSources.files("s3a://" + bucketName + "/" + s3Directory)
                        .option("fs.s3a.access.key", accessKey)
                        .option("fs.s3a.secret.key", secretKey)
                        .build();
                break;
            default:
                throw new IllegalArgumentException("Unknown JobType");
        }
        return source;
    }

    private void createSourceFiles(HazelcastInstance client) throws Exception {
        List<Integer> items = IntStream.range(0, ITEM_COUNT).boxed().collect(toList());

        // clear local directory
        IOUtil.delete(Paths.get(LOCAL_DIRECTORY));

        // clear hdfs
        URI uri = URI.create(hdfsUri);
        try (FileSystem fs = FileSystem.get(uri, new Configuration())) {
            fs.delete(new Path(hdfsPath), true);
        }

        // clear and create S3
        try (S3Client s3Client = s3Client()) {
            s3Client.listObjectsV2Paginator(b -> b.bucket(bucketName).prefix(s3Directory))
                    .contents()
                    .forEach(s3Object -> s3Client.deleteObject(b -> b.bucket(bucketName).key(s3Object.key())));

            StringBuilder builder = new StringBuilder();
            for (Integer item : items) {
                builder.append(item).append(System.lineSeparator());
            }
            s3Client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(s3Directory + "file")
                            .build(),
                    RequestBody.fromString(builder.toString())
            );
        }

        Pipeline p = Pipeline.create();
        BatchStage<Integer> sourceStage = p.readFrom(TestSources.itemsDistributed(items));

        // create local source files
        sourceStage.writeTo(Sinks.files(LOCAL_DIRECTORY + LOCAL.name()));

        // create hdfs source files
        JobConf conf = new JobConf();
        conf.set("fs.defaultFS", hdfsUri);
        conf.setOutputFormat(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(conf, new Path(hdfsPath));
        sourceStage.writeTo(HadoopSinks.outputFormat(conf, identity(), identity()));

        // create avro source files
        sourceStage
                .map(FileIngestionTest::avroUser)
                .writeTo(AvroSinks.files(LOCAL_DIRECTORY + AVRO.name(), AvroUser.SCHEMA$, ReflectDatumWriter::new));

        // create json source files
        sourceStage
                .map(FileIngestionTest::jsonUser)
                .writeTo(Sinks.json(LOCAL_DIRECTORY + JSON.name()));

        // create csv source files
        sourceStage
                .map(FileIngestionTest::jsonUser)
                .writeTo(CsvSink.sink(LOCAL_DIRECTORY + CSV.name(), JsonUser.class));

        // create parquet source files
        org.apache.hadoop.mapreduce.Job job = getInstance();
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        AvroParquetOutputFormat.setOutputPath(job, new Path(LOCAL_DIRECTORY + PARQUET));
        AvroParquetOutputFormat.setSchema(job, AvroUser.SCHEMA$);
        sourceStage
                .map(FileIngestionTest::avroUser)
                .writeTo(HadoopSinks.outputFormat(job.getConfiguration(), o -> null, identity()));

        client.getJet().newJob(p).join();
    }

    private S3Client s3Client() {
        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
        return S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .region(Region.US_EAST_1)
                .httpClientBuilder(
                        ApacheHttpClient
                                .builder()
                                .connectionTimeout(Duration.ofSeconds(S3_CLIENT_CONNECTION_TIMEOUT_SECONDS))
                                .socketTimeout(Duration.ofMinutes(S3_CLIENT_SOCKET_TIMEOUT_MINUTES))

                )
                .build();
    }

    private static String observableName(JobType jobType, int jobNumber) {
        return FileIngestionTest.class.getSimpleName() + "-" + jobType.name() + "-" + jobNumber;
    }

    private static AvroUser avroUser(int i) {
        return new AvroUser("name: " + i, "pass: " + i, i, false);
    }

    private static JsonUser jsonUser(int i) {
        return new JsonUser("name: " + i, "pass: " + i, i, false);
    }

    enum JobType {
        LOCAL,
        AVRO,
        JSON,
        CSV,
        PARQUET,
        LOCAL_WITH_HADOOP,
        AVRO_WITH_HADOOP,
        JSON_WITH_HADOOP,
        CSV_WITH_HADOOP,
        HDFS,
        S3,
    }
}
