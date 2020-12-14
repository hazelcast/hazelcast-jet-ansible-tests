package com.hazelcast.jet.tests.file.ingestion;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.hadoop.HadoopSinks;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.file.FileSources;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.tests.common.AbstractSoakTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.hazelcast.function.FunctionEx.identity;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static java.util.stream.Collectors.toList;

public class FileIngestionTest extends AbstractSoakTest {


    private static final String DEFAULT_DIRECTORY = "/tmp/" + FileIngestionTest.class.getSimpleName();
    private static final String DEFAULT_HDFS_URI = "hdfs://localhost:8020";
    private static final String DEFAULT_HDFS_PATH = DEFAULT_HDFS_URI + "/" + FileIngestionTest.class.getSimpleName();
    private static final String DEFAULT_BUCKET_NAME = "jet-soak-tests-bucket";
    private static final String DEFAULT_DIRECTORY_NAME = FileIngestionTest.class.getSimpleName();
    private static final int S3_CLIENT_CONNECTION_TIMEOUT_SECONDS = 10;
    private static final int S3_CLIENT_SOCKET_TIMEOUT_MINUTES = 5;
    private static final int DEFAULT_SLEEP_SECONDS = 4;
    private static final int ITEM_COUNT = 1000;

    private String localDirectory;
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
    protected void init(JetInstance client) throws Exception {
        localDirectory = property("localDirectory", DEFAULT_DIRECTORY);

        hdfsUri = property("hdfsUri", DEFAULT_HDFS_URI);
        hdfsPath = property("hdfsPath", DEFAULT_HDFS_PATH);

        bucketName = property("bucketName", DEFAULT_BUCKET_NAME);
        s3Directory = property("s3Directory", DEFAULT_DIRECTORY_NAME) + "/";
        accessKey = property("accessKey", "");
        secretKey = property("secretKey", "");

        sleepSeconds = propertyInt("sleepSecondsBetweenJobs", DEFAULT_SLEEP_SECONDS);

        createSourceFiles(client);
    }

    @Override
    protected void test(JetInstance client, String name) throws Throwable {
        long begin = System.currentTimeMillis();
        int jobNumber = 0;
        JobType[] jobTypes = JobType.values();
        Job[] jobs = new Job[jobTypes.length];
        while ((System.currentTimeMillis() - begin) < durationInMillis) {
            for (int i = 0; i < jobTypes.length; i++) {
                JobType jobType = jobTypes[i];
                JobConfig jobConfig = new JobConfig();
                jobConfig.setName(name + "-" + jobType + "-" + jobNumber);
                jobs[i] = client.newJob(pipeline(jobType, jobNumber), jobConfig);
            }
            for (Job job : jobs) {
                job.join();
            }
            for (JobType jobType : jobTypes) {
                verifyObservable(client.getObservable(observableName(jobType, jobNumber)));
            }
            sleepSeconds(sleepSeconds);
            jobNumber++;
        }
    }

    private void verifyObservable(Observable<Long> observable) {
        Iterator<Long> iterator = observable.iterator();
        assertTrue(observable.name() + " is empty", iterator.hasNext());
        assertEquals(ITEM_COUNT, iterator.next().intValue());
        assertFalse(iterator.hasNext());
        observable.destroy();
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
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

    private BatchSource<String> source(JobType jobType) {
        switch (jobType) {
            case LOCAL:
                return FileSources.files(localDirectory).build();
            case LOCAL_WITH_HADOOP:
                return FileSources.files(localDirectory).useHadoopForLocalFiles(true).build();
            case HDFS:
                return FileSources.files(hdfsPath)
                        .option("fs.defaultFS", hdfsUri)
                        .option("fs.hdfs.impl", DistributedFileSystem.class.getName())
                        .option("fs.file.impl", LocalFileSystem.class.getName())
                        .build();
            case S3:
                return FileSources.files("s3a://" + bucketName + "/" + s3Directory)
                        .option("fs.defaultFS", hdfsUri)
                        .option("fs.s3a.access.key", accessKey)
                        .option("fs.s3a.secret.key", secretKey)
                        .option("fs.hdfs.impl", DistributedFileSystem.class.getName())
                        .option("fs.file.impl", LocalFileSystem.class.getName())
                        .build();
            default:
                throw new IllegalArgumentException();
        }
    }

    private void createSourceFiles(JetInstance client) throws Exception {
        List<Integer> items = IntStream.range(0, ITEM_COUNT).boxed().collect(toList());

        // clear local directory
        IOUtil.delete(Paths.get(localDirectory));

        // clear hdfs
        URI uri = URI.create(hdfsUri);
        String disableCacheName = String.format("fs.%s.impl.disable.cache", uri.getScheme());
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", hdfsUri);
        configuration.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", LocalFileSystem.class.getName());
        configuration.setBoolean(disableCacheName, true);
        try (FileSystem fs = FileSystem.get(uri, configuration)) {
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
        sourceStage.writeTo(Sinks.files(localDirectory));

        // create hdfs source files
        JobConf conf = new JobConf();
        conf.set("fs.defaultFS", hdfsUri);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setOutputFormat(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(conf, new Path(hdfsPath));
        sourceStage.writeTo(HadoopSinks.outputFormat(conf, identity(), identity()));

        client.newJob(p).join();
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
        return jobType.name() + "-" + jobNumber;
    }

    enum JobType {
        LOCAL, LOCAL_WITH_HADOOP, HDFS, S3
    }
}
