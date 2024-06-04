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

package com.hazelcast.jet.tests.grpc;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.grpc.GrpcService;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.jet.tests.grpc.greeter.GreeterGrpc;
import com.hazelcast.jet.tests.grpc.greeter.GreeterOuterClass.HelloReply;
import com.hazelcast.jet.tests.grpc.greeter.GreeterOuterClass.HelloRequest;
import com.hazelcast.map.IMap;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;

import java.util.Map;

import static com.hazelcast.jet.grpc.GrpcServices.bidirectionalStreamingService;
import static com.hazelcast.jet.grpc.GrpcServices.unaryService;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static com.hazelcast.query.Predicates.alwaysTrue;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public class GrpcTest extends AbstractJetSoakTest {

    private static final String SOURCE_MAP_NAME = "grpcSourceMap";
    private static final String SINK_LIST_NAME = "grpcSinkList";

    private static final int DEFAULT_ITEM_COUNT = 10_000;
    private static final int LOG_JOB_COUNT_THRESHOLD = 100;
    private static final int DEFAULT_SLEEP_AFTER_TEST_SECONDS = 5;

    private Server server;
    private String serverAddress;
    private IMap<Integer, String> sourceMap;
    private IList<String> sinkList;
    private int itemCount;
    private int sleepAfterTestSeconds;

    public static void main(String[] args) throws Exception {
        new GrpcTest().run(args);
    }

    @Override
    protected void init(HazelcastInstance client) throws Exception {
        server = GreeterService.createServer();

        sourceMap = client.getMap(SOURCE_MAP_NAME);
        sinkList = client.getList(SINK_LIST_NAME);

        sourceMap.clear();
        sinkList.clear();

        serverAddress = property("serverAddress", "localhost").trim();
        itemCount = propertyInt("itemCount", DEFAULT_ITEM_COUNT);
        sleepAfterTestSeconds = propertyInt("sleepAfterTestSeconds", DEFAULT_SLEEP_AFTER_TEST_SECONDS);

        range(0, itemCount)
                .mapToObj(i -> sourceMap.putAsync(i, String.valueOf(i)).toCompletableFuture())
                .collect(toList())
                .forEach(f -> uncheckCall(f::get));
    }

    @Override
    protected void test(HazelcastInstance client, String name) throws Throwable {
        long begin = System.currentTimeMillis();
        long jobCount = 0;
        while (System.currentTimeMillis() - begin < durationInMillis) {
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("GrpcTest" + jobCount);
            client.getJet().newJob(pipeline(jobCount), jobConfig).join();

            assertEquals(itemCount, sinkList.size());
            assertEquals(itemCount, sinkList.stream().filter(s -> s.startsWith("Hello")).count());
            sinkList.clear();
            if (jobCount % LOG_JOB_COUNT_THRESHOLD == 0) {
                logger.info("Job count: " + jobCount);
            }
            jobCount++;
            sleepSeconds(sleepAfterTestSeconds);
        }
        logger.info("Final job count: " + jobCount);
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
        server.shutdownNow();
    }

    private Pipeline pipeline(long currentJobCount) {
        Pipeline p = Pipeline.create();

        String localServerAddress = serverAddress;
        int port = server.getPort();
        ServiceFactory<?, ? extends GrpcService<HelloRequest, HelloReply>> unaryServiceFactory =
                unaryService(
                        () -> ManagedChannelBuilder.forAddress(localServerAddress, port).usePlaintext(),
                        channel -> GreeterGrpc.newStub(channel)::sayHelloUnary
                );

        ServiceFactory<?, ? extends GrpcService<HelloRequest, HelloReply>> bidirectionalServiceFactory =
                bidirectionalStreamingService(
                        () -> ManagedChannelBuilder.forAddress(localServerAddress, port).usePlaintext(),
                        channel -> GreeterGrpc.newStub(channel)::sayHelloBidirectional
                );

        ServiceFactory<?, ? extends GrpcService<HelloRequest, HelloReply>> serviceFactory =
                currentJobCount % 2 == 0 ? unaryServiceFactory : bidirectionalServiceFactory;

        p.readFrom(Sources.map(sourceMap, alwaysTrue(), Map.Entry::getValue))
         .mapUsingServiceAsync(serviceFactory,
                 (service, input) -> {
                     HelloRequest request = HelloRequest.newBuilder().setName(input).build();
                     return service.call(request).thenApply(HelloReply::getMessage);
                 })
         .writeTo(Sinks.list(sinkList));
        return p;
    }

}
