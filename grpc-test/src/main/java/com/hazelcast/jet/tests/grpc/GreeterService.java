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

import com.hazelcast.jet.tests.grpc.greeter.GreeterGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.Executors;

import static com.hazelcast.jet.tests.grpc.greeter.GreeterOuterClass.HelloReply;
import static com.hazelcast.jet.tests.grpc.greeter.GreeterOuterClass.HelloRequest;

public final class GreeterService extends GreeterGrpc.GreeterImplBase {

    private GreeterService() {
    }

    @Override
    public void sayHelloUnary(
            HelloRequest request,
            StreamObserver<HelloReply> responseObserver
    ) {
        HelloReply reply = HelloReply.newBuilder()
                                     .setMessage("Hello " + request.getName())
                                     .build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<HelloRequest> sayHelloBidirectional(
            StreamObserver<HelloReply> responseObserver
    ) {
        return new StreamObserver<HelloRequest>() {

            @Override
            public void onNext(HelloRequest value) {
                HelloReply reply = HelloReply.newBuilder()
                                             .setMessage("Hello " + value.getName())
                                             .build();

                responseObserver.onNext(reply);
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    static Server createServer() throws IOException {
        return ServerBuilder.forPort(0).executor(Executors.newFixedThreadPool(4))
                            .addService(new GreeterService()).build().start();
    }
}
