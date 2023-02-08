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

package com.hazelcast.jet.tests.kinesis;

import com.hazelcast.com.amazonaws.SdkClientException;
import com.hazelcast.com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.hazelcast.com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.hazelcast.com.amazonaws.services.kinesis.model.DescribeStreamSummaryRequest;
import com.hazelcast.com.amazonaws.services.kinesis.model.ExpiredNextTokenException;
import com.hazelcast.com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.hazelcast.com.amazonaws.services.kinesis.model.LimitExceededException;
import com.hazelcast.com.amazonaws.services.kinesis.model.ResourceInUseException;
import com.hazelcast.com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.hazelcast.com.amazonaws.services.kinesis.model.StreamDescriptionSummary;
import com.hazelcast.com.amazonaws.services.kinesis.model.StreamStatus;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.retry.IntervalFunction;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.logging.ILogger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.lang.String.format;

public class Helper {

    static final RetryStrategy RETRY_STRATEGY = RetryStrategies.custom()
            .maxAttempts(120)
            .intervalFunction(IntervalFunction.exponentialBackoffWithCap(250L, 2.0, 1000L))
            .build();

    private final AmazonKinesisAsync kinesis;
    private final ILogger logger;

    Helper(AmazonKinesisAsync kinesis, ILogger logger) {
        this.kinesis = kinesis;
        this.logger = logger;
    }

    public void createStream(String stream, int shardCount) {
        if (streamExists(stream)) {
            throw new IllegalStateException(format("Stream[%s] already exists", stream));
        }

        callSafely(() -> {
            CreateStreamRequest request = new CreateStreamRequest();
            request.setShardCount(shardCount);
            request.setStreamName(stream);
            return kinesis.createStream(request);
        }, format("stream[%s] creation", stream));

        waitForStreamToActivate(stream);
    }

    public void deleteStream(String stream) {
        if (streamExists(stream)) {
            callSafely(() -> kinesis.deleteStream(stream), format("stream[%s] deletion", stream));
            waitForStreamToDisappear(stream);
        }
    }

    private List<String> listStreams() {
        return kinesis.listStreams().getStreamNames();
    }

    private boolean streamExists(String stream) {
        Set<String> streams = new HashSet<>(callSafely(this::listStreams, format("stream[%s] listing", stream)));
        return streams.contains(stream);
    }

    private void waitForStreamToActivate(String stream) {
        int attempt = 0;
        while (true) {
            StreamStatus status = callSafely(() -> getStreamStatus(stream), format("stream[%s] status", stream));
            switch (status) {
                case ACTIVE:
                    return;
                case CREATING:
                case UPDATING:
                    wait(++attempt, format("stream[%s] activation", stream));
                    break;
                case DELETING:
                    throw new JetException(format("Stream[%s] is being deleted: ", stream));
                default:
                    throw new JetException(format("Programming error, unhandled stream[%s] status: %s",
                            stream, status));
            }
        }
    }

    private void waitForStreamToDisappear(String stream) {
        int attempt = 0;
        while (true) {
            List<String> streams = callSafely(this::listStreams, "stream disappearance: " + stream);
            if (streams.contains(stream)) {
                wait(++attempt, "stream disappearance: " + stream);
            } else {
                return;
            }
        }
    }

    private StreamStatus getStreamStatus(String stream) {
        DescribeStreamSummaryRequest request = new DescribeStreamSummaryRequest();
        request.setStreamName(stream);

        StreamDescriptionSummary description = kinesis.describeStreamSummary(request).getStreamDescriptionSummary();
        String statusString = description.getStreamStatus();

        return StreamStatus.valueOf(statusString);
    }

    private <T> T callSafely(Callable<T> callable, String action) {
        int attempt = 0;
        while (true) {
            try {
                return callable.call();
            } catch (LimitExceededException lee) {
                String message = "The requested resource exceeds the maximum number allowed, " +
                        "or the number of concurrent stream requests exceeds the maximum number allowed. " +
                        "Will retry. The action: " + action;
                logger.warning(message, lee);
            } catch (ExpiredNextTokenException ente) {
                String message = "The pagination token passed to the operation is expired. " +
                        "Will retry. The action: " + action;
                logger.warning(message, ente);
            } catch (ResourceInUseException riue) {
                String message = "The resource is not available for this operation. For successful operation, the " +
                        "resource must be in the ACTIVE state. Will retry. The action: " + action;
                logger.warning(message, riue);
            } catch (ResourceNotFoundException rnfe) {
                String message = "The requested resource could not be found. " +
                        "The stream might not be specified correctly. The action: " + action;
                throw new JetException(message, rnfe);
            } catch (InvalidArgumentException iae) {
                String message = "A specified parameter exceeds its restrictions, is not supported, or can't be used." +
                        " The action: " + action;
                throw new JetException(message, iae);
            } catch (SdkClientException sce) {
                String message = "Amazon SDK failure, ignoring and retrying. The action: " + action;
                logger.warning(message, sce);
            } catch (Exception e) {
                throw rethrow(e);
            }

            wait(++attempt, action);
        }
    }

    private void wait(int attempt, String action) {
        if (attempt > RETRY_STRATEGY.getMaxAttempts()) {
            throw new JetException(format("Abort waiting for %s, too many attempts", action));
        }

        logger.info(format("Waiting for %s ...", action));
        long duration = RETRY_STRATEGY.getIntervalFunction().waitAfterAttempt(attempt);
        try {
            TimeUnit.MILLISECONDS.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JetException(format("Waiting for %s interrupted", action));
        }
    }

}
