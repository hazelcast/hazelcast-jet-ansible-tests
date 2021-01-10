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

import com.amazonaws.SdkClientException;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamSummaryRequest;
import com.amazonaws.services.kinesis.model.ExpiredNextTokenException;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ResourceInUseException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.StreamDescriptionSummary;
import com.amazonaws.services.kinesis.model.StreamStatus;
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

public class Helper {

    static final RetryStrategy RETRY_STRATEGY = RetryStrategies.custom()
            .maxAttempts(120)
            .intervalFunction(IntervalFunction.exponentialBackoffWithCap(250L, 2.0, 1000L))
            .build();

    private final AmazonKinesisAsync kinesis;
    private final String stream;

    private final ILogger logger;

    Helper(AmazonKinesisAsync kinesis, String stream, ILogger logger) {
        this.kinesis = kinesis;
        this.stream = stream;
        this.logger = logger;
    }

    public void createStream(int shardCount) {
        if (streamExists()) {
            throw new IllegalStateException("Stream already exists");
        }

        callSafely(() -> {
            CreateStreamRequest request = new CreateStreamRequest();
            request.setShardCount(shardCount);
            request.setStreamName(stream);
            return kinesis.createStream(request);
        }, "stream creation");

        waitForStreamToActivate();
    }

    public void deleteStream() {
        if (streamExists()) {
            callSafely(() -> kinesis.deleteStream(stream), "stream deletion");
            waitForStreamToDisappear();
        }
    }

    private List<String> listStreams() {
        return kinesis.listStreams().getStreamNames();
    }

    private boolean streamExists() {
        Set<String> streams = new HashSet<>(callSafely(this::listStreams, "stream listing"));
        return streams.contains(stream);
    }

    private void waitForStreamToActivate() {
        int attempt = 0;
        while (true) {
            StreamStatus status = callSafely(this::getStreamStatus, "stream status");
            switch (status) {
                case ACTIVE:
                    return;
                case CREATING:
                case UPDATING:
                    wait(++attempt, "stream activation");
                    break;
                case DELETING:
                    throw new JetException("Stream is being deleted");
                default:
                    throw new JetException("Programming error, unhandled stream status: " + status);
            }
        }
    }

    private void waitForStreamToDisappear() {
        int attempt = 0;
        while (true) {
            List<String> streams = callSafely(this::listStreams, "stream disappearance");
            if (streams.contains(stream)) {
                wait(++attempt, "stream disappearance");
            } else {
                return;
            }
        }
    }

    private StreamStatus getStreamStatus() {
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
                String message = "The requested resource exceeds the maximum number allowed, or the number of " +
                        "concurrent stream requests exceeds the maximum number allowed. Will retry.";
                logger.warning(message, lee);
            } catch (ExpiredNextTokenException ente) {
                String message = "The pagination token passed to the operation is expired. Will retry.";
                logger.warning(message, ente);
            } catch (ResourceInUseException riue) {
                String message = "The resource is not available for this operation. For successful operation, the " +
                        "resource must be in the ACTIVE state. Will retry.";
                logger.warning(message, riue);
            } catch (ResourceNotFoundException rnfe) {
                String message = "The requested resource could not be found. The stream might not be specified correctly.";
                throw new JetException(message, rnfe);
            } catch (InvalidArgumentException iae) {
                String message = "A specified parameter exceeds its restrictions, is not supported, or can't be used.";
                throw new JetException(message, iae);
            } catch (SdkClientException sce) {
                String message = "Amazon SDK failure, ignoring and retrying.";
                logger.warning(message, sce);
            } catch (Exception e) {
                throw rethrow(e);
            }

            wait(++attempt, action);
        }
    }

    private void wait(int attempt, String action) {
        if (attempt > RETRY_STRATEGY.getMaxAttempts()) {
            throw new JetException(String.format("Abort waiting for %s, too many attempts", action));
        }

        logger.info(String.format("Waiting for %s ...", action));
        long duration = RETRY_STRATEGY.getIntervalFunction().waitAfterAttempt(attempt);
        try {
            TimeUnit.MILLISECONDS.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new JetException(String.format("Waiting for %s interrupted", action));
        }
    }

}
