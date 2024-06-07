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

package com.hazelcast.jet.tests.common;

import java.time.Duration;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * A class that verifies a condition within a specified timeout period.
 * It allows setting custom actions for condition pass, condition fail, exceptions, and timeout.
 * This class can replace a traditional while loop that waits for a condition and handles
 * timeout, condition failure, etc.
 */
public class ConditionVerifierWithTimeout {

    private final long maxDurationMillis;
    private final long sleepBetweenAttemptsMillis;
    private BooleanSupplier condition;
    private Runnable onConditionPass = () -> { }; // Default no-op
    private Runnable onConditionFail = () -> { }; // Default no-op
    private Consumer<Exception> onException = e -> {
        e.printStackTrace();
        throw new IllegalStateException(e);
    };
    private Runnable onTimeout;

    /**
     * Constructs a ConditionVerifierWithTimeout with specified maximum loop duration and sleep interval between attempts.
     *
     * @param maxLoopDuration the maximum duration to run the loop
     * @param sleepBetweenAttempts the duration to sleep between loop attempts
     */
    public ConditionVerifierWithTimeout(Duration maxLoopDuration, Duration sleepBetweenAttempts) {
        this.maxDurationMillis = maxLoopDuration.toMillis();
        this.sleepBetweenAttemptsMillis = sleepBetweenAttempts.toMillis();
    }

    /**
     * Sets the condition to be checked in each loop iteration.
     *
     * @param condition the condition to be checked
     */
    public ConditionVerifierWithTimeout condition(BooleanSupplier condition) {
        this.condition = condition;
        return this;
    }

    /**
     * Sets the action to be performed when the condition passes.
     *
     * @param onConditionPass the action to be performed
     */
    public ConditionVerifierWithTimeout onConditionPass(Runnable onConditionPass) {
        this.onConditionPass = onConditionPass;
        return this;
    }

    /**
     * Sets the action to be performed when the condition fails.
     *
     * @param onConditionFail the action to be performed
     */
    public ConditionVerifierWithTimeout onConditionFail(Runnable onConditionFail) {
        this.onConditionFail = onConditionFail;
        return this;
    }

    /**
     * Sets the action to be performed when an exception occurs during the loop execution.
     *
     * @param onException the action to be performed
     */
    public ConditionVerifierWithTimeout onException(Consumer<Exception> onException) {
        this.onException = onException;
        return this;
    }

    /**
     * Sets the action to be performed when the loop times out without meeting the condition.
     *
     * @param onTimeout the action to be performed
     */
    public ConditionVerifierWithTimeout onTimeout(Runnable onTimeout) {
        this.onTimeout = onTimeout;
        return this;
    }

    /**
     * Executes the loop, checking the condition in each iteration and performing the appropriate actions.
     * Throws an exception if the condition or onTimeout actions are not set.
     */
    public void execute() {
        validate();
        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < maxDurationMillis) {
            try {
                if (condition.getAsBoolean()) {
                    onConditionPass.run();
                    return; // Condition passed, exiting loop
                } else {
                    onConditionFail.run();
                }
            } catch (Exception e) {
                onException.accept(e);
            }
            sleepMillis(sleepBetweenAttemptsMillis);
        }
        onTimeout.run(); // Loop timed out, executing timeout action
    }

    private void validate() {
        if (condition == null) {
            throw new IllegalStateException("condition has not been set");
        }
        if (onTimeout == null) {
            throw new IllegalStateException("onTimeout has not been set");
        }
    }


    private void sleepMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
