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
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A class that verifies a condition and stops immediately when the condition is met or a timeout occurs.
 * It allows setting custom actions for condition pass, condition fail, exceptions, and timeout.
 * This class can replace a traditional while loop that waits for a condition and handles
 * timeout, condition failure, etc.
 */
public class ConditionVerifierWithTimeout<T> {

    private final long timeoutMillis;
    private final long sleepBetweenAttemptsMillis;
    private Supplier<ConditionResult<T>> condition;
    private Consumer<T> onConditionPass = context -> { }; // Default no-op
    private Consumer<T> onConditionNotPassedYet = context -> { }; // Default no-op
    private Consumer<Exception> onException = e -> {
        e.printStackTrace();
        throw new IllegalStateException(e);
    };
    private Runnable onTimeout;

    /**
     * Constructs a ConditionVerifierWithTimeout with specified maximum verification duration
     * and sleep interval between attempts.
     *
     * @param timeout the maximum duration to run the verification
     * @param sleepBetweenAttempts the duration to sleep between verification attempts
     */
    public ConditionVerifierWithTimeout(Duration timeout, Duration sleepBetweenAttempts) {
        this.timeoutMillis = timeout.toMillis();
        this.sleepBetweenAttemptsMillis = sleepBetweenAttempts.toMillis();
    }

    /**
     * Sets the condition to be checked in each verification iteration.
     *
     * @param condition the condition to be checked
     */
    public ConditionVerifierWithTimeout<T> condition(Supplier<ConditionResult<T>> condition) {
        this.condition = condition;
        return this;
    }

    /**
     * Sets the action to be performed when the condition passes.
     *
     * @param onConditionPass the action to be performed
     */
    public ConditionVerifierWithTimeout<T> onConditionPass(Consumer<T> onConditionPass) {
        this.onConditionPass = onConditionPass;
        return this;
    }

    /**
     * Sets the action to be performed when the condition returns false.
     *
     * @param onConditionNotPassedYet the action to be performed
     */
    public ConditionVerifierWithTimeout<T> onConditionNotPassedYet(Consumer<T> onConditionNotPassedYet) {
        this.onConditionNotPassedYet = onConditionNotPassedYet;
        return this;
    }

    /**
     * Sets the action to be performed when an exception occurs during the verification execution.
     *
     * @param onException the action to be performed
     */
    public ConditionVerifierWithTimeout<T> onException(Consumer<Exception> onException) {
        this.onException = onException;
        return this;
    }

    /**
     * Sets the action to be performed when the verification times out without meeting the condition.
     *
     * @param onTimeout the action to be performed
     */
    public ConditionVerifierWithTimeout<T> onTimeout(Runnable onTimeout) {
        this.onTimeout = onTimeout;
        return this;
    }

    /**
     * Executes the verification, checking the condition in each iteration and performing the appropriate actions.
     * Throws an exception if the condition or onTimeout actions are not set.
     */
    public void execute() {
        validate();
        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < timeoutMillis) {
            try {
                ConditionResult<T> result = condition.get();
                if (result.isConditionMet()) {
                    onConditionPass.accept(result.getContext());
                    return; // Condition passed, exiting verification
                } else {
                    onConditionNotPassedYet.accept(result.getContext());
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

    /**
     * Class representing the result of a condition check.
     *
     * @param <T> the type of the context object
     */
    public static class ConditionResult<T> {
        private final boolean conditionMet;
        private final T context;

        /**
         * Constructs a ConditionResult with the specified condition result and context.
         *
         * @param conditionMet whether the condition was met
         * @param context additional context about the condition result
         */
        public ConditionResult(boolean conditionMet, T context) {
            this.conditionMet = conditionMet;
            this.context = context;
        }

        /**
         * Constructs a ConditionResult with the specified condition result and context set to null
         *
         * @param conditionMet whether the condition was met
         */
        public ConditionResult(boolean conditionMet) {
            this.conditionMet = conditionMet;
            this.context = null;
        }


        /**
         * Returns whether the condition was met.
         *
         * @return true if the condition was met, false otherwise
         */
        public boolean isConditionMet() {
            return conditionMet;
        }

        /**
         * Returns the additional context about the condition result.
         *
         * @return the context
         */
        public T getContext() {
            return context;
        }
    }

}
