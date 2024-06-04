package com.hazelcast.jet.tests.common;

import java.time.Duration;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

public class GenericLoopExecutor {

    private final long maxLoopDurationMillis;
    private final long sleepBetweenAttemptsMillis;
    private BooleanSupplier condition;
    private Runnable onConditionPass = () -> {
    }; // Default no-op
    private Runnable onConditionFail = () -> {
    }; // Default no-op
    private Consumer<Exception> onException = e -> {
        e.printStackTrace();
        throw new IllegalStateException(e);
    };
    private Runnable onLoopTimeout = () -> {
    }; // Default no-op

    public GenericLoopExecutor(Duration maxLoopDuration, Duration sleepBetweenAttempts) {
        this.maxLoopDurationMillis = maxLoopDuration.toMillis();
        this.sleepBetweenAttemptsMillis = sleepBetweenAttempts.toMillis();
    }

    protected void sleepMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public GenericLoopExecutor condition(BooleanSupplier condition) {
        this.condition = condition;
        return this;
    }

    public GenericLoopExecutor onConditionPass(Runnable onConditionPass) {
        this.onConditionPass = onConditionPass;
        return this;
    }

    public GenericLoopExecutor onConditionFail(Runnable onConditionFail) {
        this.onConditionFail = onConditionFail;
        return this;
    }

    public GenericLoopExecutor onException(Consumer<Exception> onException) {
        this.onException = onException;
        return this;
    }

    public GenericLoopExecutor onLoopTimeout(Runnable onLoopTimeout) {
        this.onLoopTimeout = onLoopTimeout;
        return this;
    }

    public void execute() {
        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < maxLoopDurationMillis) {
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
        onLoopTimeout.run(); // Loop timed out, executing timeout action
    }

    public static void main(String[] args) {
        // Example usage
        int maxWaitForExpectedConnectionInMinutes = 5; // Example value
        int sleepBetweenConnectionAssertionAttemptsInSeconds = 10; // Example value

        GenericLoopExecutor loopExecutor = new GenericLoopExecutor(
                Duration.ofMinutes(maxWaitForExpectedConnectionInMinutes),
                Duration.ofSeconds(sleepBetweenConnectionAssertionAttemptsInSeconds));

        loopExecutor
                .condition(() -> {
                    // Replace with actual condition logic
                    return true;
                })
                .onConditionPass(() -> {
                    // Replace with actual condition pass logic
                    System.out.println("Condition passed.");
                })
                .onConditionFail(() -> {
                    // Replace with actual condition fail logic
                    System.out.println("Condition failed.");
                })
                .onException(e -> {
                    // Replace with actual exception handling logic
                    e.printStackTrace();
                })
                .onLoopTimeout(() -> {
                    // Replace with actual timeout handling logic
                    System.out.println("Loop timed out.");
                })
                .execute();
    }
}
