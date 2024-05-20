package com.hazelcast.jet.tests.isolated;

import static com.hazelcast.jet.tests.common.Util.parseArguments;

public class IsolatedJobsTestStarter {
    private static final String RUN_TEST = "runTest";

    public static void main(String[] args) throws Exception {
        parseArguments(args);

        String testName = System.getProperty(RUN_TEST);
        if (testName == null || testName.isEmpty()) {
            throw new RuntimeException("runTest property has to be set");
        }

        switch (testName) {
            case "IsolatedJobsStreamTest":
                IsolatedJobsStreamTest.main(args);
                break;
            case "IsolatedJobsBatchTest": {
                IsolatedJobsBatchTest.main(args);
                break;
            }
            default:
                throw new RuntimeException("unknown test " + testName);
        }
    }
}
