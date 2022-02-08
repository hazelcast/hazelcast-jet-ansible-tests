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

package com.hazelcast.jet.tests.sql.tests;

import static com.hazelcast.jet.tests.common.Util.parseArguments;

public final class StartTest {

    private static final String RUN_TEST = "runTest";

    private StartTest() {
    }

    public static void main(String[] args) throws Exception {
        parseArguments(args);
        String testName = System.getProperty(RUN_TEST);
        if (testName == null || testName.equals("")) {
            throw new RuntimeException("runTest property has to be set");
        }
        switch (testName) {
            case "sqlObjectMap":
                SqlObjectMapTest.main(args);
                break;
            case "sqlObjectIndexedMap":
                SqlObjectIndexedMapTest.main(args);
                break;
            default:
                throw new RuntimeException("unknown test " + testName);
        }
    }
}
