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

package com.hazelcast.jet.sql.tests.json;

import com.hazelcast.jet.sql.tests.json.array.SqlJsonArrayAggTest;
import com.hazelcast.jet.sql.tests.json.array.SqlJsonArrayTest;
import com.hazelcast.jet.sql.tests.json.object.SqlJsonObjectAggTest;
import com.hazelcast.jet.sql.tests.json.object.SqlJsonObjectTest;
import com.hazelcast.jet.sql.tests.json.query.SqlJsonQueryDirectNodeTest;
import com.hazelcast.jet.sql.tests.json.query.SqlJsonQueryNodeArrayTest;
import com.hazelcast.jet.sql.tests.json.query.SqlJsonQueryRecursiveChildTest;
import com.hazelcast.jet.sql.tests.json.query.SqlJsonQueryWildcardTest;
import com.hazelcast.jet.sql.tests.json.query.SqlJsonQueryExpressionTest;
import com.hazelcast.jet.sql.tests.json.value.SqlJsonValueStringTest;

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

            case "sqlJsonQueryDirectNode":
                SqlJsonQueryDirectNodeTest.main(args);
                break;

            case "sqlJsonQueryNodeArray":
                SqlJsonQueryNodeArrayTest.main(args);
                break;

            case "sqlJsonQueryRecursiveChild":
                SqlJsonQueryRecursiveChildTest.main(args);
                break;

            case "sqlJsonQueryWildcard":
                SqlJsonQueryWildcardTest.main(args);
                break;

            case "sqlJsonQueryExpression":
                SqlJsonQueryExpressionTest.main(args);
                break;

            case "sqlJsonObject":
                SqlJsonObjectTest.main(args);
                break;

            case "sqlJsonObjectAgg":
                SqlJsonObjectAggTest.main(args);
                break;

            case "sqlJsonArray":
                SqlJsonArrayTest.main(args);
                break;

            case "sqlJsonArrayAgg":
                SqlJsonArrayAggTest.main(args);
                break;

            case "sqlJsonValueString":
                SqlJsonValueStringTest.main(args);
                break;

            default:
                throw new RuntimeException("unknown test " + testName);
        }
    }
}
