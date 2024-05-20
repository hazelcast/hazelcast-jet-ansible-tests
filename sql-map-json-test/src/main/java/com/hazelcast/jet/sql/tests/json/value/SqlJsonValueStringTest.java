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

package com.hazelcast.jet.sql.tests.json.value;

import com.hazelcast.jet.sql.tests.json.AbstractJsonInputMapTest;
import com.hazelcast.jet.sql.tests.json.JsonExtractor;

import java.io.IOException;

public class SqlJsonValueStringTest extends AbstractJsonInputMapTest {

    private static final String JSON_MAP_NAME = "json_value_string_map";
    private static final String SQL_QUERY = "SELECT JSON_VALUE(this, '$.userRecords[100].about')  FROM  "
            + JSON_MAP_NAME;
    private static final String RESULT_JSON_PATH = "$.userRecords[100].about";
    private static final boolean RESULT_REQUIRED_SORT = false;

    public SqlJsonValueStringTest(String mapName, String sqlQuery, String expectedJsonFile, Boolean resultRequiredSort)
            throws IOException {
        super(mapName, sqlQuery, expectedJsonFile, resultRequiredSort);
    }

    public static void main(String... args) throws Exception {
        new SqlJsonValueStringTest(JSON_MAP_NAME, SQL_QUERY, RESULT_JSON_PATH, RESULT_REQUIRED_SORT).run(args);
    }

    @Override
    protected String retrieveExpectedJsonStructure(String jsonInputString, String jsonPath) {
        return JsonExtractor.getJsonValueByJsonPath(jsonInputString, jsonPath);
    }
}
