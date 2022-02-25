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

package com.hazelcast.jet.tests.jquery.tests;

import com.hazelcast.core.HazelcastInstance;

import java.io.IOException;

public class SqlJsonQueryRecursiveChildTest extends AbstractJQueryMapTest {

    private static final String JSON_MAP_NAME = "json_recursive_child_sql_map";
    private static final String SQL_QUERY = "SELECT JSON_QUERY(this, '$..email' WITH WRAPPER) FROM " + JSON_MAP_NAME;
    private static final String RESULT_JSON_PATH = "$..email";
    private static final boolean RESULT_IS_ARRAY = true;
    private static final boolean RESULT_REQUIRED_SORT = false;

    public SqlJsonQueryRecursiveChildTest(String mapName, String sqlQuery, String expectedJsonFile,
                                          Boolean resultIsArray, Boolean resultRequiredSort) throws IOException {
        super(mapName, sqlQuery, expectedJsonFile, resultIsArray, resultRequiredSort);
    }

    public static void main(String... args) throws Exception {
        new SqlJsonQueryRecursiveChildTest(JSON_MAP_NAME, SQL_QUERY, RESULT_JSON_PATH, RESULT_IS_ARRAY,
                RESULT_REQUIRED_SORT).run(args);
    }

    @Override
    protected void init(HazelcastInstance client) {
        super.client = client;
        populateMap();
    }

    @Override
    protected void test(HazelcastInstance client, String name) {
        runTest();
    }

    @Override
    protected void teardown(Throwable t) {
    }
}
