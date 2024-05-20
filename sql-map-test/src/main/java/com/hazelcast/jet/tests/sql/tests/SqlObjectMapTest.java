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

package com.hazelcast.jet.tests.sql.tests;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;

public class SqlObjectMapTest extends AbstractSqlMapTest {

    public static final String OBJECT_MAP_NAME = "object_sql_map";

    public SqlObjectMapTest(String mapName, boolean isIndexed) {
        super(mapName, isIndexed);
    }

    public static void main(String[] args) throws Exception {
        new SqlObjectMapTest(OBJECT_MAP_NAME, false).run(args);
    }

    @Override
    protected void init(HazelcastInstance client) {
        super.client = client;
        setInMemoryFormat(InMemoryFormat.OBJECT);
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
