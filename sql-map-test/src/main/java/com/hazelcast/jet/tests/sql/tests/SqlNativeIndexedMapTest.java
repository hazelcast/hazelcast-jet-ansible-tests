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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.jet.JetInstance;

public class SqlNativeIndexedMapTest extends AbstractSqlMapTest {

    private static final String NATIVE_MAP_NAME = "native_sql_map";

    public SqlNativeIndexedMapTest(String mapName, boolean isIndexed) {
        super(mapName, isIndexed);
    }

    public static void main(String[] args) throws Exception {
        new SqlNativeIndexedMapTest(NATIVE_MAP_NAME, true).run(args);
    }

    @Override
    protected void init(JetInstance client) {
        super.client = client;
        setInMemoryFormat(InMemoryFormat.NATIVE);
        populateMap();
    }

    @Override
    protected void test(JetInstance client, String name) {
        runTest();
    }

    @Override
    protected void teardown(Throwable t) {
    }
}
