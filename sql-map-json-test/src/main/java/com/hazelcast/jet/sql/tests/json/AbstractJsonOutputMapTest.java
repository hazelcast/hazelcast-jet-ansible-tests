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

import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.sql.SqlResult;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_COMPACT_TYPE_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;

public abstract class AbstractJsonOutputMapTest extends AbstractJsonMapTest {


    public AbstractJsonOutputMapTest(String mapName, String sqlQuery, Boolean resultRequiredSort) {
        super(mapName, sqlQuery, resultRequiredSort);
    }

    @Override
    protected void createTable() {

        String sqlCreateMappingQueryString = "CREATE OR REPLACE MAPPING " + mapName +
                " (firstName VARCHAR, lastName VARCHAR, id integer)\n" +
                "TYPE " + IMapSqlConnector.TYPE_NAME + "\n" +
                "OPTIONS (\n" +
                '\'' + OPTION_KEY_FORMAT + "'='integer'\n" + "," +
                '\'' + OPTION_VALUE_FORMAT + "'='compact'\n" + "," +
                '\'' + OPTION_VALUE_COMPACT_TYPE_NAME + "'='" + mapName + "'\n" +
                ")";

        try (SqlResult result = client.getSql().execute(sqlCreateMappingQueryString)) {
            assertEquals(result.updateCount(), 0L);
        }
    }

    @Override
    protected void populateMap() {

        String dml = "INSERT INTO " + mapName
                + " (__key, id, firstName, lastName) "
                + "SELECT v, v, 'first-' || v, 'last-' || v"
                + " FROM TABLE(generate_series(1,100))";

        try (SqlResult result = client.getSql().execute(dml)) {
            assertEquals(result.updateCount(), 0L);
        }
    }
}
