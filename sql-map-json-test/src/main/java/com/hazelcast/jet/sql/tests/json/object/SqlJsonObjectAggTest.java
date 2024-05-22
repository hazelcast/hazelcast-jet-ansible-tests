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


package com.hazelcast.jet.sql.tests.json.object;

import com.google.gson.JsonObject;
import com.hazelcast.jet.sql.tests.json.AbstractJsonOutputMapTest;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;

import java.util.ArrayList;

public class SqlJsonObjectAggTest extends AbstractJsonOutputMapTest {

    private static final String JSON_MAP_NAME = "json_objectagg_map";
    private static final String SQL_QUERY = "SELECT JSON_OBJECTAGG(lastName : id) " +
            "FROM " + JSON_MAP_NAME;
    private static final boolean RESULT_REQUIRED_SORT = true;

    public SqlJsonObjectAggTest(String mapName, String sqlQuery, Boolean resultRequiredSort) {
        super(mapName, sqlQuery, resultRequiredSort);
    }

    public static void main(String[] args) throws Exception {
        new SqlJsonObjectAggTest(JSON_MAP_NAME, SQL_QUERY, RESULT_REQUIRED_SORT).run(args);
    }

    @Override
    protected ArrayList<String> getExpectedJsonResult() {
        ArrayList<String> jsonResults = new ArrayList<>();

        String query = "SELECT id, lastName FROM " + JSON_MAP_NAME;
        JsonObject obj = new JsonObject();
        try (SqlResult result = client.getSql().execute(query)) {
            for (SqlRow row : result) {
                obj.addProperty(row.getObject("lastName"), (Integer) row.getObject("id"));
            }
        }
        jsonResults.add(obj.toString());

        return jsonResults;
    }
}
