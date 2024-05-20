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

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;


public abstract class AbstractJsonInputMapTest extends AbstractJsonMapTest {

    private static final String JSON_DATA_PATH_DEFAULT = "/home/ec2-user/ansible/dataFile.json";
    protected final ArrayList<String> expectedJsonResults = new ArrayList<>();
    protected String inputJsonFile = property("jsonDataFilePath", JSON_DATA_PATH_DEFAULT);
    protected String jsonInputString;

    public AbstractJsonInputMapTest(String mapName, String sqlQuery, String jsonPath, Boolean resultRequiredSort)
            throws IOException {

        super(mapName, sqlQuery, resultRequiredSort);
        this.jsonInputString = readJsonFromFile(inputJsonFile);
        this.expectedJsonResults.add(retrieveExpectedJsonStructure(jsonInputString, jsonPath));
    }

    @Override
    protected ArrayList<String> getExpectedJsonResult() {
        return expectedJsonResults;
    }

    protected String retrieveExpectedJsonStructure(String jsonInputString, String jsonPath) {
        return JsonExtractor.getJsonByJsonPath(jsonInputString, jsonPath);
    }

    protected String readJsonFromFile(String fileName) throws IOException {
        Path inputJsonDataFilePath = Paths.get(fileName);
        assertTrue("Json data input file does not exist", Files.exists(inputJsonDataFilePath));
        try (Stream<String> lines = Files.lines(inputJsonDataFilePath)) {
            jsonInputString = lines.collect(Collectors.joining("\n"));
        }
        assertThat(jsonInputString.length(), is(not(0)));
        return jsonInputString;
    }

    @Override
    protected void createTable() {

        String sqlCreateMappingQueryString = "CREATE OR REPLACE MAPPING " + mapName +
                " TYPE " + IMapSqlConnector.TYPE_NAME + "\n" +
                "OPTIONS (\n" +
                '\'' + OPTION_KEY_FORMAT + "'='bigint'\n" + "," +
                '\'' + OPTION_VALUE_FORMAT + "'='json'\n" +
                ")";

        try (SqlResult result = client.getSql().execute(sqlCreateMappingQueryString)) {
            assertEquals(result.updateCount(), 0L);
        }
    }

    @Override
    protected void populateMap() {
        IMap<Long, HazelcastJsonValue> map = client.getMap(mapName);
        map.put(1L, new HazelcastJsonValue(jsonInputString));
        assertThat(map.get(1L), is(notNullValue()));
    }
}
