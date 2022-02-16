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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;

public final class JsonExtractor {

    private static JsonExtractor instance;

    private JsonExtractor() {
    }

    public static JsonExtractor getInstance() {
        if (instance == null) {
            instance = new JsonExtractor();
        }
        return instance;
    }

    public String getJsonByJsonPath(String jsonStructureString, String jsonPath, Boolean isArray) {
        Configuration conf = Configuration.builder().jsonProvider(new GsonJsonProvider()).build();
        if (isArray) {
            JsonArray jsonArray = JsonPath.using(conf).parse(jsonStructureString).read(jsonPath);
            return jsonArray.toString();
        } else {
            JsonObject jsonObject = JsonPath.using(conf).parse(jsonStructureString).read(jsonPath);
            return jsonObject.toString();
        }
    }

    public String getJsonValueByJsonPath(String jsonStructureString, String jsonPath) {
        Configuration conf = Configuration.builder().jsonProvider(new GsonJsonProvider()).build();
        JsonPrimitive jsonPrimitive = JsonPath.using(conf).parse(jsonStructureString).read(jsonPath);
        String result = jsonPrimitive.toString();
        return result.replace("\"", "");
    }
}
