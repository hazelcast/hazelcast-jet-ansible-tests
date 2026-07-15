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

package com.hazelcast.jet.tests.vector;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.tests.common.AbstractJetSoakTest;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.tests.common.Util.sleepMillis;

public class VectorCollectionTest extends AbstractJetSoakTest {

    private static final String CLASS_NAME = VectorCollectionTest.class.getSimpleName();
    private static final String INDEX_NAME = CLASS_NAME + "-INDEX";
    private static final String VECTOR_COLLECTION_NAME = CLASS_NAME + "-VECTOR_COLLECTION";

    private static final String POSTFIX_ADDED = "-Added";
    private static final String POSTFIX_UPDATED = "-Updated";

    private static final int DEFAULT_DIMENSION = 384;
    private static final int DEFAULT_MAX_DEGREE = 30;
    private static final int DEFAULT_EF_CONSTRUCTION = 100;
    private static final int DEFAULT_OPERATION_TIMEOUT = 10;
    private static final int DEFAULT_OPTIMIZE_TIMEOUT = 120;
    private static final int DEFAULT_OPTIMIZE_THRESHOLD = 1000;
    private static final int DEFAULT_SEARCH_THRESHOLD = 1500;
    private static final int DEFAULT_CLEAR_THRESHOLD = 7000;
    private static final int DEFAULT_SEARCH_LIMIT = 50;

    private int dimension;
    private int maxDegree;
    private int efConstruction;
    private boolean useDeduplication = true;
    private int operationTimeoutSeconds;
    private int optimizeTimeoutSeconds;
    private int optimizeIntervalThreshold;
    private int searchIntervalThreshold;
    private int clearIntervalThreshold;
    private int searchLimit;

    private transient HazelcastInstance remoteClient;

    public static void main(String[] args) throws Exception {
        new VectorCollectionTest().run(args);
    }

    @Override
    protected void init(HazelcastInstance client) throws Exception {
        dimension = propertyInt("dimension", DEFAULT_DIMENSION);
        maxDegree = propertyInt("maxDegree", DEFAULT_MAX_DEGREE);
        efConstruction = propertyInt("efConstruction", DEFAULT_EF_CONSTRUCTION);
        useDeduplication = propertyBoolean("useDeduplication", true);
        operationTimeoutSeconds = propertyInt("operationTimeoutSeconds", DEFAULT_OPERATION_TIMEOUT);
        optimizeTimeoutSeconds = propertyInt("optimizeTimeoutSeconds", DEFAULT_OPTIMIZE_TIMEOUT);
        optimizeIntervalThreshold = propertyInt("optimizeIntervalThreshold", DEFAULT_OPTIMIZE_THRESHOLD);
        searchIntervalThreshold = propertyInt("searchIntervalThreshold", DEFAULT_SEARCH_THRESHOLD);
        clearIntervalThreshold = propertyInt("clearIntervalThreshold", DEFAULT_CLEAR_THRESHOLD);
        searchLimit = propertyInt("searchLimit", DEFAULT_SEARCH_LIMIT);
    }

    @Override
    protected void test(HazelcastInstance client, String name) throws Throwable {
        remoteClient = HazelcastClient.newHazelcastClient(remoteClusterClientConfig());
        VectorIndexConfig indexConfig = new VectorIndexConfig();
        indexConfig.setName(INDEX_NAME);
        indexConfig.setDimension(dimension);
        indexConfig.setMetric(Metric.COSINE);
        indexConfig.setMaxDegree(maxDegree);
        indexConfig.setEfConstruction(efConstruction);
        indexConfig.setUseDeduplication(useDeduplication);

        VectorCollectionConfig vectorCollectionConfig = new VectorCollectionConfig();
        vectorCollectionConfig.setName(VECTOR_COLLECTION_NAME);
        vectorCollectionConfig.setVectorIndexConfigs(List.of(indexConfig));

        remoteClient.getConfig().addVectorCollectionConfig(vectorCollectionConfig);

        VectorCollection<Integer, String> vectorCollection = remoteClient.getVectorCollection(VECTOR_COLLECTION_NAME);

        int clearCounter = 0;
        int totalCounter = 0;

        SearchOptions searchOptions = SearchOptions.builder()
                .includeValue()
                .limit(searchLimit).build();

        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            //add + update + optimize + search + clear
            addItemToVectorCollection(vectorCollection, totalCounter);
            updateItemInVectorCollection(vectorCollection, totalCounter);

            if (clearCounter % 1000 == 0) {
                logger.info(String.format("Added %d items in vector collection", totalCounter));
            }

            if (clearCounter % optimizeIntervalThreshold == 0) {
                optimizeVectorCollection(vectorCollection);
            }

            if (clearCounter % searchIntervalThreshold == 0 && clearCounter > searchLimit) {
                performSearchOnVectorCollection(vectorCollection, searchOptions);
            }

            clearCounter++;
            totalCounter++;
            if (clearCounter % clearIntervalThreshold == 0) {
                clearVectorCollection(vectorCollection);
                clearCounter = 0;
            }
            sleepMillis(2);
        }
        clearVectorCollection(vectorCollection);

    }

    @Override
    protected void teardown(Throwable t)  {
        if (remoteClient != null) {
            remoteClient.shutdown();
        }
    }

    private void clearVectorCollection(VectorCollection<Integer, String> vectorCollection) {
        vectorCollection.clearAsync().toCompletableFuture().orTimeout(10, TimeUnit.SECONDS).join();
    }

    private void updateItemInVectorCollection(VectorCollection<Integer, String> vectorCollection, int item) {
        VectorValues value = VectorValues.of(randomFloatsVector());
        VectorDocument<String> vd = VectorDocument.of(item + POSTFIX_UPDATED, value);
        vectorCollection.setAsync(item, vd).toCompletableFuture()
                .orTimeout(operationTimeoutSeconds, TimeUnit.SECONDS).join();
    }

    private void addItemToVectorCollection(VectorCollection<Integer, String> vectorCollection, int item) {
        VectorValues value = VectorValues.of(randomFloatsVector());
        VectorDocument<String> vd = VectorDocument.of(item + POSTFIX_ADDED, value);
        vectorCollection.putAsync(item, vd).toCompletableFuture()
                .orTimeout(operationTimeoutSeconds, TimeUnit.SECONDS).join();
    }

    private void optimizeVectorCollection(VectorCollection<Integer, String> vectorCollection) {
        vectorCollection.optimizeAsync().toCompletableFuture().orTimeout(optimizeTimeoutSeconds, TimeUnit.SECONDS).join();
    }

    private void performSearchOnVectorCollection(VectorCollection<Integer, String> vectorCollection,
                                                 SearchOptions searchOptions) {
        SearchResults<Integer, String> results = vectorCollection
                .searchAsync(
                        VectorValues.of(randomFloatsVector()),
                        searchOptions)
                .toCompletableFuture()
                .orTimeout(operationTimeoutSeconds, TimeUnit.SECONDS).join();
        //verify results count and all items updated
        assertNotNull(results);
        assertEquals(searchOptions.getLimit(), results.size());
        results.results().forEachRemaining(
                result -> assertEquals(result.getKey() + POSTFIX_UPDATED, result.getValue()));
    }

    private float[] randomFloatsVector() {
        float[] vector = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            vector[i] = ThreadLocalRandom.current().nextFloat();
        }
        return vector;
    }
}
