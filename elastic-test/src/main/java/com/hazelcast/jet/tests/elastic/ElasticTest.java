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

package com.hazelcast.jet.tests.elastic;

import com.hazelcast.collection.IList;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.elastic.ElasticSinks;
import com.hazelcast.jet.elastic.ElasticSources;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.tests.common.AbstractSoakTest;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RestClient;

import static com.hazelcast.jet.tests.common.Util.sleepSeconds;
import static java.util.stream.Collectors.toList;

public class ElasticTest extends AbstractSoakTest {

    private static final String SINK_LIST_NAME = ElasticTest.class.getSimpleName() + "_listSink";
    private static final int DEFAULT_ITEM_COUNT = 10_000;
    private static final int LOG_JOB_COUNT_THRESHOLD = 100;
    private static final int SLEEP_BETWEEN_TABLE_READS_SECONDS = 5;

    private String elasticIp;
    private int itemCount;
    private List<Integer> inputItems;

    public static void main(String[] args) throws Exception {
        new ElasticTest().run(args);
    }

    @Override
    public void init(JetInstance client) throws SQLException {
        elasticIp = property("elasticIp", "localhost");
        itemCount = propertyInt("itemCount", DEFAULT_ITEM_COUNT);
        inputItems = IntStream.range(0, itemCount).boxed().collect(toList());
    }

    @Override
    public void test(JetInstance client, String name) throws Exception {
        logger.info("Elastic ip: " + elasticIp);
        clearSinkMap(client);
        int jobCounter = 0;
        long begin = System.currentTimeMillis();
        while (System.currentTimeMillis() - begin < durationInMillis) {
            clearSinkMap(client);

            int indexCounter = jobCounter;

            executeWriteToElasticPipeline(client, indexCounter);
            executeReadFromElasticPipeline(client, indexCounter);
            assertResults(client, jobCounter);

            clearSinkMap(client);

            executeDeleteFromElasticPipeline(client, jobCounter);
            executeReadFromElasticPipeline(client, indexCounter);

            assertEmptyResults(client);

            if (jobCounter % LOG_JOB_COUNT_THRESHOLD == 0) {
                logger.info("Job count: " + jobCounter);
            }

            jobCounter++;
            sleepSeconds(SLEEP_BETWEEN_TABLE_READS_SECONDS);
        }
    }

    private void executeWriteToElasticPipeline(JetInstance client, int indexCounter) {
        final String ip = elasticIp;
        Sink<Integer> elasticSink = ElasticSinks.builder()
                .clientFn(() -> RestClient.builder(new HttpHost(ip, 9200, "http")))
                .<Integer>mapToRequestFn(
                        t -> new IndexRequest("elastictest-index" + indexCounter)
                                .id(Integer.toString(t))
                                .source(asMap(indexCounter, t)))
                .bulkRequestFn(() -> new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE))
                .build();

        Pipeline toElastic = Pipeline.create();
        toElastic.readFrom(TestSources.items(inputItems))
                .rebalance()
                .writeTo(elasticSink);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("ElasticTest_writeTo_" + indexCounter);
        client.newJob(toElastic, jobConfig).join();
    }

    private void executeReadFromElasticPipeline(JetInstance client, int indexCounter) {
        final String ip = elasticIp;
        BatchSource elasticSource = ElasticSources.elastic(
                () -> RestClient.builder(new HttpHost(ip, 9200, "http")),
                () -> new SearchRequest("elastictest-index" + indexCounter),
                hit -> {
                    Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                    return sourceAsMap.get("id") + "_" + sourceAsMap.get("record");
                });

        Pipeline fromElastic = Pipeline.create();
        fromElastic.readFrom(elasticSource)
                .writeTo(Sinks.list(SINK_LIST_NAME));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("ElasticTest_readFrom_" + indexCounter);
        client.newJob(fromElastic, jobConfig).join();
    }

    private void executeDeleteFromElasticPipeline(JetInstance client, int indexCounter) {
        final String ip = elasticIp;
        Sink<Integer> elasticSink = ElasticSinks.builder()
                .clientFn(() -> RestClient.builder(new HttpHost(ip, 9200, "http")))
                .<Integer>mapToRequestFn(
                        t -> new DeleteRequest("elastictest-index" + indexCounter, Integer.toString(t)))
                .bulkRequestFn(() -> new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE))
                .build();

        Pipeline toElastic = Pipeline.create();
        toElastic.readFrom(TestSources.items(inputItems))
                .rebalance()
                .writeTo(elasticSink);

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("ElasticTest_deleteFrom_" + indexCounter);
        client.newJob(toElastic, jobConfig).join();
    }

    private void assertResults(JetInstance client, int indexCounter) {
        IList<String> list = client.getList(SINK_LIST_NAME);
        assertEquals(itemCount, list.size());
        List<String> copiedList = new ArrayList<>(list);
        Collections.sort(copiedList);
        List<String> expectedList = prepareExpectedList(indexCounter);
        assertEquals(expectedList, copiedList);
    }

    private List<String> prepareExpectedList(int indexCounter) {
        List<String> list = inputItems.stream()
                .map(t -> {
                    String parsed = Integer.toString(t);
                    return parsed + "_index-" + indexCounter + "_" + parsed;
                })
                .collect(toList());
        Collections.sort(list);
        return list;
    }

    private void assertEmptyResults(JetInstance client) {
        IList<String> list = client.getList(SINK_LIST_NAME);
        assertTrue(list.isEmpty());
    }

    private void clearSinkMap(JetInstance client) {
        client.getList(SINK_LIST_NAME).clear();
    }

    private static Map<String, Object> asMap(int indexCounter, int i) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", i);
        map.put("record", "index-" + indexCounter + "_" + i);
        return map;
    }

    @Override
    protected void teardown(Throwable t) throws Exception {
    }

}
