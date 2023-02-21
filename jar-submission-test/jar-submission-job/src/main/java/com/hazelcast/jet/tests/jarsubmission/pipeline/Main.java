package com.hazelcast.jet.tests.jarsubmission.pipeline;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.tests.jarsubmission.person.Person;
import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        long index = Long.parseLong(args[0]);
        String prefix = args[1];
        boolean batch = Boolean.parseBoolean(args[2]);

        Pipeline pipeline = batch ? batchPipeline(index, prefix) : streamPipeline(index, prefix);
        HazelcastInstance hz = Hazelcast.bootstrappedInstance();
        hz.getJet().newJob(pipeline);
    }

    private static Pipeline batchPipeline(long index, String prefix) {
        List<String> source = new ArrayList<>();
        source.add(prefix + index);
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(source))
                .rebalance()
                .map(t -> new Person(t))
                .rebalance()
                .map(t -> new AnotherPerson(t.getName()))
                .rebalance()
                .map(t -> {
                    if (index % 10 == 0) {
                        throw new RuntimeException();
                    }
                    return t;
                })
                .rebalance()
                .map(t -> Util.entry(prefix, t.getName()))
                .writeTo(Sinks.map(prefix));
        return pipeline;
    }

    private static Pipeline streamPipeline(long index, String prefix) {
        String item = prefix + index;
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemStream(10, (t, u) -> item))
                .withoutTimestamps()
                .rebalance()
                .map(t -> new Person(t))
                .rebalance()
                .map(t -> new AnotherPerson(t.getName()))
                .rebalance()
                .map(t -> {
                    if (index % 10 == 0) {
                        throw new RuntimeException();
                    }
                    return t;
                })
                .rebalance()
                .map(t -> Util.entry(prefix, t.getName()))
                .writeTo(Sinks.map(prefix));
        return pipeline;
    }
}
