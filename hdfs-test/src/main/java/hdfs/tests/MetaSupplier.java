package hdfs.tests;

import com.hazelcast.core.Member;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.nio.Address;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MetaSupplier implements ProcessorMetaSupplier {

    private final DistributedBiFunction<String, Integer, Processor> processorF;

    private transient Map<Address, String> addressUuidMap;

    MetaSupplier(DistributedBiFunction<String, Integer, Processor> processorF) {
        this.processorF = processorF;
    }

    @Override
    public void init(@Nonnull Context context) {
        addressUuidMap = context.jetInstance().getCluster().getMembers()
                                .stream()
                                .collect(Collectors.toMap(Member::getAddress, Member::getUuid));
    }

    @Nonnull
    @Override
    public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
        return address -> processorSupplier(processorF, addressUuidMap.get(address), addresses.size());
    }

    private static ProcessorSupplier processorSupplier(BiFunction<String, Integer, Processor> processorF,
                                                       String path, int total) {
        return count -> IntStream
                .range(0, count)
                .mapToObj(i -> i == 0 ? processorF.apply(path, total) : Processors.noopP().get())
                .collect(Collectors.toList());
    }
}