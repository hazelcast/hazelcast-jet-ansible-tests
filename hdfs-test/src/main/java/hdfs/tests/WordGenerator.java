package hdfs.tests;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.util.ExceptionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class WordGenerator extends AbstractProcessor {

    private final String path;
    private final long distinct;
    private final long total;

    private WordGenerator(String path, long distinct, long total) {
        this.path = path;
        this.distinct = distinct;
        this.total = total;
    }

    public static ProcessorMetaSupplier getSupplier(String inputPath, long distinct, long total) {
        return new MetaSupplier((path, memberSize) ->
                new WordGenerator(inputPath + "/" + path, distinct, total / memberSize));
    }

    @Override
    public boolean complete() {
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            Path p = new Path(path);
            DataOutputStream hdfsFile = fs.create(p);
            try (OutputStreamWriter stream = new OutputStreamWriter(hdfsFile)) {
                writeToFile(stream, distinct, total);
            }
            hdfsFile.close();
            return tryEmit("done!");
        } catch (IOException e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    private void writeToFile(OutputStreamWriter stream, long distinctWords, long numWords) throws IOException {
        for (long i = 0; i < numWords; i++) {
            stream.write(i % distinctWords + "");
            if (i % 20 == 0) {
                stream.write("\n");
            } else {
                stream.write(" ");
            }
        }
        stream.write("\n");
    }

//    public static class MetaSupplier implements ProcessorMetaSupplier {
//
//        private final String path;
//        private final long distinct;
//        private final long total;
//
//        private transient Map<Address, String> addressUuidMap;
//
//        MetaSupplier(String path, long distinct, long total) {
//            this.path = path;
//            this.distinct = distinct;
//            this.total = total;
//        }
//
//        @Override
//        public void init(@Nonnull Context context) {
//            addressUuidMap = context.jetInstance().getCluster().getMembers()
//                                    .stream()
//                                    .collect(Collectors.toMap(Member::getAddress, Member::getUuid));
//        }
//
//        @Nonnull
//        @Override
//        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
//            return address -> processorSupplier(path + "/" + addressUuidMap.get(address),
//                    distinct, total / addresses.size());
//        }
//
//        private static ProcessorSupplier processorSupplier(String inputPath, long distinct, long total) {
//            return count -> IntStream
//                    .range(0, count)
//                    .mapToObj(i -> i == 0 ? new WordGenerator(inputPath, distinct, total) : Processors.noopP().get())
//                    .collect(Collectors.toList());
//        }
//    }
}
