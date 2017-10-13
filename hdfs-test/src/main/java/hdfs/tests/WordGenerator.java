package hdfs.tests;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.util.ExceptionUtil;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * date: 10/13/17
 * author: emindemirci
 */
public class WordGenerator extends AbstractProcessor {

    private final String path;
    private final long distinct;
    private final long total;

    public WordGenerator(String path, long distinct, long total) {
        this.path = path;
        this.distinct = distinct;
        this.total = total;
    }

    @Override
    public boolean complete() {
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            DataOutputStream hdfsFile = fs.create(new Path(path));
            try (OutputStreamWriter stream = new OutputStreamWriter(hdfsFile)) {
                writeToFile(stream, distinct, total);
            }
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

    public static DistributedSupplier<Processor> getSupplier(String inputPath, long distinct, long total) {
        DistributedSupplier<Processor> supplier = new DistributedSupplier<Processor>() {
            @Override
            public Processor get() {
                return new WordGenerator(inputPath, distinct, total);
            }
        };
        return supplier;
    }
}
