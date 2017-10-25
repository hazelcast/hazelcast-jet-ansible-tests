package hdfs.tests;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.util.ExceptionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;

public class WordGenerator extends AbstractProcessor {

    private final String hdfsUri;
    private final String path;
    private final long distinct;
    private final long total;

    private WordGenerator(String hdfsUri, String path, long distinct, long total) {
        this.hdfsUri = hdfsUri;
        this.path = path;
        this.distinct = distinct;
        this.total = total;
    }

    public static ProcessorMetaSupplier getGeneratorSupplier(String hdfsUri, String inputPath, long distinct, long total) {
        return new MetaSupplier((path, memberSize) ->
                new WordGenerator(hdfsUri, inputPath + "/" + path, distinct, total / memberSize));
    }

    @Override
    public boolean complete() {
        URI uri = URI.create(hdfsUri);
        String disableCacheName = String.format("fs.%s.impl.disable.cache", uri.getScheme());
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.setBoolean(disableCacheName, true);
        try (FileSystem fs = FileSystem.get(uri, conf)) {
            Path p = new Path(path);
            try (OutputStreamWriter stream = new OutputStreamWriter(fs.create(p))) {
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
}
