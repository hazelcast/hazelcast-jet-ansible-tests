package com.hazelcast.jet.tests.kinesis;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.pipeline.SinkImpl;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;

import java.util.Collection;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;

import static com.hazelcast.jet.impl.pipeline.SinkImpl.Type.TOTAL_PARALLELISM_ONE;

public class VerificationProcessor extends AbstractProcessor {

    public static final String CONSUMED_MESSAGES_MAP_NAME = "KinesisTest_latestCounters";

    private static final int QUEUE_SIZE_LIMIT = 4_000;
    private static final int PRINT_LOG_ITEMS = 1_000;

    private final String clusterName;

    private boolean active;
    private int counter = 1;
    private final NoDuplicatesPriorityQueue<Integer> queue = new NoDuplicatesPriorityQueue<>();
    private ILogger logger;
    private IMap<String, Integer> map;

    public VerificationProcessor(String clusterName) {
        this.clusterName = clusterName;
    }

    @Override
    protected void init(Context context) {
        logger = context.logger();
        map = context.jetInstance().getMap(CONSUMED_MESSAGES_MAP_NAME);
    }

    @Override
    public boolean tryProcess(int ordinal, Object item) {
        active = true;
        int value = (Integer) item;
        queue.offer(value);
        // try to verify head of verification queue
        for (Integer peeked; (peeked = queue.peek()) != null;) {
            if (peeked > counter) {
                // the item might arrive later
                break;
            } else if (peeked == counter) {
                if (counter % PRINT_LOG_ITEMS == 0) {
                    logger.info(String.format("[%s] Processed correctly item %d", clusterName, counter));
                }
                // correct head of queue
                queue.remove();
                counter++;
                map.put(clusterName, counter);
            } else if (peeked < counter) {
                // duplicate key, ignore
            }
        }
        if (queue.size() >= QUEUE_SIZE_LIMIT) {
            throw new AssertionError(String.format("[%s] Queue size exceeded while waiting for the next "
                            + "item. Limit=%d, expected next=%d, next in queue: %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ...",
                    clusterName, QUEUE_SIZE_LIMIT, counter, queue.poll(), queue.poll(), queue.poll(), queue.poll(),
                    queue.poll(), queue.poll(), queue.poll(), queue.poll(), queue.poll(), queue.poll()));
        }
        return true;
    }

    @Override
    public boolean saveToSnapshot() {
        if (!active) {
            return true;
        }
        logger.info(String.format("[%s] saveToSnapshot counter: %d, size: %d, peek: %d",
                clusterName, counter, queue.size(), queue.peek()));
        return tryEmitToSnapshot(BroadcastKey.broadcastKey(counter), queue);
    }

    @Override
    protected void restoreFromSnapshot(Object key, Object value) {
        counter = (Integer) ((BroadcastKey) key).key();
        queue.addAll((PriorityQueue<Integer>) value);

        logger.info(String.format("[%s] restoreFromSnapshot counter: %d, size: %d, peek: %d",
                clusterName, counter, queue.size(), queue.peek()));
    }

    static Sink<String> sink(String name, String cluster) {
        return new SinkImpl<>(name,
                ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(
                        () -> new VerificationProcessor(cluster)), ""
                ),
                TOTAL_PARALLELISM_ONE);
    }

    private static final class NoDuplicatesPriorityQueue<T> {

        private final PriorityQueue<T> queue = new PriorityQueue<>();
        private final Set<T> set = new HashSet<>();

        public int size() {
            return queue.size();
        }

        public T peek() {
            return queue.peek();
        }

        public void addAll(Collection<? extends T> items) {
            for (T item : items) {
                add(item);
            }
        }

        private void add(T item) {
            if (set.add(item)) {
                queue.add(item);
            }
        }

        public boolean offer(T item) {
            if (set.contains(item)) {
                return true;
            } else {
                boolean added = queue.offer(item);
                if (added) {
                    set.add(item);
                }
                return added;
            }
        }

        public void remove() {
            T item = queue.remove();
            set.remove(item);
        }

        public T poll() {
            return queue.poll();
        }
    }
}
