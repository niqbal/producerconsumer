package queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import queue.processor.DocumentProcessor;
import queue.sharding.PerShardQueue;
import queue.sharding.ShardingFunction;

import java.util.ArrayList;
import java.util.List;

public class ShardingQueue {
    private ShardingFunction shardingFunction;
    private List<PerShardQueue> queues;
    private static Logger LOGGER = LoggerFactory.getLogger(ShardingQueue.class);

    public ShardingQueue(ShardingFunction shardingFunction, int batchSize, DocumentProcessor documentProcessor) {
        this.shardingFunction = shardingFunction;
        queues = new ArrayList<>(shardingFunction.getShardCount());
        for (int i = 0; i < shardingFunction.getShardCount(); i++)
            queues.add(new PerShardQueue(i, batchSize, documentProcessor));
    }

    public void addDocument(Document d) {
        int targetShard = shardingFunction.getShard(d);
        LOGGER.warn(String.format("Destination shard %s for document %s", targetShard, d));
        queues.get(targetShard).addDocument(d);
    }

    public void awaitTermination(int seconds) {
        for (PerShardQueue q : queues) {
            q.awaitTermination(seconds);
        }
    }
}
