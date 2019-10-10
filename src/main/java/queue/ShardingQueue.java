package queue;

import queue.processor.DocumentProcessor;
import queue.sharding.PerShardQueue;
import queue.sharding.ShardingFunction;

import java.util.ArrayList;
import java.util.List;

public class ShardingQueue {
    private ShardingFunction shardingFunction;
    private List<PerShardQueue> queues;


    public ShardingQueue(ShardingFunction shardingFunction, int batchSize, DocumentProcessor documentProcessor) {
        this.shardingFunction = shardingFunction;
        queues = new ArrayList<>(shardingFunction.getShardCount());
        for (int i = 0; i < queues.size(); i++)
            queues.add(new PerShardQueue(batchSize, documentProcessor));
    }

    public void addDocument(Document d) {
        queues.get(shardingFunction.getShard(d)).addDocument(d);
    }
}
