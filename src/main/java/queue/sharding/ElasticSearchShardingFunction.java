package queue.sharding;

import queue.Document;

public class ElasticSearchShardingFunction implements ShardingFunction {
    private int totalShards;

    public ElasticSearchShardingFunction(int totalShards) {
        this.totalShards = totalShards;
    }

    public int getShardCount() {
        return totalShards;
    }

    public int getShard(Document d) {
        int possiblyNegative = Murmur3HashFunction.hash(d.getShardingKey()) % totalShards;
        return (possiblyNegative + totalShards) % totalShards;
    }
}
