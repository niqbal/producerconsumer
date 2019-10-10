package queue.sharding;

import queue.Document;

public class SolrShardingFunction32 implements ShardingFunction {
    private int totalShards;

    SolrShardingFunction32(int totalShards) {
        this.totalShards = totalShards;
    }

    public int getShardCount() {
        return totalShards;
    }

    public int getShard(Document d) {
        return 0;
    }
}
