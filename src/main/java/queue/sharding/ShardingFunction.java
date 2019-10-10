package queue.sharding;

import queue.Document;

public interface ShardingFunction {
    int getShardCount();
    int getShard(Document d);
}
