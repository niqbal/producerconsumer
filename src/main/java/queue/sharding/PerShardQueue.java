package queue.sharding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import queue.Document;
import queue.ShardingQueue;
import queue.processor.DocumentProcessor;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class PerShardQueue {
    private static Logger LOGGER = LoggerFactory.getLogger(ShardingQueue.class);

    private final int shardId;
    private List<Document> documents = new LinkedList<>();
    private int blockingLimit = 100;

    private int workerCount = 10;

    private BoundedExecutor executor;


    private DocumentProcessor documentProcessor;
    private int batchSize;


    public PerShardQueue(int shardId, int batchSize, DocumentProcessor documentProcessor) {
        this.shardId = shardId;
        this.batchSize = batchSize;
        this.documentProcessor = documentProcessor;

        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(workerCount,
                workerCount,
                0,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>());

        this.executor = new BoundedExecutor(threadPoolExecutor, blockingLimit + batchSize * workerCount);

    }

    public void addDocument(Document d)  {
        documents.add(d);

        if (documents.size() == batchSize) {
            LOGGER.warn(String.format("Sending batch:%s from shard:%s", documents, shardId));
            try {
                executor.submit(documentProcessor.process(documents));
            } catch (InterruptedException e) {
                LOGGER.warn("Failed in submitting document", e);
            }

            documents = new LinkedList<>();
        }
    }


    public void awaitTermination(int seconds) {
            if (documents.size() > 0) {
                try {
                    executor.submit(documentProcessor.process(documents));
                } catch (InterruptedException e) {
                    LOGGER.warn("Failed in awaitTermination", e);
                }
            }

            executor.awaitTermination(seconds);
    }

}
