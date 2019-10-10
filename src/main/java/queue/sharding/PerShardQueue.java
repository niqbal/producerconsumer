package queue.sharding;

import queue.Document;
import queue.processor.DocumentProcessor;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class PerShardQueue {
    private List<Document> documents = new LinkedList<>();
    private int blockingLimit = 100;

    private int workerCount = 10;

    private ThreadPoolExecutor workerPool = new ThreadPoolExecutor(workerCount,
            workerCount,
            0,
            TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(blockingLimit));

    private DocumentProcessor documentProcessor;
    private int batchSize;


    public PerShardQueue(int batchSize, DocumentProcessor documentProcessor) {
        this.batchSize = batchSize;
        this.documentProcessor = documentProcessor;
    }

    public void addDocument(Document d) {
        documents.add(d);

        if (documents.size() == batchSize) {
            workerPool.submit(documentProcessor.process(documents));
        }
        documents = new LinkedList<>();
    }


}
