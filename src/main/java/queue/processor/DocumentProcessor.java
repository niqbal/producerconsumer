package queue.processor;

import queue.Document;

import java.util.List;

public interface DocumentProcessor {
    Runnable process(List<Document> batch);
}
