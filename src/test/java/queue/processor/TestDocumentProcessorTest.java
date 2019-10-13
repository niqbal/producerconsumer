package queue.processor;

import org.junit.Test;
import queue.Document;
import queue.ShardingQueue;
import queue.sharding.ElasticSearchShardingFunction;

import java.util.List;

class TestDocumentProcessorTest {

    public static class DocumentFake implements Document {
        int a;

        DocumentFake(int a) {
            this.a = a;
        }

        @Override
        public String getShardingKey() {
            return "" + a;
        }

        @Override
        public String createDocumentJson() {
            return "" + a;
        }

        @Override
        public String toString() {
            return "DoucmentFake{a=" + a + '}';
        }
    }

    public static class DocumentProcessorFake implements DocumentProcessor {

        public Runnable process(final List<Document> batch) {
            return () -> System.out.println(batch.toString());
        }
    }

    @Test
    void testProcess() {

        ShardingQueue q = new ShardingQueue(new ElasticSearchShardingFunction(3), 5, new DocumentProcessorFake());

        for (int i = 0; i < 100; i++) {
            q.addDocument(new DocumentFake(i));
        }
        q.awaitTermination(2);
    }

    public static void main(String [] args) {
        TestDocumentProcessorTest d = new TestDocumentProcessorTest();
        d.testProcess();

    }
}