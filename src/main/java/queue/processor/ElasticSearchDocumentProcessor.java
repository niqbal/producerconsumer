package queue.processor;

import org.apache.http.client.HttpClient;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.HttpClientBuilder;
import queue.Document;

import java.util.List;

public class ElasticSearchDocumentProcessor implements DocumentProcessor {
    HttpClient client;

    ElasticSearchDocumentProcessor(HttpClientConnectionManager connManager) {
        client = HttpClientBuilder.create().setConnectionManager(connManager).build();
    }

    @Override
    public Runnable process(List<Document> batch) {
        return new DocumentBatchProcessor(batch, client);
    }

    public static class DocumentBatchProcessor implements Runnable {
        private List<Document> batch;
        private HttpClient client;

        DocumentBatchProcessor(List<Document> batch, HttpClient client) {
            this.batch = batch;
            this.client = client;
        }

        @Override
        public void run() {
            StringBuilder totalLoad = new StringBuilder();
            for(Document d: batch)
                totalLoad.append(d.createDocumentJson());

            String requestBody = totalLoad.toString();

//            client.execute(requestBody);
        }
    }
}
