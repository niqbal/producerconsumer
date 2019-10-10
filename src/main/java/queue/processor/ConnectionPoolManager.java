package queue.processor;

import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

public class ConnectionPoolManager {
    public static PoolingHttpClientConnectionManager createConnectionPoolManager() {
        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setMaxTotal(1000);
        connManager.setDefaultMaxPerRoute(10);
        return connManager;
    }
}
