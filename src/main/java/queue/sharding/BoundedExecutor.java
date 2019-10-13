package queue.sharding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import queue.ShardingQueue;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * https://stackoverflow.com/a/2001205
 */
public class BoundedExecutor {
    private static Logger LOGGER = LoggerFactory.getLogger(ShardingQueue.class);

    private final ThreadPoolExecutor exec;
    private final Semaphore semaphore;

    public BoundedExecutor(ThreadPoolExecutor exec, int bound) {
        this.exec = exec;
        this.semaphore = new Semaphore(bound);
    }

    public void submit(final Runnable command)
            throws InterruptedException, RejectedExecutionException {
        semaphore.acquire();
        try {
            exec.execute(() -> {
                try {
                    command.run();
                } finally {
                    semaphore.release();
                }
            });
        } catch (RejectedExecutionException e) {
            semaphore.release();
            throw e;
        }
    }

    public void awaitTermination(int seconds) {
        try {
            exec.awaitTermination(seconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.warn("Failed in awaitTermination", e);
        }

        exec.shutdown();
    }
}