package com.adobe.platform.iceberg.extensions;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public interface WithExecutorService {

  default void shutdownAndAwaitTermination(ExecutorService pool) {
    pool.shutdown(); // Disable new tasks from being submitted
    try {
      if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
        pool.shutdownNow(); // Cancel currently executing tasks
        if (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
          System.err.println("Pool did not terminate");
        }
      }
    } catch (InterruptedException ie) {
      pool.shutdownNow(); // (Re-)Cancel if current thread also interrupted
      Thread.currentThread().interrupt(); // Preserve interrupt status
    }
  }
}
