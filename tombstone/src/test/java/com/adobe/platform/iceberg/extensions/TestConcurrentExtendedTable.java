package com.adobe.platform.iceberg.extensions;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static java.util.stream.Collectors.toList;

public class TestConcurrentExtendedTable extends WithSpark implements WithExecutorService {

  @Override
  public void implicitTable(ExtendedTables tables, String tableLocation) {
    // Override Iceberg table schema test provisioning with explicit schema, spec and retry conf for concurrency tests
    tables.create(SimpleRecord.SCHEMA, SimpleRecord.SPEC, tableLocation,
        ImmutableMap.of(
            "commit.retry.num-retries", "100",
            "commit.retry.min-wait-ms", "3000", // 3 seconds
            "commit.retry.max-wait-ms", "10000", // 10 seconds
            "commit.retry.total-timeout-ms", "180000")); // 3 minutes
  }

  /**
   * The test will generate 100 callable commit operations with all tombstones available from 0 to 99.
   * All commits will be executed on a fixed thread pool of 2 (two) threads.
   * So initially one of the two threads will block for a retry for 5 seconds due to concurrent access.
   * Meanwhile all the other commits should execute sequentially on the non-blocked thread, a lot faster than 5 seconds.
   * When the retry kicks in the library should have the commit added to the tombstones list of the current snapshot
   * and not to that initial snapshot against which the first commit operation was evaluated and had to be retried.
   * We should evaluate that all 100 tombstones have made it to the store on the last current snapshot.
   *
   * @throws InterruptedException in case commit threads are interrupted.
   */
  @Test
  public void testConcurrentCommitsYieldConsistentResults() throws InterruptedException {
    ExtendedTable table = TABLES.loadWithTombstoneExtension(getTableLocation());
    Types.NestedField field = table.schema().findField("batch");

    // This will generate 100 callable commit operations with all tombstones available from 0 to 100
    List<Callable<String>> commits = IntStream.range(0, 100)
        .mapToObj(i -> addCommit(table, field, String.valueOf(i))).collect(toList());

    // All commits will be executed on a fixed thread pool of two threads
    ExecutorService executorService = Executors.newFixedThreadPool(2);

    try {
      executorService.invokeAll(commits, 30, TimeUnit.SECONDS);
    } finally {
      shutdownAndAwaitTermination(executorService);
    }

    int tombstonesCount = table.getSnapshotTombstones(field, table.currentSnapshot()).size();
    Assert.assertEquals("Expect 100 tombstones", 100, tombstonesCount);
  }

  /**
   * The test will generate 200 callable commit operations with all tombstones available from 0 to 199.
   * All commits will be executed on a fixed thread pool 20 (twenty) threads as fast as possible.
   * We should evaluate that all 200 tombstones have made it to the store on the last current snapshot.
   *
   * @throws InterruptedException in case commit threads are interrupted.
   */
  @Test
  public void testConcurrentCommitsYieldConsistentResultsWithMultipleProducers() throws InterruptedException {
    ExtendedTable table = TABLES.loadWithTombstoneExtension(getTableLocation());
    Types.NestedField field = table.schema().findField("batch");

    // This will generate 100 callable commit operations with all tombstones available from 0 to 100
    List<Callable<String>> commits = IntStream.range(0, 200)
        .mapToObj(i -> addCommit(table, field, String.valueOf(i))).collect(toList());

    // All commits will be executed on a fixed thread pool of two threads
    ExecutorService executorService = Executors.newFixedThreadPool(20);

    try {
      executorService.invokeAll(commits, 120, TimeUnit.SECONDS);
    } finally {
      shutdownAndAwaitTermination(executorService);
    }

    int tombstonesCount = table.getSnapshotTombstones(field, table.currentSnapshot()).size();
    Assert.assertEquals("Expect 200 tombstones", 200, tombstonesCount);
  }

  private Callable<String> addCommit(ExtendedTable table, Types.NestedField field, String tombstone) {
    return () -> {
      table.newAppendWithTombstonesAdd(field, Lists.newArrayList(() -> tombstone), Collections.emptyMap()).commit();
      return tombstone;
    };
  }
}
