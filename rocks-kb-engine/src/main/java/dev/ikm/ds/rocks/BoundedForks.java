package dev.ikm.ds.rocks;

import dev.ikm.tinkar.common.util.thread.StructuredScopes;
import dev.ikm.tinkar.common.util.thread.SubtaskFailedException;

import java.util.concurrent.Semaphore;
import java.util.concurrent.StructuredTaskScope;
import java.util.function.Consumer;

/**
 * Runs one task per item in a structured scope, with at most {@code parallelism} of them in
 * flight at once. A scan's task holds a RocksDB snapshot and a native iterator while it runs, so
 * the number running at once is what bounds those resources; forking every task at once did not
 * (IKE-Network/ike-issues#1257; C2 and G3 of the rocks-kb reviews).
 *
 * <p>The tasks run on virtual threads, as the scope forks them. A task that fails cancels the
 * scope, the items not yet started are not started, and the failure is rethrown from
 * {@link #forkAll} as the scope's {@link SubtaskFailedException}.
 */
final class BoundedForks {

    /**
     * How many of a scan's tasks run at once: the system property {@code rocks.scan.parallelism},
     * else the processor count. The tasks are bound by JNI calls, which a virtual thread cannot
     * yield across, so more than one task per processor gains nothing.
     */
    static final int SCAN_PARALLELISM = Math.max(1, Integer.getInteger("rocks.scan.parallelism",
            Runtime.getRuntime().availableProcessors()));

    private BoundedForks() {
    }

    /**
     * Runs {@code task} once for each item, at most {@code parallelism} at a time, and returns
     * when every task has finished.
     *
     * @param items       what to run the task on
     * @param parallelism how many tasks may run at once, at least 1
     * @param task        the task
     * @param <T>         the item type
     * @throws SubtaskFailedException if a task failed, with the task's exception as its cause
     */
    static <T> void forkAll(Iterable<T> items, int parallelism, Consumer<T> task) {
        Semaphore permits = new Semaphore(Math.max(1, parallelism));
        try (StructuredTaskScope<Object, Void, SubtaskFailedException> scope = StructuredScopes.open()) {
            for (T item : items) {
                // A failed task cancels the scope. A task forked after that never runs, so it never
                // returns its permit: stop forking rather than wait for a permit no task will return.
                if (scope.isCancelled()) {
                    break;
                }
                permits.acquire();
                scope.fork(() -> {
                    try {
                        task.accept(item);
                    } finally {
                        permits.release();
                    }
                    return null;
                });
            }
            scope.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while the scan's tasks ran", e);
        }
    }
}
