package dev.ikm.ds.rocks;

import dev.ikm.tinkar.common.util.thread.SubtaskFailedException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The scans' fan-out is bounded (IKE-Network/ike-issues#1257). */
class BoundedForksTest {

    @Test
    void runsEveryTaskAndAtMostTheParallelismAtOnce() {
        List<Integer> items = IntStream.range(0, 200).boxed().toList();
        AtomicInteger running = new AtomicInteger();
        AtomicInteger highWater = new AtomicInteger();
        Set<Integer> done = ConcurrentHashMap.newKeySet();

        BoundedForks.forkAll(items, 4, item -> {
            highWater.accumulateAndGet(running.incrementAndGet(), Math::max);
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            done.add(item);
            running.decrementAndGet();
        });

        assertEquals(200, done.size(), "every item ran once");
        assertTrue(highWater.get() <= 4, highWater.get() + " tasks ran at once");
        assertTrue(highWater.get() > 1, "the tasks did not run in parallel");
    }

    @Test
    void aFailingTaskStopsTheRestAndIsRethrown() {
        List<Integer> items = IntStream.range(0, 10_000).boxed().toList();
        AtomicInteger started = new AtomicInteger();

        SubtaskFailedException failure = assertThrows(SubtaskFailedException.class,
                () -> BoundedForks.forkAll(items, 2, item -> {
                    started.incrementAndGet();
                    if (item == 3) {
                        throw new IllegalStateException("item 3 failed");
                    }
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }));

        assertTrue(failure.getCause() instanceof IllegalStateException, "the task's exception is the cause");
        assertTrue(started.get() < 10_000, started.get() + " items started after the failure");
    }

    @Test
    void aParallelismBelowOneRunsOneAtATime() {
        AtomicInteger running = new AtomicInteger();
        AtomicInteger highWater = new AtomicInteger();
        BoundedForks.forkAll(IntStream.range(0, 20).boxed().toList(), 0, item -> {
            highWater.accumulateAndGet(running.incrementAndGet(), Math::max);
            running.decrementAndGet();
        });
        assertEquals(1, highWater.get());
    }
}
