package dev.ikm.ds.rocks64;

import dev.ikm.tinkar.common.id.Nid;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScannerTest {

    private static Scanner scanner(TestDb db) {
        Counters counters = Counters.load(db.db, db.handle(Rocks64Store.Family.DEFAULT), db.handle(Rocks64Store.Family.ENTITIES));
        return new Scanner(db.db, db.handle(Rocks64Store.Family.ENTITIES), counters);
    }

    private static void fill(TestDb db, int pattern, int elements) {
        for (int element = 1; element <= elements; element++) {
            db.putRecord(Nid.compose64(pattern, element), Records.concept(Nid.compose64(pattern, element), Nid.compose64(3, 1)));
        }
    }

    @Test
    void aPatternsRangesAreContiguousAndCoverItsCounter(@TempDir File dir) {
        try (TestDb db = new TestDb(dir)) {
            fill(db, 5, 999);
            Scanner scanner = scanner(db);
            List<Scanner.Range> ranges = scanner.rangesOf(5);
            assertTrue(ranges.size() >= 1);
            assertArrayEquals(Keys.of(Nid.compose64(5, 1)), ranges.get(0).lower());
            assertArrayEquals(Keys.of(Nid.compose64(5, 1000)), ranges.get(ranges.size() - 1).upper());
            long elements = 0;
            for (int i = 0; i < ranges.size(); i++) {
                elements += ranges.get(i).elements();
                if (i > 0) {
                    assertArrayEquals(ranges.get(i - 1).upper(), ranges.get(i).lower(), "contiguous at " + i);
                }
            }
            assertEquals(999, elements);
            assertEquals(List.of(), scanner.rangesOf(6), "a pattern with no counter has no ranges");
        }
    }

    @Test
    void scansVisitEveryRecordOnceAndReleaseTheirSnapshot(@TempDir File dir) {
        try (TestDb db = new TestDb(dir)) {
            fill(db, 5, 999);
            fill(db, 7, 50);
            Scanner scanner = scanner(db);

            List<Long> inOrder = new ArrayList<>();
            scanner.forEach((record, nid) -> inOrder.add(nid));
            assertEquals(1049, inOrder.size());
            long[] sorted = inOrder.stream().mapToLong(Long::longValue).toArray();
            long[] expected = sorted.clone();
            Arrays.sort(expected);
            assertArrayEquals(expected, sorted, "key order");

            ConcurrentHashMap<Long, AtomicInteger> visits = new ConcurrentHashMap<>();
            scanner.forEachParallel((record, nid) -> {
                assertEquals(nid, dev.ikm.tinkar.common.service.EntityRecordFormat2.nid(record));
                visits.computeIfAbsent(nid, key -> new AtomicInteger()).incrementAndGet();
            });
            assertEquals(1049, visits.size());
            assertTrue(visits.values().stream().allMatch(count -> count.get() == 1), "each once");
            assertEquals(0, scanner.snapshots(), "snapshots released");
        }
    }

    @Test
    void tasksRunInParallelAndFailuresPropagate(@TempDir File dir) {
        try (TestDb db = new TestDb(dir)) {
            Scanner scanner = scanner(db);
            AtomicInteger ran = new AtomicInteger();
            List<Runnable> tasks = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                tasks.add(ran::incrementAndGet);
            }
            scanner.parallel(tasks);
            assertEquals(20, ran.get());
            tasks.add(() -> {
                throw new IllegalStateException("one task fails");
            });
            IllegalStateException failure = org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, () -> scanner.parallel(tasks));
            assertEquals("one task fails", failure.getMessage());
        }
    }
}
