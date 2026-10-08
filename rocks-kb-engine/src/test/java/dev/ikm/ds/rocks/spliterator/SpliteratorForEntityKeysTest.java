package dev.ikm.ds.rocks.spliterator;

import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Splitting the all-entity spliterator to exhaustion, as the whole-store parallel scan does,
 * yields per-pattern ranges and nothing else: the tail of the last pattern, which the composite
 * keeps as its current range once nothing splits further, is handed out whole by
 * {@link SpliteratorForEntityKeys#drainToRanges()}, not drained one element at a time
 * (IKE-Network/ike-issues#1257).
 */
class SpliteratorForEntityKeysTest {

    @Test
    void splittingToExhaustionYieldsWholeRangesAndLeavesTheCompositeEmpty() {
        SpliteratorForEntityKeys composite = new SpliteratorForEntityKeys(List.of(
                new SpliteratorForRocksKeyOfPattern(2, 1, 200_001),   // splits many times
                new SpliteratorForRocksKeyOfPattern(3, 1, 11),        // too small to split
                new SpliteratorForRocksKeyOfPattern(5, 1, 1)));       // empty

        List<SpliteratorForRocksKeyOfPattern> ranges = new ArrayList<>();
        ArrayDeque<Spliterator.OfLong> queue = new ArrayDeque<>();
        queue.add(composite);
        while (!queue.isEmpty()) {
            Spliterator.OfLong s = queue.pollFirst();
            Spliterator.OfLong split = s.trySplit();
            if (split != null) {
                queue.addFirst(s);
                queue.addFirst(split);
                continue;
            }
            switch (s) {
                case SpliteratorForRocksKeyOfPattern range -> ranges.add(range);
                case SpliteratorForEntityKeys c -> ranges.addAll(c.drainToRanges());
                default -> throw new IllegalStateException(s.getClass().getName());
            }
        }

        assertEquals(0, composite.estimateSize(), "the composite still covers elements");
        assertEquals(200_000 + 10, ranges.stream().mapToLong(Spliterator::estimateSize).sum(),
                "the ranges cover every element once");
        assertTrue(ranges.stream().allMatch(range -> range.estimateSize() > 0), "an empty range was handed out");
        assertTrue(ranges.size() < 1_000, ranges.size() + " ranges: the tail was drained per element");
        assertEquals(1, ranges.stream().filter(range -> range.patternSequence() == 3).count(),
                "the small pattern is one range");
        assertTrue(ranges.stream().noneMatch(range -> range.patternSequence() == 5), "the empty pattern is no range");
    }

    @Test
    void drainToRangesHandsOutTheCurrentAndRemainingRangesOnce() {
        SpliteratorForEntityKeys composite = new SpliteratorForEntityKeys(List.of(
                new SpliteratorForRocksKeyOfPattern(7, 1, 4),
                new SpliteratorForRocksKeyOfPattern(2, 1, 3)));
        composite.tryAdvance((long key) -> { });   // pattern 2, element 1 traversed

        List<SpliteratorForRocksKeyOfPattern> ranges = composite.drainToRanges();
        assertEquals(List.of(2, 7), ranges.stream().map(SpliteratorForRocksKeyOfPattern::patternSequence).toList());
        assertEquals(1 + 3, ranges.stream().mapToLong(Spliterator::estimateSize).sum());
        assertEquals(0, composite.estimateSize());
        assertTrue(composite.drainToRanges().isEmpty());
        assertNull(composite.trySplit());
    }
}
