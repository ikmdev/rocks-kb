package dev.ikm.ds.rocks64;

import dev.ikm.tinkar.common.id.Nid;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.SizeApproximationFlag;
import org.rocksdb.Slice;
import org.rocksdb.Snapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ObjLongConsumer;

/**
 * The scans of a 64-bit store's entity map (design {@code design-2026-10-07-64-bit-rocks-store},
 * "Scans"): one snapshot per scan, released when it ends; ranges sized by bytes, from each
 * pattern's counter and RocksDB's estimate of the pattern's size; a bounded iterator per range,
 * streamed from its lower bound; and a fixed pool of platform threads taking ranges from a
 * queue, largest first. The sequential scan is one iterator over the whole map. A parallel scan
 * calls its action from several threads at once and in no particular order, and the records
 * every scan hands over are the stored values themselves, uncopied.
 */
final class Scanner {

    /** How many of a scan's ranges run at once: {@code rocks.scan.parallelism}, else the processor count. */
    static final int PARALLELISM = Math.max(1, Integer.getInteger("rocks.scan.parallelism",
            Runtime.getRuntime().availableProcessors()));
    /** The bytes a range aims to hold: {@code rocks.scan.rangeBytes}, else 16 MB. */
    static final long RANGE_BYTES = Math.max(1L << 20, Long.getLong("rocks.scan.rangeBytes", 16L << 20));
    private static final int MIN_ELEMENTS = 4_096;
    private static final int MAX_ELEMENTS = 1 << 20;

    private final RocksDB db;
    private final ColumnFamilyHandle entities;
    private final Counters counters;
    private final AtomicInteger threadNames = new AtomicInteger();

    Scanner(RocksDB db, ColumnFamilyHandle entities, Counters counters) {
        this.db = db;
        this.entities = entities;
        this.counters = counters;
    }

    /** A key range, lower bound inclusive, upper bound exclusive. */
    record Range(byte[] lower, byte[] upper, long elements) {
    }

    /** The whole map in key order, from the calling thread. */
    void forEach(ObjLongConsumer<byte[]> action) {
        Snapshot snapshot = db.getSnapshot();
        try (ReadOptions ro = new ReadOptions().setSnapshot(snapshot).setFillCache(false).setTotalOrderSeek(true);
             RocksIterator it = db.newIterator(entities, ro)) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                action.accept(it.value(), Keys.nid(it.key()));
            }
        } finally {
            db.releaseSnapshot(snapshot);
        }
    }

    /** The whole map, the ranges in parallel. */
    void forEachParallel(ObjLongConsumer<byte[]> action) {
        forEachParallel(ranges(), action);
    }

    /** The given ranges in parallel, largest first, through one snapshot. */
    void forEachParallel(List<Range> ranges, ObjLongConsumer<byte[]> action) {
        List<Range> ordered = new ArrayList<>(ranges);
        ordered.sort((a, b) -> Long.compare(b.elements(), a.elements()));
        ConcurrentLinkedQueue<Range> queue = new ConcurrentLinkedQueue<>(ordered);
        Snapshot snapshot = db.getSnapshot();
        try {
            List<Runnable> workers = new ArrayList<>();
            for (int i = 0; i < Math.min(PARALLELISM, ranges.size()); i++) {
                workers.add(() -> {
                    Range range;
                    while ((range = queue.poll()) != null) {
                        scan(snapshot, range, action);
                    }
                });
            }
            parallel(workers);
        } finally {
            db.releaseSnapshot(snapshot);
        }
    }

    private void scan(Snapshot snapshot, Range range, ObjLongConsumer<byte[]> action) {
        try (Slice lower = new Slice(range.lower()); Slice upper = new Slice(range.upper());
             ReadOptions ro = new ReadOptions().setSnapshot(snapshot).setFillCache(false).setTotalOrderSeek(true)
                     .setIterateLowerBound(lower).setIterateUpperBound(upper);
             RocksIterator it = db.newIterator(entities, ro)) {
            for (it.seek(range.lower()); it.isValid(); it.next()) {
                action.accept(it.value(), Keys.nid(it.key()));
            }
        }
    }

    /**
     * Runs tasks on a pool of at most {@link #PARALLELISM} platform threads and waits for all
     * of them; one task, or a parallelism of one, runs on the calling thread. The first failure
     * is rethrown once every task has finished.
     */
    void parallel(List<? extends Runnable> tasks) {
        if (tasks.isEmpty()) {
            return;
        }
        if (tasks.size() == 1 || PARALLELISM == 1) {
            tasks.forEach(Runnable::run);
            return;
        }
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(PARALLELISM, tasks.size()), task -> {
            Thread thread = new Thread(task, "rocks64-scan-" + threadNames.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
        try {
            List<Future<?>> futures = new ArrayList<>(tasks.size());
            for (Runnable task : tasks) {
                futures.add(pool.submit(task));
            }
            Throwable failure = null;
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    if (failure == null) {
                        failure = e.getCause();
                    }
                }
            }
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            if (failure != null) {
                throw new RuntimeException(failure);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while scanning", e);
        } finally {
            pool.shutdownNow();
        }
    }

    /** The ranges of the whole map: each pattern cut into pieces of about {@link #RANGE_BYTES}. */
    List<Range> ranges() {
        List<Range> ranges = new ArrayList<>();
        counters.patternSequences().forEach(pattern -> ranges.addAll(rangesOf(pattern)));
        return ranges;
    }

    /** The ranges of one pattern, from its first element to its counter. */
    List<Range> rangesOf(int pattern) {
        long next = counters.next(pattern);
        long elements = next - Counters.FIRST_ELEMENT;
        List<Range> ranges = new ArrayList<>();
        if (elements <= 0) {
            return ranges;
        }
        long perRange = elementsPerRange(pattern, elements);
        for (long first = Counters.FIRST_ELEMENT; first < next; first += perRange) {
            long end = Math.min(next, first + perRange);
            ranges.add(new Range(Keys.of(Nid.compose64(pattern, (int) first)),
                    end > Nid.MAX_SEQUENCE_64 ? Keys.endOf(pattern) : Keys.of(Nid.compose64(pattern, (int) end)),
                    end - first));
        }
        return ranges;
    }

    private long elementsPerRange(int pattern, long elements) {
        long bytes;
        try (Slice start = new Slice(Keys.firstOf(pattern)); Slice limit = new Slice(Keys.endOf(pattern))) {
            long[] sizes = db.getApproximateSizes(entities, List.of(new org.rocksdb.Range(start, limit)),
                    SizeApproximationFlag.INCLUDE_FILES, SizeApproximationFlag.INCLUDE_MEMTABLES);
            bytes = sizes.length == 0 ? 0 : sizes[0];
        }
        if (bytes <= 0) {
            return MAX_ELEMENTS;
        }
        long perRange = RANGE_BYTES * elements / bytes;
        return Math.max(MIN_ELEMENTS, Math.min(MAX_ELEMENTS, perRange));
    }

    /** How many snapshots the database holds; 0 when no scan is running. */
    long snapshots() {
        try {
            return Long.parseLong(db.getProperty("rocksdb.num-snapshots"));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }
}
