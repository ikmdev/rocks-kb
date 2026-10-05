package dev.ikm.ds.rocks.maps;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class RocksDbMap<DB extends RocksDB> {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDbMap.class);
    protected final DB db;
    protected final ColumnFamilyHandle mapHandle;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // Point reads borrow an iterator rather than create one: creating a native iterator was
    // most of a read's cost. An iterator sees the database as of its creation or last refresh,
    // so each pooled one carries the write epoch it has seen, and a reader refreshes it when a
    // write has landed since. Every write advances the epoch once it has reached RocksDB, so a
    // read that begins after a write returns sees it.
    private record PooledIterator(RocksIterator iterator, long epoch) {}
    private static final int MAX_POOLED_ITERATORS = Math.max(8, 2 * Runtime.getRuntime().availableProcessors());
    private final AtomicLong writeEpoch = new AtomicLong(0);
    private final ConcurrentLinkedQueue<PooledIterator> iteratorPool = new ConcurrentLinkedQueue<>();
    private final AtomicInteger pooledIterators = new AtomicInteger(0);
    private final AtomicBoolean readResourcesReleased = new AtomicBoolean(false);
    // Read-only once built, so shared by every point read on every thread. Total-order seek,
    // with the reader bounding its own prefix: a column family's prefix extractor may be longer
    // than the prefix a read seeks (the referencing-semantic map's covers its whole 16 byte
    // key), and prefix_same_as_start would then end the read before its first key.
    private final ReadOptions pointReadOptions = new ReadOptions()
            .setTotalOrderSeek(true);

    public RocksDbMap(DB db, ColumnFamilyHandle mapHandle) {
        this.db = db;
        this.mapHandle = mapHandle;
    }

    protected RocksIterator rocksIterator() {

        return db.newIterator(mapHandle);
    }

    protected RocksIterator rocksIterator(ReadOptions ro) {
        return db.newIterator(mapHandle, ro);
    }

    public final void put(byte[] key, byte[] value) {
        try {
            db.put(this.mapHandle, key, value);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        advanceWriteEpoch();
    }

    /** Marks that a write has reached RocksDB: pooled iterators refresh before they read again. */
    protected final void advanceWriteEpoch() {
        writeEpoch.incrementAndGet();
    }

    /** The write epoch a point read must see; read it before borrowing an iterator. */
    protected final long readEpoch() {
        return writeEpoch.get();
    }

    /** An iterator for a point read that sees at least every write up to {@code epoch}. */
    protected final RocksIterator borrowIterator(long epoch) {
        PooledIterator pooled = iteratorPool.poll();
        if (pooled == null) {
            return rocksIterator(pointReadOptions);
        }
        pooledIterators.decrementAndGet();
        if (pooled.epoch() < epoch) {
            try {
                pooled.iterator().refresh();
            } catch (RocksDBException e) {
                pooled.iterator().close();
                return rocksIterator(pointReadOptions);
            }
        }
        return pooled.iterator();
    }

    /** Gives back an iterator from {@link #borrowIterator}, which has seen writes up to {@code epoch}. */
    protected final void returnIterator(RocksIterator iterator, long epoch) {
        if (!readResourcesReleased.get() && pooledIterators.incrementAndGet() <= MAX_POOLED_ITERATORS) {
            iteratorPool.offer(new PooledIterator(iterator, epoch));
            return;
        }
        if (!readResourcesReleased.get()) {
            pooledIterators.decrementAndGet();
        }
        iterator.close();
    }

    public final byte[] get(byte[] key) {
        try {
            return db.get(mapHandle, key);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public final void delete(byte[] key) throws RocksDBException {
        db.delete(mapHandle, key);
        advanceWriteEpoch();
    }

    public final boolean keyExists(byte[] key) {
        return db.keyExists(mapHandle, key);
    }

    public final void save() {
        writeMemoryToDb();
        try (FlushOptions flushOptions = new FlushOptions()) {
            db.flush(flushOptions);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract void writeMemoryToDb();

    public final void close() {
        if (!closed.compareAndSet(false, true)) {
            return; // already closed
        }
        // Allow subclasses to finish their work and stop threads before closing the handle
        closeMap();
        save();
        releaseReadResources();
        try {
            if (mapHandle != null) {
                mapHandle.close();
            }
        } catch (Exception ignore) {
            LOG.warn("Error closing DB column family", ignore);
        }

    }

    protected abstract void closeMap();

    /**
     * Releases the native resources the map holds for point reads. Called last, after
     * {@link #closeMap()} and the final save and just before the column family closes: the map
     * stays readable until then.
     */
    private void releaseReadResources() {
        readResourcesReleased.set(true);
        for (PooledIterator pooled = iteratorPool.poll(); pooled != null; pooled = iteratorPool.poll()) {
            pooled.iterator().close();
        }
        pointReadOptions.close();
    }
}
