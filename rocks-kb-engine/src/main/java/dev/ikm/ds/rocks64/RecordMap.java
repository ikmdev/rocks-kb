package dev.ikm.ds.rocks64;

import dev.ikm.tinkar.common.service.EntityRecordFormat2;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongPredicate;

/**
 * The entity map of a 64-bit store: one record per entity under its nid, written behind by one
 * writer thread, with the references a semantic makes written in the same batch. Writes are a
 * union at every step, so no interleaving loses a version (settled 2026-10-07): a producer
 * merges its record into the pending record with {@link EntityRecordFormat2#merge}; the writer
 * unions a pending record with the stored one before it writes; and a read unions the pending
 * record with the stored one while a record is pending. The store never parses a record beyond
 * what the envelope exposes.
 *
 * <p>Records are written without the write-ahead log: a save flushes them to disk, and the
 * counters are recovered from the keys at open, so a crash loses at most the records since the
 * last save, never a nid's uniqueness.
 */
final class RecordMap {

    private static final Logger LOG = LoggerFactory.getLogger(RecordMap.class);
    private static final int BATCH = 16_384;
    /** Records a producer may run ahead of the writer: {@code rocks.record.backlog}. */
    static final long BACKLOG = Long.getLong("rocks.record.backlog", 2_000_000);

    private final RocksDB db;
    private final ColumnFamilyHandle entities;
    private final ColumnFamilyHandle references;
    private final LongPredicate canceledStamp;

    private final ConcurrentHashMap<Long, byte[]> pending = new ConcurrentHashMap<>();
    private final LinkedBlockingQueue<Long> queue = new LinkedBlockingQueue<>();
    /** The references not yet written, by referenced nid, so a read can see them. */
    private final ConcurrentHashMap<Long, Set<Long>> pendingReferences = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<long[]> referenceQueue = new ConcurrentLinkedQueue<>();

    // Monotonic: a put counts before it enqueues, the writer counts after a batch lands.
    private final AtomicLong enqueued = new AtomicLong();
    private final AtomicLong flushed = new AtomicLong();
    private final AtomicLong referencesEnqueued = new AtomicLong();
    private final AtomicLong referencesFlushed = new AtomicLong();
    /** Advanced after each batch that wrote references, before they leave the pending set: an iterator older than it may not see them. */
    private final AtomicLong referenceEpoch = new AtomicLong();
    private final ReentrantLock flushLock = new ReentrantLock();
    private final Condition landed = flushLock.newCondition();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private volatile Throwable writerFailure;
    private final Thread writer;

    RecordMap(RocksDB db, ColumnFamilyHandle entities, ColumnFamilyHandle references, LongPredicate canceledStamp) {
        this.db = db;
        this.entities = entities;
        this.references = references;
        this.canceledStamp = canceledStamp;
        this.writer = new Thread(this::drain, "rocks64-writer");
        this.writer.setDaemon(true);
        this.writer.start();
    }

    private byte[] union(byte[] older, byte[] newer) {
        return EntityRecordFormat2.merge(older, newer, canceledStamp);
    }

    /** Merges a record into what is pending for its nid and hands it to the writer. */
    void put(long nid, byte[] record) {
        failIfWriterDied();
        pending.merge(nid, record, this::union);
        long sequence = enqueued.incrementAndGet();
        queue.add(nid);
        if (sequence - flushed.get() > BACKLOG) {
            awaitBacklogBelow(BACKLOG / 2);
        }
    }

    /** Records that a semantic references a component, in the writer's next batch. */
    void addReference(long referencedNid, long referencingNid) {
        pendingReferences.computeIfAbsent(referencedNid, key -> ConcurrentHashMap.newKeySet()).add(referencingNid);
        referencesEnqueued.incrementAndGet();
        referenceQueue.add(new long[]{referencedNid, referencingNid});
    }

    /** The entity's record: the stored one, unioned with the pending one while there is one; null if the store holds none. */
    byte[] get(long nid) {
        byte[] pendingRecord = pending.get(nid);
        byte[] stored = stored(nid);
        return pendingRecord == null ? stored : union(stored, pendingRecord);
    }

    /**
     * The records of several entities, by position; null where the store holds none. As in
     * {@link #get}, what is pending is read before what is stored: the writer moves a record
     * from the one to the other, so a read in that order sees it in at least one.
     */
    byte[][] getAll(long[] nids) {
        byte[][] pendingRecords = new byte[nids.length][];
        List<byte[]> keys = new ArrayList<>(nids.length);
        for (int i = 0; i < nids.length; i++) {
            pendingRecords[i] = pending.get(nids[i]);
            keys.add(Keys.of(nids[i]));
        }
        List<byte[]> stored;
        try {
            stored = db.multiGetAsList(Collections.nCopies(nids.length, entities), keys);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        byte[][] records = new byte[nids.length][];
        for (int i = 0; i < nids.length; i++) {
            records[i] = pendingRecords[i] == null ? stored.get(i) : union(stored.get(i), pendingRecords[i]);
        }
        return records;
    }

    byte[] stored(long nid) {
        try {
            return db.get(entities, Keys.of(nid));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    /** The references to a component that are not yet written, if any. */
    Set<Long> pendingReferencesTo(long referencedNid) {
        return pendingReferences.getOrDefault(referencedNid, Set.of());
    }

    /**
     * Waits until every record and reference handed to the writer before this call is in
     * RocksDB. A scan reads through a snapshot and does not consult what is pending, so it waits
     * here first.
     */
    void awaitPendingWrites() {
        awaitFlushedUpTo(enqueued.get(), referencesEnqueued.get());
    }

    private void awaitBacklogBelow(long backlog) {
        awaitFlushedUpTo(enqueued.get() - backlog, 0);
    }

    private void awaitFlushedUpTo(long records, long refs) {
        failIfWriterDied();
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(10);
        flushLock.lock();
        try {
            while (flushed.get() < records || referencesFlushed.get() < refs) {
                failIfWriterDied();
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    // A scan over what is stored would miss what is pending: fail rather than read stale.
                    throw new IllegalStateException("The store's writer did not catch up within 10 minutes: flushed "
                            + flushed.get() + " of " + records + " records and " + referencesFlushed.get() + " of "
                            + refs + " references");
                }
                try {
                    landed.awaitNanos(Math.min(TimeUnit.MILLISECONDS.toNanos(200), remaining));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        } finally {
            flushLock.unlock();
        }
    }

    private void failIfWriterDied() {
        if (writerFailure != null) {
            throw new IllegalStateException("The store's writer thread failed; the store accepts no more writes", writerFailure);
        }
    }

    private void drain() {
        try (WriteOptions options = new WriteOptions().setDisableWAL(true)) {
            while (running.get() || !queue.isEmpty() || !referenceQueue.isEmpty()) {
                Long first;
                try {
                    first = queue.poll(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (first == null && referenceQueue.isEmpty()) {
                    continue;
                }
                writeBatch(options, first);
            }
        } catch (Throwable t) {
            writerFailure = t;
            LOG.error("The rocks64 writer failed", t);
        }
    }

    private void writeBatch(WriteOptions options, Long first) throws RocksDBException {
        List<Long> nids = new ArrayList<>();
        List<byte[]> taken = new ArrayList<>();
        List<long[]> refs = new ArrayList<>();
        // Every entry polled is written and counted: the batch fills before the next poll,
        // never after it, so no entry is taken from the queue and dropped.
        Long nid = first;
        while (nid != null) {
            nids.add(nid);
            taken.add(pending.get(nid));
            if (nids.size() >= BATCH) {
                break;
            }
            nid = queue.poll();
        }
        // The stored records of the batch in one read, for the unions; an import's records are
        // mostly new, and one multi-get per batch costs far less than a get per record.
        List<byte[]> keys = new ArrayList<>(nids.size());
        for (int i = 0; i < nids.size(); i++) {
            if (taken.get(i) != null) {
                keys.add(Keys.of(nids.get(i)));
            }
        }
        List<byte[]> stored = keys.isEmpty() ? List.of() : db.multiGetAsList(Collections.nCopies(keys.size(), entities), keys);
        try (WriteBatch batch = new WriteBatch()) {
            int read = 0;
            for (int i = 0; i < nids.size(); i++) {
                byte[] record = taken.get(i);
                if (record != null) {
                    batch.put(entities, keys.get(read), union(stored.get(read), record));
                    read++;
                }
            }
            long[] reference;
            while ((reference = referenceQueue.poll()) != null) {
                batch.put(references, Keys.reference(reference[0], reference[1]), new byte[0]);
                refs.add(reference);
            }
            db.write(options, batch);
        }
        if (!refs.isEmpty()) {
            referenceEpoch.incrementAndGet();
        }
        for (int i = 0; i < nids.size(); i++) {
            // A record superseded by a later merge stays pending; its nid is queued again.
            if (taken.get(i) != null) {
                pending.remove(nids.get(i), taken.get(i));
            }
        }
        for (long[] reference : refs) {
            Set<Long> to = pendingReferences.get(reference[0]);
            if (to != null) {
                to.remove(reference[1]);
                if (to.isEmpty()) {
                    pendingReferences.remove(reference[0], to);
                }
            }
        }
        flushed.addAndGet(nids.size());
        referencesFlushed.addAndGet(refs.size());
        flushLock.lock();
        try {
            landed.signalAll();
        } finally {
            flushLock.unlock();
        }
    }

    long enqueued() {
        return enqueued.get();
    }

    /** The epoch of the references column: unchanged while no references are written. */
    long referenceEpoch() {
        return referenceEpoch.get();
    }

    long flushed() {
        return flushed.get();
    }

    /** Writes everything pending and stops the writer. */
    void close() {
        awaitPendingWrites();
        running.set(false);
        try {
            writer.join(30_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (writer.isAlive()) {
            LOG.warn("The rocks64 writer did not stop within 30 s");
        }
    }
}
