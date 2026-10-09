package dev.ikm.ds.rocks64;

import dev.ikm.tinkar.common.service.EntityRecordFormat2;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import dev.ikm.tinkar.common.id.Nid;
import org.rocksdb.EnvOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.SstFileWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
import java.util.concurrent.locks.LockSupport;
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

    /**
     * Load-phase ingestion: the options the SST files of the two columns are written with, and
     * the directory they are written in before RocksDB moves them. Null disables it, and every
     * record goes through the writer.
     */
    record Ingest(Options entityFileOptions, Options referenceFileOptions, File directory, long runBytes, int chunkPairs) {
        /** With the run size and the chunk size of the system properties. */
        Ingest(Options entityFileOptions, Options referenceFileOptions, File directory) {
            this(entityFileOptions, referenceFileOptions, directory, RUN_BYTES, CHUNK_PAIRS);
        }
    }

    /** Bytes of fresh records a pattern's run holds before it is written as an SST file: {@code rocks.record.runBytes}. */
    static final long RUN_BYTES = Long.getLong("rocks.record.runBytes", 64L << 20);
    /** References a chunk holds before it is sorted and written as an SST file: {@code rocks.reference.chunk}. */
    static final int CHUNK_PAIRS = Integer.getInteger("rocks.reference.chunk", 8_000_000);

    private final Ingest ingest;
    private volatile boolean loadPhase;
    /**
     * Held shared by a put while it decides between a run and the queue, exclusively by the
     * phase switch: no offer is in flight across the switch, so the drain that follows an end
     * leaves no run behind.
     */
    private final ReentrantReadWriteLock phase = new ReentrantReadWriteLock();
    /** Load phase: the fresh records of each pattern, sorted by nid, until the run is written. */
    private final ConcurrentHashMap<Integer, Run> runs = new ConcurrentHashMap<>();
    /** Runs sealed and being written: readable until RocksDB has them. */
    private final Set<Run> sealed = ConcurrentHashMap.newKeySet();
    private final AtomicLong runsWritten = new AtomicLong();
    private final AtomicLong recordsIngested = new AtomicLong();
    /** Load phase: references collected into a chunk, sorted and written as an SST file when full. */
    private final Object chunkLock = new Object();
    private long[] chunkReferenced = new long[0];
    private long[] chunkReferencing = new long[0];
    private int chunkSize;
    private final AtomicLong referencesIngested = new AtomicLong();

    /** A pattern's fresh records of a load phase, in nid order: one SST file when written. */
    private static final class Run {
        final int pattern;
        final ConcurrentSkipListMap<Long, byte[]> records = new ConcurrentSkipListMap<>();
        final AtomicLong bytes = new AtomicLong();
        final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        /** Won by the one thread that seals and writes this run. */
        final AtomicBoolean sealing = new AtomicBoolean();
        volatile boolean closed;

        Run(int pattern) {
            this.pattern = pattern;
        }
    }

    RecordMap(RocksDB db, ColumnFamilyHandle entities, ColumnFamilyHandle references, LongPredicate canceledStamp) {
        this(db, entities, references, canceledStamp, null);
    }

    RecordMap(RocksDB db, ColumnFamilyHandle entities, ColumnFamilyHandle references, LongPredicate canceledStamp, Ingest ingest) {
        this.db = db;
        this.entities = entities;
        this.references = references;
        this.canceledStamp = canceledStamp;
        this.ingest = ingest;
        this.writer = new Thread(this::drain, "rocks64-writer");
        this.writer.setDaemon(true);
        this.writer.start();
    }

    /**
     * Begins or ends a load phase. During one, a fresh record (its nid minted in the phase, so
     * nothing is stored for it) joins its pattern's run instead of the writer's queue, and a
     * reference joins a chunk; runs and chunks are written as sorted SST files and ingested,
     * which bypasses the memtable and compaction. Ending the phase writes what remains.
     */
    void setLoadPhase(boolean loadPhase) {
        phase.writeLock().lock();
        try {
            this.loadPhase = loadPhase && ingest != null;
        } finally {
            phase.writeLock().unlock();
        }
        if (!this.loadPhase) {
            // Every end drains the runs and the chunk, a repeated end included: a record left in
            // a run by a put that saw the phase still on would read fine, but reach RocksDB only
            // at the next drain.
            ingestEverything();
        }
    }

    boolean loadPhase() {
        return loadPhase;
    }

    private byte[] union(byte[] older, byte[] newer) {
        return EntityRecordFormat2.merge(older, newer, canceledStamp);
    }

    /** Merges a record into what is pending for its nid and hands it to the writer. */
    void put(long nid, byte[] record) {
        put(nid, record, false);
    }

    /**
     * As {@link #put(long, byte[])}; a fresh record of a load phase, one whose nid was minted in
     * the phase, joins its pattern's run instead, to be ingested in nid order.
     */
    void put(long nid, byte[] record, boolean fresh) {
        failIfWriterDied();
        if (fresh) {
            phase.readLock().lock();
            try {
                if (loadPhase) {
                    offerToRun(nid, record);
                    return;
                }
            } finally {
                phase.readLock().unlock();
            }
        }
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
        if (loadPhase) {
            long[] referenced = null;
            long[] referencing = null;
            int size = 0;
            synchronized (chunkLock) {
                if (chunkReferenced.length == 0) {
                    chunkReferenced = new long[ingest.chunkPairs()];
                    chunkReferencing = new long[ingest.chunkPairs()];
                }
                chunkReferenced[chunkSize] = referencedNid;
                chunkReferencing[chunkSize] = referencingNid;
                chunkSize++;
                if (chunkSize == ingest.chunkPairs()) {
                    referenced = chunkReferenced;
                    referencing = chunkReferencing;
                    size = chunkSize;
                    chunkReferenced = new long[0];
                    chunkReferencing = new long[0];
                    chunkSize = 0;
                }
            }
            if (referenced != null) {
                writeReferenceChunk(referenced, referencing, size);
            }
            return;
        }
        referenceQueue.add(new long[]{referencedNid, referencingNid});
    }

    /** The entity's record: the stored one, unioned with the pending one while there is one; null if the store holds none. */
    byte[] get(long nid) {
        byte[] pendingRecord = pending.get(nid);
        if (pendingRecord == null) {
            pendingRecord = inRuns(nid);
        }
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
            if (pendingRecords[i] == null) {
                pendingRecords[i] = inRuns(nids[i]);
            }
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
        if (loadPhase || !sealed.isEmpty() || !runs.isEmpty()) {
            ingestEverything();
        }
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

    // ------------------------------------------------------------ load-phase ingestion

    private void offerToRun(long nid, byte[] record) {
        int pattern = Nid.patternSequence64(nid);
        while (true) {
            Run run = runs.computeIfAbsent(pattern, Run::new);
            run.lock.readLock().lock();
            try {
                if (run.closed) {
                    continue; // sealed between the lookup and the lock: the next lookup makes a new run
                }
                run.records.merge(nid, record, this::union);
                if (run.bytes.addAndGet(record.length) < ingest.runBytes()) {
                    return;
                }
            } finally {
                run.lock.readLock().unlock();
            }
            sealAndWrite(run);
            return;
        }
    }

    /** A run no offer joins any more: taken out of the map, then closed under its write lock, after every offer in flight. */
    /**
     * Takes a full run out of the open runs and ingests it. One thread does, whichever wins the
     * run's seal; the other offers, and the end of the phase, leave it to that thread. The run
     * joins the sealed set before it leaves the open runs, so a reader finds a record offered
     * to it in one or the other at every instant. The hand-off the other way round left a gap
     * in which a SNOMED CT import read nine records as absent right after their put
     * (2026-10-08).
     *
     * @return whether this call sealed the run, or another thread had
     */
    private boolean sealAndWrite(Run run) {
        if (!run.sealing.compareAndSet(false, true)) {
            return false;
        }
        sealed.add(run);
        runs.remove(run.pattern, run);
        run.lock.writeLock().lock();
        try {
            run.closed = true;
        } finally {
            run.lock.writeLock().unlock();
        }
        writeRun(run);
        return true;
    }

    private byte[] inRuns(long nid) {
        if (runs.isEmpty() && sealed.isEmpty()) {
            return null;
        }
        Run run = runs.get(Nid.patternSequence64(nid));
        byte[] record = run == null ? null : run.records.get(nid);
        if (record == null) {
            for (Run beingWritten : sealed) {
                record = beingWritten.records.get(nid);
                if (record != null) {
                    break;
                }
            }
        }
        return record;
    }

    /** Writes a sealed run as one SST file in nid order and ingests it; the run stays readable until then. */
    private void writeRun(Run run) {
        if (run.records.isEmpty()) {
            sealed.remove(run);
            return;
        }
        File file = new File(ingest.directory(), "records-" + run.pattern + "-" + runsWritten.incrementAndGet() + ".sst");
        try {
            Files.createDirectories(ingest.directory().toPath());
            try (EnvOptions env = new EnvOptions(); SstFileWriter writer = new SstFileWriter(env, ingest.entityFileOptions())) {
                writer.open(file.getAbsolutePath());
                for (Map.Entry<Long, byte[]> entry : run.records.entrySet()) {
                    writer.put(Keys.of(entry.getKey()), entry.getValue());
                }
                writer.finish();
            }
            try (IngestExternalFileOptions options = new IngestExternalFileOptions().setMoveFiles(true)) {
                db.ingestExternalFile(entities, List.of(file.getAbsolutePath()), options);
            }
            recordsIngested.addAndGet(run.records.size());
            LOG.debug("Ingested {} fresh record(s) of pattern {} as {}", run.records.size(), run.pattern, file.getName());
        } catch (RocksDBException | IOException e) {
            writerFailure = e;
            throw new IllegalStateException("Could not ingest a run of pattern " + run.pattern, e);
        } finally {
            file.delete();
            sealed.remove(run);
        }
    }

    /** Sorts a chunk of references by key and writes it as one SST file, ingested; the references stay pending until then. */
    private void writeReferenceChunk(long[] referenced, long[] referencing, int size) {
        if (size == 0) {
            return;
        }
        sortPairs(referenced, referencing, size);
        File file = new File(ingest.directory(), "references-" + runsWritten.incrementAndGet() + ".sst");
        try {
            Files.createDirectories(ingest.directory().toPath());
            byte[] none = new byte[0];
            long written = 0;
            try (EnvOptions env = new EnvOptions(); SstFileWriter writer = new SstFileWriter(env, ingest.referenceFileOptions())) {
                writer.open(file.getAbsolutePath());
                for (int i = 0; i < size; i++) {
                    if (i > 0 && referenced[i] == referenced[i - 1] && referencing[i] == referencing[i - 1]) {
                        continue; // the same reference twice
                    }
                    writer.put(Keys.reference(referenced[i], referencing[i]), none);
                    written++;
                }
                writer.finish();
            }
            if (written > 0) {
                try (IngestExternalFileOptions options = new IngestExternalFileOptions().setMoveFiles(true)) {
                    db.ingestExternalFile(references, List.of(file.getAbsolutePath()), options);
                }
            }
            referenceEpoch.incrementAndGet();
            for (int i = 0; i < size; i++) {
                Set<Long> to = pendingReferences.get(referenced[i]);
                if (to != null) {
                    to.remove(referencing[i]);
                    if (to.isEmpty()) {
                        pendingReferences.remove(referenced[i], to);
                    }
                }
            }
            referencesIngested.addAndGet(size);
            referencesFlushed.addAndGet(size);
            flushLock.lock();
            try {
                landed.signalAll();
            } finally {
                flushLock.unlock();
            }
            LOG.debug("Ingested {} reference(s) as {}", written, file.getName());
        } catch (RocksDBException | IOException e) {
            writerFailure = e;
            throw new IllegalStateException("Could not ingest a chunk of references", e);
        } finally {
            file.delete();
        }
    }

    /** Writes every run and the partial chunk: the end of a load phase, or a scan that must see everything. */
    void ingestEverything() {
        for (Run run : new ArrayList<>(runs.values())) {
            sealAndWrite(run);
        }
        // A run another thread is sealing is that thread's to write; everything is ingested
        // once the sealed set is empty.
        while (!sealed.isEmpty()) {
            failIfWriterDied();
            LockSupport.parkNanos(100_000);
        }
        long[] referenced;
        long[] referencing;
        int size;
        synchronized (chunkLock) {
            referenced = chunkReferenced;
            referencing = chunkReferencing;
            size = chunkSize;
            chunkReferenced = new long[0];
            chunkReferencing = new long[0];
            chunkSize = 0;
        }
        writeReferenceChunk(referenced, referencing, size);
    }

    /** Where a nid's record is, or is not, at this instant: for the diagnostic of a read that found nothing after a put. */
    String whereIs(long nid) {
        int pattern = Nid.patternSequence64(nid);
        Run run = runs.get(pattern);
        StringBuilder where = new StringBuilder("loadPhase=").append(loadPhase)
                .append(" pending=").append(pending.containsKey(nid))
                .append(" run=").append(run == null ? "none" : (run.closed ? "closed" : "open") + "/" + run.records.containsKey(nid) + "/" + run.records.size())
                .append(" sealed=").append(sealed.size());
        for (Run beingWritten : sealed) {
            where.append(beingWritten.records.containsKey(nid) ? " [holds it]" : " [not]");
        }
        where.append(" stored=").append(stored(nid) != null)
                .append(" ingestedRecords=").append(recordsIngested.get())
                .append(" runsWritten=").append(runsWritten.get());
        return where.toString();
    }

    /** The runs open at this instant: none, once a phase has ended. */
    int openRuns() {
        return runs.size();
    }

    /** Records and references ingested as SST files in load phases, for the log and the tests. */
    long recordsIngested() {
        return recordsIngested.get();
    }

    long referencesIngested() {
        return referencesIngested.get();
    }

    /** Sorts two parallel arrays by (a, b), ascending, in place: a quicksort with insertion sort below 16. */
    static void sortPairs(long[] a, long[] b, int size) {
        quicksort(a, b, 0, size - 1);
    }

    private static void quicksort(long[] a, long[] b, int lo, int hi) {
        while (hi - lo > 16) {
            int mid = (lo + hi) >>> 1;
            if (less(a, b, mid, lo)) swap(a, b, mid, lo);
            if (less(a, b, hi, lo)) swap(a, b, hi, lo);
            if (less(a, b, hi, mid)) swap(a, b, hi, mid);
            long pa = a[mid];
            long pb = b[mid];
            int i = lo;
            int j = hi;
            while (i <= j) {
                while (a[i] < pa || (a[i] == pa && b[i] < pb)) i++;
                while (a[j] > pa || (a[j] == pa && b[j] > pb)) j--;
                if (i <= j) {
                    swap(a, b, i, j);
                    i++;
                    j--;
                }
            }
            // Recurse into the smaller side, loop on the larger: bounded depth.
            if (j - lo < hi - i) {
                quicksort(a, b, lo, j);
                lo = i;
            } else {
                quicksort(a, b, i, hi);
                hi = j;
            }
        }
        for (int i = lo + 1; i <= hi; i++) {
            long ka = a[i];
            long kb = b[i];
            int j = i - 1;
            while (j >= lo && (a[j] > ka || (a[j] == ka && b[j] > kb))) {
                a[j + 1] = a[j];
                b[j + 1] = b[j];
                j--;
            }
            a[j + 1] = ka;
            b[j + 1] = kb;
        }
    }

    private static boolean less(long[] a, long[] b, int i, int j) {
        return a[i] < a[j] || (a[i] == a[j] && b[i] < b[j]);
    }

    private static void swap(long[] a, long[] b, int i, int j) {
        long t = a[i]; a[i] = a[j]; a[j] = t;
        t = b[i]; b[i] = b[j]; b[j] = t;
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
