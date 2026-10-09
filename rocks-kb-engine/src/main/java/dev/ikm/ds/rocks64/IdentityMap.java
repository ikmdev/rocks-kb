package dev.ikm.ds.rocks64;

import dev.ikm.tinkar.common.id.Nid;
import dev.ikm.tinkar.common.id.PublicId;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.common.service.IdentityAdvisories;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.util.time.Stopwatch;
import dev.ikm.tinkar.terms.EntityBinding;
import dev.ikm.tinkar.terms.EntityProxy;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.EnvOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.BitSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;

/**
 * The UUID to nid map of a 64-bit store, and the allocation of nids: every UUID of a component
 * maps to the component's nid, and a component gets its nid from its pattern's counter the
 * first time any of its UUIDs is seen. A pattern is itself an element of the pattern-of-patterns,
 * sequence {@value Counters#PATTERN_OF_PATTERNS}, and a pattern's sequence is its element
 * sequence there: the concept pattern is element {@value Counters#CONCEPT_PATTERN}, the stamp
 * pattern element {@value Counters#STAMP_PATTERN}, under the governed {@link EntityBinding}
 * UUIDs (settled 2026-10-07).
 *
 * <p>New entries are held in memory, in a {@link UuidNidTable}, until a write puts them in the
 * column, and a lookup reads memory before the column. During a load phase every entry of the
 * import is held, and the column is written once at the phase's end (IKE-Network/ike-issues#1273:
 * on DeX's 60 million identifiers, looking each one up in the column as it was registered, and
 * flushing a million at a time, made the registration seventeen times slower than the spined
 * array's); outside one, the memory is written whenever it holds more than
 * {@value #FLUSH_THRESHOLD} entries. A save and a close write it too. A write of
 * {@link SstIngest#threshold()} entries or more goes as sorted SST files, ingested, which
 * bypasses the memtable, the write-ahead log and compaction; a smaller one goes as write
 * batches through the log. A store created in this process skips the column while nothing
 * beyond its fixed bindings has been written there.
 */
final class IdentityMap {

    private static final Logger LOG = LoggerFactory.getLogger(IdentityMap.class);

    /** Entries held in memory before they are written to the column, outside a load phase: {@code rocks.identity.flushThreshold}. */
    static final int FLUSH_THRESHOLD = Integer.getInteger("rocks.identity.flushThreshold", 1_000_000);
    /** Entries from which a write goes as ingested SST files rather than write batches: {@code rocks.identity.sstThreshold}. */
    static final long SST_THRESHOLD = Long.getLong("rocks.identity.sstThreshold", 100_000);
    /** Entries per SST file, so a large write is several files written at once. */
    private static final long ENTRIES_PER_FILE = 4_000_000;
    private static final int BATCH = 16_384;

    static final long PATTERN_OF_PATTERNS_NID = Nid.compose64(Counters.PATTERN_OF_PATTERNS, Counters.PATTERN_OF_PATTERNS);
    static final long CONCEPT_PATTERN_NID = Nid.compose64(Counters.PATTERN_OF_PATTERNS, Counters.CONCEPT_PATTERN);
    static final long STAMP_PATTERN_NID = Nid.compose64(Counters.PATTERN_OF_PATTERNS, Counters.STAMP_PATTERN);

    /**
     * How a large write is made: the options the SST files are written with, which must be the
     * column's so the files match it, a directory on the database's file system to write them
     * in, and the number of entries from which a write is made this way.
     */
    record SstIngest(Options options, File directory, long threshold) {
    }

    private final RocksDB db;
    private final ColumnFamilyHandle handle;
    private final Counters counters;
    private final SstIngest ingest;
    /** The fixed bindings, always in memory: a lookup never goes to the column for them. */
    private final Map<UUID, Long> fixed = new HashMap<>();
    /** The entries not yet written. Replaced by an empty table when a write begins. */
    private volatile UuidNidTable held = new UuidNidTable();
    /** The entries a write is writing, readable until they are in the column; null between writes. */
    private volatile UuidNidTable writing;
    private volatile boolean loadPhase;
    /** The nids minted in the current load phase, by pattern and element: nothing is stored for them yet. */
    private final ConcurrentHashMap<Integer, BitSet> fresh = new ConcurrentHashMap<>();
    /** Whether the column holds nothing but the fixed bindings, which memory answers: true for a store created here until a write. */
    private volatile boolean columnHoldsOnlyFixed;
    private final LockTable locks = new LockTable();
    private final ReentrantLock flushLock = new ReentrantLock();
    /**
     * Held shared by a mint while it puts into the held table, exclusively by a flush while it
     * swaps that table out: a mint never lands in a table being written, whose sorted stripes
     * would not carry it, and whose entries are dropped once written.
     */
    private final ReentrantReadWriteLock swap = new ReentrantReadWriteLock();

    IdentityMap(RocksDB db, ColumnFamilyHandle handle, Counters counters, SstIngest ingest) {
        this.db = db;
        this.handle = handle;
        this.counters = counters;
        this.ingest = ingest;
        for (FixedPattern pattern : fixedPatterns()) {
            for (UUID uuid : pattern.uuids()) {
                fixed.put(uuid, pattern.nid());
            }
        }
    }

    /** A fixed pattern: the UUIDs of its binding and the nid it has in every 64-bit store. */
    record FixedPattern(String description, UUID[] uuids, long nid) {
        private static FixedPattern of(EntityProxy.Pattern binding, long nid) {
            // Read the proxy's UUIDs and description only: its nid and hash ask the running provider.
            return new FixedPattern(binding.description(), binding.asUuidArray(), nid);
        }
    }

    /** The three fixed patterns and their nids. */
    static List<FixedPattern> fixedPatterns() {
        return List.of(FixedPattern.of(EntityBinding.Pattern.pattern(), PATTERN_OF_PATTERNS_NID),
                FixedPattern.of(EntityBinding.Concept.pattern(), CONCEPT_PATTERN_NID),
                FixedPattern.of(EntityBinding.Stamp.pattern(), STAMP_PATTERN_NID));
    }

    /**
     * Gives a new store its three fixed patterns: the pattern-of-patterns at element 1 of itself,
     * the concept pattern at 2, the stamp pattern at 3, every UUID of each binding mapped and
     * written to the column through the log, so the store reopens with its bindings.
     */
    void bootstrap() {
        int patternOfPatterns = counters.nextPatternSequence();
        int concept = counters.nextPatternSequence();
        int stamp = counters.nextPatternSequence();
        if (patternOfPatterns != Counters.PATTERN_OF_PATTERNS || concept != Counters.CONCEPT_PATTERN
                || stamp != Counters.STAMP_PATTERN) {
            throw new IllegalStateException("A new store's first pattern sequences must be 1, 2 and 3; got "
                    + patternOfPatterns + ", " + concept + ", " + stamp);
        }
        try (WriteBatch batch = new WriteBatch(); WriteOptions options = new WriteOptions()) {
            for (Map.Entry<UUID, Long> binding : fixed.entrySet()) {
                batch.put(handle, Keys.of(binding.getKey()), Keys.of(binding.getValue()));
            }
            db.write(options, batch);
        } catch (RocksDBException e) {
            throw new RuntimeException("Could not write the fixed bindings", e);
        }
        columnHoldsOnlyFixed = true;
    }

    /** Checks that an existing store's column maps the three bindings to their fixed nids. */
    void verifyBindings() {
        for (FixedPattern fixed : fixedPatterns()) {
            for (UUID uuid : fixed.uuids()) {
                long nid = stored(uuid);
                if (nid != fixed.nid()) {
                    throw new IllegalStateException("The store maps " + uuid + " of " + fixed.description() + " to "
                            + (nid == 0 ? "nothing" : Long.toString(nid)) + ", not to its fixed nid " + fixed.nid());
                }
            }
        }
    }

    /** The nid of a UUID the store knows, from memory first and then the column. */
    /**
     * The UUIDs minted for a nid: the tables held in memory, then the column, scanned whole.
     * Correctness over speed, for a component referenced but never written, which has no
     * record to read its UUIDs from. Empty when the nid was never minted here.
     */
    Optional<PublicId> publicIdOf(long nid) {
        List<UUID> uuids = new ArrayList<>();
        fixed.forEach((uuid, bound) -> {
            if (bound == nid) {
                uuids.add(uuid);
            }
        });
        for (UuidNidTable table : new UuidNidTable[]{held, writing}) {
            if (table != null) {
                table.forEachSorted(0, UuidNidTable.STRIPES, (msb, lsb, found) -> {
                    if (found == nid) {
                        uuids.add(new UUID(msb, lsb));
                    }
                });
            }
        }
        if (uuids.isEmpty() && !columnHoldsOnlyFixed) {
            try (RocksIterator iterator = db.newIterator(handle)) {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    if (Keys.nid(iterator.value()) == nid) {
                        uuids.add(Keys.uuid(iterator.key()));
                    }
                }
            }
        }
        return uuids.isEmpty() ? Optional.empty() : Optional.of(PublicIds.of(uuids.toArray(new UUID[0])));
    }

    Optional<Long> nid(UUID uuid) {
        long nid = lookup(uuid);
        return nid == 0 ? Optional.empty() : Optional.of(nid);
    }

    boolean knows(UUID uuid) {
        return lookup(uuid) != 0;
    }

    /**
     * The nid of a UUID, or zero: the entries held, the entries being written, the fixed
     * bindings, then the column, unless it is known to hold only the bindings. A write moves the
     * held table to the writing one before it starts and drops it only once the column has the
     * entries, so a lookup finds an entry in one of the three throughout.
     */
    private long lookup(UUID uuid) {
        long nid = held.get(uuid);
        if (nid != 0) {
            return nid;
        }
        UuidNidTable beingWritten = writing;
        if (beingWritten != null) {
            nid = beingWritten.get(uuid);
            if (nid != 0) {
                return nid;
            }
        }
        Long binding = fixed.get(uuid);
        if (binding != null) {
            return binding;
        }
        if (columnHoldsOnlyFixed) {
            return 0;
        }
        return stored(uuid);
    }

    /** The nid the column maps a UUID to, or zero. */
    private long stored(UUID uuid) {
        try {
            byte[] value = db.get(handle, Keys.of(uuid));
            return value == null ? 0 : Keys.nid(value);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * The nid of an entity of a pattern, allocated if the entity is new. The pattern is itself
     * resolved first, and allocated under the pattern-of-patterns if it is new.
     *
     * @param patternId the pattern's public id
     * @param entityId  the entity's public id
     * @return the entity's nid
     */
    long nidFor(PublicId patternId, PublicId entityId) {
        long patternNid = keyFor(patternId, this::allocatePattern);
        if (Nid.patternSequence64(patternNid) != Counters.PATTERN_OF_PATTERNS) {
            throw new IllegalStateException(patternId + " is not a pattern of this store: its nid " + patternNid
                    + " is an element of pattern " + Nid.patternSequence64(patternNid));
        }
        return keyFor(entityId, () -> allocateElementOf(patternNid));
    }

    /**
     * The nid of a component by its UUIDs: the one known for the least known UUID, else a new
     * one under the pattern in scope ({@link PrimitiveData#SCOPED_PATTERN_PUBLICID_FOR_NID}),
     * else a failure, since every nid of this store names its pattern.
     */
    long nidForUuids(UUID... uuids) {
        UUID[] ordered = uuids;
        if (uuids.length > 1) {
            ordered = uuids.clone();
            Arrays.sort(ordered);
            adviseIfSeveralComponents(ordered);
        }
        for (UUID uuid : ordered) {
            long known = lookup(uuid);
            if (known != 0) {
                return known;
            }
        }
        if (PrimitiveData.SCOPED_PATTERN_PUBLICID_FOR_NID.isBound()) {
            return nidFor(PrimitiveData.SCOPED_PATTERN_PUBLICID_FOR_NID.get(), PublicIds.of(uuids));
        }
        throw new IllegalStateException("No entity key found for UUIDs: " + Arrays.toString(uuids)
                + ", and no pattern in scope to allocate one under");
    }

    private long allocatePattern() {
        return Nid.compose64(Counters.PATTERN_OF_PATTERNS, counters.nextPatternSequence());
    }

    private long allocateElementOf(long patternNid) {
        if (patternNid == PATTERN_OF_PATTERNS_NID) {
            return allocatePattern();
        }
        int patternSequence = Nid.elementSequence64(patternNid);
        return Nid.compose64(patternSequence, counters.nextElementSequence(patternSequence));
    }

    /**
     * The nid of a public id, allocated under the stripes of its UUIDs if none of them has one,
     * and mapped from every UUID of the id. Ids that share a UUID serialize on its stripe, so
     * each id gets exactly one nid (IKE-Network/ike-issues#1140).
     */
    private long keyFor(PublicId id, LongSupplier allocator) {
        UUID[] uuids = id.asUuidArray();
        if (uuids.length == 1) {
            long known = lookup(uuids[0]);
            if (known != 0) {
                return known;
            }
        } else {
            Long existing = existing(uuids);
            if (existing != null && allMapped(uuids)) {
                return existing;
            }
        }
        boolean allocated = false;
        long nid;
        locks.lock(id);
        try {
            adviseIfSeveralComponents(uuids);
            Long known = existing(uuids);
            if (known == null) {
                nid = allocator.getAsLong();
                allocated = true;
                if (loadPhase) {
                    markFresh(nid);
                }
            } else {
                nid = known;
            }
            swap.readLock().lock();
            try {
                for (UUID uuid : uuids) {
                    if (lookup(uuid) == 0) {
                        held.putIfAbsent(uuid, nid);
                    }
                }
            } finally {
                swap.readLock().unlock();
            }
        } finally {
            locks.unlock(id);
        }
        if (allocated) {
            flushIfLarge();
        }
        return nid;
    }

    private Long existing(UUID[] uuids) {
        for (UUID uuid : uuids) {
            long nid = lookup(uuid);
            if (nid != 0) {
                return nid;
            }
        }
        return null;
    }

    private boolean allMapped(UUID[] uuids) {
        for (UUID uuid : uuids) {
            if (lookup(uuid) == 0) {
                return false;
            }
        }
        return true;
    }

    private void adviseIfSeveralComponents(UUID[] uuids) {
        if (uuids.length < 2) {
            return;
        }
        TreeSet<Long> nids = new TreeSet<>();
        for (UUID uuid : uuids) {
            long nid = lookup(uuid);
            if (nid != 0) {
                nids.add(nid);
            }
        }
        if (nids.size() > 1) {
            IdentityAdvisories.componentsShareUuids(List.of(uuids), nids);
        }
    }

    /**
     * Enters or leaves a load phase. While one is on, the entries are held whatever their
     * number; leaving it writes them all, once.
     */
    void setLoadPhase(boolean loadPhase) {
        this.loadPhase = loadPhase;
        if (!loadPhase) {
            fresh.clear();
            flush();
        }
    }

    private void markFresh(long nid) {
        BitSet elements = fresh.computeIfAbsent(Nid.patternSequence64(nid), pattern -> new BitSet());
        synchronized (elements) {
            elements.set(Nid.elementSequence64(nid));
        }
    }

    /** Whether a nid was minted in the current load phase, so that nothing is stored for it. */
    boolean fresh(long nid) {
        if (!loadPhase) {
            return false;
        }
        BitSet elements = fresh.get(Nid.patternSequence64(nid));
        if (elements == null) {
            return false;
        }
        synchronized (elements) {
            return elements.get(Nid.elementSequence64(nid));
        }
    }

    /** Writes the entries held in memory to the column, if there are more than the threshold and no load phase is on. */
    void flushIfLarge() {
        if (!loadPhase && held.size() > FLUSH_THRESHOLD && flushLock.tryLock()) {
            try {
                flush();
            } finally {
                flushLock.unlock();
            }
        }
    }

    /**
     * Writes every entry held in memory to the column. The held table becomes the one being
     * written and a new, empty table takes new entries meanwhile; the written table is dropped
     * once the column has its entries. A write that fails leaves its table readable, and is
     * retried by the next write.
     */
    void flush() {
        flushLock.lock();
        try {
            UuidNidTable retry = writing;
            if (retry != null) {
                write(retry);
                columnHoldsOnlyFixed = false;
                writing = null;
            }
            UuidNidTable table;
            swap.writeLock().lock();
            try {
                table = held;
                if (table.isEmpty()) {
                    return;
                }
                writing = table;
                held = new UuidNidTable();
            } finally {
                swap.writeLock().unlock();
            }
            write(table);
            columnHoldsOnlyFixed = false;
            writing = null;
        } finally {
            flushLock.unlock();
        }
    }

    private void write(UuidNidTable table) {
        Stopwatch stopwatch = new Stopwatch();
        long count = table.size();
        table.sort();
        int files = count >= ingest.threshold() ? writeAsSstFiles(table, count) : writeAsBatches(table);
        stopwatch.stop();
        if (count >= ingest.threshold()) {
            LOG.info("Wrote {} identities as {} ingested SST file(s) in {}", String.format("%,d", count), files,
                    stopwatch.durationString());
        } else {
            LOG.debug("Wrote {} identities in {} batch(es) in {}", count, files, stopwatch.durationString());
        }
    }

    /** Writes the table in key order as write batches through the log; the number of batches. */
    private int writeAsBatches(UuidNidTable table) {
        int[] batches = {0};
        try (WriteOptions options = new WriteOptions()) {
            WriteBatch[] batch = {new WriteBatch()};
            int[] inBatch = {0};
            try {
                table.forEachSorted(0, UuidNidTable.STRIPES, (msb, lsb, nid) -> {
                    try {
                        batch[0].put(handle, Keys.of(new UUID(msb, lsb)), Keys.of(nid));
                        if (++inBatch[0] == BATCH) {
                            db.write(options, batch[0]);
                            batch[0].close();
                            batch[0] = new WriteBatch();
                            inBatch[0] = 0;
                            batches[0]++;
                        }
                    } catch (RocksDBException e) {
                        throw new RuntimeException("Could not write the identity map", e);
                    }
                });
                if (inBatch[0] > 0) {
                    db.write(options, batch[0]);
                    batches[0]++;
                }
            } catch (RocksDBException e) {
                throw new RuntimeException("Could not write the identity map", e);
            } finally {
                batch[0].close();
            }
        }
        return batches[0];
    }

    /**
     * Writes the table as SST files, each a range of stripes and so a range of the key space,
     * several at once, and ingests them in one call; the number of files. The files are moved
     * into the database, so the directory is on its file system.
     */
    private int writeAsSstFiles(UuidNidTable table, long count) {
        int files = (int) Math.min(UuidNidTable.STRIPES, Math.max(1, count / ENTRIES_PER_FILE));
        int stripesPerFile = (UuidNidTable.STRIPES + files - 1) / files;
        File directory = ingest.directory();
        try {
            Files.createDirectories(directory.toPath());
        } catch (IOException e) {
            throw new UncheckedIOException("Could not create " + directory, e);
        }
        String[] paths = new String[files];
        IntStream.range(0, files).parallel().forEach(file -> {
            int from = file * stripesPerFile;
            int to = Math.min(UuidNidTable.STRIPES, from + stripesPerFile);
            if (from < to && table.sizeOf(from, to) > 0) {
                paths[file] = writeSstFile(table, from, to, new File(directory, "identities-" + file + ".sst"));
            }
        });
        List<String> written = new ArrayList<>();
        for (String path : paths) {
            if (path != null) {
                written.add(path);
            }
        }
        try (IngestExternalFileOptions options = new IngestExternalFileOptions().setMoveFiles(true)) {
            db.ingestExternalFile(handle, written, options);
        } catch (RocksDBException e) {
            throw new RuntimeException("Could not ingest the identity map's SST files", e);
        } finally {
            for (String path : written) {
                new File(path).delete();
            }
            directory.delete();
        }
        return written.size();
    }

    private String writeSstFile(UuidNidTable table, int fromStripe, int toStripe, File file) {
        ByteBuffer key = ByteBuffer.allocateDirect(16);
        ByteBuffer value = ByteBuffer.allocateDirect(8);
        try (EnvOptions env = new EnvOptions(); SstFileWriter writer = new SstFileWriter(env, ingest.options())) {
            writer.open(file.getAbsolutePath());
            table.forEachSorted(fromStripe, toStripe, (msb, lsb, nid) -> {
                key.clear();
                key.putLong(msb).putLong(lsb).flip();
                value.clear();
                value.putLong(nid).flip();
                try {
                    writer.put(key, value);
                } catch (RocksDBException e) {
                    throw new RuntimeException("Could not write " + file, e);
                }
            });
            writer.finish();
            return file.getAbsolutePath();
        } catch (RocksDBException e) {
            throw new RuntimeException("Could not write " + file, e);
        }
    }

    /** The entries held in memory and not yet written. */
    long heldCount() {
        UuidNidTable beingWritten = writing;
        return held.size() + (beingWritten == null ? 0 : beingWritten.size());
    }
}
