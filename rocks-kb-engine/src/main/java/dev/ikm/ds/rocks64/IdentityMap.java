package dev.ikm.ds.rocks64;

import dev.ikm.tinkar.common.id.Nid;
import dev.ikm.tinkar.common.id.PublicId;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.common.service.IdentityAdvisories;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.terms.EntityBinding;
import dev.ikm.tinkar.terms.EntityProxy;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * The UUID to nid map of a 64-bit store, and the allocation of nids: every UUID of a component
 * maps to the component's nid, and a component gets its nid from its pattern's counter the
 * first time any of its UUIDs is seen. A pattern is itself an element of the pattern-of-patterns,
 * sequence {@value Counters#PATTERN_OF_PATTERNS}, and a pattern's sequence is its element
 * sequence there: the concept pattern is element {@value Counters#CONCEPT_PATTERN}, the stamp
 * pattern element {@value Counters#STAMP_PATTERN}, under the governed {@link EntityBinding}
 * UUIDs (settled 2026-10-07).
 *
 * <p>Entries live in memory until a flush writes them to the column, which happens at save, at
 * close, and whenever the memory holds more than {@value #FLUSH_THRESHOLD} entries, so that an
 * import of tens of millions of components does not hold them all in heap. A lookup reads memory
 * first and then the column. The flush writes through the write-ahead log: a record whose nid
 * has lost its UUIDs is the one loss a store cannot recover from, so identities are the durable
 * side of every crash.
 */
final class IdentityMap {

    private static final Logger LOG = LoggerFactory.getLogger(IdentityMap.class);

    /** Entries held in memory before they are written to the column: {@code rocks.identity.flushThreshold}. */
    static final int FLUSH_THRESHOLD = Integer.getInteger("rocks.identity.flushThreshold", 1_000_000);

    static final long PATTERN_OF_PATTERNS_NID = Nid.compose64(Counters.PATTERN_OF_PATTERNS, Counters.PATTERN_OF_PATTERNS);
    static final long CONCEPT_PATTERN_NID = Nid.compose64(Counters.PATTERN_OF_PATTERNS, Counters.CONCEPT_PATTERN);
    static final long STAMP_PATTERN_NID = Nid.compose64(Counters.PATTERN_OF_PATTERNS, Counters.STAMP_PATTERN);

    private final RocksDB db;
    private final ColumnFamilyHandle handle;
    private final Counters counters;
    private final ConcurrentHashMap<UUID, Long> unwritten = new ConcurrentHashMap<>();
    private final LockTable locks = new LockTable();
    private final ReentrantLock flushLock = new ReentrantLock();

    IdentityMap(RocksDB db, ColumnFamilyHandle handle, Counters counters) {
        this.db = db;
        this.handle = handle;
        this.counters = counters;
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
     * the concept pattern at 2, the stamp pattern at 3, every UUID of each binding mapped.
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
        for (FixedPattern fixed : fixedPatterns()) {
            for (UUID uuid : fixed.uuids()) {
                unwritten.put(uuid, fixed.nid());
            }
        }
    }

    /** Checks that an existing store maps the three bindings to their fixed nids. */
    void verifyBindings() {
        for (FixedPattern fixed : fixedPatterns()) {
            for (UUID uuid : fixed.uuids()) {
                Optional<Long> nid = nid(uuid);
                if (nid.isEmpty() || nid.get() != fixed.nid()) {
                    throw new IllegalStateException("The store maps " + uuid + " of " + fixed.description() + " to "
                            + nid.map(Object::toString).orElse("nothing") + ", not to its fixed nid " + fixed.nid());
                }
            }
        }
    }

    /** The nid of a UUID the store knows, from memory first and then the column. */
    Optional<Long> nid(UUID uuid) {
        Long inMemory = unwritten.get(uuid);
        if (inMemory != null) {
            return Optional.of(inMemory);
        }
        try {
            byte[] stored = db.get(handle, Keys.of(uuid));
            return stored == null ? Optional.empty() : Optional.of(Keys.nid(stored));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    boolean knows(UUID uuid) {
        return nid(uuid).isPresent();
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
            Optional<Long> known = nid(uuid);
            if (known.isPresent()) {
                return known.get();
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
        Long existing = existing(uuids);
        if (existing != null && allMapped(uuids)) {
            return existing;
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
            } else {
                nid = known;
            }
            for (UUID uuid : uuids) {
                if (!knows(uuid)) {
                    unwritten.put(uuid, nid);
                }
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
            Optional<Long> nid = nid(uuid);
            if (nid.isPresent()) {
                return nid.get();
            }
        }
        return null;
    }

    private boolean allMapped(UUID[] uuids) {
        for (UUID uuid : uuids) {
            if (!knows(uuid)) {
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
            nid(uuid).ifPresent(nids::add);
        }
        if (nids.size() > 1) {
            IdentityAdvisories.componentsShareUuids(List.of(uuids), nids);
        }
    }

    /** Writes the entries held in memory to the column, if there are more than the threshold. */
    void flushIfLarge() {
        if (unwritten.size() > FLUSH_THRESHOLD && flushLock.tryLock()) {
            try {
                flush();
            } finally {
                flushLock.unlock();
            }
        }
    }

    /** Writes every entry held in memory to the column, and drops it from memory once written. */
    void flush() {
        flushLock.lock();
        try {
            List<UUID> uuids = new ArrayList<>(unwritten.keySet());
            if (uuids.isEmpty()) {
                return;
            }
            try (WriteOptions options = new WriteOptions()) {
                for (int from = 0; from < uuids.size(); from += 16_384) {
                    int to = Math.min(uuids.size(), from + 16_384);
                    List<UUID> written = new ArrayList<>(to - from);
                    try (WriteBatch batch = new WriteBatch()) {
                        for (int i = from; i < to; i++) {
                            UUID uuid = uuids.get(i);
                            Long nid = unwritten.get(uuid);
                            if (nid != null) {
                                batch.put(handle, Keys.of(uuid), Keys.of(nid));
                                written.add(uuid);
                            }
                        }
                        db.write(options, batch);
                    }
                    // Removed only after the write landed, so a lookup finds the entry in memory or in the column.
                    written.forEach(unwritten::remove);
                }
            } catch (RocksDBException e) {
                throw new RuntimeException("Could not write the identity map", e);
            }
            LOG.debug("Flushed {} identities", uuids.size());
        } finally {
            flushLock.unlock();
        }
    }

    int unwrittenCount() {
        return unwritten.size();
    }
}
