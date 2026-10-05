package dev.ikm.ds.rocks.maps;

import dev.ikm.tinkar.common.id.EntityKey;
import dev.ikm.tinkar.common.id.impl.KeyUtil;
import dev.ikm.ds.rocks.spliterator.LongSpliteratorOfPattern;
import dev.ikm.ds.rocks.spliterator.SpliteratorForEntityKeys;
import dev.ikm.ds.rocks.spliterator.SpliteratorForLongKeyOfPattern;
import dev.ikm.tinkar.common.id.impl.NidLayout;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.terms.EntityBinding;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.impl.factory.Lists;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


public class SequenceMap extends RocksDbMap<RocksDB> {
    private static final Logger LOG = LoggerFactory.getLogger(SequenceMap.class);

    public static final int FIRST_ELEMENT_SEQUENCE_OF_PATTERN = 1;
    private static int nextPatternElementSequence = FIRST_ELEMENT_SEQUENCE_OF_PATTERN;

    /** The UUID of the pattern-of-patterns ({@link EntityBinding.Pattern#pattern()}, made from this one UUID). */
    public static final UUID PATTERN_PATTERN_UUID = EntityBinding.Pattern.pattern().leastUuid();
    private static final int patternPatternElementSequence = nextPatternElementSequence++;

    /**
     * Pattern sequence under which every pattern is keyed (the pattern-of-patterns):
     * 63 in a 6-bit database, 255 in an 8-bit one (ike-issues#1138). Its counter
     * also issues the pattern sequences of ordinary patterns.
     *
     * @return the pattern-of-patterns sequence of the open database's layout
     */
    public static int patternPatternSequence() {
        return NidLayout.active().patternPatternSequence();
    }

    public static EntityKey patternPatternEntityKey() {
        return EntityKey.of(patternPatternSequence(), patternPatternElementSequence);
    }
    /**
     * TODO: Temporary fixed UUID until concepts provide their own pattern PublicId field (currently only semantics do).
     */
    public static final UUID conceptPatternUUID = UUID.fromString("8e9a8888-9d06-45c8-af47-aacc78ed66ee");
    private static final int conceptPatternElementSequence = nextPatternElementSequence++;

    public static EntityKey conceptPatternEntityKey() {
        return EntityKey.of(patternPatternSequence(), conceptPatternElementSequence);
    }
    /**
     * TODO: Temporary fixed UUID until concepts provide their own pattern PublicId field (currently only semantics do).
     */
    public static final UUID stampPatternUUID = UUID.fromString("15687f5d-6028-4491-b005-7bb6f9f6ebad");
    private static final int stampPatternElementSequence = nextPatternElementSequence++;

    public static EntityKey stampPatternEntityKey() {
        return EntityKey.of(patternPatternSequence(), stampPatternElementSequence);
    }

    /**
     * Map of pattern sequences to atomic counters used to generate a unique, ordered, sequence for each new element.
     */
    public final ConcurrentHashMap<Integer, AtomicLong> nextSequenceMap = new ConcurrentHashMap<>();

    public SequenceMap(RocksDB db, ColumnFamilyHandle mapHandle) {
        super(db, mapHandle);
        open();
    }

    public String sequenceReport() {
        // {1=16, 2=519122, 3=743, 4=400755, 5=371292, 6=371292, 7=1670516, 8=1037842, 9=320, 10=399114, 11=418, 12=969, 13=2, 14=4, 15=2, 63=16}
        StringBuilder sequenceReport = new StringBuilder();

        sequenceReport.append("Sequences\n").append(nextSequenceMap).append("\n");

        for (Map.Entry<Integer, AtomicLong> entry : nextSequenceMap.entrySet().stream().sorted(Map.Entry.comparingByKey()).toList()) {
            EntityKey patternKey = EntityKey.of(patternPatternSequence(), entry.getKey());
            int patternNid = NidLayout.active().encode(patternKey.patternSequence(), patternKey.elementSequence());
            String patternName = PrimitiveData.textWithNid(patternNid);
            sequenceReport.append(String.format("%,d=%,d, ", entry.getKey(), entry.getValue().get()));
            sequenceReport.append(String.format(
                    "%s | EntityKey: %,d (0x%016X) | EntityNid: %,d (0x%08X)%n\n",
                    patternName,
                    patternKey.longKey(),          // decimal
                    patternKey.longKey(),          // hex (zero-padded to 16 for a long)
                    patternNid,                    // decimal
                    patternNid                     // hex (zero-padded to 8 for an int)
            ));
        }

        return sequenceReport.toString();
    }

    /**
     * Open the nextSequenceMap from RocksDB and activate the database's nid layout
     * (ike-issues#1138): {@linkplain NidLayout#detect detected} from the loaded
     * counters, or 8-bit for a new database.
     */
    public void open() {
        try (RocksIterator it = rocksIterator()) {
            it.seekToFirst();
            if (it.isValid()) {
                LOG.info("═══════════════════════════════════════════════════════════");
                LOG.info("SequenceMap.open() - Loading from existing RocksDB data");
                LOG.info("═══════════════════════════════════════════════════════════");
                // At least one entry: populate from DB.
                for (; it.isValid(); it.next()) {
                    byte[] keyBytes = it.key();
                    byte[] valueBytes = it.value();
                    if (keyBytes.length == 4 && valueBytes.length == 8) {
                        int key = KeyUtil.byteArrayToInt(keyBytes);
                        long value = KeyUtil.byteArrayToLong(valueBytes);
                        nextSequenceMap.put(key, new AtomicLong(value));
                        LOG.info("  Loaded: pattern={} -> nextSequence={}", key, value);
                    } else {
                        throw new IllegalStateException("key/value lengths out of bounds: " + keyBytes.length + "/" + valueBytes.length);
                    }
                }
                LOG.info("═══════════════════════════════════════════════════════════");
                NidLayout layout = NidLayout.detect(nextSequenceMap.keySet());
                NidLayout.activate(layout);
                if (layout == NidLayout.SIX_BIT) {
                    LOG.warn("SequenceMap.open() - {} uses the 6-bit nid layout (at most {} patterns); "
                            + "opened in 6-bit mode. Migrate it to an 8-bit database for up to {} patterns.",
                            db.getName(), NidLayout.SIX_BIT.patternPatternSequence(),
                            NidLayout.EIGHT_BIT.patternPatternSequence());
                } else {
                    LOG.info("SequenceMap.open() - {} nid layout", layout.displayName());
                }
            } else {
                // A new database always gets the current layout.
                NidLayout.activate(NidLayout.EIGHT_BIT);
                LOG.info("═══════════════════════════════════════════════════════════");
                LOG.info("SequenceMap.open() - Empty DB, initializing bootstrap state");
                LOG.info("  Setting pattern[{}] = {} (PATTERN_PATTERN_SEQUENCE)", 
                        patternPatternSequence(), nextPatternElementSequence);
                LOG.info("═══════════════════════════════════════════════════════════");
                // Column family is empty: do identifier bootstrap initialization.
                nextSequenceMap.put(patternPatternSequence(), new AtomicLong(nextPatternElementSequence)); // Pattern pattern sequences
            }
        }
    }

    /**
     * Save the nextSequenceMap to RocksDB.
     */
    @Override
    protected void writeMemoryToDb() {
        try (WriteBatch batch = new WriteBatch();
             WriteOptions writeOptions = new WriteOptions()) {
            for (Map.Entry<Integer, AtomicLong> entry : nextSequenceMap.entrySet()) {
                int key = entry.getKey();
                long value = entry.getValue().get();
                batch.put(mapHandle, KeyUtil.intToByteArray(key), KeyUtil.longToByteArray(value));
            }
            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void closeMap() {
        save();
    }

    /**
     * Get the next sequence for a given pattern. Represented as a 48-bit unsigned integer inside a long value.
     * @param patternSequence
     * @return next sequence for the given pattern.
     */
    public long nextElementSequence(int patternSequence) {
    AtomicLong counter = nextSequenceMap.computeIfAbsent(patternSequence, 
            k -> {
                LOG.info("Created new counter for pattern {}", patternSequence);
                return new AtomicLong(FIRST_ELEMENT_SEQUENCE_OF_PATTERN);
            });
    return counter.getAndIncrement();
}

    /**
     * Generates the next pattern sequence as a long value. This method ensures that
     * sequences are incrementally generated starting at 1. Sequences are maintained per
     * pattern group using a map with atomic counters to provide thread-safe increments.
     *
     * @return the next pattern sequence value starting from 1.
     * @throws IllegalStateException if every assignable pattern sequence of the
     *         active layout (1..{@link NidLayout#maxAssignablePatternSequence()}) is taken
     */
    public int nextPatternSequence() {
        AtomicLong patternCounter = nextSequenceMap.get(patternPatternSequence());
        int maxAssignable = NidLayout.active().maxAssignablePatternSequence();
        // Never hand out the pattern-of-patterns' own sequence: the counter stops,
        // it does not wrap into it (ike-issues#1138).
        long candidate = patternCounter.getAndUpdate(
                next -> next > maxAssignable ? next : next + 1);
        if (candidate > maxAssignable) {
            throw new IllegalStateException("Pattern limit reached: all " + maxAssignable
                    + " assignable pattern sequences of the " + NidLayout.active().displayName()
                    + " nid layout are in use; no new pattern can be created."
                    + (NidLayout.active() == NidLayout.SIX_BIT
                        ? " Migrate this database to the 8-bit layout for up to "
                          + NidLayout.EIGHT_BIT.maxAssignablePatternSequence() + " patterns."
                        : ""));
        }
        int newPatternSequence = (int) candidate;
        nextSequenceMap.put(newPatternSequence, new AtomicLong(FIRST_ELEMENT_SEQUENCE_OF_PATTERN));
        // Diagnostic for ikmdev/komet-desktop#12: this should fire on every new-pattern publish.
        // If it doesn't, the publish path took the wrong branch in UuidEntityKeyMap.makeEntityKey.
        LOG.info("nextPatternSequence: allocated element {} in PATTERN_PATTERN namespace (counter now {})",
                newPatternSequence, nextSequenceMap.get(patternPatternSequence()).get());
        return newPatternSequence;
    }

    public SpliteratorForEntityKeys allEntityLongKeySpliterator() {
    // Every pattern sequence, the pattern-of-patterns included: its elements are the pattern
    // entities themselves, which a scan of every entity must visit.
    Collection<SpliteratorForLongKeyOfPattern> spliterators = nextSequenceMap.entrySet().stream()
                .map(entry -> new SpliteratorForLongKeyOfPattern(entry.getKey(), FIRST_ELEMENT_SEQUENCE_OF_PATTERN,
                        entry.getValue().get()))
                .toList();
        return new SpliteratorForEntityKeys(spliterators);
    }

    public ImmutableList<SpliteratorForLongKeyOfPattern> allPatternSpliterators() {
        return Lists.immutable.ofAll(
                nextSequenceMap.entrySet().stream()
                .filter(entry -> entry.getKey() != patternPatternSequence()) // Exclude the meta-entry
                .map(entry -> new SpliteratorForLongKeyOfPattern(entry.getKey(), FIRST_ELEMENT_SEQUENCE_OF_PATTERN,
                        entry.getValue().get())).toList());
    }

    public LongSpliteratorOfPattern spliteratorOfPattern(int patternSequence) {
        AtomicLong counter = nextSequenceMap.get(patternSequence);
        if (counter == null) {
            // Pattern has no elements yet — return an empty spliterator
            return new SpliteratorForLongKeyOfPattern(patternSequence, FIRST_ELEMENT_SEQUENCE_OF_PATTERN, FIRST_ELEMENT_SEQUENCE_OF_PATTERN);
        }
        return new SpliteratorForLongKeyOfPattern(patternSequence, FIRST_ELEMENT_SEQUENCE_OF_PATTERN, counter.get());
    }

    public LongSpliteratorOfPattern spliteratorOfPatterns() {
        long maxPatternSequenceExclusive = nextSequenceMap.get(patternPatternSequence()).get();
        return new SpliteratorForLongKeyOfPattern(patternPatternSequence(),  FIRST_ELEMENT_SEQUENCE_OF_PATTERN, maxPatternSequenceExclusive);
    }
}
