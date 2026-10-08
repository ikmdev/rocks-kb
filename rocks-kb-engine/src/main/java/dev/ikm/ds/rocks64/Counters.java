package dev.ikm.ds.rocks64;

import dev.ikm.tinkar.common.id.Nid;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * The per-pattern element counters, the store's nid generator: one {@link AtomicInteger} per
 * pattern sequence, in an array indexed by the sequence, each stopping at
 * {@value Nid#MAX_SEQUENCE_64} and never wrapping. The pattern-of-patterns' counter, at
 * sequence {@value #PATTERN_OF_PATTERNS}, issues the pattern sequences themselves.
 *
 * <p>The counters are persisted in the default column family at every save, but correctness
 * does not rest on that: at open each counter is set to one past the highest key its pattern
 * holds, so a crash between a flush of the entity map and a save can never reissue a sequence
 * (design {@code design-2026-10-07-64-bit-rocks-store}, "The counters").
 */
final class Counters {

    private static final Logger LOG = LoggerFactory.getLogger(Counters.class);

    /** The pattern-of-patterns: the pattern whose elements are the patterns. */
    static final int PATTERN_OF_PATTERNS = 1;
    /** The concept pattern: element 2 of the pattern-of-patterns (settled 2026-10-07). */
    static final int CONCEPT_PATTERN = 2;
    /** The stamp pattern: element 3 of the pattern-of-patterns. */
    static final int STAMP_PATTERN = 3;
    /** The first pattern sequence a semantic pattern can have; the three before it are fixed. */
    static final int FIRST_SEMANTIC_PATTERN = 4;
    /** The first element sequence of every pattern. */
    static final int FIRST_ELEMENT = 1;

    private volatile AtomicInteger[] next;
    private final Object growth = new Object();

    private Counters(AtomicInteger[] next) {
        this.next = next;
    }

    /**
     * Loads the counters: what the default column family stores, raised to one past the highest
     * key of each pattern in the entity map, patterns the column family does not know included.
     */
    static Counters load(RocksDB db, ColumnFamilyHandle counters, ColumnFamilyHandle entities) {
        AtomicInteger[] next = new AtomicInteger[64];
        try (RocksIterator it = db.newIterator(counters)) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                byte[] key = it.key();
                byte[] value = it.value();
                if (key.length == 4 && value.length == 4) {
                    int pattern = ByteBuffer.wrap(key).getInt();
                    int stored = ByteBuffer.wrap(value).getInt();
                    next = ensure(next, pattern);
                    next[pattern] = new AtomicInteger(stored);
                }
            }
        }
        // The key space is the truth: every pattern with a key, at one past its last element.
        try (ReadOptions ro = new ReadOptions().setTotalOrderSeek(true); RocksIterator it = db.newIterator(entities, ro)) {
            it.seekToFirst();
            while (it.isValid()) {
                long firstKey = ByteBuffer.wrap(it.key()).getLong();
                int pattern = Nid.patternSequence64(firstKey);
                it.seekForPrev(Keys.of(Nid.compose64(pattern, Nid.MAX_SEQUENCE_64)));
                int last = Nid.elementSequence64(ByteBuffer.wrap(it.key()).getLong());
                next = ensure(next, pattern);
                AtomicInteger counter = next[pattern];
                if (counter == null) {
                    LOG.warn("Pattern {} holds keys up to element {} but had no stored counter; recovered", pattern, last);
                    next[pattern] = new AtomicInteger(last + 1);
                } else if (counter.get() <= last) {
                    LOG.warn("Pattern {}'s stored counter {} is behind its last element {}; recovered", pattern, counter.get(), last);
                    counter.set(last + 1);
                }
                if (pattern >= Nid.MAX_SEQUENCE_64) {
                    break;
                }
                it.seek(Keys.of(Nid.compose64(pattern + 1, FIRST_ELEMENT)));
            }
        }
        Counters loaded = new Counters(next);
        if (next[PATTERN_OF_PATTERNS] == null) {
            loaded.ensureCounter(PATTERN_OF_PATTERNS);
        }
        return loaded;
    }

    private static AtomicInteger[] ensure(AtomicInteger[] array, int pattern) {
        if (pattern < array.length) {
            return array;
        }
        return Arrays.copyOf(array, Math.max(pattern + 1, array.length * 2));
    }

    /** The counter of a pattern, created at the first element if the pattern has none yet. */
    private AtomicInteger ensureCounter(int patternSequence) {
        AtomicInteger[] array = next;
        if (patternSequence < array.length && array[patternSequence] != null) {
            return array[patternSequence];
        }
        synchronized (growth) {
            array = ensure(next, patternSequence);
            if (array[patternSequence] == null) {
                array[patternSequence] = new AtomicInteger(FIRST_ELEMENT);
            }
            next = array;
            return array[patternSequence];
        }
    }

    /**
     * Issues the next element sequence of a pattern.
     *
     * @throws IllegalStateException if the pattern has issued every sequence it may
     */
    int nextElementSequence(int patternSequence) {
        if (patternSequence < 1 || patternSequence > Nid.MAX_SEQUENCE_64) {
            throw new IllegalArgumentException("pattern sequence " + patternSequence + " is out of range");
        }
        AtomicInteger counter = ensureCounter(patternSequence);
        // One fetch-and-add: a compare-and-set loop here contended across every thread of an
        // import registering one pattern's members (IKE-Network/ike-issues#1273). The counter
        // stops at the ceiling: a call past it puts the counter back there, so it never wraps
        // into a sequence, and a value below the first element can only be a wrap caught here.
        int issued = counter.getAndIncrement();
        if (issued > Nid.MAX_SEQUENCE_64 || issued < FIRST_ELEMENT) {
            counter.set(Nid.MAX_SEQUENCE_64 + 1);
            throw new IllegalStateException("Pattern " + patternSequence + " has issued every element sequence it may, "
                    + Nid.MAX_SEQUENCE_64 + "; no new element can be created in it");
        }
        return issued;
    }

    /** Issues the next pattern sequence, and gives the new pattern its counter. */
    int nextPatternSequence() {
        int patternSequence = nextElementSequence(PATTERN_OF_PATTERNS);
        ensureCounter(patternSequence);
        return patternSequence;
    }

    /** One past the last element sequence a pattern has issued; {@value #FIRST_ELEMENT} for a pattern without a counter. */
    int next(int patternSequence) {
        AtomicInteger[] array = next;
        if (patternSequence < 0 || patternSequence >= array.length || array[patternSequence] == null) {
            return FIRST_ELEMENT;
        }
        return array[patternSequence].get();
    }

    /** The pattern sequences that have a counter, ascending. */
    IntStream patternSequences() {
        AtomicInteger[] array = next;
        return IntStream.range(1, array.length).filter(pattern -> array[pattern] != null);
    }

    /** Writes every counter into the batch, a four-byte key and a four-byte value each. */
    void save(WriteBatch batch, ColumnFamilyHandle counters) throws RocksDBException {
        AtomicInteger[] array = next;
        for (int pattern = 1; pattern < array.length; pattern++) {
            if (array[pattern] != null) {
                batch.put(counters, ByteBuffer.allocate(4).putInt(pattern).array(),
                        ByteBuffer.allocate(4).putInt(array[pattern].get()).array());
            }
        }
    }

    String report() {
        StringBuilder report = new StringBuilder();
        patternSequences().forEach(pattern -> report.append(pattern).append('=').append(next(pattern)).append(' '));
        return report.toString();
    }
}
