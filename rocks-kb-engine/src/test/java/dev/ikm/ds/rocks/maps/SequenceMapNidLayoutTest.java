package dev.ikm.ds.rocks.maps;

import dev.ikm.tinkar.common.id.impl.KeyUtil;
import dev.ikm.tinkar.common.id.impl.NidCodec8;
import dev.ikm.tinkar.common.service.IncompatibleNidLayoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link SequenceMap}'s nid-layout detection and pattern-sequence
 * allocation limit against a real RocksDB instance (IKE-Network/ike-issues#1138).
 */
class SequenceMapNidLayoutTest {

    /** Pattern-of-patterns sequence under the 6-bit layout. */
    private static final int SIX_BIT_PATTERN_PATTERN_SEQUENCE = 63;

    @TempDir
    Path tempDir;

    private DBOptions dbOptions;
    private RocksDB db;
    private List<ColumnFamilyHandle> handles;

    @BeforeEach
    void openDb() throws RocksDBException {
        RocksDB.loadLibrary();
        List<ColumnFamilyDescriptor> descriptors = List.of(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
        handles = new ArrayList<>();
        dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);
        db = RocksDB.open(dbOptions, tempDir.resolve("rocks").toString(), descriptors, handles);
    }

    @AfterEach
    void closeDb() {
        for (ColumnFamilyHandle handle : handles) {
            handle.close();
        }
        db.close();
        dbOptions.close();
    }

    private ColumnFamilyHandle defaultHandle() {
        return handles.get(0);
    }

    private void writeCounters(Map<Integer, Long> counters) throws RocksDBException {
        for (Map.Entry<Integer, Long> counter : counters.entrySet()) {
            db.put(defaultHandle(), KeyUtil.intToByteArray(counter.getKey()),
                    KeyUtil.longToByteArray(counter.getValue()));
        }
    }

    private Map<Integer, Long> readCounters() {
        Map<Integer, Long> counters = new TreeMap<>();
        try (RocksIterator it = db.newIterator(defaultHandle())) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                counters.put(KeyUtil.byteArrayToInt(it.key()), KeyUtil.byteArrayToLong(it.value()));
            }
        }
        return counters;
    }

    @Test
    void emptyDatabase_bootstrapsThePatternPatternCounterAt255() {
        SequenceMap sequenceMap = new SequenceMap(db, defaultHandle());

        assertEquals(255, SequenceMap.PATTERN_PATTERN_SEQUENCE);
        assertTrue(sequenceMap.nextSequenceMap.containsKey(NidCodec8.PATTERN_PATTERN_SEQUENCE));

        sequenceMap.save();
        assertTrue(readCounters().containsKey(NidCodec8.PATTERN_PATTERN_SEQUENCE),
                "the persisted counter table identifies the database as 8-bit");
    }

    @Test
    void savedEightBitDatabase_reopens() {
        SequenceMap created = new SequenceMap(db, defaultHandle());
        int patternSequence = created.nextPatternSequence();
        created.save();

        SequenceMap reopened = new SequenceMap(db, defaultHandle());
        assertTrue(reopened.nextSequenceMap.containsKey(patternSequence));
    }

    @Test
    void sixBitDatabase_isRefusedAndLeftUnmodified() throws RocksDBException {
        Map<Integer, Long> sixBit = new TreeMap<>(Map.of(
                1, 16L,
                2, 519_122L,
                7, 1_670_516L,
                SIX_BIT_PATTERN_PATTERN_SEQUENCE, 16L));
        writeCounters(sixBit);

        IncompatibleNidLayoutException refused = assertThrows(IncompatibleNidLayoutException.class,
                () -> new SequenceMap(db, defaultHandle()));

        assertTrue(refused.getMessage().contains("6-bit"), refused.getMessage());
        assertTrue(refused.getMessage().contains("Export"), refused.getMessage());
        assertEquals(db.getName(), refused.dataStorePath());
        assertTrue(dev.ikm.tinkar.common.service.NonRetryableStartupFailure.class.isInstance(refused),
                "a refusal is terminal, not retried");
        assertEquals(sixBit, readCounters(), "a refused database is not written to");
    }

    @Test
    void eightBitDatabase_withAnOrdinaryPatternAt63_opens() throws RocksDBException {
        // Under the 8-bit layout 63 is an ordinary pattern sequence, so its
        // presence alone must not read as a 6-bit database.
        writeCounters(Map.of(
                SIX_BIT_PATTERN_PATTERN_SEQUENCE, 5L,
                NidCodec8.PATTERN_PATTERN_SEQUENCE, 70L));

        SequenceMap sequenceMap = new SequenceMap(db, defaultHandle());

        assertEquals(70L, sequenceMap.nextSequenceMap.get(NidCodec8.PATTERN_PATTERN_SEQUENCE).get());
    }

    @Test
    void checkNidLayout_acceptsEmptyAndEightBit_refusesSixBit() {
        SequenceMap.checkNidLayout("db", java.util.Set.of());
        SequenceMap.checkNidLayout("db", java.util.Set.of(2, 255));
        assertThrows(IncompatibleNidLayoutException.class,
                () -> SequenceMap.checkNidLayout("db", java.util.Set.of(2, 63)));
    }

    @Test
    void nextPatternSequence_stopsAt254_neverIssuingThePatternPatternSequence() {
        SequenceMap sequenceMap = new SequenceMap(db, defaultHandle());
        sequenceMap.nextSequenceMap.get(NidCodec8.PATTERN_PATTERN_SEQUENCE)
                .set(NidCodec8.MAX_ASSIGNABLE_PATTERN_SEQUENCE);

        assertEquals(254, sequenceMap.nextPatternSequence());

        IllegalStateException full = assertThrows(IllegalStateException.class,
                sequenceMap::nextPatternSequence);
        assertTrue(full.getMessage().contains("Pattern limit reached"), full.getMessage());
        assertThrows(IllegalStateException.class, sequenceMap::nextPatternSequence,
                "the limit holds on every later attempt");
        assertEquals(255L, sequenceMap.nextSequenceMap.get(NidCodec8.PATTERN_PATTERN_SEQUENCE).get(),
                "the counter stops at the limit instead of advancing past it");
    }
}
