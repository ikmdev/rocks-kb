package dev.ikm.ds.rocks.maps;

import dev.ikm.tinkar.common.id.impl.KeyUtil;
import dev.ikm.tinkar.common.id.impl.NidLayout;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link SequenceMap}'s nid-layout detection and per-layout
 * pattern-sequence allocation against a real RocksDB instance
 * (IKE-Network/ike-issues#1138).
 */
class SequenceMapNidLayoutTest {

    private static final int SIX_BIT_PATTERN_PATTERN_SEQUENCE = 63;
    private static final int EIGHT_BIT_PATTERN_PATTERN_SEQUENCE = 255;

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
        // The layout is process-wide; leave it at the default for other tests.
        NidLayout.activate(NidLayout.EIGHT_BIT);
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
    void newDatabase_isEightBit_withThePatternPatternCounterAt255() {
        NidLayout.activate(NidLayout.SIX_BIT); // a previous database's layout must not leak in

        SequenceMap sequenceMap = new SequenceMap(db, defaultHandle());

        assertEquals(NidLayout.EIGHT_BIT, NidLayout.active());
        assertEquals(EIGHT_BIT_PATTERN_PATTERN_SEQUENCE, SequenceMap.patternPatternSequence());
        sequenceMap.save();
        assertTrue(readCounters().containsKey(EIGHT_BIT_PATTERN_PATTERN_SEQUENCE),
                "the persisted counter table identifies the database as 8-bit");
    }

    @Test
    void savedEightBitDatabase_reopensAsEightBit() {
        SequenceMap created = new SequenceMap(db, defaultHandle());
        int patternSequence = created.nextPatternSequence();
        created.save();

        NidLayout.activate(NidLayout.SIX_BIT);
        SequenceMap reopened = new SequenceMap(db, defaultHandle());

        assertEquals(NidLayout.EIGHT_BIT, NidLayout.active());
        assertTrue(reopened.nextSequenceMap.containsKey(patternSequence));
    }

    @Test
    void sixBitDatabase_opensInSixBitMode_andItsCountersAreKept() throws RocksDBException {
        Map<Integer, Long> sixBit = new TreeMap<>(Map.of(
                1, 16L,
                2, 519_122L,
                7, 1_670_516L,
                SIX_BIT_PATTERN_PATTERN_SEQUENCE, 16L));
        writeCounters(sixBit);

        SequenceMap sequenceMap = new SequenceMap(db, defaultHandle());

        assertEquals(NidLayout.SIX_BIT, NidLayout.active());
        assertEquals(SIX_BIT_PATTERN_PATTERN_SEQUENCE, SequenceMap.patternPatternSequence());
        assertEquals(SIX_BIT_PATTERN_PATTERN_SEQUENCE, SequenceMap.patternPatternEntityKey().patternSequence());
        assertFalse(sequenceMap.nextSequenceMap.containsKey(EIGHT_BIT_PATTERN_PATTERN_SEQUENCE),
                "opening a 6-bit database never adds an 8-bit counter");
        assertEquals(sixBit, readCounters(), "opening does not write");

        sequenceMap.save();
        assertEquals(sixBit, readCounters(),
                "saving a 6-bit database keeps it 6-bit — it still reopens in 6-bit mode");
    }

    @Test
    void eightBitDatabase_withAnOrdinaryPatternAt63_isEightBit() throws RocksDBException {
        writeCounters(Map.of(
                SIX_BIT_PATTERN_PATTERN_SEQUENCE, 5L,
                EIGHT_BIT_PATTERN_PATTERN_SEQUENCE, 70L));

        SequenceMap sequenceMap = new SequenceMap(db, defaultHandle());

        assertEquals(NidLayout.EIGHT_BIT, NidLayout.active());
        assertEquals(70L, sequenceMap.nextSequenceMap.get(EIGHT_BIT_PATTERN_PATTERN_SEQUENCE).get());
    }

    @Test
    void eightBit_nextPatternSequence_stopsAt254() {
        SequenceMap sequenceMap = new SequenceMap(db, defaultHandle());
        sequenceMap.nextSequenceMap.get(EIGHT_BIT_PATTERN_PATTERN_SEQUENCE).set(254);

        assertEquals(254, sequenceMap.nextPatternSequence());
        IllegalStateException full = assertThrows(IllegalStateException.class,
                sequenceMap::nextPatternSequence);
        assertTrue(full.getMessage().contains("Pattern limit reached"), full.getMessage());
        assertThrows(IllegalStateException.class, sequenceMap::nextPatternSequence,
                "the limit holds on every later attempt");
        assertEquals(255L, sequenceMap.nextSequenceMap.get(EIGHT_BIT_PATTERN_PATTERN_SEQUENCE).get(),
                "the counter stops at the limit instead of advancing past it");
    }

    @Test
    void sixBit_nextPatternSequence_stopsAt62_andPointsToMigration() throws RocksDBException {
        writeCounters(Map.of(2, 10L, SIX_BIT_PATTERN_PATTERN_SEQUENCE, 62L));
        SequenceMap sequenceMap = new SequenceMap(db, defaultHandle());

        assertEquals(62, sequenceMap.nextPatternSequence());
        IllegalStateException full = assertThrows(IllegalStateException.class,
                sequenceMap::nextPatternSequence);
        assertTrue(full.getMessage().contains("6-bit"), full.getMessage());
        assertTrue(full.getMessage().contains("Migrate"), full.getMessage());
        assertEquals(63L, sequenceMap.nextSequenceMap.get(SIX_BIT_PATTERN_PATTERN_SEQUENCE).get());
    }
}
