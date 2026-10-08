package dev.ikm.ds.rocks64;

import dev.ikm.tinkar.common.id.Nid;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CountersTest {

    private static Counters load(TestDb db) {
        return Counters.load(db.db, db.handle(Rocks64Store.Family.DEFAULT), db.handle(Rocks64Store.Family.ENTITIES));
    }

    @Test
    void aNewStoreHasOnlyThePatternOfPatternsCounter(@TempDir File dir) {
        try (TestDb db = new TestDb(dir)) {
            Counters counters = load(db);
            assertEquals(List.of(Counters.PATTERN_OF_PATTERNS), counters.patternSequences().boxed().toList());
            assertEquals(Counters.FIRST_ELEMENT, counters.next(Counters.PATTERN_OF_PATTERNS));
            assertEquals(1, counters.nextPatternSequence());
            assertEquals(2, counters.nextPatternSequence());
            assertEquals(1, counters.nextElementSequence(2));
            assertEquals(2, counters.nextElementSequence(2));
            assertEquals(3, counters.next(2));
            assertEquals(List.of(1, 2), counters.patternSequences().boxed().toList());
        }
    }

    /** The keys are the truth: a counter behind its last key is raised, one with no keys is kept. */
    @Test
    void countersAreRecoveredFromTheKeySpace(@TempDir File dir) {
        try (TestDb db = new TestDb(dir)) {
            for (int element = 1; element <= 10; element++) {
                db.putRecord(Nid.compose64(5, element), new byte[]{2});
            }
            db.putRecord(Nid.compose64(7, 3), new byte[]{2});
            db.putCounter(5, 4);
            db.putCounter(9, 12);
            Counters counters = load(db);
            assertEquals(11, counters.next(5), "raised past the last key");
            assertEquals(4, counters.next(7), "recovered from the keys alone");
            assertEquals(12, counters.next(9), "kept where there are no keys");
            assertEquals(List.of(1, 5, 7, 9), counters.patternSequences().boxed().toList());
        }
    }

    @Test
    void countersSurviveASave(@TempDir File dir) throws Exception {
        try (TestDb db = new TestDb(dir)) {
            Counters counters = load(db);
            counters.nextPatternSequence();
            counters.nextPatternSequence();
            counters.nextElementSequence(2);
            try (WriteBatch batch = new WriteBatch(); WriteOptions options = new WriteOptions()) {
                counters.save(batch, db.handle(Rocks64Store.Family.DEFAULT));
                db.db.write(options, batch);
            }
            Counters reloaded = load(db);
            assertEquals(3, reloaded.next(1));
            assertEquals(2, reloaded.next(2));
            assertEquals(counters.report(), reloaded.report());
        }
    }

    @Test
    void aCounterStopsAtTheCeilingAndNeverWraps(@TempDir File dir) {
        try (TestDb db = new TestDb(dir)) {
            db.putCounter(4, Nid.MAX_SEQUENCE_64);
            Counters counters = load(db);
            assertEquals(Nid.MAX_SEQUENCE_64, counters.nextElementSequence(4));
            assertThrows(IllegalStateException.class, () -> counters.nextElementSequence(4));
            assertThrows(IllegalStateException.class, () -> counters.nextElementSequence(4));
            assertEquals(Nid.MAX_SEQUENCE_64 + 1, counters.next(4));
        }
    }

    @Test
    void anOutOfRangePatternIsRefused(@TempDir File dir) {
        try (TestDb db = new TestDb(dir)) {
            Counters counters = load(db);
            assertThrows(IllegalArgumentException.class, () -> counters.nextElementSequence(0));
            assertThrows(IllegalArgumentException.class, () -> counters.nextElementSequence(Integer.MAX_VALUE));
        }
    }
}
