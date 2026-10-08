package dev.ikm.ds.rocks64;

import dev.ikm.tinkar.common.id.Nid;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordMapTest {

    private static final long CONCEPT = Nid.compose64(2, 1);
    private static final long STAMP_A = Nid.compose64(3, 1);
    private static final long STAMP_B = Nid.compose64(3, 2);

    private static RecordMap map(TestDb db) {
        return new RecordMap(db.db, db.handle(Rocks64Store.Family.ENTITIES), db.handle(Rocks64Store.Family.REFERENCES), stamp -> false);
    }

    @Test
    void aRecordIsReadBackAtOnceAndWrittenBehind(@TempDir File dir) {
        try (TestDb db = new TestDb(dir)) {
            RecordMap map = map(db);
            byte[] record = Records.concept(CONCEPT, STAMP_A);
            map.put(CONCEPT, record);
            assertArrayEquals(record, map.get(CONCEPT), "read while pending");
            map.awaitPendingWrites();
            assertArrayEquals(record, map.stored(CONCEPT), "written behind");
            assertArrayEquals(record, map.get(CONCEPT), "read after the write");
            assertEquals(1, map.flushed());
            map.close();
        }
    }

    @Test
    void aSecondVersionUnionsWithTheFirstAtEveryStep(@TempDir File dir) {
        try (TestDb db = new TestDb(dir)) {
            RecordMap map = map(db);
            map.put(CONCEPT, Records.concept(CONCEPT, STAMP_A));
            map.awaitPendingWrites();
            map.put(CONCEPT, Records.concept(CONCEPT, STAMP_B));
            assertEquals(2, Records.versionCount(map.get(CONCEPT)), "pending unioned with stored");
            map.awaitPendingWrites();
            assertEquals(2, Records.versionCount(map.stored(CONCEPT)), "the writer unioned with stored");
            map.put(CONCEPT, Records.concept(CONCEPT, STAMP_A));
            map.put(CONCEPT, Records.concept(CONCEPT, STAMP_B));
            assertEquals(2, Records.versionCount(map.get(CONCEPT)), "the same versions again change nothing");
            map.close();
            assertEquals(2, Records.versionCount(map.stored(CONCEPT)));
        }
    }

    /** More puts than a batch holds: every one is written and counted, so a wait for the writer ends. */
    @Test
    void everyPutOfSeveralBatchesIsWrittenAndCounted(@TempDir File dir) {
        try (TestDb db = new TestDb(dir)) {
            RecordMap map = map(db);
            int puts = 40_000;
            for (int element = 1; element <= puts; element++) {
                long nid = Nid.compose64(2, element);
                map.put(nid, Records.concept(nid, STAMP_A));
            }
            map.awaitPendingWrites();
            assertEquals(puts, map.enqueued());
            assertEquals(puts, map.flushed(), "every queued entry counted");
            for (int element = 1; element <= puts; element += 997) {
                assertNotNull(map.stored(Nid.compose64(2, element)), "element " + element + " written");
            }
            assertNotNull(map.stored(Nid.compose64(2, puts)));
            map.close();
        }
    }

    @Test
    void aMissingRecordIsNull(@TempDir File dir) {
        try (TestDb db = new TestDb(dir)) {
            RecordMap map = map(db);
            assertNull(map.get(Nid.compose64(2, 99)));
            map.close();
        }
    }

    @Test
    void severalRecordsAreReadAtOnce(@TempDir File dir) {
        try (TestDb db = new TestDb(dir)) {
            RecordMap map = map(db);
            long first = Nid.compose64(2, 1);
            long second = Nid.compose64(2, 2);
            long missing = Nid.compose64(2, 3);
            map.put(first, Records.concept(first, STAMP_A));
            map.awaitPendingWrites();
            map.put(second, Records.concept(second, STAMP_A));
            byte[][] records = map.getAll(new long[]{first, second, missing});
            assertNotNull(records[0]);
            assertNotNull(records[1], "pending records are read too");
            assertNull(records[2]);
            map.close();
        }
    }

    @Test
    void referencesArePendingUntilWritten(@TempDir File dir) throws Exception {
        try (TestDb db = new TestDb(dir)) {
            RecordMap map = map(db);
            long semantic = Nid.compose64(4, 1);
            map.addReference(CONCEPT, semantic);
            assertTrue(map.pendingReferencesTo(CONCEPT).contains(semantic));
            map.awaitPendingWrites();
            assertTrue(map.pendingReferencesTo(CONCEPT).isEmpty());
            assertNotNull(db.db.get(db.handle(Rocks64Store.Family.REFERENCES), Keys.reference(CONCEPT, semantic)));
            map.close();
        }
    }
}
