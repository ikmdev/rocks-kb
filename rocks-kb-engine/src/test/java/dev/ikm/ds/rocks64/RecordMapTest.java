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

    private static RecordMap ingestingMap(TestDb db) {
        return new RecordMap(db.db, db.handle(Rocks64Store.Family.ENTITIES), db.handle(Rocks64Store.Family.REFERENCES), stamp -> false, db.recordIngest());
    }

    /** A load phase: fresh records join their pattern's run, readable at once, ingested in nid order when the phase ends. */
    @Test
    void freshRecordsOfALoadPhaseAreIngestedInNidOrder(@TempDir File dir) {
        try (TestDb db = new TestDb(dir)) {
            RecordMap map = ingestingMap(db);
            map.setLoadPhase(true);
            long a = Nid.compose64(3, 1);
            long b = Nid.compose64(3, 2);
            long c = Nid.compose64(4, 1);
            byte[] recordA = Records.concept(a, STAMP_A);
            byte[] recordB = Records.concept(b, STAMP_A);
            byte[] recordC = Records.concept(c, STAMP_A);
            map.put(b, recordB, true); // offered out of nid order on purpose
            map.put(a, recordA, true);
            map.put(c, recordC, true);
            assertArrayEquals(recordA, map.get(a), "readable from its run");
            assertNull(map.stored(a), "not in RocksDB yet");
            map.put(a, Records.concept(a, STAMP_B), true);
            assertEquals(2, Records.versionCount(map.get(a)), "a second version unions into the run");
            map.setLoadPhase(false);
            assertEquals(3, map.recordsIngested());
            assertEquals(2, Records.versionCount(map.stored(a)), "ingested with both versions");
            assertArrayEquals(recordC, map.stored(c));
            assertArrayEquals(recordB, map.get(b), "read after the phase");
            assertEquals(0, map.flushed(), "nothing went through the writer");
            map.close();
        }
    }

    /** A record that is not fresh goes through the writer even in a load phase, unioned with what is stored. */
    @Test
    void aRecordWithSomethingStoredGoesThroughTheWriterInALoadPhase(@TempDir File dir) {
        try (TestDb db = new TestDb(dir)) {
            RecordMap map = ingestingMap(db);
            map.put(CONCEPT, Records.concept(CONCEPT, STAMP_A));
            map.awaitPendingWrites();
            map.setLoadPhase(true);
            map.put(CONCEPT, Records.concept(CONCEPT, STAMP_B), false);
            map.setLoadPhase(false);
            map.awaitPendingWrites();
            assertEquals(2, Records.versionCount(map.stored(CONCEPT)));
            assertEquals(0, map.recordsIngested());
            map.close();
        }
    }

    /** A load phase's references are collected, sorted, written once, and pending until then. */
    @Test
    void referencesOfALoadPhaseAreIngestedSorted(@TempDir File dir) throws Exception {
        try (TestDb db = new TestDb(dir)) {
            RecordMap map = ingestingMap(db);
            map.setLoadPhase(true);
            long first = Nid.compose64(4, 1);
            long second = Nid.compose64(4, 2);
            map.addReference(CONCEPT, second);
            map.addReference(CONCEPT, first);
            map.addReference(CONCEPT, first); // the same reference twice
            assertTrue(map.pendingReferencesTo(CONCEPT).containsAll(java.util.Set.of(first, second)));
            assertNull(db.db.get(db.handle(Rocks64Store.Family.REFERENCES), Keys.reference(CONCEPT, first)), "not in RocksDB yet");
            map.setLoadPhase(false);
            assertTrue(map.pendingReferencesTo(CONCEPT).isEmpty());
            assertNotNull(db.db.get(db.handle(Rocks64Store.Family.REFERENCES), Keys.reference(CONCEPT, first)));
            assertNotNull(db.db.get(db.handle(Rocks64Store.Family.REFERENCES), Keys.reference(CONCEPT, second)));
            assertEquals(3, map.referencesIngested());
            map.awaitPendingWrites();
            map.close();
        }
    }

    /**
     * Many threads putting fresh records of one pattern while runs fill and seal: every record
     * is readable the moment its put returns, as the store's merge requires, and every one is
     * in RocksDB when the phase ends. With runs this small, a few thousand puts seal hundreds.
     */
    @Test
    void freshPutsUnderContentionAreAlwaysReadableAndAllIngested(@TempDir File dir) throws Exception {
        try (TestDb db = new TestDb(dir)) {
            RecordMap map = new RecordMap(db.db, db.handle(Rocks64Store.Family.ENTITIES), db.handle(Rocks64Store.Family.REFERENCES),
                    stamp -> false, db.recordIngest(4096, 64));
            map.setLoadPhase(true);
            int threads = 8;
            int perThread = 2_000;
            java.util.concurrent.atomic.AtomicInteger unreadable = new java.util.concurrent.atomic.AtomicInteger();
            java.util.List<Thread> workers = new java.util.ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int from = t * perThread;
                Thread worker = new Thread(() -> {
                    for (int i = 0; i < perThread; i++) {
                        long nid = Nid.compose64(3, from + i + 1);
                        long referencing = Nid.compose64(4, from + i + 1);
                        map.put(nid, Records.concept(nid, STAMP_A), true);
                        map.addReference(nid, referencing);
                        if (map.get(nid) == null) {
                            unreadable.incrementAndGet();
                        }
                    }
                });
                workers.add(worker);
                worker.start();
            }
            for (Thread worker : workers) {
                worker.join();
            }
            assertEquals(0, unreadable.get(), "records unreadable right after their put");
            map.setLoadPhase(false);
            assertEquals((long) threads * perThread, map.recordsIngested(), "records ingested");
            assertEquals((long) threads * perThread, map.referencesIngested(), "references ingested");
            int missing = 0;
            for (int i = 1; i <= threads * perThread; i++) {
                if (map.stored(Nid.compose64(3, i)) == null) {
                    missing++;
                }
                if (db.db.get(db.handle(Rocks64Store.Family.REFERENCES), Keys.reference(Nid.compose64(3, i), Nid.compose64(4, i))) == null) {
                    missing++;
                }
            }
            assertEquals(0, missing, "records or references missing from RocksDB after the phase");
            map.close();
        }
    }

    /**
     * A record offered to a run that another thread is sealing at that instant must still read
     * back: the SNOMED CT import (2026-10-08) read nine records as absent right after their put,
     * every one of them in a run that had left the open runs but had not yet joined the sealed
     * set. Many threads on one pattern with small runs make that hand-off frequent.
     */
    @Test
    void putsRacingASealAreReadable(@TempDir File dir) throws Exception {
        try (TestDb db = new TestDb(dir)) {
            RecordMap map = new RecordMap(db.db, db.handle(Rocks64Store.Family.ENTITIES), db.handle(Rocks64Store.Family.REFERENCES),
                    stamp -> false, db.recordIngest(4096, 64));
            map.setLoadPhase(true);
            int threads = 32;
            int perThread = 2_000;
            java.util.concurrent.atomic.AtomicInteger unreadable = new java.util.concurrent.atomic.AtomicInteger();
            java.util.List<Thread> workers = new java.util.ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int from = t * perThread;
                Thread worker = new Thread(() -> {
                    for (int i = 0; i < perThread; i++) {
                        long nid = Nid.compose64(3, from + i + 1);
                        map.put(nid, Records.concept(nid, STAMP_A), true);
                        if (map.get(nid) == null) {
                            unreadable.incrementAndGet();
                        }
                    }
                });
                workers.add(worker);
                worker.start();
            }
            for (Thread worker : workers) {
                worker.join();
            }
            map.setLoadPhase(false);
            assertEquals(0, unreadable.get(), "records unreadable right after their put");
            assertEquals((long) threads * perThread, map.recordsIngested(), "records ingested");
            int missing = 0;
            for (int i = 1; i <= threads * perThread; i++) {
                if (map.stored(Nid.compose64(3, i)) == null) {
                    missing++;
                }
            }
            assertEquals(0, missing, "records missing from RocksDB after the phase");
            map.close();
        }
    }

    /**
     * A put that saw the phase on an instant before it ended must not leave a run behind: the
     * switch waits for every offer in flight, and the drain that follows it finds every run.
     */
    @Test
    void putsRacingThePhaseEndLeaveNoRunBehind(@TempDir File dir) throws Exception {
        try (TestDb db = new TestDb(dir)) {
            RecordMap map = new RecordMap(db.db, db.handle(Rocks64Store.Family.ENTITIES), db.handle(Rocks64Store.Family.REFERENCES),
                    stamp -> false, db.recordIngest(4096, 64));
            map.setLoadPhase(true);
            int threads = 16;
            int perThread = 3_000;
            java.util.concurrent.CountDownLatch started = new java.util.concurrent.CountDownLatch(threads);
            java.util.List<Thread> workers = new java.util.ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int from = t * perThread;
                Thread worker = new Thread(() -> {
                    started.countDown();
                    for (int i = 0; i < perThread; i++) {
                        long nid = Nid.compose64(3, from + i + 1);
                        map.put(nid, Records.concept(nid, STAMP_A), true);
                    }
                });
                workers.add(worker);
                worker.start();
            }
            started.await();
            Thread.sleep(20);
            map.setLoadPhase(false);
            int openRuns = map.openRuns();
            for (Thread worker : workers) {
                worker.join();
            }
            assertEquals(0, openRuns, "runs open right after the phase ended");
            map.awaitPendingWrites();
            int missing = 0;
            for (int i = 1; i <= threads * perThread; i++) {
                if (map.stored(Nid.compose64(3, i)) == null) {
                    missing++;
                }
            }
            assertEquals(0, missing, "records missing from RocksDB after the phase and the queue drained");
            map.close();
        }
    }

    @Test
    void sortPairsOrdersByBothKeys() {
        long[] a = {5, 3, 5, 1, 3, 5, 2, 9, 9, 0, 7, 7, 7, 4, 6, 8, 5, 3, 1, 2};
        long[] b = {2, 9, 1, 1, 3, 0, 2, 1, 0, 5, 7, 1, 3, 4, 6, 8, 2, 3, 0, 2};
        RecordMap.sortPairs(a, b, a.length);
        for (int i = 1; i < a.length; i++) {
            assertTrue(a[i - 1] < a[i] || (a[i - 1] == a[i] && b[i - 1] <= b[i]), "sorted at " + i);
        }
        assertEquals(0, a[0]);
        assertEquals(9, a[a.length - 1]);
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
