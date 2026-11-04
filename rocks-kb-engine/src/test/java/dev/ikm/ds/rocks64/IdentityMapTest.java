package dev.ikm.ds.rocks64;

import dev.ikm.tinkar.common.id.Nid;
import dev.ikm.tinkar.common.id.PublicId;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.terms.EntityBinding;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IdentityMapTest {

    private record Maps(Counters counters, IdentityMap identities) {
    }

    /** Entries from which a test map writes SST files: small, so the tests exercise that path. */
    private static final long SST_THRESHOLD = 1_000;

    private static Maps open(TestDb db) {
        Counters counters = Counters.load(db.db, db.handle(Rocks64Store.Family.DEFAULT), db.handle(Rocks64Store.Family.ENTITIES));
        return new Maps(counters, new IdentityMap(db.db, db.handle(Rocks64Store.Family.IDENTITIES), counters,
                db.identityIngest(SST_THRESHOLD)));
    }

    private static PublicId id(UUID... uuids) {
        return PublicIds.of(uuids);
    }

    /** What a save writes for the identity map, then a new map on the same column, as a reopen builds. */
    private static Maps savedAndReopened(TestDb db, Maps maps) throws RocksDBException {
        maps.identities().flush();
        try (WriteBatch batch = new WriteBatch(); WriteOptions options = new WriteOptions()) {
            maps.counters().save(batch, db.handle(Rocks64Store.Family.DEFAULT));
            maps.identities().saveUnrecorded(batch, db.handle(Rocks64Store.Family.DEFAULT));
            db.db.write(options, batch);
        }
        Maps reopened = open(db);
        reopened.identities().loadUnrecorded(db.handle(Rocks64Store.Family.DEFAULT), db.handle(Rocks64Store.Family.ENTITIES));
        return reopened;
    }

    /** A map with one nid minted and flushed, never given a record, in a session after its mint. */
    private static Maps reopenedWithoutTheMint(TestDb db, Maps maps) throws RocksDBException {
        maps.identities().flush();
        try (WriteBatch batch = new WriteBatch(); WriteOptions options = new WriteOptions()) {
            maps.counters().save(batch, db.handle(Rocks64Store.Family.DEFAULT));
            db.db.write(options, batch);
        }
        return open(db);
    }

    /**
     * A nid with no record costs one pass over the column, however often it is asked for, and the
     * answer is the same each time: the mint is in the column and no longer in memory, as it is
     * for a concept named only in a block once the session that first read it has closed. Without
     * this, drawing one such concept asked for it dozens of times, at seconds a time.
     */
    @Test
    void aNidWithNoRecordIsPassedOverOnceHoweverOftenItIsAskedFor(@TempDir File dir) throws RocksDBException {
        try (TestDb db = new TestDb(dir)) {
            Maps maps = open(db);
            maps.identities().bootstrap();
            UUID uuid = UUID.randomUUID();
            long nid = maps.identities().nidFor(EntityBinding.Concept.pattern(), id(uuid));
            IdentityMap identities = reopenedWithoutTheMint(db, maps).identities();

            for (int i = 0; i < 100; i++) {
                assertEquals(List.of(uuid), identities.publicIdOf(nid).orElseThrow().asUuidList());
            }
            assertEquals(1, identities.passes());
        }
    }

    /** A nid nothing maps to is passed over once too, and stays unknown. */
    @Test
    void aNidNeverMintedIsPassedOverOnce(@TempDir File dir) throws RocksDBException {
        try (TestDb db = new TestDb(dir)) {
            Maps maps = open(db);
            maps.identities().bootstrap();
            maps.identities().nidFor(EntityBinding.Concept.pattern(), id(UUID.randomUUID()));
            IdentityMap identities = reopenedWithoutTheMint(db, maps).identities();

            long never = Nid.compose64(Counters.CONCEPT_PATTERN, 1_000);
            for (int i = 0; i < 100; i++) {
                assertTrue(identities.publicIdOf(never).isEmpty());
            }
            assertEquals(1, identities.passes());
        }
    }

    /** Threads asking at once share one pass: each waits for it and finds the answer. */
    @Test
    void threadsAskingForOneNidShareOnePass(@TempDir File dir) throws Exception {
        try (TestDb db = new TestDb(dir)) {
            Maps maps = open(db);
            maps.identities().bootstrap();
            UUID uuid = UUID.randomUUID();
            long nid = maps.identities().nidFor(EntityBinding.Concept.pattern(), id(uuid));
            IdentityMap identities = reopenedWithoutTheMint(db, maps).identities();

            List<Callable<PublicId>> asks = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                asks.add(() -> identities.publicIdOf(nid).orElseThrow());
            }
            try (ExecutorService executor = Executors.newFixedThreadPool(asks.size())) {
                for (Future<PublicId> answer : executor.invokeAll(asks)) {
                    assertEquals(List.of(uuid), answer.get().asUuidList());
                }
            }
            assertEquals(1, identities.passes());
        }
    }

    /**
     * A later session pays for nothing: what is known of nids with no record outlasts the
     * reopen, a mint and what a pass learned, found or not.
     */
    @Test
    void whatIsKnownOfNidsWithNoRecordOutlastsAReopen(@TempDir File dir) throws RocksDBException {
        try (TestDb db = new TestDb(dir)) {
            Maps first = open(db);
            first.identities().bootstrap();
            UUID learned = UUID.randomUUID();
            long learnedNid = first.identities().nidFor(EntityBinding.Concept.pattern(), id(learned));
            Maps second = reopenedWithoutTheMint(db, first);
            long never = Nid.compose64(Counters.CONCEPT_PATTERN, 1_000);
            assertEquals(List.of(learned), second.identities().publicIdOf(learnedNid).orElseThrow().asUuidList());
            assertTrue(second.identities().publicIdOf(never).isEmpty());
            UUID minted = UUID.randomUUID();
            UUID alsoMinted = UUID.randomUUID();
            long mintedNid = second.identities().nidFor(EntityBinding.Concept.pattern(), id(minted, alsoMinted));

            IdentityMap third = savedAndReopened(db, second).identities();

            assertEquals(List.of(learned), third.publicIdOf(learnedNid).orElseThrow().asUuidList());
            assertEquals(id(minted, alsoMinted).asUuidList().toSet(),
                    third.publicIdOf(mintedNid).orElseThrow().asUuidList().toSet());
            assertTrue(third.publicIdOf(never).isEmpty());
            assertEquals(0, third.passes());
        }
    }

    /** A nid that has since been written, or was written before a failed session's save, is not kept. */
    @Test
    void aWrittenNidIsNotKept(@TempDir File dir) throws RocksDBException {
        try (TestDb db = new TestDb(dir)) {
            Maps maps = open(db);
            maps.identities().bootstrap();
            UUID written = UUID.randomUUID();
            UUID recordedAfterTheSave = UUID.randomUUID();
            long writtenNid = maps.identities().nidFor(EntityBinding.Concept.pattern(), id(written));
            long recordedNid = maps.identities().nidFor(EntityBinding.Concept.pattern(), id(recordedAfterTheSave));
            maps.identities().written(writtenNid);
            Maps saved = savedAndReopened(db, maps);
            assertEquals(List.of(written), saved.identities().publicIdOf(writtenNid).orElseThrow().asUuidList());
            assertEquals(1, saved.identities().passes(), "a written nid was not saved, so its column is read");

            db.db.put(db.handle(Rocks64Store.Family.ENTITIES), Keys.of(recordedNid), new byte[]{1});
            Maps failedOver = open(db);
            failedOver.identities().loadUnrecorded(db.handle(Rocks64Store.Family.DEFAULT), db.handle(Rocks64Store.Family.ENTITIES));
            assertEquals(List.of(recordedAfterTheSave), failedOver.identities().publicIdOf(recordedNid).orElseThrow().asUuidList());
            assertEquals(1, failedOver.identities().passes(), "a nid with a record now is not kept from a save before it");
        }
    }

    /** The memory of a miss never hides a nid: a mint of that very nid answers. */
    @Test
    void aMintAnswersForANidAMissRemembers(@TempDir File dir) {
        try (TestDb db = new TestDb(dir)) {
            Maps maps = open(db);
            maps.identities().bootstrap();
            long next = Nid.compose64(Counters.CONCEPT_PATTERN, maps.counters().next(Counters.CONCEPT_PATTERN));
            assertTrue(maps.identities().publicIdOf(next).isEmpty());

            UUID uuid = UUID.randomUUID();
            assertEquals(next, maps.identities().nidFor(EntityBinding.Concept.pattern(), id(uuid)));
            assertEquals(List.of(uuid), maps.identities().publicIdOf(next).orElseThrow().asUuidList());
        }
    }

    /** An erased nid's memory goes with it: the column no longer answers, and neither does memory. */
    @Test
    void anErasedNidIsForgotten(@TempDir File dir) throws RocksDBException {
        try (TestDb db = new TestDb(dir)) {
            Maps maps = open(db);
            maps.identities().bootstrap();
            UUID uuid = UUID.randomUUID();
            long nid = maps.identities().nidFor(EntityBinding.Concept.pattern(), id(uuid));
            IdentityMap identities = reopenedWithoutTheMint(db, maps).identities();
            assertEquals(List.of(uuid), identities.publicIdOf(nid).orElseThrow().asUuidList());

            identities.forget(nid, new UUID[]{uuid});

            assertTrue(identities.publicIdOf(nid).isEmpty());
        }
    }

    @Test
    void bootstrapGivesTheFixedPatternsTheirNids(@TempDir File dir) {
        try (TestDb db = new TestDb(dir)) {
            Maps maps = open(db);
            maps.identities().bootstrap();
            for (IdentityMap.FixedPattern fixed : IdentityMap.fixedPatterns()) {
                for (UUID uuid : fixed.uuids()) {
                    assertEquals(fixed.nid(), maps.identities().nid(uuid).orElseThrow(), fixed.description());
                }
            }
            assertEquals(Nid.compose64(1, 1), maps.identities().nidFor(EntityBinding.Pattern.pattern(), EntityBinding.Pattern.pattern()));
            assertEquals(Nid.compose64(1, 2), maps.identities().nidFor(EntityBinding.Pattern.pattern(), EntityBinding.Concept.pattern()));
            assertEquals(Nid.compose64(1, 3), maps.identities().nidFor(EntityBinding.Pattern.pattern(), EntityBinding.Stamp.pattern()));
            assertEquals(4, maps.counters().next(Counters.PATTERN_OF_PATTERNS));

            maps.identities().flush();
            assertEquals(0, maps.identities().heldCount());
            maps.identities().verifyBindings();
            Maps reopened = open(db);
            reopened.identities().verifyBindings();
        }
    }

    /**
     * A mint racing a flush is never lost: the mint waits for the swap of the held table, or
     * the swap waits for the mint, so no entry lands in a table whose sorted stripes are
     * already being written and dropped.
     */
    @Test
    void mintsRacingAFlushAreNeverLost(@TempDir File dir) throws Exception {
        try (TestDb db = new TestDb(dir)) {
            Maps maps = open(db);
            maps.identities().bootstrap();
            PublicId patternId = id(UUID.randomUUID());
            maps.identities().nidFor(EntityBinding.Pattern.pattern(), patternId);
            int threads = 8;
            int perThread = 4_000;
            UUID[][] minted = new UUID[threads][perThread];
            long[][] nids = new long[threads][perThread];
            java.util.concurrent.atomic.AtomicBoolean minting = new java.util.concurrent.atomic.AtomicBoolean(true);
            Thread flusher = new Thread(() -> {
                while (minting.get()) {
                    maps.identities().flush();
                    Thread.onSpinWait();
                }
            });
            java.util.List<Thread> workers = new java.util.ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int thread = t;
                Thread worker = new Thread(() -> {
                    for (int i = 0; i < perThread; i++) {
                        UUID uuid = UUID.randomUUID();
                        minted[thread][i] = uuid;
                        nids[thread][i] = maps.identities().nidFor(patternId, id(uuid));
                    }
                });
                workers.add(worker);
            }
            flusher.start();
            workers.forEach(Thread::start);
            for (Thread worker : workers) {
                worker.join();
            }
            minting.set(false);
            flusher.join();
            maps.identities().flush();
            int lost = 0;
            int wrong = 0;
            for (int t = 0; t < threads; t++) {
                for (int i = 0; i < perThread; i++) {
                    long found = maps.identities().nid(minted[t][i]).orElse(0L);
                    if (found == 0) {
                        lost++;
                    } else if (found != nids[t][i]) {
                        wrong++;
                    }
                }
            }
            assertEquals(0, lost, "identities minted during flushes that read as unknown afterwards");
            assertEquals(0, wrong, "identities that read as another nid afterwards");
        }
    }

    @Test
    void aColumnWithoutTheBindingsIsRefused(@TempDir File dir) {
        try (TestDb db = new TestDb(dir)) {
            IllegalStateException failure = assertThrows(IllegalStateException.class, () -> open(db).identities().verifyBindings());
            assertTrue(failure.getMessage().contains("to nothing, not to its fixed nid"), failure.getMessage());
        }
    }

    @Test
    void entitiesAreKeyedUnderTheirPattern(@TempDir File dir) {
        try (TestDb db = new TestDb(dir)) {
            IdentityMap identities = open(db).identities();
            identities.bootstrap();
            PublicId concept = id(UUID.randomUUID());
            assertEquals(Nid.compose64(2, 1), identities.nidFor(EntityBinding.Concept.pattern(), concept));
            assertEquals(Nid.compose64(2, 1), identities.nidFor(EntityBinding.Concept.pattern(), concept), "the same again");
            assertEquals(Nid.compose64(2, 2), identities.nidFor(EntityBinding.Concept.pattern(), id(UUID.randomUUID())));
            assertEquals(Nid.compose64(3, 1), identities.nidFor(EntityBinding.Stamp.pattern(), id(UUID.randomUUID())));

            PublicId pattern = id(UUID.randomUUID());
            assertEquals(Nid.compose64(1, 4), identities.nidFor(EntityBinding.Pattern.pattern(), pattern), "a new pattern under the pattern-of-patterns");
            assertEquals(Nid.compose64(4, 1), identities.nidFor(pattern, id(UUID.randomUUID())), "a semantic under the new pattern");
            assertEquals(Nid.compose64(4, 2), identities.nidFor(pattern, id(UUID.randomUUID())));

            PublicId unknownPattern = id(UUID.randomUUID());
            assertEquals(Nid.compose64(5, 1), identities.nidFor(unknownPattern, id(UUID.randomUUID())), "a pattern first seen as a pattern is allocated");
            assertThrows(IllegalStateException.class, () -> identities.nidFor(concept, id(UUID.randomUUID())), "a concept is not a pattern");
        }
    }

    @Test
    void everyUuidOfAnIdMapsToItsNid(@TempDir File dir) {
        try (TestDb db = new TestDb(dir)) {
            IdentityMap identities = open(db).identities();
            identities.bootstrap();
            UUID first = UUID.randomUUID();
            UUID second = UUID.randomUUID();
            long nid = identities.nidFor(EntityBinding.Concept.pattern(), id(first, second));
            assertEquals(nid, identities.nidForUuids(first));
            assertEquals(nid, identities.nidForUuids(second));
            assertEquals(nid, identities.nidForUuids(second, first));
            assertTrue(identities.knows(first));
            assertFalse(identities.knows(UUID.randomUUID()));
            identities.flush();
            assertEquals(0, identities.heldCount());
            assertEquals(nid, identities.nidForUuids(second), "from the column after a flush");
            assertEquals(nid, open(db).identities().nidForUuids(first), "from the column, through a fresh map");
        }
    }

    @Test
    void aLoadPhaseHoldsEveryEntryAndWritesThemOnceAtItsEnd(@TempDir File dir) {
        try (TestDb db = new TestDb(dir)) {
            IdentityMap identities = open(db).identities();
            identities.bootstrap();
            identities.setLoadPhase(true);
            int count = (int) (SST_THRESHOLD * 3);
            UUID[] uuids = new UUID[count];
            long[] nids = new long[count];
            for (int i = 0; i < count; i++) {
                uuids[i] = UUID.randomUUID();
                nids[i] = identities.nidFor(EntityBinding.Concept.pattern(), id(uuids[i]));
            }
            assertEquals(count, identities.heldCount(), "held through the load phase, whatever their number");
            identities.flushIfLarge();
            assertEquals(count, identities.heldCount(), "the threshold does not apply during a load phase");

            identities.setLoadPhase(false);
            assertEquals(0, identities.heldCount(), "written at the end of the load phase");
            for (int i = 0; i < count; i++) {
                assertEquals(nids[i], identities.nidForUuids(uuids[i]));
            }
            IdentityMap reopened = open(db).identities();
            for (int i = 0; i < count; i += 97) {
                assertEquals(nids[i], reopened.nidForUuids(uuids[i]), "in the column, from the ingested files");
            }
            assertFalse(new File(dir, "ingest").exists(), "the files were moved into the database");
        }
    }

    @Test
    void entriesRegisteredDuringAWriteAreKept(@TempDir File dir) {
        try (TestDb db = new TestDb(dir)) {
            IdentityMap identities = open(db).identities();
            identities.bootstrap();
            UUID before = UUID.randomUUID();
            long beforeNid = identities.nidFor(EntityBinding.Concept.pattern(), id(before));
            identities.flush();
            UUID after = UUID.randomUUID();
            long afterNid = identities.nidFor(EntityBinding.Concept.pattern(), id(after));
            assertEquals(beforeNid, identities.nidForUuids(before));
            assertEquals(afterNid, identities.nidForUuids(after));
            assertEquals(1, identities.heldCount());
            identities.flush();
            assertEquals(afterNid, open(db).identities().nidForUuids(after));
        }
    }

    @Test
    void anUnknownUuidNeedsAPatternInScope(@TempDir File dir) throws Exception {
        try (TestDb db = new TestDb(dir)) {
            IdentityMap identities = open(db).identities();
            identities.bootstrap();
            UUID uuid = UUID.randomUUID();
            assertThrows(IllegalStateException.class, () -> identities.nidForUuids(uuid));
            long nid = ScopedValue.where(PrimitiveData.SCOPED_PATTERN_PUBLICID_FOR_NID, EntityBinding.Concept.pattern())
                    .call(() -> identities.nidForUuids(uuid));
            assertEquals(2, Nid.patternSequence64(nid));
            assertEquals(nid, identities.nidForUuids(uuid), "known from then on, without a scope");
        }
    }
}
