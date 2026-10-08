package dev.ikm.ds.rocks64;

import dev.ikm.tinkar.common.id.Nid;
import dev.ikm.tinkar.common.id.PublicId;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.terms.EntityBinding;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.UUID;

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
