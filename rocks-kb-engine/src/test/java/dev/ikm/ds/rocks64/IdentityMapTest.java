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

    private static Maps open(TestDb db) {
        Counters counters = Counters.load(db.db, db.handle(Rocks64Store.Family.DEFAULT), db.handle(Rocks64Store.Family.ENTITIES));
        return new Maps(counters, new IdentityMap(db.db, db.handle(Rocks64Store.Family.IDENTITIES), counters));
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
            assertEquals(0, maps.identities().unwrittenCount());
            maps.identities().verifyBindings();
            Maps reopened = open(db);
            reopened.identities().verifyBindings();
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
            assertEquals(nid, identities.nidForUuids(second), "from the column after a flush");
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
