package dev.ikm.ds.rocks64;

import dev.ikm.tinkar.common.id.Nid;
import dev.ikm.tinkar.common.id.PublicId;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.common.id.impl.NidLayout;
import dev.ikm.tinkar.common.service.DataActivity;
import dev.ikm.tinkar.common.service.ServiceKeys;
import dev.ikm.tinkar.common.service.ServiceProperties;
import dev.ikm.tinkar.entity.EntityFormat2;
import dev.ikm.tinkar.terms.EntityBinding;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The store on its own, without the service lifecycle: what it writes at creation, what it
 * keeps across a close and reopen, and what it refuses. The provider contract is covered by
 * the conformance suite in kb-validation.
 */
class Rocks64StoreTest {

    private static final long NOT_A_SEMANTIC = Integer.MAX_VALUE;

    @TempDir
    File root;

    @BeforeEach
    void pointTheStoreAtTheTempDir() {
        ServiceProperties.set(ServiceKeys.DATA_STORE_ROOT, root);
        ServiceProperties.set(ServiceKeys.DATA_STORE_EXPECT_EMPTY, Boolean.FALSE);
    }

    @Test
    void aNewStoreNamesItsFormatAndKeepsItsStateAcrossAReopen() {
        UUID uuid = UUID.randomUUID();
        long concept;
        byte[] record;
        Rocks64Store store = Rocks64Store.open();
        try {
            assertTrue(StoreFormat.isSixtyFourBit(new File(root, "rocks")));
            assertEquals(NidLayout.SIXTY_FOUR_BIT, NidLayout.active());
            assertEquals("64-bit", StoreFormat.read(store.db(), store.db().getDefaultColumnFamily()).getOrDefault("layout", "64-bit"));
            concept = store.getEntityKey(EntityBinding.Concept.pattern(), PublicIds.of(uuid)).nid();
            assertEquals(Nid.compose64(2, 1), concept);
            record = Records.concept(concept, new UUID[]{uuid}, Nid.compose64(3, 1));
            byte[] merged = store.merge(concept, NOT_A_SEMANTIC, NOT_A_SEMANTIC, record, null, DataActivity.SYNCHRONIZABLE_EDIT);
            assertArrayEquals(record, merged);
            assertArrayEquals(record, store.getBytes(concept));
            assertEquals(1, store.writeSequence());
            assertEquals(List.of(uuid), store.publicIdForNid(concept).asUuidList());
        } finally {
            store.close();
        }
        assertFalse(store.running());

        Rocks64Store reopened = Rocks64Store.open();
        try {
            assertEquals(concept, reopened.nidForUuids(uuid), "identity kept");
            assertArrayEquals(record, reopened.getBytes(concept), "record kept");
            assertEquals(Nid.compose64(2, 2), reopened.getEntityKey(EntityBinding.Concept.pattern(), PublicIds.of(UUID.randomUUID())).nid(),
                    "counter kept");
            assertTrue(reopened.sequenceReport().contains("2=3"), reopened.sequenceReport());
        } finally {
            reopened.close();
        }
    }

    @Test
    void aNidMintedButNotYetWrittenNamesItsUuids_fromTheMint() {
        // Vertex identity derivation asks this of every component a definition references, most
        // minted before they are written in a parallel load: answered from what the mint recorded,
        // then from the record once it is written, never by a scan of the identity column.
        UUID uuid = UUID.randomUUID();
        Rocks64Store store = Rocks64Store.open();
        try {
            long concept = store.getEntityKey(EntityBinding.Concept.pattern(), PublicIds.of(uuid)).nid();
            assertEquals(List.of(uuid), store.publicIdForNid(concept).asUuidList(), "minted, unwritten");
            store.merge(concept, NOT_A_SEMANTIC, NOT_A_SEMANTIC, Records.concept(concept, new UUID[]{uuid}, Nid.compose64(3, 1)),
                    null, DataActivity.SYNCHRONIZABLE_EDIT);
            assertEquals(List.of(uuid), store.publicIdForNid(concept).asUuidList(), "written");
        } finally {
            store.close();
        }
    }

    @Test
    void enumerationsFollowTheCountersAndTheRecords() {
        Rocks64Store store = Rocks64Store.open();
        try {
            long written = store.getEntityKey(EntityBinding.Concept.pattern(), PublicIds.of(UUID.randomUUID())).nid();
            long unwritten = store.getEntityKey(EntityBinding.Concept.pattern(), PublicIds.of(UUID.randomUUID())).nid();
            store.merge(written, NOT_A_SEMANTIC, NOT_A_SEMANTIC, Records.concept(written, Nid.compose64(3, 1)), null,
                    DataActivity.SYNCHRONIZABLE_EDIT);
            PublicId patternId = PublicIds.of(UUID.randomUUID());
            long pattern = store.getEntityKey(EntityBinding.Pattern.pattern(), patternId).nid();
            long semantic = store.getEntityKey(patternId, PublicIds.of(UUID.randomUUID())).nid();
            assertEquals(Nid.compose64(4, 1), semantic);

            assertEquals(Set.of(written, unwritten), collect(store::forEachConceptNid), "concepts by counter, written or not");
            assertEquals(Set.of(), collect(store::forEachPatternNid), "a pattern without a record is not a pattern of the store");
            assertEquals(Set.of(semantic), collect(store::forEachSemanticNid));
            assertEquals(Set.of(semantic), collect(procedure -> store.forEachSemanticNidOfPattern(pattern, procedure)));
            assertThrows(IllegalStateException.class, () -> store.forEachSemanticNidOfPattern(written, nid -> { }));
            assertNull(store.getBytes(unwritten));

            ConcurrentHashMap<Long, byte[]> scanned = new ConcurrentHashMap<>();
            store.forEachParallel((bytes, nid) -> scanned.put(nid, bytes));
            assertEquals(Set.of(written), scanned.keySet());
        } finally {
            store.close();
        }
    }

    /**
     * Beyond the legacy layouts: a pattern sequence above 255 and an element sequence above
     * 16,777,215 are keyed, written, read and enumerated like any other.
     */
    @Test
    void sequencesBeyondTheLegacyLimitsAreKeyedAndRead() {
        Rocks64Store store = Rocks64Store.open();
        try {
            PublicId patternId = null;
            for (int i = 0; i < 300; i++) {
                patternId = PublicIds.of(UUID.randomUUID());
                store.getEntityKey(EntityBinding.Pattern.pattern(), patternId);
            }
            long pattern = store.nidForUuids(patternId.asUuidArray());
            assertEquals(Nid.compose64(1, 303), pattern, "the 300th pattern after the three fixed ones");
            long semantic = store.getEntityKey(patternId, PublicIds.of(UUID.randomUUID())).nid();
            assertEquals(Nid.compose64(303, 1), semantic);
            store.merge(semantic, pattern, NOT_A_SEMANTIC, Records.concept(semantic, Nid.compose64(3, 1)), null,
                    DataActivity.SYNCHRONIZABLE_EDIT);
            assertEquals(semantic, dev.ikm.tinkar.common.service.EntityRecordFormat3.nid(store.getBytes(semantic)));

            int beyondEightBit = 16_777_216;
            while (store.counters().next(Counters.CONCEPT_PATTERN) < beyondEightBit) {
                store.counters().nextElementSequence(Counters.CONCEPT_PATTERN);
            }
            UUID uuid = UUID.randomUUID();
            long concept = store.getEntityKey(EntityBinding.Concept.pattern(), PublicIds.of(uuid)).nid();
            assertEquals(Nid.compose64(2, beyondEightBit), concept);
            byte[] record = Records.concept(concept, new UUID[]{uuid}, Nid.compose64(3, 1));
            store.merge(concept, NOT_A_SEMANTIC, NOT_A_SEMANTIC, record, null, DataActivity.SYNCHRONIZABLE_EDIT);
            assertArrayEquals(record, store.getBytes(concept));
            assertTrue(collect(store::forEachConceptNid).contains(concept));
            ConcurrentHashMap<Long, byte[]> scanned = new ConcurrentHashMap<>();
            store.forEachParallel((bytes, nid) -> scanned.put(nid, bytes));
            assertEquals(Set.of(semantic, concept), scanned.keySet());
            assertEquals(Set.of(semantic), collect(procedure -> store.forEachSemanticNidOfPattern(pattern, procedure)));
        } finally {
            store.close();
        }
    }

    /**
     * A reference lookup sees a reference while the writer still holds it and after it is
     * written, and a pooled iterator made before a later reference landed is not reused for it.
     */
    @Test
    void referencesAreFoundPendingWrittenAndAfterLaterOnes() {
        Rocks64Store store = Rocks64Store.open();
        try {
            PublicId patternId = PublicIds.of(UUID.randomUUID());
            long pattern = store.getEntityKey(EntityBinding.Pattern.pattern(), patternId).nid();
            long concept = store.getEntityKey(EntityBinding.Concept.pattern(), PublicIds.of(UUID.randomUUID())).nid();
            long first = mergeSemantic(store, patternId, pattern, concept);
            assertArrayEquals(new long[]{first}, store.semanticNidsForComponent(concept), "pending");
            store.recordMap().awaitPendingWrites();
            assertArrayEquals(new long[]{first}, store.semanticNidsForComponent(concept), "written");
            assertArrayEquals(new long[]{first}, store.semanticNidsForComponentOfPattern(concept, pattern), "written, by pattern");

            long second = mergeSemantic(store, patternId, pattern, concept);
            store.recordMap().awaitPendingWrites();
            assertArrayEquals(new long[]{first, second}, store.semanticNidsForComponent(concept), "a later reference, through a fresh iterator");
            assertArrayEquals(new long[]{first, second}, store.semanticNidsForComponentOfPattern(concept, pattern));
            assertEquals(0, store.semanticNidsForComponentOfPattern(concept, Nid.compose64(1, 2)).length, "no reference from the concept pattern");
        } finally {
            store.close();
        }
    }

    /**
     * A reference is published after its record: a reader following references from the
     * referenced component finds a record at every nid it is given, even while the semantics
     * are being merged.
     */
    @Test
    void aReferenceIsNeverFoundBeforeItsRecord() throws Exception {
        Rocks64Store store = Rocks64Store.open();
        try {
            PublicId patternId = PublicIds.of(UUID.randomUUID());
            long pattern = store.getEntityKey(EntityBinding.Pattern.pattern(), patternId).nid();
            long concept = store.getEntityKey(EntityBinding.Concept.pattern(), PublicIds.of(UUID.randomUUID())).nid();
            int merges = 1_000;
            java.util.concurrent.atomic.AtomicInteger dangling = new java.util.concurrent.atomic.AtomicInteger();
            java.util.concurrent.atomic.AtomicBoolean merging = new java.util.concurrent.atomic.AtomicBoolean(true);
            Thread reader = new Thread(() -> {
                while (merging.get()) {
                    for (long nid : store.semanticNidsForComponent(concept)) {
                        if (store.getBytes(nid) == null) {
                            dangling.incrementAndGet();
                        }
                    }
                }
            });
            reader.start();
            for (int i = 0; i < merges; i++) {
                mergeSemantic(store, patternId, pattern, concept);
            }
            merging.set(false);
            reader.join();
            assertEquals(merges, store.semanticNidsForComponent(concept).length, "references to the concept");
            assertEquals(0, dangling.get(), "references found before their record");
        } finally {
            store.close();
        }
    }

    private static long mergeSemantic(Rocks64Store store, PublicId patternId, long pattern, long concept) {
        UUID uuid = UUID.randomUUID();
        long nid = store.getEntityKey(patternId, PublicIds.of(uuid)).nid();
        dev.ikm.tinkar.entity.RecordListBuilder<dev.ikm.tinkar.entity.SemanticVersionRecord> versions =
                dev.ikm.tinkar.entity.RecordListBuilder.make();
        dev.ikm.tinkar.entity.SemanticRecord semantic = new dev.ikm.tinkar.entity.SemanticRecord(
                uuid.getMostSignificantBits(), uuid.getLeastSignificantBits(),
                org.eclipse.collections.api.factory.primitive.LongLists.immutable.empty(), nid, pattern, concept, versions);
        versions.build();
        store.merge(nid, pattern, concept, Records.concept(nid, new UUID[]{uuid}, Nid.compose64(3, 1)), semantic,
                DataActivity.SYNCHRONIZABLE_EDIT);
        return nid;
    }

    @Test
    void aStoreInALegacyLayoutIsRefused() throws Exception {
        File rocks = new File(root, "rocks");
        assertTrue(rocks.mkdirs());
        RocksDB.loadLibrary();
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        try (DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
             RocksDB legacy = RocksDB.open(options, rocks.getAbsolutePath(),
                     List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY), new ColumnFamilyDescriptor("EntityMap".getBytes(UTF_8))),
                     handles)) {
            handles.forEach(ColumnFamilyHandle::close);
        }
        assertTrue(StoreFormat.holdsADatabase(rocks));
        assertFalse(StoreFormat.isSixtyFourBit(rocks));
        assertThrows(IllegalStateException.class, Rocks64Store::open);
    }

    /** A concept with two UUIDs and two versions in entity format 2, as a store created before format 3 holds it. */
    private static final byte[] FORMAT_2_CONCEPT = HexFormat.of().parseHex(
            "0227010282dac4090f6c6a6e6b1d4c0c8b8e3f1a2b3c4d01020f6c6a6e6b1d4c0c8b8e3f1a2b3c4d02020304036a0304036b");
    /** The same concept in format 3. */
    private static final byte[] FORMAT_3_CONCEPT = HexFormat.of().parseHex(
            "03260282dac4090f6c6a6e6b1d4c0c8b8e3f1a2b3c4d01020f6c6a6e6b1d4c0c8b8e3f1a2b3c4d0202016a016b");
    private static final long FORMAT_2_CONCEPT_NID = Nid.compose64(2, 20_000_002);

    /**
     * A store created in entity format 2 opens as it is and reads as format 3, its stored records
     * untouched; it is upgraded only when asked, after which its descriptor names format 3
     * (IKE-Network/ike-issues#1313).
     */
    @Test
    void aStoreCreatedInFormat2IsReadAsFormat3AndUpgradedOnlyWhenAsked() throws Exception {
        long concept = FORMAT_2_CONCEPT_NID;
        Rocks64Store created = Rocks64Store.open();
        try {
            plantFormat2(created, concept, FORMAT_2_CONCEPT);
        } finally {
            created.close();
        }

        Rocks64Store compatible = Rocks64Store.open();
        try {
            assertEquals("2", descriptor(compatible).get(StoreFormat.ENTITY_FORMAT), "opened as it is");
            assertArrayEquals(FORMAT_3_CONCEPT, compatible.getBytes(concept), "read as format 3");
            ConcurrentHashMap<Long, byte[]> scanned = new ConcurrentHashMap<>();
            compatible.forEachParallel((bytes, nid) -> scanned.put(nid, bytes));
            assertArrayEquals(FORMAT_3_CONCEPT, scanned.get(concept), "scanned as format 3");
            assertArrayEquals(FORMAT_2_CONCEPT, stored(compatible, concept), "stored as it was");
        } finally {
            compatible.close();
        }

        System.setProperty(FormatUpgrade.UPGRADE_PROPERTY, "true");
        try {
            Rocks64Store upgraded = Rocks64Store.open();
            try {
                Map<String, String> descriptor = descriptor(upgraded);
                assertEquals("3", descriptor.get(StoreFormat.ENTITY_FORMAT), "upgraded");
                assertNotNull(descriptor.get(StoreFormat.UPGRADED_BY));
                assertNotNull(descriptor.get(StoreFormat.UPGRADED_AT));
                assertArrayEquals(FORMAT_3_CONCEPT, stored(upgraded, concept), "rewritten in format 3");
            } finally {
                upgraded.close();
            }
        } finally {
            System.clearProperty(FormatUpgrade.UPGRADE_PROPERTY);
        }

        Rocks64Store reopened = Rocks64Store.open();
        try {
            assertEquals("3", descriptor(reopened).get(StoreFormat.ENTITY_FORMAT), "kept upgraded");
            assertArrayEquals(FORMAT_3_CONCEPT, reopened.getBytes(concept));
        } finally {
            reopened.close();
        }
    }

    /** A record written to a format 2 store is written in format 3, its format 2 versions kept. */
    @Test
    void aRecordWrittenToAFormat2StoreIsWrittenInFormat3() throws Exception {
        long concept = FORMAT_2_CONCEPT_NID;
        Rocks64Store created = Rocks64Store.open();
        try {
            plantFormat2(created, concept, FORMAT_2_CONCEPT);
        } finally {
            created.close();
        }
        Rocks64Store compatible = Rocks64Store.open();
        try {
            UUID uuid = new UUID(0x0f6c6a6e6b1d4c0cL, 0x8b8e3f1a2b3c4d01L);
            compatible.merge(concept, NOT_A_SEMANTIC, NOT_A_SEMANTIC, Records.concept(concept, new UUID[]{uuid}, Nid.compose64(3, 108)),
                    null, DataActivity.SYNCHRONIZABLE_EDIT);
            byte[] stored = stored(compatible, concept);
            assertFalse(EntityFormat2.isFormat2(stored), "written in format 3");
            assertEquals(3, Records.versionCount(stored), "the two format 2 versions and the new one");
            assertEquals("2", descriptor(compatible).get(StoreFormat.ENTITY_FORMAT),
                    "other records may still be in format 2");
        } finally {
            compatible.close();
        }
    }

    /** A format 2 record format 3 refuses stops the upgrade, naming its nid, and leaves the store in format 2. */
    @Test
    void anUpgradeThatMeetsARecordFormat3RefusesStopsNamingItsNid() throws Exception {
        // The concept with its nid in a semantic's pattern, as the format 2 fixtures held it.
        long misfiled = Nid.compose64(200, 20_000_002);
        Rocks64Store created = Rocks64Store.open();
        try {
            plantFormat2(created, misfiled, HexFormat.of().parseHex(
                    "022801c80182dac4090f6c6a6e6b1d4c0c8b8e3f1a2b3c4d01020f6c6a6e6b1d4c0c8b8e3f1a2b3c4d02020304036a0304036b"));
        } finally {
            created.close();
        }
        System.setProperty(FormatUpgrade.UPGRADE_PROPERTY, "true");
        try {
            IllegalStateException refused = assertThrows(IllegalStateException.class, Rocks64Store::open);
            assertTrue(stackMentions(refused, Long.toString(misfiled)), "the refusal names the nid");
        } finally {
            System.clearProperty(FormatUpgrade.UPGRADE_PROPERTY);
        }
        Rocks64Store compatible = Rocks64Store.open();
        try {
            assertEquals("2", descriptor(compatible).get(StoreFormat.ENTITY_FORMAT), "left in format 2");
        } finally {
            compatible.close();
        }
    }

    /** Writes a record in format 2 and makes the store's descriptor name format 2, as a store created before format 3. */
    private static void plantFormat2(Rocks64Store store, long nid, byte[] record) throws Exception {
        try (WriteBatch batch = new WriteBatch(); WriteOptions options = new WriteOptions()) {
            batch.put(store.handle(Rocks64Store.Family.ENTITIES), Keys.of(nid), record);
            batch.put(store.handle(Rocks64Store.Family.FORMAT), StoreFormat.ENTITY_FORMAT.getBytes(UTF_8), "2".getBytes(UTF_8));
            store.db().write(options, batch);
        }
    }

    private static Map<String, String> descriptor(Rocks64Store store) {
        return StoreFormat.read(store.db(), store.handle(Rocks64Store.Family.FORMAT));
    }

    private static byte[] stored(Rocks64Store store, long nid) throws Exception {
        return store.db().get(store.handle(Rocks64Store.Family.ENTITIES), Keys.of(nid));
    }

    private static boolean stackMentions(Throwable thrown, String text) {
        for (Throwable t = thrown; t != null; t = t.getCause()) {
            if (t.getMessage() != null && t.getMessage().contains(text)) {
                return true;
            }
        }
        return false;
    }

    private interface Enumeration {
        void forEach(org.eclipse.collections.api.block.procedure.primitive.LongProcedure procedure);
    }

    private static Set<Long> collect(Enumeration enumeration) {
        Set<Long> nids = ConcurrentHashMap.newKeySet();
        enumeration.forEach(nids::add);
        return nids;
    }
}
