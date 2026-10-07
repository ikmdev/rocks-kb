package dev.ikm.ike.kb.validation;

import dev.ikm.tinkar.entity.EntityHandle;
import dev.ikm.tinkar.entity.SemanticEntity;
import dev.ikm.tinkar.entity.SemanticEntityVersion;
import dev.ikm.tinkar.entity.StampEntity;
import dev.ikm.tinkar.entity.load.LoadEntitiesFromProtobufFile;
import dev.ikm.tinkar.fixtures.ForkedJvm;
import dev.ikm.tinkar.fixtures.StoreDigest;
import dev.ikm.tinkar.schema.TinkarMsg;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.jar.Attributes;

import static dev.ikm.ike.kb.validation.ChangeSets.FORMAT_VERSION;
import static dev.ikm.ike.kb.validation.ChangeSets.IDENTITY_INDEX;
import static dev.ikm.ike.kb.validation.ChangeSets.LOADED;
import static dev.ikm.ike.kb.validation.ChangeSets.files;
import static dev.ikm.ike.kb.validation.ChangeSets.hasEntry;
import static dev.ikm.ike.kb.validation.ChangeSets.load;
import static dev.ikm.ike.kb.validation.ChangeSets.loadAndExport;
import static dev.ikm.ike.kb.validation.ChangeSets.manifest;
import static dev.ikm.ike.kb.validation.ChangeSets.parsedRecords;
import static dev.ikm.ike.kb.validation.ChangeSets.properties;
import static dev.ikm.ike.kb.validation.ChangeSets.publicIdOf;
import static dev.ikm.ike.kb.validation.ChangeSets.resource;
import static dev.ikm.ike.kb.validation.ChangeSets.reverseRecords;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * New software reads old data: changesets in format version 1 — public ids as UUID strings,
 * no format version in the manifest, no identity index — load into every store provider.
 *
 * <p>The fixtures under {@code format-v1/} are frozen copies, never regenerated (see the
 * README beside them): the IKE starter set as exported before format version 2, and two
 * changesets Komet wrote. A version 1 file holds its records in whatever order its writer
 * chose, so the starter set is also loaded with its records reversed — every reference a
 * forward reference — which a store whose nids encode the pattern (Rocks) can only load by
 * finding every component's pattern before it reads the records.
 *
 * <p>Each store lifetime is a stage in its own JVM ({@link ForkedJvm}). Every stage starts
 * from a new, empty store, so the ephemeral store needs no special handling.
 */
class FormatVersion1ReadIT {

    static final String STARTER_SET = "format-v1/ike-starter-set-reasoned-pb.zip";
    private static final String FQN_CHANGE = "format-v1/active-state-fqn-change-ike-cs.zip";
    private static final String REGULAR_NAME_CHANGE = "format-v1/active-state-other-change-ike-cs.zip";

    private static final String TEST = "format-v1-read";

    /** The stamps and semantics the two Komet changesets write, as tinkar-core's SpinedArrayImportIT names them. */
    private static final UUID FQN_CHANGE_STAMP = UUID.fromString("3d296499-654f-566a-83ea-334cbec2c2e1");
    private static final UUID FQN_CHANGE_SEMANTIC = UUID.fromString("65378077-2984-413d-a9f5-b43e1c611732");
    private static final UUID REGULAR_NAME_CHANGE_STAMP = UUID.fromString("cf1e9214-42be-51fe-99f1-4eaf3e6c95ad");
    private static final UUID REGULAR_NAME_CHANGE_SEMANTIC = UUID.fromString("101cea57-bfe4-4840-9cf4-da61ffb8463e");

    @Test
    void theFixturesAreInFormatVersion1() throws Exception {
        for (String fixture : List.of(STARTER_SET, FQN_CHANGE, REGULAR_NAME_CHANGE)) {
            File file = resource(fixture);
            assertNull(manifest(file).getValue(FORMAT_VERSION), fixture + " names a format version");
            assertFalse(hasEntry(file, IDENTITY_INDEX), fixture + " carries an identity index");
            for (TinkarMsg record : parsedRecords(file)) {
                var publicId = publicIdOf(record);
                assertTrue(publicId.getUuidsCount() > 0 && publicId.getUuidBitsCount() == 0,
                        fixture + ": a record's public id is not UUID strings alone: " + publicId);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(Provider.class)
    void theStarterSetLoads(Provider provider) throws Exception {
        File starterSet = resource(STARTER_SET);
        StoreDigest loaded = load(TEST, provider, "in-order", starterSet);

        Attributes manifest = manifest(starterSet);
        assertAll(provider.name(),
                () -> assertEquals(Long.parseLong(manifest.getValue("Concept-Count")), loaded.concepts(), "Concepts against the manifest"),
                () -> assertEquals(Long.parseLong(manifest.getValue("Semantic-Count")), loaded.semantics(), "Semantics against the manifest"),
                () -> assertEquals(Long.parseLong(manifest.getValue("Pattern-Count")), loaded.patterns(), "Patterns against the manifest"),
                () -> assertEquals(Long.parseLong(manifest.getValue("Stamp-Count")), loaded.stamps(), "Stamps against the manifest"),
                () -> assertEquals(0, loaded.unrendered(), "Field values the digest could not render"));
    }

    @ParameterizedTest
    @EnumSource(Provider.class)
    void theStarterSetLoadsWithEveryReferenceAForwardReference(Provider provider) throws Exception {
        File starterSet = resource(STARTER_SET);
        Path work = ChangeSets.work(TEST, provider, "reversed-file");
        File reversed = reverseRecords(starterSet, work.resolve("reversed-pb.zip").toFile());

        StoreDigest inOrder = load(TEST, provider, "in-order", starterSet);
        StoreDigest fromReversed = load(TEST, provider, "reversed", reversed);

        assertEquals(List.of(), fromReversed.differencesFrom(inOrder),
                provider + ": the store loaded from the reversed records, against the store loaded in order");
    }

    @ParameterizedTest
    @EnumSource(Provider.class)
    void theStarterSetIsExportedAndRestoredUnchanged(Provider provider) {
        Path work = ChangeSets.work(TEST, provider, "re-export");
        File export = work.resolve("export-pb.zip").toFile();
        StoreDigest loaded = StoreDigest.load(loadAndExport(provider, work, export, resource(STARTER_SET)), LOADED);

        StoreDigest restored = load(TEST, provider, "restored", export);

        assertEquals(List.of(), restored.differencesFrom(loaded),
                provider + ": the store restored from the export, against the store loaded from version 1");
    }

    @ParameterizedTest
    @EnumSource(Provider.class)
    void kometChangeSetsLoadOnTheStarterSet(Provider provider) {
        Path work = ChangeSets.work(TEST, provider, "komet-changesets");
        Properties in = properties(provider, work.resolve("store"),
                resource(STARTER_SET), resource(FQN_CHANGE), resource(REGULAR_NAME_CHANGE));
        Properties result = ForkedJvm.run(LoadKometChangeSets.class, in, Duration.ofMinutes(10));

        assertAll(provider.name(),
                () -> assertEquals("2025-05-12T20:18:42.789Z", result.getProperty("time." + FQN_CHANGE_STAMP),
                        "The first changeset's stamp"),
                () -> assertEquals("Active state (test change)", result.getProperty("text." + FQN_CHANGE_SEMANTIC + "." + FQN_CHANGE_STAMP),
                        "The fully qualified name the first changeset writes"),
                () -> assertEquals("2025-05-13T16:42:44.408Z", result.getProperty("time." + REGULAR_NAME_CHANGE_STAMP),
                        "The second changeset's stamp"),
                () -> assertEquals("Active (test change)", result.getProperty("text." + REGULAR_NAME_CHANGE_SEMANTIC + "." + REGULAR_NAME_CHANGE_STAMP),
                        "The regular name the second changeset writes"));
    }

    /**
     * A new store, the starter set loaded into it, then each Komet changeset; for each stamp
     * the changesets write, its time ({@code time.<stamp>}), and for each description they
     * change, the text of its version on the changeset's stamp ({@code text.<semantic>.<stamp>}).
     *
     * <p>The changesets were written against starter data that is not the IKE starter set, so
     * each also carries a version on a stamp the store never receives; that version is
     * loaded, and has no stamp to report.
     */
    static class LoadKometChangeSets extends StoreStage {
        @Override
        void work(Properties in, Properties out) {
            for (File file : files(in)) {
                new LoadEntitiesFromProtobufFile(file).compute();
            }
            for (UUID uuid : List.of(FQN_CHANGE_STAMP, REGULAR_NAME_CHANGE_STAMP)) {
                StampEntity<?> stamp = (StampEntity<?>) EntityHandle.get(uuid).expectEntity();
                out.setProperty("time." + uuid, Instant.ofEpochMilli(stamp.time()).toString());
            }
            recordText(out, FQN_CHANGE_SEMANTIC, FQN_CHANGE_STAMP);
            recordText(out, REGULAR_NAME_CHANGE_SEMANTIC, REGULAR_NAME_CHANGE_STAMP);
        }

        private static void recordText(Properties out, UUID semanticUuid, UUID stampUuid) {
            SemanticEntity<?> semantic = (SemanticEntity<?>) EntityHandle.get(semanticUuid).expectEntity();
            for (SemanticEntityVersion version : semantic.versions()) {
                StampEntity<?> stamp = version.stamp();
                if (stamp != null && stamp.publicId().contains(stampUuid)) {
                    out.setProperty("text." + semanticUuid + "." + stampUuid, (String) version.fieldValues().get(1));
                }
            }
        }
    }
}
