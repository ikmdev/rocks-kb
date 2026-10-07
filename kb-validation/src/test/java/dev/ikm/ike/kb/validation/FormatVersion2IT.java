package dev.ikm.ike.kb.validation;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import dev.ikm.tinkar.fixtures.StoreDigest;
import dev.ikm.tinkar.schema.PatternMembers;
import dev.ikm.tinkar.schema.PublicId;
import dev.ikm.tinkar.schema.TinkarMsg;
import dev.ikm.tinkar.schema.VertexUUID;
import dev.ikm.tinkar.terms.EntityBinding;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

import static dev.ikm.ike.kb.validation.ChangeSets.FORMAT_VERSION;
import static dev.ikm.ike.kb.validation.ChangeSets.IDENTITY_INDEX;
import static dev.ikm.ike.kb.validation.ChangeSets.LOADED;
import static dev.ikm.ike.kb.validation.ChangeSets.entry;
import static dev.ikm.ike.kb.validation.ChangeSets.load;
import static dev.ikm.ike.kb.validation.ChangeSets.loadAndExport;
import static dev.ikm.ike.kb.validation.ChangeSets.manifest;
import static dev.ikm.ike.kb.validation.ChangeSets.parsedRecords;
import static dev.ikm.ike.kb.validation.ChangeSets.publicIdOf;
import static dev.ikm.ike.kb.validation.ChangeSets.recordBytes;
import static dev.ikm.ike.kb.validation.ChangeSets.records;
import static dev.ikm.ike.kb.validation.ChangeSets.resource;
import static dev.ikm.ike.kb.validation.ChangeSets.reverseRecords;
import static dev.ikm.ike.kb.validation.ChangeSets.withManifestAttribute;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * New software writes format version 2, and reads it back in one pass, in every store provider.
 *
 * <p>Format version 2 is what this release writes, and only that: no reader before it can
 * read it, by design. A version 2 changeset
 * <ul>
 *   <li>names its format in its manifest: {@value ChangeSets#FORMAT_VERSION}: 2;
 *   <li>writes every UUID as two longs, most significant bits first ({@code PublicId.uuid_bits},
 *       {@code VertexUUID}'s two bit fields), and none as text;
 *   <li>carries an identity index ({@value ChangeSets#IDENTITY_INDEX}) naming the pattern of
 *       every component it holds, each exactly once, so a store whose nids encode the pattern
 *       loads it in one pass, forward references included;
 *   <li>is reproducible: a store exported, restored, and exported again writes the same records
 *       byte for byte, whatever order the restored store assigned its nids in. (Format version 1
 *       is not: a logical definition's vertex properties follow nid order.)
 * </ul>
 * A reader refuses a format version newer than it knows rather than misread it.
 *
 * <p>The starter set comes from the frozen format version 1 fixture, so these tests also
 * carry version 1 data into version 2. Each store lifetime is a stage in its own JVM.
 */
class FormatVersion2IT {

    private static final Logger LOG = LoggerFactory.getLogger(FormatVersion2IT.class);

    private static final String TEST = "format-v2";

    /** A version 2 export of the starter set, written once from the ephemeral store, for the store-independent tests. */
    private static File export;

    @BeforeAll
    static void exportTheStarterSet() {
        Path work = ChangeSets.work(TEST, Provider.EPHEMERAL, "shared-export");
        export = work.resolve("export-pb.zip").toFile();
        loadAndExport(Provider.EPHEMERAL, work, export, resource(FormatVersion1ReadIT.STARTER_SET));
    }

    @ParameterizedTest
    @EnumSource(Provider.class)
    void theStarterSetIsExportedInFormatVersion2AndRestoredUnchanged(Provider provider) throws IOException {
        File starterSet = resource(FormatVersion1ReadIT.STARTER_SET);
        Path work = ChangeSets.work(TEST, provider, "round-trip");
        File first = work.resolve("export-pb.zip").toFile();
        File second = work.resolve("re-export-pb.zip").toFile();
        File reversed = work.resolve("reversed-export-pb.zip").toFile();

        // Version 1 in, version 2 out; that export restored and exported again
        StoreDigest loaded = StoreDigest.load(loadAndExport(provider, ChangeSets.work(TEST, provider, "load"), first, starterSet), LOADED);
        StoreDigest restored = StoreDigest.load(loadAndExport(provider, ChangeSets.work(TEST, provider, "restore"), second, first), LOADED);
        // Every reference a forward reference, loaded in one pass: only the identity index makes that possible on Rocks
        reverseRecords(first, reversed);
        StoreDigest onePass = load(TEST, provider, "one-pass", "one-pass", reversed);

        List<Executable> checks = new ArrayList<>(formatVersion2Checks(first));
        checks.add(() -> assertEquals(List.of(), restored.differencesFrom(loaded),
                "The store restored from the version 2 export, against the store loaded from version 1"));
        checks.add(() -> assertEquals(List.of(), differences(records(first), records(second)),
                "Records of the first export, against the export of the store restored from it, byte for byte"));
        checks.add(() -> assertTrue(Objects.equals(index(first), index(second)),
                "The export of the restored store lists the identities of the first export"));
        checks.add(() -> assertEquals(List.of(), onePass.differencesFrom(loaded),
                "The store loaded in one pass from the export's records reversed, against the store loaded from version 1"));
        assertAll(provider.name(), checks);
    }

    @Test
    void aNewerFormatVersionIsRefused() throws IOException {
        Path work = ChangeSets.work(TEST, Provider.EPHEMERAL, "newer-version");
        File newer = withManifestAttribute(export, work.resolve("version-3-pb.zip").toFile(), FORMAT_VERSION, "3");

        AssertionError refused = assertThrows(AssertionError.class,
                () -> load(TEST, Provider.EPHEMERAL, "newer-version-store", newer),
                "A changeset of format version 3 is loaded by a reader that knows version 2 at most");
        assertTrue(refused.getMessage().contains(FORMAT_VERSION) && refused.getMessage().contains("3"),
                "The refusal names the format version it does not know:\n" + refused.getMessage());
    }

    @Test
    void formatVersion2IsSmallerThanFormatVersion1() throws IOException {
        File starterSet = resource(FormatVersion1ReadIT.STARTER_SET);
        long version1 = recordBytes(starterSet);
        long version2 = recordBytes(export);
        LOG.info("Starter set records: format version 1 {} bytes ({} compressed), format version 2 {} bytes ({} compressed): {}%",
                version1, starterSet.length(), version2, export.length(), 100 * version2 / version1);
        assertTrue(version2 < version1, "Format version 2 records take " + version2
                + " bytes; the same records in format version 1 take " + version1);
    }

    /** The checks a file must pass to be format version 2, each reported on its own. */
    private static List<Executable> formatVersion2Checks(File file) throws IOException {
        List<Executable> checks = new ArrayList<>();
        checks.add(() -> assertEquals("2", manifest(file).getValue(FORMAT_VERSION), "The manifest's " + FORMAT_VERSION));

        List<TinkarMsg> records = parsedRecords(file);
        List<String> textUuids = new ArrayList<>();
        for (TinkarMsg record : records) {
            forEachMessage(record, message -> {
                if (message instanceof PublicId publicId && !isLongs(publicId)) {
                    textUuids.add(publicId.toString().strip());
                } else if (message instanceof VertexUUID vertex && (!vertex.getUuid().isEmpty()
                        || (vertex.getMostSignificantBits() == 0 && vertex.getLeastSignificantBits() == 0))) {
                    textUuids.add(vertex.toString().strip());
                }
            });
        }
        checks.add(() -> assertEquals(List.of(), textUuids.stream().limit(5).toList(),
                textUuids.size() + " UUIDs in the records are not written as longs alone; the first five"));

        checks.add(() -> {
            Map<List<UUID>, List<UUID>> index = index(file);
            assertNotNull(index, "No identity index, " + IDENTITY_INDEX);
            assertEquals(records.size(), index.size(), "Components the index lists, against records in the file");
            List<String> wrong = new ArrayList<>();
            for (TinkarMsg record : records) {
                List<UUID> component = uuids(publicIdOf(record));
                List<UUID> expected = expectedPattern(record);
                if (!expected.equals(index.get(component))) {
                    wrong.add(component + " listed under " + index.get(component) + ", not " + expected);
                }
            }
            assertEquals(List.of(), wrong.stream().limit(5).toList(),
                    wrong.size() + " components the index lists under the wrong pattern, or not at all; the first five");
        });
        return checks;
    }

    /** Component to pattern, as the file's identity index lists them, or null if it has none; fails on a component listed twice. */
    private static Map<List<UUID>, List<UUID>> index(File file) throws IOException {
        byte[] bytes = entry(file, IDENTITY_INDEX);
        if (bytes == null) {
            return null;
        }
        Map<List<UUID>, List<UUID>> index = new HashMap<>();
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        PatternMembers members;
        while ((members = PatternMembers.parseDelimitedFrom(in)) != null) {
            assertTrue(isLongs(members.getPatternPublicId()), "The index writes a pattern's UUIDs as text");
            List<UUID> pattern = uuids(members.getPatternPublicId());
            for (PublicId component : members.getComponentPublicIdsList()) {
                assertTrue(isLongs(component), "The index writes a component's UUIDs as text");
                List<UUID> previous = index.put(uuids(component), pattern);
                assertTrue(previous == null, "The index lists " + uuids(component) + " more than once");
            }
        }
        return index;
    }

    /** The pattern a record's component is an element of: a semantic names its own; the others have their kind's. */
    private static List<UUID> expectedPattern(TinkarMsg record) {
        return switch (record.getValueCase()) {
            case SEMANTIC_CHRONOLOGY -> uuids(record.getSemanticChronology().getPatternForSemanticPublicId());
            case CONCEPT_CHRONOLOGY -> EntityBinding.Concept.pattern().publicId().asUuidList().castToList();
            case PATTERN_CHRONOLOGY -> EntityBinding.Pattern.pattern().publicId().asUuidList().castToList();
            case STAMP_CHRONOLOGY -> EntityBinding.Stamp.pattern().publicId().asUuidList().castToList();
            case VALUE_NOT_SET -> throw new IllegalStateException("Tinkar message value not set");
        };
    }

    /** Whether a public id is written as longs alone: no text, and two longs for each UUID. */
    private static boolean isLongs(PublicId publicId) {
        return publicId.getUuidsCount() == 0 && publicId.getUuidBitsCount() > 0 && publicId.getUuidBitsCount() % 2 == 0;
    }

    /** A public id's UUIDs, from its longs or, failing those, its text. */
    private static List<UUID> uuids(PublicId publicId) {
        List<UUID> uuids = new ArrayList<>();
        if (publicId.getUuidBitsCount() > 0) {
            for (int i = 0; i + 1 < publicId.getUuidBitsCount(); i += 2) {
                uuids.add(new UUID(publicId.getUuidBits(i), publicId.getUuidBits(i + 1)));
            }
        } else {
            publicId.getUuidsList().forEach(text -> uuids.add(UUID.fromString(text)));
        }
        return uuids;
    }

    /** Visits the message and every message nested in it, at any depth. */
    private static void forEachMessage(Message message, Consumer<Message> visitor) {
        visitor.accept(message);
        for (Map.Entry<FieldDescriptor, Object> field : message.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                continue;
            }
            if (field.getKey().isRepeated()) {
                for (Object element : (List<?>) field.getValue()) {
                    forEachMessage((Message) element, visitor);
                }
            } else {
                forEachMessage((Message) field.getValue(), visitor);
            }
        }
    }

    /**
     * How two files' records differ, as multisets of their bytes: a line for each side's count
     * of records the other lacks, with up to three of them in hex; empty if they hold the same.
     */
    private static List<String> differences(List<byte[]> one, List<byte[]> other) {
        Map<String, Integer> counts = new HashMap<>();
        HexFormat hex = HexFormat.of();
        one.forEach(record -> counts.merge(hex.formatHex(record), 1, Integer::sum));
        other.forEach(record -> counts.merge(hex.formatHex(record), -1, Integer::sum));
        List<String> onlyInOne = new ArrayList<>();
        List<String> onlyInOther = new ArrayList<>();
        counts.forEach((record, count) -> {
            for (int i = 0; i < Math.abs(count); i++) {
                (count > 0 ? onlyInOne : onlyInOther).add(record);
            }
        });
        List<String> differences = new ArrayList<>();
        if (!onlyInOne.isEmpty()) {
            differences.add(onlyInOne.size() + " only in the first: " + onlyInOne.stream().limit(3).toList());
        }
        if (!onlyInOther.isEmpty()) {
            differences.add(onlyInOther.size() + " only in the second: " + onlyInOther.stream().limit(3).toList());
        }
        return differences;
    }
}
