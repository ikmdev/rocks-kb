package dev.ikm.ds.rocks;

import dev.ikm.tinkar.common.id.PublicId;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.common.service.DataServiceController;
import dev.ikm.tinkar.common.service.EntityCountSummary;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.entity.load.IdentityIndex;
import dev.ikm.tinkar.entity.load.LoadEntitiesFromProtobufFile;
import dev.ikm.tinkar.schema.TinkarMsg;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A Rocks KB — whose nids encode the pattern — imports a changeset full of forward references in
 * one read of its records when the changeset carries an identity index.
 *
 * <p>The changeset is the starter data with its records reversed, so semantics come before the
 * concepts, patterns and stamps they reference. Without an index a single pass cannot assign those
 * references nids; with one, every nid is assigned from the index first.
 *
 * <p>One store for the whole class: the Rocks provider is one per process.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RocksIdentityIndexImportTest {

    private static final File STARTER_DATA = new File("target/data/tinkar-starter-data-reasoned-pb.zip");

    @TempDir
    static Path home;

    private static String originalHome;
    private static List<TinkarMsg> reversed;
    private static File withIndex;
    private static File withoutIndex;

    @BeforeAll
    static void startEmptyRocksStore() throws IOException {
        List<TinkarMsg> records = readRecords(STARTER_DATA);
        reversed = new ArrayList<>(records);
        Collections.reverse(reversed);
        withIndex = writeChangeSet(home.resolve("reversed-indexed.zip").toFile(), reversed, true);
        withoutIndex = writeChangeSet(home.resolve("reversed-plain.zip").toFile(), reversed, false);

        // New Rocks KB creates its store under ${user.home}/Solor — keep it inside the temp folder.
        originalHome = System.getProperty("user.home");
        System.setProperty("user.home", home.toString());
        DataServiceController<?> controller = PrimitiveData.getControllerOptions().stream()
                .filter(c -> RocksProvider.NewController.CONTROLLER_NAME.equals(c.controllerName()))
                .findFirst()
                .orElseThrow();
        controller.setDataServiceProperty(RocksProvider.NewController.NEW_FOLDER_PROPERTY, "RocksKb");
        PrimitiveData.selectControllerByClass(RocksProvider.NewController.class);
        PrimitiveData.start();
    }

    @AfterAll
    static void stopStore() {
        try {
            PrimitiveData.stop();
        } finally {
            System.setProperty("user.home", originalHome);
        }
    }

    @Test
    @Order(1)
    void withoutAnIndex_aSinglePassCannotResolveForwardReferences() {
        // The control: proves the reversed file really needs its identities up front on Rocks.
        assertThrows(RuntimeException.class, () -> new LoadEntitiesFromProtobufFile(withoutIndex, false).compute());
    }

    @Test
    @Order(2)
    void withAnIndex_theRecordsAreReadOnce_andEveryComponentImports() {
        LoadEntitiesFromProtobufFile loader = new LoadEntitiesFromProtobufFile(withIndex, true);

        EntityCountSummary summary = loader.compute();

        assertTrue(loader.usedIdentityIndex(), "the identities should have come from the index");
        assertEquals(reversed.size(), summary.getTotalCount());
        Set<PublicId> components = new LinkedHashSet<>();
        for (TinkarMsg record : reversed) {
            components.add(PublicIds.of(IdentityIndex.componentOf(record).getUuidsList().stream()
                    .map(UUID::fromString).toArray(UUID[]::new)));
        }
        for (PublicId component : components) {
            int nid = PrimitiveData.nid(component);
            assertNotNull(PrimitiveData.get().getBytes(nid), "no entity stored for " + component);
        }
    }

    private static List<TinkarMsg> readRecords(File zip) throws IOException {
        List<TinkarMsg> records = new ArrayList<>();
        try (ZipInputStream in = new ZipInputStream(new FileInputStream(zip))) {
            ZipEntry entry;
            while ((entry = in.getNextEntry()) != null) {
                if (IdentityIndex.isMetadata(entry.getName())) {
                    continue;
                }
                TinkarMsg record;
                while ((record = TinkarMsg.parseDelimitedFrom(in)) != null) {
                    records.add(record);
                }
            }
        }
        return records;
    }

    private static File writeChangeSet(File file, List<TinkarMsg> records, boolean index) throws IOException {
        try (ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(file));
             IdentityIndex.Writer identities = new IdentityIndex.Writer()) {
            zip.putNextEntry(new ZipEntry("Entities"));
            for (TinkarMsg record : records) {
                record.writeDelimitedTo(zip);
                identities.add(record);
            }
            zip.closeEntry();
            if (index) {
                identities.writeTo(zip);
            }
            zip.putNextEntry(new ZipEntry("META-INF/MANIFEST.MF"));
            zip.write(("Manifest-Version: 1.0\nTotal-Count: " + records.size() + "\n\n").getBytes(StandardCharsets.UTF_8));
            zip.closeEntry();
        }
        return file;
    }
}
