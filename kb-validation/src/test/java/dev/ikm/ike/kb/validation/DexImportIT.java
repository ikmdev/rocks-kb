package dev.ikm.ike.kb.validation;

import dev.ikm.ds.rocks.RocksProvider;
import dev.ikm.tinkar.common.id.impl.NidLayout;
import dev.ikm.tinkar.entity.export.ExportEntitiesToProtobufFile;
import dev.ikm.tinkar.entity.load.LoadEntitiesFromProtobufFile;
import dev.ikm.tinkar.fixtures.ForkedJvm;
import dev.ikm.tinkar.fixtures.StoreDigest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.zip.ZipFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The 64-bit Rocks store against the legacy one on the largest knowledge base the fleet holds,
 * DeX, 60 million entities (IKE-Network/ike-issues#1258, the exit of the 64-bit store): the
 * legacy DeX store exported through the legacy engine, the export loaded into a new 64-bit
 * store, that store closed, reopened and digested again, and the export loaded into a new
 * legacy store in the 6-bit layout; the three digests must agree, and the first must match
 * the export's manifest. The comparison store is 6-bit, as the design's exit says, because
 * the 8-bit layout holds at most 16,777,215 elements per pattern and DeX's largest pattern
 * has 19.8 million: an 8-bit import stops short by three million semantics (2026-10-07).
 *
 * <p>Tagged {@code dex} and off by default; hours long. Run it with
 * {@code -Pdex -Ddex.store=<the legacy DeX store's data root>}, or with
 * {@code -Ddex.export=<protobuf zip>} to start from an export already made.
 */
@Tag("dex")
class DexImportIT {

    private static final Logger LOG = LoggerFactory.getLogger(DexImportIT.class);

    static final String EXPORT = "dex.export";
    static final String SOURCE_STORE = "dex.store";
    private static final String LEGACY_STORE = "legacy.store";
    private static final String LOADED_64 = "loaded64.";
    private static final String REOPENED_64 = "reopened64.";
    private static final String LOADED_LEGACY = "loadedLegacy.";

    @Test
    void theExportLoadsIntoTheSixtyFourBitStoreAsItDoesIntoTheLegacyStore() throws IOException {
        String exportProperty = System.getProperty(EXPORT);
        String sourceProperty = System.getProperty(SOURCE_STORE);
        assumeTrue((exportProperty != null && !exportProperty.isBlank()) || (sourceProperty != null && !sourceProperty.isBlank()),
                "Neither -D" + EXPORT + " nor -D" + SOURCE_STORE + " is set");
        Path work = Path.of("target", "dex-import").toAbsolutePath();
        // The stores are made afresh; an export under the work directory is kept when it is reused.
        SnomedRoundTripIT.deleteTree(work.resolve("store-64bit"));
        SnomedRoundTripIT.deleteTree(work.resolve("store-legacy"));
        Files.createDirectories(work);

        Properties in = new Properties();
        in.setProperty(StoreStage.PROVIDER, Provider.ROCKS.name());
        in.setProperty(LEGACY_STORE, work.resolve("store-legacy").toString());
        Path export;
        if (exportProperty != null && !exportProperty.isBlank()) {
            export = Path.of(exportProperty);
            assertTrue(Files.isRegularFile(export), "No export at " + export);
            in.setProperty(EXPORT, export.toString());
        } else {
            Path source = Path.of(sourceProperty);
            assertTrue(Files.isDirectory(source.resolve("rocks")), "No Rocks store at " + source);
            export = work.resolve("dex-export-pb.zip");
            Files.deleteIfExists(export);
            in.setProperty(SOURCE_STORE, source.toString());
            in.setProperty(EXPORT, export.toString());
            // The legacy store opened in place through the provider's routing, and exported.
            in.setProperty(StoreStage.STORE, source.toString());
            in = ForkedJvm.run(ExportSource.class, in, Duration.ofHours(4));
            assertTrue(Files.isRegularFile(export), "The export was not written to " + export);
        }
        in.setProperty(StoreStage.STORE, work.resolve("store-64bit").toString());

        Properties result = ForkedJvm.run(Load64.class, in, Duration.ofHours(8));
        result = ForkedJvm.run(Reopen64.class, result, Duration.ofHours(2));
        result.setProperty(StoreStage.PROVIDER, Provider.ROCKS_LEGACY.name());
        result = ForkedJvm.run(LoadLegacy.class, result, Duration.ofHours(8));

        StoreDigest loaded = StoreDigest.load(result, LOADED_64);
        StoreDigest reopened = StoreDigest.load(result, REOPENED_64);
        StoreDigest legacy = StoreDigest.load(result, LOADED_LEGACY);

        TreeMap<String, String> observed = new TreeMap<>();
        for (String key : result.stringPropertyNames()) {
            if (key.endsWith(".millis") || key.endsWith(".layout") || key.endsWith(".counters")) {
                observed.put(key, result.getProperty(key));
            }
        }
        LOG.info("DeX: 64-bit {} entities, {} versions; legacy {} entities, {} versions; {}",
                loaded.entities(), loaded.versions(), legacy.entities(), legacy.versions(), observed);

        assertEquals("64-bit", result.getProperty("load64.layout"), "The new store's layout");
        assertEquals("6-bit", result.getProperty("legacy.layout"), "The comparison store's layout");
        if (result.getProperty("source.layout") != null) {
            assertTrue(List.of("6-bit", "8-bit").contains(result.getProperty("source.layout")),
                    "The source store opened in a legacy layout: " + result.getProperty("source.layout"));
            assertEquals(Long.parseLong(result.getProperty("source.export.count")), loaded.entities(),
                    "Entities exported from the source store, against the 64-bit store's digest");
        }

        Attributes manifest = manifest(export);
        assertEquals(Long.parseLong(manifest.getValue("Concept-Count")), loaded.concepts(), "Concepts against the manifest");
        assertEquals(Long.parseLong(manifest.getValue("Semantic-Count")), loaded.semantics(), "Semantics against the manifest");
        assertEquals(Long.parseLong(manifest.getValue("Pattern-Count")), loaded.patterns(), "Patterns against the manifest");
        assertEquals(Long.parseLong(manifest.getValue("Stamp-Count")), loaded.stamps(), "Stamps against the manifest");
        assertEquals(0, loaded.unrendered(), "Field values the digest could not render");

        assertEquals(List.of(), reopened.differencesFrom(loaded),
                "The 64-bit store after a close and reopen, against the store as loaded");
        assertEquals(List.of(), loaded.differencesFrom(legacy),
                "The 64-bit store, against a legacy store loaded from the same export");
    }

    private static Attributes manifest(Path export) throws IOException {
        try (ZipFile zip = new ZipFile(export.toFile())) {
            var entry = zip.getEntry("META-INF/MANIFEST.MF");
            assertTrue(entry != null, export + " has no manifest");
            return new Manifest(zip.getInputStream(entry)).getMainAttributes();
        }
    }

    private static void load(Properties in, Properties out, String prefix) {
        long start = System.currentTimeMillis();
        new LoadEntitiesFromProtobufFile(new File(in.getProperty(EXPORT))).compute();
        out.setProperty(prefix + "load.millis", Long.toString(System.currentTimeMillis() - start));
        out.setProperty(prefix + "layout", NidLayout.active().displayName());
    }

    private static void digest(Properties out, String prefix) {
        long start = System.currentTimeMillis();
        StoreDigest.ofOpenStore().store(out, prefix);
        out.setProperty(prefix + "digest.millis", Long.toString(System.currentTimeMillis() - start));
    }

    /** Stage 0: the legacy DeX store opened in place and exported to protobuf. */
    static class ExportSource extends StoreStage {
        @Override
        void work(Properties in, Properties out) throws Exception {
            out.setProperty("source.layout", NidLayout.active().displayName());
            File exportFile = new File(in.getProperty(EXPORT));
            long start = System.currentTimeMillis();
            long exported = new ExportEntitiesToProtobufFile(exportFile).compute().getTotalCount();
            out.setProperty("source.export.millis", Long.toString(System.currentTimeMillis() - start));
            out.setProperty("source.export.count", Long.toString(exported));
            out.setProperty("source.export.bytes", Long.toString(exportFile.length()));
        }
    }

    /** Stage 1: a new 64-bit store and the export loaded into it. */
    static class Load64 extends StoreStage {
        @Override
        void work(Properties in, Properties out) {
            load(in, out, "load64.");
            digest(out, LOADED_64);
        }
    }

    /** Stage 2: the 64-bit store reopened and digested again. */
    static class Reopen64 extends StoreStage {
        @Override
        void work(Properties in, Properties out) {
            digest(out, REOPENED_64);
        }
    }

    /** Stage 3: a new store in the legacy 6-bit layout and the export loaded into it. */
    static class LoadLegacy extends StoreStage {
        @Override
        String storeProperty() {
            return LEGACY_STORE;
        }

        /** The 6-bit layout, which the 8-bit one cannot replace for DeX; set after the stage's own provider default. */
        @Override
        void configure(Properties in) {
            System.setProperty(RocksProvider.Controller.NEW_STORE_LAYOUT_PROPERTY, "6-bit");
        }

        @Override
        void work(Properties in, Properties out) {
            load(in, out, "legacy.");
            digest(out, LOADED_LEGACY);
        }
    }
}
