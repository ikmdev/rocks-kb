package dev.ikm.ike.kb.validation;

import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.util.uuid.UuidUtil;
import dev.ikm.tinkar.coordinate.Calculators;
import dev.ikm.tinkar.coordinate.view.calculator.ViewCalculator;
import dev.ikm.tinkar.entity.export.ExportEntitiesToProtobufFile;
import dev.ikm.tinkar.entity.load.LoadEntitiesFromProtobufFile;
import dev.ikm.tinkar.fixtures.ForkedJvm;
import dev.ikm.tinkar.fixtures.StoreDigest;
import dev.ikm.tinkar.reasoner.service.ClassifierResults;
import dev.ikm.tinkar.reasoner.service.ReasonerService;
import dev.ikm.tinkar.terms.TinkarTerm;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.stream.Stream;
import java.util.zip.ZipFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The SNOMED CT knowledge base through its whole life, in each store provider: loaded
 * from its protobuf export; classified and the inferred results written; queried;
 * exported; and the export loaded into a fresh store that must hold what the exported
 * store held. Each store lifetime is a stage in its own JVM ({@link ForkedJvm}).
 *
 * <p>What the load, the classification and the queries observe is compared with one
 * recorded reference, {@code snomed-round-trip.properties}, for every provider: the
 * providers must agree with each other as well as with the last release.
 */
@Tag("snomed")
class SnomedRoundTripIT {

    private static final Logger LOG = LoggerFactory.getLogger(SnomedRoundTripIT.class);

    static final String REFERENCE = "snomed-round-trip.properties";

    private static final String RESTORED_STORE = "restored.store";
    private static final String KB_FILE = "kb.file";
    private static final String EXPORT_FILE = "export.file";

    private static final String LOADED = "loaded.";
    private static final String CLASSIFIED = "classified.";
    private static final String EXPORTED = "exported.";
    private static final String RESTORED = "restored.";
    private static final String CLASSIFICATION = "classification.";
    private static final String QUERY = "query.";

    /** The SNOMED CT concepts whose descendants and names the queries read. */
    private static final List<String> QUERIED_SCTIDS = List.of(
            "138875005", // SNOMED CT Concept
            "404684003", // Clinical finding
            "71388002",  // Procedure
            "123037004", // Body structure
            "105590001", // Substance
            "22298006"); // Myocardial infarction

    static Path dataDirectory() {
        return Path.of(System.getProperty("snomed.data.directory", "target/data"));
    }

    static Path knowledgeBase() {
        Path kb = dataDirectory().resolve("snomedct-kb-pb.zip");
        assertTrue(Files.isRegularFile(kb), "No SNOMED CT knowledge base at " + kb + "; run with -Psnomed");
        return kb;
    }

    @ParameterizedTest
    @EnumSource(Provider.class)
    void theKnowledgeBaseIsLoadedClassifiedQueriedExportedAndRestored(Provider provider) throws IOException {
        Path work = Path.of("target", "snomed-round-trip", provider.name().toLowerCase()).toAbsolutePath();
        deleteTree(work);
        Files.createDirectories(work);
        Path kb = knowledgeBase();

        Properties in = new Properties();
        in.setProperty(StoreStage.PROVIDER, provider.name());
        in.setProperty(StoreStage.STORE, work.resolve("store").toString());
        in.setProperty(RESTORED_STORE, work.resolve("restored").toString());
        in.setProperty(KB_FILE, kb.toString());
        in.setProperty(EXPORT_FILE, work.resolve("export-pb.zip").toString());

        Properties result = ForkedJvm.run(Load.class, in, Duration.ofHours(2));
        result = ForkedJvm.run(Classify.class, result, Duration.ofHours(1));
        result = ForkedJvm.run(Query.class, result, Duration.ofHours(1));
        result = ForkedJvm.run(Export.class, result, Duration.ofHours(1));
        result = ForkedJvm.run(Restore.class, result, Duration.ofHours(2));

        StoreDigest loaded = StoreDigest.load(result, LOADED);
        StoreDigest classified = StoreDigest.load(result, CLASSIFIED);
        StoreDigest exported = StoreDigest.load(result, EXPORTED);
        StoreDigest restored = StoreDigest.load(result, RESTORED);

        // Load: everything the export's manifest says it holds, all of it readable
        Attributes manifest = manifest(kb);
        assertEquals(Long.parseLong(manifest.getValue("Concept-Count")), loaded.concepts(), "Concepts against the manifest");
        assertEquals(Long.parseLong(manifest.getValue("Semantic-Count")), loaded.semantics(), "Semantics against the manifest");
        assertEquals(Long.parseLong(manifest.getValue("Pattern-Count")), loaded.patterns(), "Patterns against the manifest");
        assertEquals(Long.parseLong(manifest.getValue("Stamp-Count")), loaded.stamps(), "Stamps against the manifest");
        assertEquals(0, loaded.unrendered(), "Field values the digest could not render");

        // Classify: the same concepts
        assertEquals(loaded.concepts(), classified.concepts(), "Classification must not add or remove concepts");

        // Export and restore: closing, reopening and a protobuf round trip lose nothing
        assertEquals(List.of(), exported.differencesFrom(classified),
                "The store after it was closed and reopened, against the store as classified");
        assertEquals(exported.entities(), Long.parseLong(result.getProperty("export.count")), "Entities written to the export file");
        assertEquals(List.of(), restored.differencesFrom(exported),
                "The fresh store after loading the export, against the store that was exported");

        TreeMap<String, String> timings = new TreeMap<>();
        for (String key : result.stringPropertyNames()) {
            if (key.endsWith(".millis") || key.endsWith(".bytes")) {
                timings.put(key, result.getProperty(key));
            }
        }
        LOG.info("{}: {} entities, {} versions; {}", provider, exported.entities(), exported.versions(), timings);

        // The same reference for every provider
        TreeMap<String, String> observed = new TreeMap<>();
        for (String prefix : List.of(LOADED, CLASSIFICATION, QUERY)) {
            for (String key : result.stringPropertyNames()) {
                if (key.startsWith(prefix) && !key.endsWith(".millis")) {
                    observed.put(key, result.getProperty(key));
                }
            }
        }
        Reference.check(observed, REFERENCE, work);
    }

    private static Attributes manifest(Path kb) throws IOException {
        try (ZipFile zip = new ZipFile(kb.toFile())) {
            var entry = zip.getEntry("META-INF/MANIFEST.MF");
            assertTrue(entry != null, kb + " has no manifest");
            return new Manifest(zip.getInputStream(entry)).getMainAttributes();
        }
    }

    static void deleteTree(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }

    /** Stage 1: a new store and the knowledge base loaded into it. */
    static class Load extends StoreStage {
        @Override
        void work(Properties in, Properties out) {
            long start = System.currentTimeMillis();
            new LoadEntitiesFromProtobufFile(new File(in.getProperty(KB_FILE))).compute();
            out.setProperty("load.millis", Long.toString(System.currentTimeMillis() - start));
            start = System.currentTimeMillis();
            StoreDigest.ofOpenStore().store(out, LOADED);
            out.setProperty("digest.millis", Long.toString(System.currentTimeMillis() - start));
        }
    }

    /** Stage 2: the store reopened, classified under the default view, and the inferred results written. */
    static class Classify extends StoreStage {
        @Override
        void work(Properties in, Properties out) throws Exception {
            long start = System.currentTimeMillis();
            ReasonerService reasoner = Classification.classify(Calculators.View.Default(), out, CLASSIFICATION);
            Classification.of(reasoner).store(out, CLASSIFICATION);
            long write = System.currentTimeMillis();
            ClassifierResults results = reasoner.writeInferredResults();
            out.setProperty(CLASSIFICATION + "write.millis", Long.toString(System.currentTimeMillis() - write));
            out.setProperty(CLASSIFICATION + "concepts.with.inferred.changes", Long.toString(results.getConceptsWithInferredChanges().size()));
            out.setProperty("classify.millis", Long.toString(System.currentTimeMillis() - start));
            StoreDigest.ofOpenStore().store(out, CLASSIFIED);
        }
    }

    /** Stage 3: the store reopened and read through the default view's calculators. */
    static class Query extends StoreStage {
        @Override
        void work(Properties in, Properties out) {
            ViewCalculator view = Calculators.View.Default();
            long start = System.currentTimeMillis();
            AtomicLong concepts = new AtomicLong();
            AtomicLong withoutDescription = new AtomicLong();
            PrimitiveData.get().forEachConceptNid(nid -> {
                concepts.incrementAndGet();
                if (view.getDescriptionText(nid).filter(text -> !text.isBlank()).isEmpty()) {
                    withoutDescription.incrementAndGet();
                }
            });
            var descendants = view.navigationCalculator().descendentsOf(TinkarTerm.ROOT_VERTEX.nid());
            AtomicLong withoutParents = new AtomicLong();
            descendants.intStream().forEach(nid -> {
                if (view.navigationCalculator().parentsOf(nid).isEmpty()) {
                    withoutParents.incrementAndGet();
                }
            });
            out.setProperty(QUERY + "concepts", Long.toString(concepts.get()));
            out.setProperty(QUERY + "concepts.without.description", Long.toString(withoutDescription.get()));
            out.setProperty(QUERY + "descendants.of.root", Long.toString(descendants.size()));
            out.setProperty(QUERY + "descendants.without.parents", Long.toString(withoutParents.get()));
            for (String sctid : QUERIED_SCTIDS) {
                int nid = PrimitiveData.nid(UuidUtil.fromSNOMED(sctid));
                out.setProperty(QUERY + sctid + ".name", view.getDescriptionTextOrNid(nid));
                out.setProperty(QUERY + sctid + ".parents", Integer.toString(view.navigationCalculator().parentsOf(nid).size()));
                out.setProperty(QUERY + sctid + ".children", Integer.toString(view.navigationCalculator().childrenOf(nid).size()));
                out.setProperty(QUERY + sctid + ".descendants", Integer.toString(view.navigationCalculator().descendentsOf(nid).size()));
            }
            out.setProperty("query.millis", Long.toString(System.currentTimeMillis() - start));
        }
    }

    /** Stage 4: the store reopened and exported to protobuf. */
    static class Export extends StoreStage {
        @Override
        void work(Properties in, Properties out) throws Exception {
            StoreDigest.ofOpenStore().store(out, EXPORTED);
            File exportFile = new File(in.getProperty(EXPORT_FILE));
            long start = System.currentTimeMillis();
            long exported = new ExportEntitiesToProtobufFile(exportFile).compute().getTotalCount();
            out.setProperty("export.millis", Long.toString(System.currentTimeMillis() - start));
            out.setProperty("export.count", Long.toString(exported));
            out.setProperty("export.bytes", Long.toString(exportFile.length()));
        }
    }

    /** Stage 5: a fresh store and the export loaded into it. */
    static class Restore extends StoreStage {
        @Override
        String storeProperty() {
            return RESTORED_STORE;
        }

        @Override
        void work(Properties in, Properties out) {
            long start = System.currentTimeMillis();
            new LoadEntitiesFromProtobufFile(new File(in.getProperty(EXPORT_FILE))).compute();
            out.setProperty("restore.millis", Long.toString(System.currentTimeMillis() - start));
            StoreDigest.ofOpenStore().store(out, RESTORED);
        }
    }
}
