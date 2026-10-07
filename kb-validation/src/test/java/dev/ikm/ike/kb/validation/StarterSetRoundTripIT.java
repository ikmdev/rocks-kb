package dev.ikm.ike.kb.validation;

import dev.ikm.tinkar.common.service.internal.EntityStore;
import dev.ikm.tinkar.common.id.IntIdList;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.service.PrimitiveDataSearchResult;
import dev.ikm.tinkar.common.service.SearchService;
import dev.ikm.tinkar.common.service.ServiceLifecycleManager;
import dev.ikm.tinkar.coordinate.Calculators;
import dev.ikm.tinkar.coordinate.language.LanguageCoordinateRecord;
import dev.ikm.tinkar.coordinate.stamp.calculator.Latest;
import dev.ikm.tinkar.coordinate.view.calculator.ViewCalculator;
import dev.ikm.tinkar.entity.EntityHandle;
import dev.ikm.tinkar.entity.EntityVersion;
import dev.ikm.tinkar.entity.SemanticEntityVersion;
import dev.ikm.tinkar.entity.export.ExportEntitiesToProtobufFile;
import dev.ikm.tinkar.entity.load.LoadEntitiesFromProtobufFile;
import dev.ikm.tinkar.fixtures.ForkedJvm;
import dev.ikm.tinkar.fixtures.StoreDigest;
import org.apache.lucene.queryparser.flexible.standard.QueryParserUtil;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.zip.ZipFile;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The IKE starter set through a store's whole life, in each store provider (Rocks, spined
 * array, MVStore, and the in-memory ephemeral store): loaded from its
 * protobuf export; read and searched; exported; and the export loaded into a fresh store,
 * which is read and searched again and must hold what the exported store held. Each store
 * lifetime is a stage in its own JVM ({@link ForkedJvm}), started as an application starts
 * one, through the service lifecycle. An ephemeral store does not outlive its JVM, so it is
 * loaded, read and exported in one lifetime and restored and read in a second.
 *
 * <p>Reading checks every description the default view's language coordinate covers: it has
 * a latest version under the default view, that version is active, and a search for its text
 * finds it, which needs the search index built and the search service running. Every concept
 * must have a fully qualified name under the default view; a regular name is optional.
 */
@Tag("starter-set")
class StarterSetRoundTripIT {

    private static final Logger LOG = LoggerFactory.getLogger(StarterSetRoundTripIT.class);

    private static final String RESTORED_STORE = "restored.store";
    private static final String KB_FILE = "kb.file";
    private static final String EXPORT_FILE = "export.file";

    private static final String LOADED = "loaded.";
    private static final String EXPORTED = "exported.";
    private static final String RESTORED = "restored.";
    private static final String QUERY = "query.";
    private static final String RESTORED_QUERY = "restored.query.";

    static Path starterSet() {
        Path kb = Path.of(System.getProperty("starter.set.data.directory", "target/data"))
                .resolve("ike-starter-set-reasoned-pb.zip");
        assertTrue(Files.isRegularFile(kb), "No IKE starter set at " + kb + "; run with -Pstarter-set");
        return kb;
    }

    @ParameterizedTest
    @EnumSource(Provider.class)
    void theStarterSetIsLoadedReadSearchedExportedAndRestored(Provider provider) throws IOException {
        Path work = Path.of("target", "starter-set-round-trip", provider.name().toLowerCase()).toAbsolutePath();
        SnomedRoundTripIT.deleteTree(work);
        Files.createDirectories(work);
        Path kb = starterSet();

        Properties in = new Properties();
        in.setProperty(StoreStage.PROVIDER, provider.name());
        in.setProperty(StoreStage.STORE, work.resolve("store").toString());
        in.setProperty(RESTORED_STORE, work.resolve("restored").toString());
        in.setProperty(KB_FILE, kb.toString());
        in.setProperty(EXPORT_FILE, work.resolve("export-pb.zip").toString());

        Properties result;
        if (provider.persistent) {
            Properties loadedResult = ForkedJvm.run(Load.class, in, Duration.ofMinutes(10));
            Properties queried = ForkedJvm.run(Query.class, loadedResult, Duration.ofMinutes(10));
            Properties exportedResult = ForkedJvm.run(Export.class, queried, Duration.ofMinutes(10));
            Properties restoredResult = ForkedJvm.run(Restore.class, exportedResult, Duration.ofMinutes(10));
            result = ForkedJvm.run(RestoredQuery.class, restoredResult, Duration.ofMinutes(10));
        } else {
            Properties exportedResult = ForkedJvm.run(LoadQueryExport.class, in, Duration.ofMinutes(10));
            result = ForkedJvm.run(RestoreAndQuery.class, exportedResult, Duration.ofMinutes(10));
        }

        StoreDigest loaded = StoreDigest.load(result, LOADED);
        StoreDigest exported = StoreDigest.load(result, EXPORTED);
        StoreDigest restored = StoreDigest.load(result, RESTORED);

        // Every check runs, and every failure is reported, not only the first.
        List<Executable> checks = new ArrayList<>();

        // Load: everything the export's manifest says it holds, all of it readable
        Attributes manifest = manifest(kb);
        checks.add(() -> assertEquals(Long.parseLong(manifest.getValue("Concept-Count")), loaded.concepts(), "Concepts against the manifest"));
        checks.add(() -> assertEquals(Long.parseLong(manifest.getValue("Semantic-Count")), loaded.semantics(), "Semantics against the manifest"));
        checks.add(() -> assertEquals(Long.parseLong(manifest.getValue("Pattern-Count")), loaded.patterns(), "Patterns against the manifest"));
        checks.add(() -> assertEquals(Long.parseLong(manifest.getValue("Stamp-Count")), loaded.stamps(), "Stamps against the manifest"));
        checks.add(() -> assertEquals(0, loaded.unrendered(), "Field values the digest could not render"));

        // Read and search, in the loaded store and in the store restored from its export
        for (String prefix : List.of(QUERY, RESTORED_QUERY)) {
            String store = prefix.equals(QUERY) ? "loaded store" : "restored store";
            checks.add(() -> assertEquals("true", result.getProperty(prefix + "search.service.running"),
                    store + ": the search service is running"));
            checks.add(() -> assertTrue(number(result, prefix + "descriptions") > 0, store + ": descriptions under the default view"));
            checks.add(() -> assertEquals(0, number(result, prefix + "descriptions.without.latest"),
                    store + ": descriptions with no latest version under the default view: "
                            + result.getProperty(prefix + "descriptions.without.latest.examples")));
            checks.add(() -> assertEquals(0, number(result, prefix + "descriptions.inactive"),
                    store + ": descriptions whose latest version is not active: "
                            + result.getProperty(prefix + "descriptions.inactive.examples")));
            checks.add(() -> assertEquals(0, number(result, prefix + "descriptions.not.found.by.search"),
                    store + ": descriptions a search for their text does not find ("
                            + result.getProperty(prefix + "descriptions.searched") + " searched): "
                            + result.getProperty(prefix + "descriptions.not.found.by.search.examples")));
            checks.add(() -> assertEquals(0, number(result, prefix + "concepts.without.fully.qualified.name"),
                    store + ": concepts with no fully qualified name under the default view ("
                            + result.getProperty(prefix + "concepts") + " concepts): "
                            + result.getProperty(prefix + "concepts.without.fully.qualified.name.examples")));
        }

        // Export and restore: closing, reopening and a protobuf round trip lose nothing
        checks.add(() -> assertEquals(List.of(), exported.differencesFrom(loaded), provider.persistent
                ? "The store after it was closed and reopened, against the store as loaded"
                : "The store when it was exported, against the store as loaded"));
        checks.add(() -> assertEquals(exported.entities(), number(result, "export.count"), "Entities written to the export file"));
        checks.add(() -> assertEquals(List.of(), restored.differencesFrom(exported),
                "The fresh store after loading the export, against the store that was exported"));

        TreeMap<String, String> timings = new TreeMap<>();
        for (String key : result.stringPropertyNames()) {
            if (key.endsWith(".millis") || key.endsWith(".bytes")) {
                timings.put(key, result.getProperty(key));
            }
        }
        LOG.info("{}: {} entities, {} versions, {} descriptions searched; {}", provider, exported.entities(),
                exported.versions(), result.getProperty(QUERY + "descriptions.searched"), timings);
        assertAll(provider.name(), checks);
    }

    private static long number(Properties properties, String key) {
        String value = properties.getProperty(key);
        assertTrue(value != null, "No " + key + " was recorded");
        return Long.parseLong(value);
    }

    private static Attributes manifest(Path kb) throws IOException {
        try (ZipFile zip = new ZipFile(kb.toFile())) {
            var entry = zip.getEntry("META-INF/MANIFEST.MF");
            assertTrue(entry != null, kb + " has no manifest");
            return new Manifest(zip.getInputStream(entry)).getMainAttributes();
        }
    }

    /** Stage 1: a new store and the starter set loaded into it. */
    static class Load extends StoreStage {
        @Override
        void work(Properties in, Properties out) {
            long start = System.currentTimeMillis();
            new LoadEntitiesFromProtobufFile(new File(in.getProperty(KB_FILE))).compute();
            out.setProperty("load.millis", Long.toString(System.currentTimeMillis() - start));
            StoreDigest.ofOpenStore().store(out, LOADED);
        }
    }

    /** Stage 2: the store reopened, its descriptions read through the default view and searched. */
    static class Query extends StoreStage {
        String prefix() {
            return QUERY;
        }

        @Override
        void work(Properties in, Properties out) throws Exception {
            String prefix = prefix();
            long start = System.currentTimeMillis();
            out.setProperty(prefix + "search.service.running",
                    Boolean.toString(ServiceLifecycleManager.get().getRunningService(SearchService.class).isPresent()));

            ViewCalculator view = Calculators.View.Default();
            // The description patterns the default view reads names from.
            MutableIntSet descriptionPatterns = IntSets.mutable.empty();
            for (LanguageCoordinateRecord language : view.languageCalculator().languageCoordinateList()) {
                IntIdList patterns = language.descriptionPatternPreferenceNidList();
                patterns.forEach(descriptionPatterns::add);
            }

            Count descriptions = new Count();
            Count withoutLatest = new Count();
            Count inactive = new Count();
            Count searched = new Count();
            Count notFound = new Count();
            descriptionPatterns.forEach(patternNid -> EntityStore.current().forEachSemanticNidOfPattern(patternNid, nid -> {
                descriptions.add();
                Latest<EntityVersion> latest = view.latest(nid);
                if (latest.isAbsent()) {
                    withoutLatest.add(PrimitiveData.textWithNid(nid));
                    return;
                }
                if (!latest.get().active()) {
                    inactive.add(PrimitiveData.textWithNid(nid));
                }
                String text = textOf((SemanticEntityVersion) latest.get());
                if (text == null || text.codePoints().noneMatch(Character::isLetterOrDigit)) {
                    return; // Nothing a text search can match.
                }
                searched.add();
                if (!foundBySearch(nid, text)) {
                    notFound.add('"' + text + '"');
                }
            }));

            Count concepts = new Count();
            Count withoutFullyQualifiedName = new Count();
            EntityStore.current().forEachConceptNid(nid -> {
                if (EntityHandle.get(nid).isAbsent()) {
                    return; // A nid allocated for a concept the starter set does not hold.
                }
                concepts.add();
                if (view.languageCalculator().getFullyQualifiedNameText(nid).filter(text -> !text.isBlank()).isEmpty()) {
                    withoutFullyQualifiedName.add(view.getDescriptionTextOrNid(nid));
                }
            });

            descriptions.store(out, prefix + "descriptions");
            withoutLatest.store(out, prefix + "descriptions.without.latest");
            inactive.store(out, prefix + "descriptions.inactive");
            searched.store(out, prefix + "descriptions.searched");
            notFound.store(out, prefix + "descriptions.not.found.by.search");
            concepts.store(out, prefix + "concepts");
            withoutFullyQualifiedName.store(out, prefix + "concepts.without.fully.qualified.name");
            out.setProperty(prefix + "millis", Long.toString(System.currentTimeMillis() - start));
        }

        private static String textOf(SemanticEntityVersion version) {
            for (Object value : version.fieldValues()) {
                if (value instanceof String text) {
                    return text;
                }
            }
            return null;
        }

        /** Whether a phrase search for the text returns the description itself. */
        private static boolean foundBySearch(int nid, String text) {
            try {
                String phrase = '"' + QueryParserUtil.escape(text) + '"';
                for (PrimitiveDataSearchResult result : PrimitiveData.get().search(phrase, 10_000)) {
                    if (result.nid() == nid) {
                        return true;
                    }
                }
                return false;
            } catch (Exception e) {
                return false;
            }
        }
    }

    /** Stage 3: the store reopened and exported to protobuf. */
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

    /** Stage 4: a fresh store and the export loaded into it. */
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

    /** Stage 5: the restored store reopened, read and searched as the loaded store was. */
    static class RestoredQuery extends Query {
        @Override
        String storeProperty() {
            return RESTORED_STORE;
        }

        @Override
        String prefix() {
            return RESTORED_QUERY;
        }
    }

    /** An ephemeral store does not outlive its JVM: loaded, read, searched and exported in one lifetime. */
    static class LoadQueryExport extends StoreStage {
        @Override
        void work(Properties in, Properties out) throws Exception {
            new Load().work(in, out);
            new Query().work(in, out);
            new Export().work(in, out);
        }
    }

    /** The second lifetime of an ephemeral store: the export loaded, read and searched. */
    static class RestoreAndQuery extends StoreStage {
        @Override
        String storeProperty() {
            return RESTORED_STORE;
        }

        @Override
        void work(Properties in, Properties out) throws Exception {
            new Restore().work(in, out);
            new RestoredQuery().work(in, out);
        }
    }
}
