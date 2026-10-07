package dev.ikm.ike.kb.validation;

import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.service.internal.EntityStore;
import dev.ikm.tinkar.entity.EntityRecordFactory;
import dev.ikm.tinkar.entity.load.LoadEntitiesFromProtobufFile;
import dev.ikm.tinkar.fixtures.ForkedJvm;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.stream.Stream;
import java.util.zip.ZipFile;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The three baselines of the 64-bit nid design on the SNOMED CT store (design
 * {@code design-2026-09-30-64-bit-nids}, "Specification and fixtures";
 * IKE-Network/ike-issues#1244), against which later steps, RocksDB compression first, are
 * measured:
 * <ul>
 *   <li><b>Import time</b>: a new Rocks store loaded from the SNOMED CT protobuf export and saved,
 *   search index included, as an import runs.</li>
 *   <li><b>Size on disk</b>: the closed store, in all and by what is at its top level.</li>
 *   <li><b>Full iteration time</b>: in a JVM that did not load the store, so the first round is a
 *   cold read, every entity's bytes in order, the same in parallel, and every entity decoded.
 *   Each runs {@value #DEFAULT_ROUNDS} rounds by default ({@code -Dbaseline.rounds}); the first is
 *   reported apart and the median of the rest is the measure.</li>
 * </ul>
 * <p>The measures are compared with a reference for this machine,
 * {@code src/test/resources/benchmarks/<machine>/snomed-rocks.properties}, where the machine is
 * the lease identity in {@code ~/.ike-machine-id}. A time more than {@value #BOUND} times its
 * reference, or a store more than {@value #SIZE_BOUND} times its reference size, fails; a time
 * whose reference is under {@value #NOISE_FLOOR_MILLIS} ms is reported but not bounded, being
 * noise at this size; counts must equal the reference's. With no reference for the machine, the measures are written to
 * {@code target/store-benchmarks/<machine>/snomed-rocks.properties}, to be copied into place.
 * Runs with {@code -Psnomed}.
 */
@Tag("snomed")
class SnomedBaselineIT {

    private static final Logger LOG = LoggerFactory.getLogger(SnomedBaselineIT.class);

    private static final int DEFAULT_ROUNDS = 5;
    private static final double BOUND = 2.0;
    private static final double SIZE_BOUND = 1.25;
    private static final long NOISE_FLOOR_MILLIS = 1_000;
    private static final String ROUNDS = "baseline.rounds";
    private static final String KB_FILE = "kb.file";
    private static final String REFERENCE = "snomed-rocks.properties";

    @Test
    void importSizeAndIterationStayWithinTheirReference() throws IOException {
        Path kb = SnomedRoundTripIT.knowledgeBase();
        Path work = Path.of("target", "snomed-baseline").toAbsolutePath();
        SnomedRoundTripIT.deleteTree(work);
        Files.createDirectories(work);
        Path store = work.resolve("store");

        Properties in = new Properties();
        in.setProperty(StoreStage.PROVIDER, Provider.ROCKS.name());
        in.setProperty(StoreStage.STORE, store.toString());
        in.setProperty(KB_FILE, kb.toString());
        in.setProperty(ROUNDS, Integer.toString(Integer.getInteger(ROUNDS, DEFAULT_ROUNDS)));

        Properties result = ForkedJvm.run(Import.class, in, Duration.ofHours(2));
        sizes(store, result);
        result = ForkedJvm.run(Iterate.class, result, Duration.ofHours(1));

        long manifestEntities = manifestEntities(kb);
        assertEquals(manifestEntities, Long.parseLong(result.getProperty("iterate.bytes.count")),
                "entities iterated, against the export's manifest");

        TreeMap<String, String> measures = new TreeMap<>();
        for (String key : result.stringPropertyNames()) {
            if (key.endsWith(".millis") || key.endsWith(".bytes") || key.endsWith(".count")) {
                measures.put(key, result.getProperty(key));
            }
        }
        String machine = StoreBenchmarkIT.machine();
        LOG.info("SNOMED CT baselines, Rocks on {}:\n{}", machine, table(measures));

        Properties reference = reference(machine);
        if (reference == null) {
            Path written = write(machine, measures);
            LOG.warn("No SNOMED CT baseline for {}; this run's measures are in {}. "
                    + "Copy it to src/test/resources/benchmarks/{}/ to adopt it.", machine, written, machine);
            return;
        }
        List<org.junit.jupiter.api.function.Executable> checks = new ArrayList<>();
        for (Map.Entry<String, String> measure : measures.entrySet()) {
            String key = measure.getKey();
            String referenceValue = reference.getProperty(key);
            if (referenceValue == null || key.endsWith(".first.millis")) {
                continue;
            }
            long observed = Long.parseLong(measure.getValue());
            long expected = Long.parseLong(referenceValue);
            if (key.endsWith(".count")) {
                checks.add(() -> assertEquals(expected, observed, key));
            } else if (key.equals("size.total.bytes")) {
                checks.add(() -> assertTrue(observed <= expected * SIZE_BOUND, key + ": " + observed
                        + " bytes is more than " + SIZE_BOUND + " times the reference " + expected));
            } else if (key.endsWith(".millis") && expected >= NOISE_FLOOR_MILLIS) {
                checks.add(() -> assertTrue(observed <= expected * BOUND, key + ": " + observed
                        + " ms is more than " + BOUND + " times the reference " + expected + " ms"));
            }
        }
        assertAll(checks);
    }

    /** The bytes of the closed store, in all and by each entry at its top level. */
    private static void sizes(Path store, Properties out) throws IOException {
        long total = 0;
        try (Stream<Path> entries = Files.list(store)) {
            for (Path entry : entries.sorted().toList()) {
                long bytes = bytes(entry);
                total += bytes;
                out.setProperty("size." + entry.getFileName() + ".bytes", Long.toString(bytes));
            }
        }
        out.setProperty("size.total.bytes", Long.toString(total));
    }

    private static long bytes(Path path) throws IOException {
        try (Stream<Path> files = Files.walk(path)) {
            return files.filter(Files::isRegularFile).mapToLong(file -> {
                try {
                    return Files.size(file);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }).sum();
        }
    }

    private static long manifestEntities(Path kb) throws IOException {
        try (ZipFile zip = new ZipFile(kb.toFile())) {
            var entry = zip.getEntry("META-INF/MANIFEST.MF");
            assertTrue(entry != null, kb + " has no manifest");
            Attributes manifest = new Manifest(zip.getInputStream(entry)).getMainAttributes();
            return Stream.of("Concept-Count", "Semantic-Count", "Pattern-Count", "Stamp-Count")
                    .mapToLong(name -> Long.parseLong(manifest.getValue(name))).sum();
        }
    }

    private static String table(Map<String, String> measures) {
        StringBuilder table = new StringBuilder();
        measures.forEach((key, value) -> table.append(String.format("%-48s %,20d%n", key, Long.parseLong(value))));
        return table.toString();
    }

    private static Properties reference(String machine) throws IOException {
        try (InputStream stream = SnomedBaselineIT.class.getResourceAsStream("/benchmarks/" + machine + "/" + REFERENCE)) {
            if (stream == null) {
                return null;
            }
            Properties reference = new Properties();
            reference.load(stream);
            return reference;
        }
    }

    private static Path write(String machine, Map<String, String> measures) throws IOException {
        Path file = Path.of("target", "store-benchmarks", machine, REFERENCE).toAbsolutePath();
        Files.createDirectories(file.getParent());
        Properties written = new Properties();
        measures.forEach(written::setProperty);
        try (OutputStream stream = Files.newOutputStream(file)) {
            written.store(stream, "SNOMED CT baselines: Rocks on " + machine);
        }
        return file;
    }

    /** Stage 1: a new Rocks store, the SNOMED CT export loaded into it, and saved. */
    static class Import extends StoreStage {
        @Override
        void work(Properties in, Properties out) {
            long start = System.currentTimeMillis();
            new LoadEntitiesFromProtobufFile(new File(in.getProperty(KB_FILE))).compute();
            long loaded = System.currentTimeMillis();
            PrimitiveData.save();
            long saved = System.currentTimeMillis();
            out.setProperty("import.load.millis", Long.toString(loaded - start));
            out.setProperty("import.save.millis", Long.toString(saved - loaded));
            out.setProperty("import.millis", Long.toString(saved - start));
        }
    }

    /** Stage 2: the store reopened in a new JVM, and every entity iterated. */
    static class Iterate extends StoreStage {
        @Override
        void work(Properties in, Properties out) {
            int rounds = Integer.parseInt(in.getProperty(ROUNDS));
            time("iterate.bytes", rounds, out, () -> {
                LongAdder count = new LongAdder();
                EntityStore.current().forEach((bytes, nid) -> count.increment());
                return count.sum();
            });
            time("iterate.bytes.parallel", rounds, out, () -> {
                LongAdder count = new LongAdder();
                EntityStore.current().forEachParallel((bytes, nid) -> count.increment());
                return count.sum();
            });
            time("iterate.entities", rounds, out, () -> {
                LongAdder versions = new LongAdder();
                EntityStore.current().forEach((bytes, nid) ->
                        versions.add(EntityRecordFactory.make(bytes).versions().size()));
                return versions.sum();
            });
        }

        /**
         * Times an operation: the first round, the median of the rest, and what it counted, which
         * must be the same in every round.
         */
        private static void time(String op, int rounds, Properties out, LongSupplier operation) {
            long[] millis = new long[rounds];
            long count = -1;
            for (int round = 0; round < rounds; round++) {
                long start = System.nanoTime();
                long counted = operation.getAsLong();
                millis[round] = (System.nanoTime() - start) / 1_000_000;
                if (count >= 0 && counted != count) {
                    throw new IllegalStateException(op + " counted " + counted + " after " + count);
                }
                count = counted;
            }
            long[] warm = Arrays.copyOfRange(millis, rounds > 1 ? 1 : 0, rounds);
            Arrays.sort(warm);
            out.setProperty(op + ".first.millis", Long.toString(millis[0]));
            out.setProperty(op + ".median.millis", Long.toString(warm[warm.length / 2]));
            out.setProperty(op + ".count", Long.toString(count));
        }
    }
}
