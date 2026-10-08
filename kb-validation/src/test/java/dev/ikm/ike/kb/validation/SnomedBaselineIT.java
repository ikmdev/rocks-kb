/*
 * Copyright © 2015 Integrated Knowledge Management (support@ikm.dev)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.ikm.ike.kb.validation;

import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.service.internal.EntityStore;
import dev.ikm.tinkar.entity.EntityRecordFactory;
import dev.ikm.tinkar.entity.load.LoadEntitiesFromProtobufFile;
import dev.ikm.tinkar.fixtures.ForkedJvm;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
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
 * The baselines of the 64-bit nid design on the SNOMED CT knowledge base (design
 * {@code design-2026-09-30-64-bit-nids}, "Specification and fixtures";
 * IKE-Network/ike-issues#1244), on every store, against which later steps are measured and
 * the stores are compared:
 * <ul>
 *   <li><b>Import time</b>: a new store loaded from the SNOMED CT protobuf export and saved,
 *   search index included, as an import runs.</li>
 *   <li><b>Size on disk</b>: the closed store, in all and by what is at its top level. The
 *   ephemeral store has none.</li>
 *   <li><b>Full iteration time</b>: in a JVM that did not load the store, so the first round is a
 *   cold read, every entity's bytes in order, the same in parallel, and every entity decoded.
 *   Each runs {@value #DEFAULT_ROUNDS} rounds by default ({@code -Dbaseline.rounds}); the first is
 *   reported apart and the median of the rest is the measure.</li>
 *   <li><b>Retrieval</b>: the store reopened once more and the operations of
 *   {@link StoreBenchmarkIT} run against it, {@value #DEFAULT_BENCHMARK_ROUNDS} rounds by default
 *   ({@code -Dbenchmark.rounds}): whole-store scans, the same nids as a list, single reads, the
 *   pattern and component indexes, the entity-level enumerations, and the names and parents a
 *   view computes, each a median in microseconds with the count it visited.</li>
 * </ul>
 * <p>A persistent store runs each lifetime in a JVM of its own; the ephemeral store, which does
 * not outlive its JVM, loads, iterates and retrieves in one.
 * <p>The measures are compared with a reference for this machine,
 * {@code src/test/resources/benchmarks/<machine>/snomed-<store>.properties}, where the machine is
 * the lease identity in {@code ~/.ike-machine-id}. A time more than {@value #BOUND} times its
 * reference, or a store more than {@value #SIZE_BOUND} times its reference size, fails; a time
 * whose reference is under {@value #NOISE_FLOOR_MILLIS} ms, or a retrieval whose reference is
 * under {@value #NOISE_FLOOR_MICROS} µs, is reported but not bounded, being noise at this size;
 * counts must equal the reference's. Every run's measures are written to
 * {@code target/store-benchmarks/<machine>/snomed-<store>.properties}, to be copied into place
 * as the reference or compared with it. Runs with {@code -Psnomed}. A subset of the stores runs with
 * {@code -Dstore.providers=<name>,...}.
 * <p>The stages run without the agents of the test JVM ({@link ForkedJvm#runWithoutAgents}):
 * the JaCoCo agent failsafe attaches made the parallel scan fifteen times slower, and was what
 * IKE-Network/ike-issues#1246 measured. A reference records what its stages ran with, under
 * {@value ForkedJvm#AGENTS_PROPERTY}, and a run with other agents fails against it.
 */
@Tag("snomed")
class SnomedBaselineIT {

    private static final Logger LOG = LoggerFactory.getLogger(SnomedBaselineIT.class);

    /** The stores to run on: every one, or those {@code -Dstore.providers} names. */
    static List<Provider> providers() {
        return Provider.selected();
    }

    private static final int DEFAULT_ROUNDS = 5;
    private static final int DEFAULT_BENCHMARK_ROUNDS = 3;
    private static final double BOUND = 2.0;
    private static final double SIZE_BOUND = 1.25;
    private static final long NOISE_FLOOR_MILLIS = 1_000;
    private static final long NOISE_FLOOR_MICROS = 2_000;
    private static final String ROUNDS = "baseline.rounds";
    private static final String KB_FILE = "kb.file";

    @ParameterizedTest
    @MethodSource("providers")
    void importSizeIterationAndRetrievalStayWithinTheirReference(Provider provider) throws IOException {
        Path kb = SnomedRoundTripIT.knowledgeBase();
        Path work = Path.of("target", "snomed-baseline", provider.name().toLowerCase()).toAbsolutePath();
        SnomedRoundTripIT.deleteTree(work);
        Files.createDirectories(work);
        Path store = work.resolve("store");

        Properties in = new Properties();
        in.setProperty(StoreStage.PROVIDER, provider.name());
        in.setProperty(StoreStage.STORE, store.toString());
        in.setProperty(KB_FILE, kb.toString());
        in.setProperty(ROUNDS, Integer.toString(Integer.getInteger(ROUNDS, DEFAULT_ROUNDS)));
        in.setProperty(StoreBenchmarkIT.ROUNDS,
                Integer.toString(Integer.getInteger(StoreBenchmarkIT.ROUNDS, DEFAULT_BENCHMARK_ROUNDS)));

        Properties result;
        if (provider.persistent) {
            result = ForkedJvm.runWithoutAgents(Import.class, in, Duration.ofHours(2));
            sizes(store, result);
            result = ForkedJvm.runWithoutAgents(Iterate.class, result, Duration.ofHours(1));
            result = ForkedJvm.runWithoutAgents(StoreBenchmarkIT.Measure.class, result, Duration.ofHours(2));
        } else {
            result = ForkedJvm.runWithoutAgents(LoadIterateAndRetrieve.class, in, Duration.ofHours(3));
        }
        String agents = result.getProperty(ForkedJvm.AGENTS_PROPERTY);

        long manifestEntities = manifestEntities(kb);
        assertEquals(manifestEntities, Long.parseLong(result.getProperty("iterate.bytes.count")),
                "entities iterated, against the export's manifest");

        TreeMap<String, String> measures = new TreeMap<>();
        for (String key : result.stringPropertyNames()) {
            if (key.endsWith(".millis") || key.endsWith(".bytes") || key.endsWith(".count") || key.endsWith(".micros")) {
                measures.put(key, result.getProperty(key));
            }
        }
        String machine = StoreBenchmarkIT.machine();
        LOG.info("SNOMED CT baselines, {} on {} (agents: {}):\n{}", provider, machine, agents, table(measures));

        Path written = write(machine, provider, measures, agents);
        Properties reference = reference(machine, provider);
        if (reference == null) {
            LOG.warn("No SNOMED CT baseline for {} on {}; this run's measures are in {}. "
                    + "Copy it to src/test/resources/benchmarks/{}/ to adopt it.", provider, machine, written, machine);
            return;
        }
        LOG.info("This run's measures are in {}", written);
        List<org.junit.jupiter.api.function.Executable> checks = new ArrayList<>();
        String referenceAgents = reference.getProperty(ForkedJvm.AGENTS_PROPERTY);
        if (referenceAgents != null) {
            checks.add(() -> assertEquals(referenceAgents, agents,
                    "the stages ran with other agents than the reference was measured with"));
        }
        for (Map.Entry<String, String> measure : measures.entrySet()) {
            String key = measure.getKey();
            String referenceValue = reference.getProperty(key);
            if (referenceValue == null || key.contains(".first.") || key.contains(".min.")) {
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
            } else if (key.endsWith(".median.micros") && expected >= NOISE_FLOOR_MICROS) {
                checks.add(() -> assertTrue(observed <= expected * BOUND, key + ": " + observed
                        + " µs is more than " + BOUND + " times the reference " + expected + " µs"));
            }
        }
        assertAll(provider.name(), checks);
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
        measures.forEach((key, value) -> table.append(String.format("%-56s %,20d%n", key, Long.parseLong(value))));
        return table.toString();
    }

    private static String referenceName(Provider provider) {
        return "snomed-" + provider.name().toLowerCase() + ".properties";
    }

    private static Properties reference(String machine, Provider provider) throws IOException {
        String resource = "/benchmarks/" + machine + "/" + referenceName(provider);
        try (InputStream stream = SnomedBaselineIT.class.getResourceAsStream(resource)) {
            if (stream == null) {
                return null;
            }
            Properties reference = new Properties();
            reference.load(stream);
            return reference;
        }
    }

    private static Path write(String machine, Provider provider, Map<String, String> measures, String agents)
            throws IOException {
        Path file = Path.of("target", "store-benchmarks", machine, referenceName(provider)).toAbsolutePath();
        Files.createDirectories(file.getParent());
        Properties written = new Properties();
        measures.forEach(written::setProperty);
        written.setProperty(ForkedJvm.AGENTS_PROPERTY, agents);
        try (OutputStream stream = Files.newOutputStream(file)) {
            written.store(stream, "SNOMED CT baselines: " + provider + " on " + machine);
        }
        return file;
    }

    /** Stage 1: a new store, the SNOMED CT export loaded into it, and saved. */
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

    /**
     * The ephemeral store's one lifetime: loaded, every entity iterated, and the retrieval
     * operations run, in the JVM that loaded it. Its import includes no save of consequence and
     * its iteration is never cold.
     */
    static class LoadIterateAndRetrieve extends StoreStage {
        @Override
        void work(Properties in, Properties out) {
            new Import().work(in, out);
            new Iterate().work(in, out);
            StoreBenchmarkIT.Operations.measure(Integer.parseInt(in.getProperty(StoreBenchmarkIT.ROUNDS)), out);
        }
    }
}
