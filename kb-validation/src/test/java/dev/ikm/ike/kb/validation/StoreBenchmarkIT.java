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
import dev.ikm.tinkar.coordinate.Coordinates;
import dev.ikm.tinkar.coordinate.view.ViewCoordinateRecord;
import dev.ikm.tinkar.coordinate.view.calculator.ViewCalculator;
import dev.ikm.tinkar.coordinate.view.calculator.ViewCalculatorWithCache;
import dev.ikm.tinkar.entity.EntityHandle;
import dev.ikm.tinkar.entity.EntityService;
import dev.ikm.tinkar.fixtures.ForkedJvm;
import dev.ikm.tinkar.terms.TinkarTerm;
import org.eclipse.collections.api.factory.primitive.IntLists;
import org.eclipse.collections.api.list.primitive.ImmutableIntList;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Times the retrieval the stores and the entity service offer, against the IKE starter set on
 * every store: whole-store scans sequential and parallel, the same nids as a list in order and
 * shuffled, single reads in order and shuffled, the pattern and component indexes, the
 * entity-level enumerations, and the names and parents a view computes.
 *
 * <p>A persistent store is measured after it is reopened, in a JVM that did not load it, so the
 * first round is a cold read. Each operation runs {@value #DEFAULT_ROUNDS} rounds by default
 * ({@code -Dbenchmark.rounds}); the first is reported apart, and the median of the rest is the
 * measure. Every operation also counts what it visited, and the ordered and shuffled forms of
 * one question must visit the same number of things.
 *
 * <p>Timings are compared with a reference for this machine, under
 * {@code src/test/resources/benchmarks/<machine>/<store>.properties}, where the machine is the
 * lease identity in {@code ~/.ike-machine-id} (else the host name). An operation more than
 * {@value #BOUND} times slower than its reference fails; an operation whose reference is under
 * {@value #NOISE_FLOOR_MICROS} microseconds is reported but not bounded, being noise at this
 * size. With no reference for the machine, the timings are written to
 * {@code target/store-benchmarks/<machine>/<store>.properties}, to be copied into place.
 */
@Tag("starter-set")
class StoreBenchmarkIT {
    private static final Logger LOG = LoggerFactory.getLogger(StoreBenchmarkIT.class);

    private static final int DEFAULT_ROUNDS = 7;
    private static final long SEED = 20261004L;
    private static final double BOUND = 2.0;
    private static final long NOISE_FLOOR_MICROS = 2_000;

    private static final String BENCH = "bench.";
    private static final String FIRST = ".first.micros";
    private static final String MEDIAN = ".median.micros";
    private static final String MIN = ".min.micros";
    private static final String COUNT = ".count";
    private static final String ROUNDS = "benchmark.rounds";

    @ParameterizedTest
    @EnumSource(Provider.class)
    void retrievalStaysWithinTwiceItsReference(Provider provider) throws IOException {
        Path work = Path.of("target", "store-benchmarks", "stores", provider.name().toLowerCase()).toAbsolutePath();
        SnomedRoundTripIT.deleteTree(work);
        Files.createDirectories(work);

        Properties in = new Properties();
        in.setProperty(StoreStage.PROVIDER, provider.name());
        in.setProperty(StoreStage.STORE, work.resolve("store").toString());
        in.setProperty(StarterSetProbeIT.KB_FILE, StarterSetRoundTripIT.starterSet().toString());
        in.setProperty(ROUNDS, System.getProperty(ROUNDS, Integer.toString(DEFAULT_ROUNDS)));

        Properties result;
        if (provider.persistent) {
            Properties loaded = ForkedJvm.run(StarterSetProbeIT.Load.class, in, Duration.ofMinutes(10));
            result = ForkedJvm.run(Measure.class, loaded, Duration.ofMinutes(20));
        } else {
            result = ForkedJvm.run(LoadAndMeasure.class, in, Duration.ofMinutes(20));
        }

        Map<String, long[]> timings = timings(result);
        String machine = machine();
        LOG.info("{} on {}:\n{}", provider, machine, table(timings));

        List<Executable> checks = new ArrayList<>();
        // The same question asked in order and shuffled visits the same things.
        same(checks, result, "store.list.ordered", "store.list.shuffled", "store.list.parallel.shuffled",
                "store.bytes.ordered", "store.bytes.shuffled", "entity.handle.ordered", "entity.handle.shuffled",
                "entities.of.list.shuffled");
        same(checks, result, "store.scan", "store.scan.parallel");
        same(checks, result, "store.semantics.of.pattern", "store.for.each.semantic.of.pattern",
                "entities.semantics.of.pattern");

        Properties reference = reference(machine, provider);
        if (reference == null) {
            Path written = write(machine, provider, timings);
            LOG.warn("No benchmark reference for {} on {}; this run's timings are in {}. "
                    + "Copy it to src/test/resources/benchmarks/{}/ to adopt it.", provider, machine, written, machine);
        } else {
            for (Map.Entry<String, long[]> timing : timings.entrySet()) {
                String op = timing.getKey();
                String referenceMedian = reference.getProperty(op + MEDIAN);
                if (referenceMedian == null || Long.parseLong(referenceMedian) < NOISE_FLOOR_MICROS) {
                    continue;
                }
                long bound = (long) (Long.parseLong(referenceMedian) * BOUND);
                long median = timing.getValue()[1];
                checks.add(() -> assertTrue(median <= bound, op + ": median " + median
                        + " µs is more than " + BOUND + " times the reference " + referenceMedian + " µs"));
            }
        }
        assertAll(provider.name(), checks);
    }

    private static void same(List<Executable> checks, Properties result, String... ops) {
        String first = result.getProperty(BENCH + ops[0] + COUNT);
        for (String op : ops) {
            checks.add(() -> assertEquals(first, result.getProperty(BENCH + op + COUNT),
                    op + " visited a different number of things than " + ops[0]));
        }
    }

    /** Stage: a persistent store reopened, cold, and measured. */
    static class Measure extends StoreStage {
        @Override
        void work(Properties in, Properties out) {
            Operations.measure(Integer.parseInt(in.getProperty(ROUNDS)), out);
        }
    }

    /** An ephemeral store does not outlive its JVM: loaded and measured in one lifetime. */
    static class LoadAndMeasure extends StoreStage {
        @Override
        void work(Properties in, Properties out) {
            new StarterSetProbeIT.Load().work(in, out);
            Operations.measure(Integer.parseInt(in.getProperty(ROUNDS)), out);
        }
    }

    /** The operations timed, run against the open store. */
    static final class Operations {
        private Operations() {
        }

        static void measure(int rounds, Properties out) {
            MutableIntList concepts = present(PrimitiveData.get()::forEachConceptNid);
            MutableIntList patterns = present(PrimitiveData.get()::forEachPatternNid);
            MutableIntList semantics = present(PrimitiveData.get()::forEachSemanticNid);
            MutableIntList stamps = present(PrimitiveData.get()::forEachStampNid);
            MutableIntList all = IntLists.mutable.empty();
            all.addAll(concepts);
            all.addAll(patterns);
            all.addAll(semantics);
            all.addAll(stamps);
            ImmutableIntList ordered = all.toSortedList().toImmutable();
            ImmutableIntList shuffled = shuffle(ordered);
            ImmutableIntList componentsShuffled = shuffle(IntLists.immutable.withAll(concepts).newWithAll(semantics));
            int descriptionPattern = TinkarTerm.DESCRIPTION_PATTERN.nid();
            ViewCalculator view = ViewCalculatorWithCache.getCalculator(ViewCoordinateRecord.make(
                    Coordinates.Stamp.DevelopmentLatest(), Coordinates.Language.UsEnglishRegularName(),
                    Coordinates.Logic.ElPlusPlus(), Coordinates.Navigation.inferred(), Coordinates.Edit.Default()));
            EntityService entities = EntityService.get();

            Map<String, LongSupplier> operations = new LinkedHashMap<>();
            // The store: whole-store scans, the same nids as a list, single reads.
            operations.put("store.scan", () -> {
                LongAdder count = new LongAdder();
                PrimitiveData.get().forEach((bytes, nid) -> count.increment());
                return count.sum();
            });
            operations.put("store.scan.parallel", () -> {
                LongAdder count = new LongAdder();
                PrimitiveData.get().forEachParallel((bytes, nid) -> count.increment());
                return count.sum();
            });
            operations.put("store.list.ordered", () -> {
                LongAdder count = new LongAdder();
                PrimitiveData.get().forEach(ordered, (bytes, nid) -> count.increment());
                return count.sum();
            });
            operations.put("store.list.shuffled", () -> {
                LongAdder count = new LongAdder();
                PrimitiveData.get().forEach(shuffled, (bytes, nid) -> count.increment());
                return count.sum();
            });
            operations.put("store.list.parallel.shuffled", () -> {
                LongAdder count = new LongAdder();
                PrimitiveData.get().forEachParallel(shuffled, (bytes, nid) -> count.increment());
                return count.sum();
            });
            operations.put("store.bytes.ordered", () -> readBytes(ordered));
            operations.put("store.bytes.shuffled", () -> readBytes(shuffled));
            operations.put("entity.handle.ordered", () -> handles(ordered));
            operations.put("entity.handle.shuffled", () -> handles(shuffled));
            // The store's indexes.
            operations.put("store.semantics.of.pattern", () -> {
                long count = 0;
                for (int pattern : patterns.toArray()) {
                    count += PrimitiveData.get().semanticNidsOfPattern(pattern).length;
                }
                return count;
            });
            operations.put("store.for.each.semantic.of.pattern", () -> {
                LongAdder count = new LongAdder();
                patterns.forEach(pattern -> PrimitiveData.get().forEachSemanticNidOfPattern(pattern, nid -> count.increment()));
                return count.sum();
            });
            operations.put("store.semantics.for.component.shuffled", () -> {
                long count = 0;
                for (int nid : componentsShuffled.toArray()) {
                    count += PrimitiveData.get().semanticNidsForComponent(nid).length;
                }
                return count;
            });
            operations.put("store.descriptions.for.concept", () -> {
                long count = 0;
                for (int concept : concepts.toArray()) {
                    count += PrimitiveData.get().semanticNidsForComponentOfPattern(concept, descriptionPattern).length;
                }
                return count;
            });
            // The entity service.
            operations.put("entities.concepts", () -> {
                LongAdder count = new LongAdder();
                entities.forEachConceptEntity(concept -> count.increment());
                return count.sum();
            });
            operations.put("entities.patterns", () -> {
                LongAdder count = new LongAdder();
                entities.forEachPatternEntity(pattern -> count.increment());
                return count.sum();
            });
            operations.put("entities.stamps", () -> {
                LongAdder count = new LongAdder();
                entities.forEachStampEntity(stamp -> count.increment());
                return count.sum();
            });
            operations.put("entities.semantics.of.pattern", () -> {
                LongAdder count = new LongAdder();
                patterns.forEach(pattern -> entities.forEachSemanticOfPattern(pattern, semantic -> count.increment()));
                return count.sum();
            });
            operations.put("entities.semantics.for.component.shuffled", () -> {
                LongAdder count = new LongAdder();
                componentsShuffled.forEach(nid -> entities.forEachSemanticForComponent(nid, semantic -> count.increment()));
                return count.sum();
            });
            operations.put("entities.descriptions.for.concept", () -> {
                LongAdder count = new LongAdder();
                concepts.forEach(concept -> entities.forEachSemanticForComponentOfPattern(concept, descriptionPattern,
                        semantic -> count.increment()));
                return count.sum();
            });
            operations.put("entities.of.list.shuffled", () -> {
                LongAdder count = new LongAdder();
                entities.forEachEntity(shuffled, entity -> count.increment());
                return count.sum();
            });
            // What a view computes from them.
            operations.put("view.fully.qualified.names", () -> {
                long count = 0;
                for (int concept : concepts.toArray()) {
                    if (view.languageCalculator().getFullyQualifiedNameText(concept).isPresent()) {
                        count++;
                    }
                }
                return count;
            });
            operations.put("view.regular.names", () -> {
                long count = 0;
                for (int concept : concepts.toArray()) {
                    if (view.languageCalculator().getPreferredDescriptionTextWithFallbackOrNid(concept) != null) {
                        count++;
                    }
                }
                return count;
            });
            operations.put("view.parents", () -> {
                long count = 0;
                for (int concept : concepts.toArray()) {
                    count += view.navigationCalculator().parentsOf(concept).size();
                }
                return count;
            });

            for (Map.Entry<String, LongSupplier> operation : operations.entrySet()) {
                time(operation.getKey(), operation.getValue(), rounds, out);
            }
        }

        private static void time(String op, LongSupplier operation, int rounds, Properties out) {
            long[] micros = new long[rounds];
            long count = -1;
            for (int round = 0; round < rounds; round++) {
                long start = System.nanoTime();
                long visited = operation.getAsLong();
                micros[round] = (System.nanoTime() - start) / 1_000;
                if (count != -1 && visited != count) {
                    throw new IllegalStateException(op + " visited " + visited + " in round " + round
                            + " but " + count + " before");
                }
                count = visited;
            }
            long[] warm = Arrays.copyOfRange(micros, rounds > 1 ? 1 : 0, rounds);
            Arrays.sort(warm);
            out.setProperty(BENCH + op + FIRST, Long.toString(micros[0]));
            out.setProperty(BENCH + op + MEDIAN, Long.toString(warm[warm.length / 2]));
            out.setProperty(BENCH + op + MIN, Long.toString(warm[0]));
            out.setProperty(BENCH + op + COUNT, Long.toString(count));
        }

        private static long readBytes(ImmutableIntList nids) {
            long count = 0;
            for (int nid : nids.toArray()) {
                if (PrimitiveData.get().getBytes(nid) != null) {
                    count++;
                }
            }
            return count;
        }

        private static long handles(ImmutableIntList nids) {
            long count = 0;
            for (int nid : nids.toArray()) {
                if (EntityHandle.get(nid).isPresent()) {
                    count++;
                }
            }
            return count;
        }

        private static MutableIntList present(NidSource source) {
            // A store may call the procedure from several threads at once.
            MutableIntList nids = IntLists.mutable.empty().asSynchronized();
            source.forEach(nid -> {
                if (EntityHandle.get(nid).isPresent()) {
                    nids.add(nid);
                }
            });
            return nids.toSortedList();
        }

        private static ImmutableIntList shuffle(ImmutableIntList nids) {
            int[] array = nids.toArray();
            Random random = new Random(SEED);
            for (int i = array.length - 1; i > 0; i--) {
                int j = random.nextInt(i + 1);
                int swap = array[i];
                array[i] = array[j];
                array[j] = swap;
            }
            return IntLists.immutable.with(array);
        }

        private interface NidSource {
            void forEach(org.eclipse.collections.api.block.procedure.primitive.IntProcedure procedure);
        }
    }

    /** Each operation's first, median and minimum microseconds, and its count, in run order. */
    private static Map<String, long[]> timings(Properties result) {
        Map<String, long[]> timings = new LinkedHashMap<>();
        result.stringPropertyNames().stream()
                .filter(key -> key.startsWith(BENCH) && key.endsWith(MEDIAN))
                .map(key -> key.substring(BENCH.length(), key.length() - MEDIAN.length()))
                .sorted()
                .forEach(op -> timings.put(op, new long[]{
                        Long.parseLong(result.getProperty(BENCH + op + FIRST)),
                        Long.parseLong(result.getProperty(BENCH + op + MEDIAN)),
                        Long.parseLong(result.getProperty(BENCH + op + MIN)),
                        Long.parseLong(result.getProperty(BENCH + op + COUNT))}));
        return timings;
    }

    private static String table(Map<String, long[]> timings) {
        StringBuilder table = new StringBuilder(String.format("%-44s %12s %12s %12s %10s%n",
                "operation", "first µs", "median µs", "min µs", "count"));
        timings.forEach((op, t) -> table.append(String.format("%-44s %12d %12d %12d %10d%n", op, t[0], t[1], t[2], t[3])));
        return table.toString();
    }

    private static String machine() {
        try {
            Path id = Path.of(System.getProperty("user.home"), ".ike-machine-id");
            if (Files.isRegularFile(id)) {
                return Files.readString(id).strip();
            }
            return InetAddress.getLocalHost().getHostName();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Properties reference(String machine, Provider provider) throws IOException {
        String resource = "/benchmarks/" + machine + "/" + provider.name().toLowerCase() + ".properties";
        try (InputStream stream = StoreBenchmarkIT.class.getResourceAsStream(resource)) {
            if (stream == null) {
                return null;
            }
            Properties reference = new Properties();
            reference.load(stream);
            return reference;
        }
    }

    private static Path write(String machine, Provider provider, Map<String, long[]> timings) throws IOException {
        Path file = Path.of("target", "store-benchmarks", machine, provider.name().toLowerCase() + ".properties").toAbsolutePath();
        Files.createDirectories(file.getParent());
        Properties written = new Properties();
        timings.forEach((op, t) -> {
            written.setProperty(op + MEDIAN, Long.toString(t[1]));
            written.setProperty(op + COUNT, Long.toString(t[3]));
        });
        try (OutputStream stream = Files.newOutputStream(file)) {
            written.store(stream, "Store benchmark reference: " + provider + " on " + machine);
        }
        return file;
    }
}
