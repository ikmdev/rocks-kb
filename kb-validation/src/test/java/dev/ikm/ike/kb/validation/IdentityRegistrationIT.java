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
import dev.ikm.tinkar.entity.EntityService;
import dev.ikm.tinkar.entity.changeset.ChangeSetFormat;
import dev.ikm.tinkar.entity.changeset.IdentityIndex;
import dev.ikm.tinkar.fixtures.ForkedJvm;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.zip.ZipFile;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The import's first pass on its own: the identity index of a changeset registered into a new
 * store on every store, as {@code LoadEntitiesFromProtobufFile} does it, in a load phase, and
 * the store saved (IKE-Network/ike-issues#1273). Measured: the registration, in milliseconds
 * and identifiers a second; the end of the load phase, which is where the 64-bit Rocks store
 * writes the identities it held; the save; and, for a persistent store, its size on disk.
 * The count registered must equal the changeset's manifest.
 *
 * <p>The changeset is the DeX export, {@code -Ddex.export=<protobuf zip>}, sixty million
 * identifiers, so the test is tagged {@code dex} and off by default; it runs with
 * {@code -Pdex}. Another changeset with an identity index can be named by
 * {@code -D}{@value #CHANGE_SET}. A subset of the stores runs with
 * {@code -Dstore.providers=<name>,...}.
 *
 * <p>The measures are compared with a reference for this machine,
 * {@code src/test/resources/benchmarks/<machine>/identities-<store>.properties}, where the
 * machine is the lease identity in {@code ~/.ike-machine-id}: a time more than {@value #BOUND}
 * times its reference fails, unless the reference is under {@value #NOISE_FLOOR_MILLIS} ms; a
 * count must equal the reference's. Every run's measures are written to
 * {@code target/store-benchmarks/<machine>/identities-<store>.properties}, to be copied into
 * place as the reference, and to {@code target/measurements/}, where the build report reads
 * them (IKE-Network/ike-issues#1207). The stage runs without the agents of the test JVM
 * ({@link ForkedJvm#runWithoutAgents}), and under JFR with {@code -Dtinkar.jfr.dir=<directory>}
 * ({@link StoreStage}).
 */
@Tag("dex")
@Tag("performance")
class IdentityRegistrationIT {

    private static final Logger LOG = LoggerFactory.getLogger(IdentityRegistrationIT.class);

    /** The changeset whose identity index is registered; the DeX export when unset. */
    static final String CHANGE_SET = "identity.changeset";
    private static final double BOUND = 2.0;
    private static final long NOISE_FLOOR_MILLIS = 1_000;

    /** The stores to run on: every one, or those {@code -Dstore.providers} names. */
    static List<Provider> providers() {
        return Provider.selected();
    }

    @ParameterizedTest
    @MethodSource("providers")
    void theIdentityIndexRegistersWithinItsReference(Provider provider) throws IOException {
        Path changeSet = changeSet();
        Path work = Path.of("target", "identity-registration", provider.name().toLowerCase()).toAbsolutePath();
        SnomedRoundTripIT.deleteTree(work);
        Files.createDirectories(work);
        Path store = work.resolve("store");

        Properties in = new Properties();
        in.setProperty(StoreStage.PROVIDER, provider.name());
        in.setProperty(StoreStage.STORE, store.toString());
        in.setProperty(CHANGE_SET, changeSet.toString());
        Properties result = ForkedJvm.runWithoutAgents(Register.class, in, Duration.ofHours(2));
        if (provider.persistent) {
            SnomedBaselineIT.sizes(store, result);
        }
        String agents = result.getProperty(ForkedJvm.AGENTS_PROPERTY);

        assertEquals(manifestEntities(changeSet), Long.parseLong(result.getProperty("register.count")),
                "identifiers registered, against the changeset's manifest");

        TreeMap<String, String> measures = new TreeMap<>();
        for (String key : result.stringPropertyNames()) {
            if (key.endsWith(".millis") || key.endsWith(".bytes") || key.endsWith(".count") || key.endsWith(".per.second")) {
                measures.put(key, result.getProperty(key));
            }
        }
        String machine = StoreBenchmarkIT.machine();
        LOG.info("Identity registration, {} on {} (agents: {}):\n{}", provider, machine, agents, table(measures));

        Path written = write(machine, provider, measures, agents);
        Properties reference = reference(machine, provider);
        if (reference == null) {
            LOG.warn("No identity registration reference for {} on {}; this run's measures are in {}. "
                    + "Copy it to src/test/resources/benchmarks/{}/ to adopt it.", provider, machine, written, machine);
            return;
        }
        LOG.info("This run's measures are in {}", written);
        List<Executable> checks = new ArrayList<>();
        String referenceAgents = reference.getProperty(ForkedJvm.AGENTS_PROPERTY);
        if (referenceAgents != null) {
            checks.add(() -> assertEquals(referenceAgents, agents,
                    "the stage ran with other agents than the reference was measured with"));
        }
        for (Map.Entry<String, String> measure : measures.entrySet()) {
            String key = measure.getKey();
            String referenceValue = reference.getProperty(key);
            if (referenceValue == null) {
                continue;
            }
            long observed = Long.parseLong(measure.getValue());
            long expected = Long.parseLong(referenceValue);
            if (key.endsWith(".count")) {
                checks.add(() -> assertEquals(expected, observed, key));
            } else if (key.endsWith(".millis") && expected >= NOISE_FLOOR_MILLIS) {
                checks.add(() -> assertTrue(observed <= expected * BOUND, key + ": " + observed
                        + " ms is more than " + BOUND + " times the reference " + expected + " ms"));
            }
        }
        assertAll(provider.name(), checks);
    }

    /** The changeset named by {@value #CHANGE_SET}, else by {@value DexImportIT#EXPORT}; the test is skipped without one. */
    private static Path changeSet() throws IOException {
        String named = System.getProperty(CHANGE_SET);
        if (named == null || named.isBlank()) {
            named = System.getProperty(DexImportIT.EXPORT);
        }
        assumeTrue(named != null && !named.isBlank(), "Neither -D" + CHANGE_SET + " nor -D" + DexImportIT.EXPORT + " is set");
        Path changeSet = Path.of(named);
        assertTrue(Files.isRegularFile(changeSet), "No changeset at " + changeSet);
        assertTrue(ChangeSetFormat.hasIdentityIndex(changeSet.toFile()), changeSet + " carries no identity index");
        return changeSet;
    }

    private static long manifestEntities(Path changeSet) throws IOException {
        try (ZipFile zip = new ZipFile(changeSet.toFile())) {
            var entry = zip.getEntry("META-INF/MANIFEST.MF");
            assertTrue(entry != null, changeSet + " has no manifest");
            Attributes manifest = new Manifest(zip.getInputStream(entry)).getMainAttributes();
            return Long.parseLong(manifest.getValue("Total-Count"));
        }
    }

    private static String table(Map<String, String> measures) {
        StringBuilder table = new StringBuilder();
        measures.forEach((key, value) -> table.append(String.format("%-40s %,20d%n", key, Long.parseLong(value))));
        return table.toString();
    }

    private static String referenceName(Provider provider) {
        return "identities-" + provider.name().toLowerCase() + ".properties";
    }

    private static Properties reference(String machine, Provider provider) throws IOException {
        String resource = "/benchmarks/" + machine + "/" + referenceName(provider);
        try (InputStream stream = IdentityRegistrationIT.class.getResourceAsStream(resource)) {
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
            written.store(stream, "Identity registration: " + provider + " on " + machine);
        }
        Measurements.publish("bench.identities." + provider.name().toLowerCase(), written);
        return file;
    }

    /**
     * The one stage: a new store, the changeset's identity index registered into it in a load
     * phase, the load phase ended, and the store saved; each timed.
     */
    static class Register extends StoreStage {
        @Override
        void work(Properties in, Properties out) throws Exception {
            File changeSet = new File(in.getProperty(CHANGE_SET));
            long start = System.currentTimeMillis();
            EntityService.get().beginLoadPhase();
            long[] lastReport = {start, 0};
            long count = IdentityIndex.registerNids(changeSet, registered -> {
                long now = System.currentTimeMillis();
                if (now - lastReport[0] >= 10_000) {
                    LOG.info("{} registered, {}/s over the last {} s", String.format("%,d", registered),
                            String.format("%,d", (registered - lastReport[1]) * 1_000 / (now - lastReport[0])),
                            (now - lastReport[0]) / 1_000);
                    lastReport[0] = now;
                    lastReport[1] = registered;
                }
            });
            long registered = System.currentTimeMillis();
            EntityService.get().endLoadPhase();
            long loadPhaseEnded = System.currentTimeMillis();
            PrimitiveData.save();
            long saved = System.currentTimeMillis();
            out.setProperty("register.count", Long.toString(count));
            out.setProperty("register.millis", Long.toString(registered - start));
            out.setProperty("register.per.second", Long.toString(count * 1_000 / Math.max(1, registered - start)));
            out.setProperty("end.load.phase.millis", Long.toString(loadPhaseEnded - registered));
            out.setProperty("save.millis", Long.toString(saved - loadPhaseEnded));
            out.setProperty("total.millis", Long.toString(saved - start));
        }
    }
}
