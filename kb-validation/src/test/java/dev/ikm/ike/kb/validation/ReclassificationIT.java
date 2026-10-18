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

import dev.ikm.tinkar.coordinate.Calculators;
import dev.ikm.tinkar.coordinate.stamp.calculator.Latest;
import dev.ikm.tinkar.coordinate.view.calculator.ViewCalculator;
import dev.ikm.tinkar.coordinate.view.calculator.ViewCalculatorWithCache;
import dev.ikm.tinkar.entity.EntityService;
import dev.ikm.tinkar.entity.SemanticEntity;
import dev.ikm.tinkar.entity.SemanticEntityVersion;
import dev.ikm.tinkar.entity.graph.DiTreeEntity;
import dev.ikm.tinkar.entity.graph.DiTreeIsomorphism;
import dev.ikm.tinkar.entity.graph.adaptor.axiom.LogicalExpression;
import dev.ikm.tinkar.fixtures.ForkedJvm;
import dev.ikm.tinkar.reasoner.service.ClassifierResults;
import dev.ikm.tinkar.reasoner.service.ReasonerService;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Enumeration;
import java.util.Optional;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reclassifies a classified knowledge base and measures writing the results: the case where
 * every classified concept already has an inferred axiom semantic, so the writer decides for
 * each one whether its newly computed necessary normal form differs from the latest inferred form
 * and a new version must be written.
 *
 * <p>Run against a SpinedArray store zip ({@code reasoned-sa}), one per run, for example SOLOR or
 * SOLOR + GUDID from the solor-kb-ws working set:
 *
 * <pre>{@code
 * ./mvnw verify -pl rocks-kb/kb-validation -Preclassify -Dit.test=ReclassificationIT \
 *     -Dreclassify.kb=<...>/solor-kb-1-SNAPSHOT-reasoned-sa.zip -Dreclassify.label=after
 * }</pre>
 *
 * <p>Before writing, it surveys every concept: the writer's comparison ({@link DiTreeIsomorphism}),
 * timed on one thread, for the normal form against the latest inferred form (what the writer
 * compares) and against the latest stated form (a mix in which most pairs differ). The decision
 * the writer made before (two leaf-hash isomorphic analyses) was surveyed beside it until the
 * leaf-hash analysis retired (IKE-Network/ike-issues#1310). After writing, every classified concept's latest
 * inferred form must be its normal form. The figures go to
 * {@code target/reclassify/<kb>-<label>.properties} and to the build report as
 * {@code reclassify.<kb>.*}; {@code -Dtinkar.jfr.dir} records the stage.
 */
@Tag("reclassify")
class ReclassificationIT {

    static final String KB = "reclassify.kb";
    static final String LABEL = "reclassify.label";
    /** Survey every n-th concept; 0 skips the survey. */
    static final String SURVEY_EVERY = "reclassify.survey.every";

    @Test
    void reclassifyingWritesOnlyWhatChangedAndLeavesEveryInferredFormItsNormalForm() throws Exception {
        String kbProperty = System.getProperty(KB);
        assertTrue(kbProperty != null && !kbProperty.isBlank(), "Name a reasoned-sa store zip with -D" + KB);
        Path zip = Path.of(kbProperty);
        String name = zip.getFileName().toString().replaceAll("-1-SNAPSHOT-reasoned-sa\\.zip$", "")
                .replaceAll("\\.zip$", "");
        String label = System.getProperty(LABEL, "run");
        Path work = Path.of("target", "reclassify", name).toAbsolutePath();
        SnomedRoundTripIT.deleteTree(work);
        long unzipStart = System.currentTimeMillis();
        Path store = unzip(zip, work);
        Properties in = new Properties();
        in.setProperty(StoreStage.PROVIDER, Provider.SPINED_ARRAY.name());
        in.setProperty(StoreStage.STORE, store.toString());
        in.setProperty(SURVEY_EVERY, System.getProperty(SURVEY_EVERY, "1"));
        in.setProperty("unzip.millis", Long.toString(System.currentTimeMillis() - unzipStart));
        Properties result = ForkedJvm.run(Reclassify.class, in, Duration.ofHours(4));

        Path report = Path.of("target", "reclassify", name + "-" + label + ".properties").toAbsolutePath();
        try (OutputStream stream = Files.newOutputStream(report)) {
            result.store(stream, "Reclassification of " + zip + " (" + label + ")");
        }
        Measurements.publish("reclassify." + name, result);
        System.out.println("Reclassification figures: " + report);
    }

    /** Unpacks the zip under the directory; returns the store, its one top-level directory. */
    private static Path unzip(Path zip, Path directory) throws IOException {
        Files.createDirectories(directory);
        String top = null;
        try (ZipFile file = new ZipFile(zip.toFile())) {
            Enumeration<? extends ZipEntry> entries = file.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                Path target = directory.resolve(entry.getName()).normalize();
                if (!target.startsWith(directory)) {
                    throw new IOException("Entry outside the directory: " + entry.getName());
                }
                if (top == null) {
                    top = entry.getName().split("/")[0];
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(target);
                } else {
                    Files.createDirectories(target.getParent());
                    try (InputStream stream = file.getInputStream(entry)) {
                        Files.copy(stream, target);
                    }
                }
            }
        }
        return directory.resolve(top);
    }

    /** Classifies, surveys the decision, writes the results and checks them. */
    static class Reclassify extends StoreStage {

        @Override
        void work(Properties in, Properties out) throws Exception {
            ViewCalculator view = Calculators.View.Default();
            long statedPattern = view.viewCoordinateRecord().logicCoordinate().statedAxiomsPatternNid();
            long inferredPattern = view.viewCoordinateRecord().logicCoordinate().inferredAxiomsPatternNid();
            ReasonerService reasoner = Classification.classify(view, out, "classification.");

            int every = Integer.parseInt(in.getProperty(SURVEY_EVERY, "1"));
            if (every > 0) {
                survey(reasoner, view, inferredPattern, every, out, "survey.inferred.");
                survey(reasoner, view, statedPattern, every, out, "survey.stated.");
            }

            long start = System.currentTimeMillis();
            ClassifierResults results = reasoner.writeInferredResults();
            out.setProperty("write.millis", Long.toString(System.currentTimeMillis() - start));
            out.setProperty("write.classified.concepts", Integer.toString(results.getClassificationConceptSet().size()));
            out.setProperty("write.inferred.changes", Integer.toString(results.getConceptsWithInferredChanges().size()));
            out.setProperty("write.navigation.changes", Integer.toString(results.getConceptsWithNavigationChanges().size()));

            ViewCalculator after = ViewCalculatorWithCache.getCalculator(results.getViewCoordinate());
            long[] checked = {0, 0, 0};
            reasoner.getReasonerConceptSet().forEach(nid -> {
                LogicalExpression normalForm = reasoner.getNecessaryNormalForm(nid);
                if (normalForm == null) {
                    return;
                }
                checked[0]++;
                Optional<DiTreeEntity> inferred = latestTree(after, nid, inferredPattern);
                if (inferred.isEmpty()) {
                    checked[1]++;
                } else if (!DiTreeIsomorphism.isomorphic((DiTreeEntity) normalForm.sourceGraph(), inferred.get())) {
                    checked[2]++;
                }
            });
            out.setProperty("after.concepts.with.normal.form", Long.toString(checked[0]));
            out.setProperty("after.without.inferred.form", Long.toString(checked[1]));
            out.setProperty("after.inferred.form.not.normal.form", Long.toString(checked[2]));
        }

        /**
         * For every n-th classified concept with a normal form and a latest form of the pattern,
         * the writer's comparison, timed: "same" means nothing is written.
         */
        private void survey(ReasonerService reasoner, ViewCalculator view, long patternNid, int every,
                            Properties out, String prefix) {
            long[] counts = new long[3]; // pairs, same, different
            long[] nanos = {0};
            long[] index = {0};
            reasoner.getReasonerConceptSet().forEach(nid -> {
                if (index[0]++ % every != 0) {
                    return;
                }
                LogicalExpression normalForm = reasoner.getNecessaryNormalForm(nid);
                if (normalForm == null) {
                    return;
                }
                Optional<DiTreeEntity> latest = latestTree(view, nid, patternNid);
                if (latest.isEmpty()) {
                    return;
                }
                DiTreeEntity form = (DiTreeEntity) normalForm.sourceGraph();
                counts[0]++;

                long start = System.nanoTime();
                boolean same = DiTreeIsomorphism.isomorphic(form, latest.get());
                nanos[0] += System.nanoTime() - start;
                counts[same ? 1 : 2]++;
            });
            out.setProperty(prefix + "pairs", Long.toString(counts[0]));
            out.setProperty(prefix + "same", Long.toString(counts[1]));
            out.setProperty(prefix + "different", Long.toString(counts[2]));
            out.setProperty(prefix + "now.millis", Long.toString(nanos[0] / 1_000_000));
        }

        private static Optional<DiTreeEntity> latestTree(ViewCalculator view, long conceptNid, long patternNid) {
            return EntityService.get().semanticsForComponentOfPattern(conceptNid, patternNid).findFirst()
                    .flatMap(semantic -> {
                        Latest<SemanticEntityVersion> latest = view.latest((SemanticEntity<SemanticEntityVersion>) semantic);
                        return latest.isPresent()
                                ? Optional.of((DiTreeEntity) latest.get().fieldValues().get(0))
                                : Optional.empty();
                    });
        }
    }
}
