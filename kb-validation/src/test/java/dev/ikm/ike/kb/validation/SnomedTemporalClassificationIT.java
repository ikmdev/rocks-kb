package dev.ikm.ike.kb.validation;

import dev.ikm.tinkar.common.service.internal.EntityStore;
import dev.ikm.elk.snomed.SnomedIsa;
import dev.ikm.tinkar.common.id.IntIds;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.util.uuid.UuidUtil;
import dev.ikm.tinkar.coordinate.Coordinates;
import dev.ikm.tinkar.coordinate.stamp.StampCoordinateRecord;
import dev.ikm.tinkar.coordinate.stamp.StampPositionRecord;
import dev.ikm.tinkar.coordinate.stamp.StateSet;
import dev.ikm.tinkar.coordinate.view.ViewCoordinateRecord;
import dev.ikm.tinkar.coordinate.view.calculator.ViewCalculator;
import dev.ikm.tinkar.coordinate.view.calculator.ViewCalculatorWithCache;
import dev.ikm.tinkar.entity.EntityHandle;
import dev.ikm.tinkar.entity.StampEntity;
import dev.ikm.tinkar.entity.StampEntityVersion;
import dev.ikm.tinkar.entity.load.LoadEntitiesFromProtobufFile;
import dev.ikm.tinkar.fixtures.ForkedJvm;
import dev.ikm.tinkar.reasoner.service.ReasonerService;
import dev.ikm.tinkar.terms.TinkarTerm;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The SNOMED CT knowledge base classified as it stood at each of its release dates. At
 * each date a view is fixed at that time on the development path, the concepts are read
 * through its calculators, and the knowledge base is classified; nothing is written to
 * the store. Two comparisons follow:
 *
 * <ul>
 * <li>with the recorded reference, {@code snomed-temporal-classification.properties}, so
 * a release infers and reads the same at every date as the release before it; and</li>
 * <li>with SNOMED International's own classification at that date, from the full
 * inferred relationship file: every concept's inferred parents.</li>
 * </ul>
 *
 * <p>The release dates are the times of the SNOMED CT modules' stamps on the development
 * path that fall on a date SNOMED International released, by the effective times of its
 * full relationship file; authoring in those modules on other dates is left out. Each date
 * is a stage in its own JVM ({@link ForkedJvm}); the
 * {@value #MAX_DATES_PROPERTY} system property limits a run to the first, the last and
 * dates spaced evenly between them, for development.
 */
@Tag("snomed")
class SnomedTemporalClassificationIT {

    private static final Logger LOG = LoggerFactory.getLogger(SnomedTemporalClassificationIT.class);

    static final String REFERENCE = "snomed-temporal-classification.properties";
    static final String MAX_DATES_PROPERTY = "snomed.temporal.max.dates";

    private static final String KB_FILE = "kb.file";
    private static final String ISA_FILE = "isa.file";
    private static final String TIMES = "times";
    private static final String TIME = "time";

    /** The modules SNOMED International releases in, by SCTID. */
    private static final List<String> SNOMED_MODULE_SCTIDS = List.of(
            "900000000000207008", // SNOMED CT core module
            "900000000000012004"); // SNOMED CT model component

    private static final DateTimeFormatter RF2_DATE = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);

    @Test
    void everyReleaseDateClassifiesAsRecordedAndAsSnomedInferred() throws IOException {
        Path work = Path.of("target", "snomed-temporal-classification").toAbsolutePath();
        SnomedRoundTripIT.deleteTree(work);
        Files.createDirectories(work);
        Path isaHistory = SnomedInferred.isaHistory(
                SnomedInferred.fullRelationshipFile(SnomedRoundTripIT.dataDirectory().resolve("rf2")),
                SnomedRoundTripIT.dataDirectory().resolve("isa-history.txt"));

        Properties in = new Properties();
        in.setProperty(StoreStage.PROVIDER, Provider.ROCKS.name());
        in.setProperty(StoreStage.STORE, work.resolve("store").toString());
        in.setProperty(KB_FILE, SnomedRoundTripIT.knowledgeBase().toString());
        in.setProperty(ISA_FILE, isaHistory.toString());

        Properties loaded = ForkedJvm.run(LoadAndFindReleaseDates.class, in, Duration.ofHours(2));
        List<String> allTimes = releaseTimes(loaded.getProperty(TIMES), SnomedInferred.effectiveTimes(isaHistory));
        assertTrue(!allTimes.isEmpty(), "The knowledge base holds no SNOMED CT release dates");
        List<String> times = select(allTimes, Integer.getInteger(MAX_DATES_PROPERTY, Integer.MAX_VALUE));
        LOG.info("Classifying at {} of the {} release dates", times.size(), allTimes.size());

        TreeMap<String, String> observed = new TreeMap<>();
        observed.put("release.dates", Integer.toString(allTimes.size()));
        for (String time : times) {
            Properties at = new Properties();
            at.putAll(loaded);
            at.setProperty(TIME, time);
            Properties result = ForkedJvm.run(ClassifyAt.class, at, Duration.ofHours(1));
            Reference.copyPrefixed(result, "observed.", observed);
            String date = date(time);
            LOG.info("{}: {} concepts classified in {} ms; SNOMED's parents agree for {}, differ for {}{}", date,
                    result.getProperty("observed.at." + date + ".classified.concepts"), result.getProperty("compute.millis"),
                    result.getProperty("observed.at." + date + ".snomed.parents.agree"),
                    result.getProperty("observed.at." + date + ".snomed.parents.differ"),
                    result.getProperty("snomed.differences.examples", "").isBlank() ? ""
                            : "\n" + result.getProperty("snomed.differences.examples"));
        }

        // The knowledge base's own release: the reasoner infers exactly what SNOMED did
        String last = date(allTimes.getLast());
        assertAll(
                () -> Reference.check(observed, REFERENCE, work, times.size() < allTimes.size()),
                () -> {
                    if (times.contains(allTimes.getLast())) {
                        assertEquals("0", observed.get("at." + last + ".snomed.parents.differ"),
                                "Concepts whose inferred parents differ from SNOMED's at the knowledge base's release, " + last);
                    }
                });
    }

    /**
     * The times of the SNOMED CT modules' stamps that fall on a date SNOMED released, the
     * latest on each such date, in order.
     */
    static List<String> releaseTimes(String moduleTimes, java.util.Set<Integer> releaseDates) {
        TreeMap<Integer, Long> latestOnDate = new TreeMap<>();
        for (String time : moduleTimes.split(",")) {
            if (!time.isBlank()) {
                int date = Integer.parseInt(date(time));
                if (releaseDates.contains(date)) {
                    latestOnDate.merge(date, Long.parseLong(time), Math::max);
                }
            }
        }
        return latestOnDate.values().stream().map(String::valueOf).toList();
    }

    static String date(String time) {
        return RF2_DATE.format(Instant.ofEpochMilli(Long.parseLong(time)));
    }

    /** The first and last of the times and others spaced evenly between them by position, {@code max} in all. */
    static List<String> select(List<String> sortedTimes, int max) {
        if (max < 2 || sortedTimes.size() <= max) {
            return sortedTimes;
        }
        TreeSet<Integer> positions = new TreeSet<>();
        for (int i = 0; i < max; i++) {
            positions.add((int) Math.round(i * (sortedTimes.size() - 1) / (double) (max - 1)));
        }
        List<String> selected = new ArrayList<>();
        for (int position : positions) {
            selected.add(sortedTimes.get(position));
        }
        return selected;
    }

    /** Stage 1: a new store, the knowledge base loaded, and the times SNOMED CT's modules changed. */
    static class LoadAndFindReleaseDates extends StoreStage {
        @Override
        void work(Properties in, Properties out) {
            long start = System.currentTimeMillis();
            new LoadEntitiesFromProtobufFile(new File(in.getProperty(KB_FILE))).compute();
            out.setProperty("load.millis", Long.toString(System.currentTimeMillis() - start));
            TreeSet<Integer> modules = new TreeSet<>();
            for (String sctid : SNOMED_MODULE_SCTIDS) {
                UUID uuid = UuidUtil.fromSNOMED(sctid);
                assertTrue(PrimitiveData.get().hasUuid(uuid), "The knowledge base has no module " + sctid);
                modules.add(PrimitiveData.nid(uuid));
            }
            int path = TinkarTerm.DEVELOPMENT_PATH.nid();
            TreeSet<Long> times = new TreeSet<>();
            EntityStore.current().forEachStampNid(nid -> {
                StampEntity<?> stamp = EntityHandle.get(nid).expectStamp();
                for (StampEntityVersion version : stamp.versions()) {
                    if (version.pathNid() == path && modules.contains(version.moduleNid())
                            && version.time() != Long.MAX_VALUE && version.time() != Long.MIN_VALUE) {
                        times.add(version.time());
                    }
                }
            });
            StringBuilder list = new StringBuilder();
            for (long time : times) {
                list.append(list.isEmpty() ? "" : ",").append(time);
            }
            out.setProperty(TIMES, list.toString());
        }
    }

    /** One release date: the knowledge base as of that date, read, classified, and compared with SNOMED's classification. */
    static class ClassifyAt extends StoreStage {
        @Override
        void work(Properties in, Properties out) throws Exception {
            String time = in.getProperty(TIME);
            String date = date(time);
            String prefix = "observed.at." + date + ".";
            StampPositionRecord position = StampPositionRecord.make(Long.parseLong(time), TinkarTerm.DEVELOPMENT_PATH.nid());
            StampCoordinateRecord stamps = StampCoordinateRecord.make(StateSet.ACTIVE, position, IntIds.set.empty());
            ViewCoordinateRecord coordinate = ViewCoordinateRecord.make(stamps,
                    Coordinates.Language.UsEnglishRegularName(), Coordinates.Logic.ElPlusPlus(),
                    Coordinates.Navigation.stated(), Coordinates.Edit.Default());
            ViewCalculator view = ViewCalculatorWithCache.getCalculator(coordinate);

            // Read through the view's calculators as of the date
            AtomicLong active = new AtomicLong();
            AtomicLong described = new AtomicLong();
            EntityStore.current().forEachConceptNid(nid -> {
                if (view.latestIsActive(nid)) {
                    active.incrementAndGet();
                    if (view.getDescriptionText(nid).isPresent()) {
                        described.incrementAndGet();
                    }
                }
            });
            out.setProperty(prefix + "active.concepts", Long.toString(active.get()));
            out.setProperty(prefix + "described.concepts", Long.toString(described.get()));
            int snomedRoot = PrimitiveData.nid(UuidUtil.fromSNOMED("138875005"));
            out.setProperty(prefix + "stated.descendants.of.snomed.root",
                    Integer.toString(view.navigationCalculator().descendentsOf(snomedRoot).size()));
            ViewCalculator inferredView = ViewCalculatorWithCache.getCalculator(ViewCoordinateRecord.make(stamps,
                    Coordinates.Language.UsEnglishRegularName(), Coordinates.Logic.ElPlusPlus(),
                    Coordinates.Navigation.inferred(), Coordinates.Edit.Default()));
            out.setProperty(prefix + "inferred.descendants.of.snomed.root",
                    Integer.toString(inferredView.navigationCalculator().descendentsOf(snomedRoot).size()));

            // Classify as of the date
            ReasonerService reasoner = Classification.classify(view, out, "");
            Classification.of(reasoner).store(out, prefix);

            // SNOMED's own classification at the date
            long start = System.currentTimeMillis();
            SnomedIsa snomed = SnomedIsa.init(Path.of(in.getProperty(ISA_FILE)), Integer.parseInt(date));
            SnomedInferred.compare(reasoner, snomed, out, prefix);
            out.setProperty("snomed.compare.millis", Long.toString(System.currentTimeMillis() - start));
        }
    }
}
