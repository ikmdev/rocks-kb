package dev.ikm.ike.kb.validation;

import dev.ikm.tinkar.common.service.internal.EntityStore;
import dev.ikm.tinkar.common.id.IntIdList;
import dev.ikm.tinkar.common.id.IntIdSet;
import dev.ikm.tinkar.common.id.IntIds;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.service.ServiceProperties;
import dev.ikm.tinkar.common.service.ServiceKeys;
import dev.ikm.tinkar.coordinate.Coordinates;
import dev.ikm.tinkar.coordinate.language.LanguageCoordinateRecord;
import dev.ikm.tinkar.coordinate.navigation.NavigationCoordinate;
import dev.ikm.tinkar.coordinate.navigation.calculator.NavigationCalculator;
import dev.ikm.tinkar.coordinate.stamp.StampCoordinateRecord;
import dev.ikm.tinkar.coordinate.stamp.calculator.Latest;
import dev.ikm.tinkar.coordinate.view.ViewCoordinateRecord;
import dev.ikm.tinkar.coordinate.view.calculator.ViewCalculator;
import dev.ikm.tinkar.coordinate.view.calculator.ViewCalculatorWithCache;
import dev.ikm.tinkar.entity.Entity;
import dev.ikm.tinkar.entity.EntityHandle;
import dev.ikm.tinkar.entity.EntityService;
import dev.ikm.tinkar.entity.EntityVersion;
import dev.ikm.tinkar.entity.SemanticEntity;
import dev.ikm.tinkar.entity.SemanticEntityVersion;
import dev.ikm.tinkar.entity.StampEntity;
import dev.ikm.tinkar.entity.StampRecord;
import dev.ikm.tinkar.fixtures.ForkedJvm;
import dev.ikm.tinkar.terms.EntityFacade;
import dev.ikm.tinkar.terms.State;
import dev.ikm.tinkar.terms.TinkarTerm;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The IKE starter set, loaded into each store provider, probed through the calculators and the
 * entity service: view coordinates, the stated and inferred hierarchies, names, dialects, the
 * entity-level contract against the store-level one, and uncommitted stamps: kept across a
 * restart by default, canceled when asked, or at startup when the option is on.
 *
 * <p>Every expectation is computed from the set's own stamps and semantics, not from a
 * recording: a probe asks the same question two ways, or asks what the data says must hold.
 * Each count of a violation must be zero; what is only observed (how many concepts have a
 * regular name, for one) is recorded and logged.
 */
@Tag("starter-set")
class StarterSetProbeIT {

    static final String KB_FILE = "kb.file";
    private static final String UNCOMMITTED_STAMP = "uncommitted.stamp";
    private static final String PROBE = "probe.";
    private static final String REOPENED = "reopened.";

    @ParameterizedTest
    @EnumSource(Provider.class)
    void theStarterSetAnswersEveryProbeConsistently(Provider provider) throws IOException {
        Path work = Path.of("target", "starter-set-probes", provider.name().toLowerCase()).toAbsolutePath();
        SnomedRoundTripIT.deleteTree(work);
        Files.createDirectories(work);

        Properties in = new Properties();
        in.setProperty(StoreStage.PROVIDER, provider.name());
        in.setProperty(StoreStage.STORE, work.resolve("store").toString());
        in.setProperty(KB_FILE, StarterSetRoundTripIT.starterSet().toString());

        Properties result;
        if (provider.persistent) {
            Properties loaded = ForkedJvm.run(Load.class, in, Duration.ofMinutes(10));
            Properties probed = ForkedJvm.run(Probe.class, loaded, Duration.ofMinutes(10));
            Properties reopened = ForkedJvm.run(Reopened.class, probed, Duration.ofMinutes(10));
            result = ForkedJvm.run(ReopenedCanceling.class, reopened, Duration.ofMinutes(10));
        } else {
            result = ForkedJvm.run(LoadAndProbe.class, in, Duration.ofMinutes(10));
        }

        List<Executable> checks = new ArrayList<>();
        zero(checks, result, "crashes", "probes that threw");
        // View coordinates
        zero(checks, result, "active.only.disagrees.with.latest.state",
                "entities whose latest version under an active-only view is not what the latest version's state says");
        // Hierarchy
        for (String navigation : List.of("inferred", "stated")) {
            checks.add(() -> assertEquals(1, number(result, PROBE + navigation + ".roots"),
                    navigation + " hierarchy: roots, " + result.getProperty(PROBE + navigation + ".roots.examples")));
            zero(checks, result, navigation + ".unreachable", navigation + " hierarchy: active concepts not under the root");
            zero(checks, result, navigation + ".cycles", navigation + " hierarchy: concepts among their own ancestors");
            zero(checks, result, navigation + ".asymmetric", navigation + " hierarchy: a parent that does not list the child, or the reverse");
        }
        // Names
        zero(checks, result, "concepts.without.fully.qualified.name", "active concepts with no fully qualified name");
        zero(checks, result, "concepts.with.two.preferred.fully.qualified.names",
                "active concepts with more than one preferred fully qualified name in one language and dialect");
        // Dialects
        zero(checks, result, "descriptions.without.acceptability",
                "active descriptions acceptable in no dialect");
        zero(checks, result, "descriptions.with.malformed.fields", "descriptions without language, text, case and type");
        // The entity-level contract against the store-level one
        for (String contract : List.of("concept.entities", "pattern.entities", "stamp.entities",
                "semantics.of.pattern", "semantics.for.component", "semantics.for.component.of.pattern",
                "entities.of.list", "store.for.each", "store.for.each.parallel",
                "entities.every", "entities.every.parallel", "entities.every.semantic", "entities.count")) {
            zero(checks, result, contract + ".differences", "entity service against the store: " + contract);
        }
        // Uncommitted stamps
        checks.add(() -> assertEquals("true", result.getProperty(PROBE + "uncommitted.cancelled.when.listed"),
                "an uncommitted stamp outside a transaction is cancelled when listAndCancelUncommittedStamps is given it"));
        if (provider.persistent) {
            checks.add(() -> assertEquals("true", result.getProperty(REOPENED + "uncommitted.survives.restart"),
                    "an uncommitted stamp survives the store's close and reopening, uncommitted: state "
                            + result.getProperty(REOPENED + "uncommitted.state.after.restart")));
            checks.add(() -> assertEquals("true", result.getProperty(REOPENED + "uncommitted.cancelled.at.startup"),
                    "with the option to cancel uncommitted stamps at startup, the stamp is canceled as the store opens: state "
                            + result.getProperty(REOPENED + "uncommitted.state.after.canceling.startup")));
        }

        StringBuilder observed = new StringBuilder();
        for (String key : new java.util.TreeSet<>(result.stringPropertyNames())) {
            if (key.startsWith(PROBE + "observed.")) {
                observed.append(key.substring((PROBE + "observed.").length())).append('=')
                        .append(result.getProperty(key)).append("; ");
            }
        }
        org.slf4j.LoggerFactory.getLogger(StarterSetProbeIT.class).info("{} observed: {}", provider, observed);
        assertAll(provider.name(), checks);
    }

    private static void zero(List<Executable> checks, Properties result, String key, String meaning) {
        checks.add(() -> assertEquals(0, number(result, PROBE + key),
                meaning + ": " + result.getProperty(PROBE + key + ".examples")));
    }

    private static long number(Properties properties, String key) {
        String value = properties.getProperty(key);
        assertTrue(value != null, "No " + key + " was recorded (its probe threw; see the crashes)");
        return Long.parseLong(value);
    }

    /** Stage 1: a new store and the starter set loaded into it. */
    static class Load extends StoreStage {
        @Override
        void work(Properties in, Properties out) {
            new dev.ikm.tinkar.entity.load.LoadEntitiesFromProtobufFile(new File(in.getProperty(KB_FILE))).compute();
        }
    }

    /** Stage 2: the store reopened and probed; an uncommitted stamp left across the close. */
    static class Probe extends StoreStage {
        @Override
        void work(Properties in, Properties out) {
            Probes.run(out);
            out.setProperty(UNCOMMITTED_STAMP, Probes.leaveUncommittedStamp().toString());
        }
    }

    /**
     * Stage 3: the store reopened. An uncommitted stamp survives a restart, still uncommitted:
     * work in progress outlives the session that began it, and change sets share it.
     */
    static class Reopened extends StoreStage {
        @Override
        void work(Properties in, Properties out) {
            StampEntity<?> stamp = EntityHandle.get(PublicIds.of(UUID.fromString(in.getProperty(UNCOMMITTED_STAMP)))).expectStamp();
            String state = stamp.versions().isEmpty() ? "no version left" : stamp.state() + " at " + stamp.time();
            out.setProperty(REOPENED + "uncommitted.state.after.restart", state);
            out.setProperty(REOPENED + "uncommitted.survives.restart",
                    Boolean.toString(!stamp.versions().isEmpty() && stamp.state() == State.ACTIVE
                            && stamp.time() == Long.MAX_VALUE));
        }
    }

    /** Stage 4: the store reopened with the option to cancel uncommitted stamps as it starts. */
    static class ReopenedCanceling extends StoreStage {
        @Override
        void configure(Properties in) {
            ServiceProperties.set(ServiceKeys.CANCEL_UNCOMMITTED_STAMPS_AT_STARTUP, Boolean.TRUE);
        }

        @Override
        void work(Properties in, Properties out) {
            StampEntity<?> stamp = EntityHandle.get(PublicIds.of(UUID.fromString(in.getProperty(UNCOMMITTED_STAMP)))).expectStamp();
            String state = stamp.versions().isEmpty() ? "no version left" : stamp.state() + " at " + stamp.time();
            out.setProperty(REOPENED + "uncommitted.state.after.canceling.startup", state);
            out.setProperty(REOPENED + "uncommitted.cancelled.at.startup",
                    Boolean.toString(!stamp.versions().isEmpty() && stamp.state() == State.CANCELED));
        }
    }

    /** An ephemeral store does not outlive its JVM: loaded and probed in one lifetime. */
    static class LoadAndProbe extends StoreStage {
        @Override
        void work(Properties in, Properties out) {
            new Load().work(in, out);
            Probes.run(out);
        }
    }

    /** The probes, run against the open store. */
    static final class Probes {
        private Probes() {
        }

        static void run(Properties out) {
            LanguageCoordinateRecord usRegular = Coordinates.Language.UsEnglishRegularName();
            LanguageCoordinateRecord usFullyQualified = Coordinates.Language.UsEnglishFullyQualifiedName();
            ViewCalculator view = view(Coordinates.Stamp.DevelopmentLatest(), usRegular, Coordinates.Navigation.inferred());
            ViewCalculator activeOnly = view(Coordinates.Stamp.DevelopmentLatestActiveOnly(), usRegular, Coordinates.Navigation.inferred());
            ViewCalculator master = view(Coordinates.Stamp.MasterLatest(), usRegular, Coordinates.Navigation.inferred());
            ViewCalculator stated = view(Coordinates.Stamp.DevelopmentLatest(), usRegular, Coordinates.Navigation.stated());
            ViewCalculator fullyQualifiedFirst = view(Coordinates.Stamp.DevelopmentLatest(), usFullyQualified, Coordinates.Navigation.inferred());
            ViewCalculator gb = view(Coordinates.Stamp.DevelopmentLatest(), Coordinates.Language.GbEnglishPreferredName(), Coordinates.Navigation.inferred());

            MutableIntSet concepts = IntSets.mutable.empty().asSynchronized();
            MutableIntSet activeConcepts = IntSets.mutable.empty().asSynchronized();
            EntityStore.current().forEachConceptNid(nid -> {
                if (EntityHandle.get(nid).isPresent()) {
                    concepts.add(nid);
                    Latest<EntityVersion> latest = view.latest(nid);
                    if (latest.isPresent() && latest.get().active()) {
                        activeConcepts.add(nid);
                    }
                }
            });
            observe(out, "concepts", concepts.size());
            observe(out, "active.concepts", activeConcepts.size());

            Count crashes = new Count();
            guard(crashes, "view coordinates", () -> viewCoordinates(out, view, activeOnly, master));
            guard(crashes, "inferred hierarchy", () -> hierarchy(out, "inferred", view.navigationCalculator(), activeConcepts));
            guard(crashes, "stated hierarchy", () -> hierarchy(out, "stated", stated.navigationCalculator(), activeConcepts));
            guard(crashes, "names and dialects", () -> names(out, view, fullyQualifiedFirst, gb, activeConcepts, usFullyQualified));
            guard(crashes, "entity-level contract", () -> contract(out, concepts));
            guard(crashes, "uncommitted stamp", () -> uncommittedWhenListed(out));
            crashes.store(out, PROBE + "crashes");
        }

        /** Runs one probe; a probe that throws is itself a finding, and the others still run. */
        private static void guard(Count crashes, String probe, Runnable body) {
            try {
                body.run();
            } catch (RuntimeException | Error e) {
                StackTraceElement at = e.getStackTrace().length > 0 ? e.getStackTrace()[0] : null;
                crashes.add(probe + ": " + e + (at == null ? "" : " at " + at));
            }
        }

        private static ViewCalculator view(StampCoordinateRecord stamp, LanguageCoordinateRecord language,
                                           NavigationCoordinate navigation) {
            return ViewCalculatorWithCache.getCalculator(ViewCoordinateRecord.make(stamp, language,
                    Coordinates.Logic.ElPlusPlus(), navigation, Coordinates.Edit.Default()));
        }

        private static void observe(Properties out, String key, long value) {
            out.setProperty(PROBE + "observed." + key, Long.toString(value));
        }

        /** Every entity: an active-only view agrees with the state of the latest version. */
        private static void viewCoordinates(Properties out, ViewCalculator view, ViewCalculator activeOnly, ViewCalculator master) {
            Count disagrees = new Count();
            Count visibleOnMaster = new Count();
            // A store may call a forEach...Nid procedure from several threads at once.
            List<Integer> nids = Collections.synchronizedList(new ArrayList<>());
            EntityStore.current().forEachConceptNid(nids::add);
            EntityStore.current().forEachPatternNid(nids::add);
            EntityStore.current().forEachSemanticNid(nids::add);
            for (int nid : nids) {
                if (EntityHandle.get(nid).isAbsent()) {
                    continue;
                }
                Latest<EntityVersion> latest = view.latest(nid);
                boolean expectedActive = latest.isPresent() && latest.get().active();
                if (activeOnly.latest(nid).isPresent() != expectedActive) {
                    disagrees.add(PrimitiveData.textWithNid(nid));
                }
                if (master.latest(nid).isPresent()) {
                    visibleOnMaster.add();
                }
            }
            disagrees.store(out, PROBE + "active.only.disagrees.with.latest.state");
            observe(out, "visible.on.master", visibleOnMaster.get());
        }

        /** One root; every active concept under it; no cycles; parents and children agree. */
        private static void hierarchy(Properties out, String name, NavigationCalculator navigation, MutableIntSet activeConcepts) {
            Count roots = new Count();
            int[] root = {0};
            Count cycles = new Count();
            Count asymmetric = new Count();
            activeConcepts.forEach(nid -> {
                IntIdList parents = navigation.parentsOf(nid);
                if (parents.isEmpty()) {
                    roots.add(PrimitiveData.textWithNid(nid));
                    root[0] = nid;
                }
                if (navigation.ancestorsOf(nid).contains(nid)) {
                    cycles.add(PrimitiveData.textWithNid(nid));
                }
                parents.forEach(parent -> {
                    if (!navigation.childrenOf(parent).contains(nid)) {
                        asymmetric.add(PrimitiveData.text(parent) + " does not list child " + PrimitiveData.text(nid));
                    }
                });
                navigation.childrenOf(nid).forEach(child -> {
                    if (!navigation.parentsOf(child).contains(nid)) {
                        asymmetric.add(PrimitiveData.text(child) + " does not list parent " + PrimitiveData.text(nid));
                    }
                });
            });
            Count unreachable = new Count();
            if (roots.get() == 1) {
                IntIdSet descendants = navigation.descendentsOf(root[0]);
                activeConcepts.forEach(nid -> {
                    if (nid != root[0] && !descendants.contains(nid)) {
                        unreachable.add(PrimitiveData.textWithNid(nid));
                    }
                });
            }
            roots.store(out, PROBE + name + ".roots");
            unreachable.store(out, PROBE + name + ".unreachable");
            cycles.store(out, PROBE + name + ".cycles");
            asymmetric.store(out, PROBE + name + ".asymmetric");
        }

        /**
         * Fully qualified names, regular names, definitions, and the dialect acceptability of every
         * active description, read from the description semantics themselves and through the
         * language calculator.
         */
        private static void names(Properties out, ViewCalculator view, ViewCalculator fullyQualifiedFirst,
                                  ViewCalculator gb, MutableIntSet activeConcepts, LanguageCoordinateRecord usFullyQualified) {
            int descriptionPattern = usFullyQualified.descriptionPatternPreferenceNidList().get(0);
            int fullyQualifiedType = usFullyQualified.descriptionTypePreferenceNidList().get(0);
            int preferred = TinkarTerm.PREFERRED.nid();

            Count withoutFullyQualifiedName = new Count();
            Count withoutRegularName = new Count();
            Count withDefinition = new Count();
            Count gbDiffers = new Count();
            Count fullyQualifiedFirstDiffers = new Count();
            Count twoPreferred = new Count();
            Count withoutAcceptability = new Count();
            Count malformed = new Count();
            Count descriptions = new Count();
            Map<Integer, Count> acceptabilityPatterns = new HashMap<>();

            activeConcepts.forEach(nid -> {
                var language = view.languageCalculator();
                if (language.getFullyQualifiedNameText(nid).filter(text -> !text.isBlank()).isEmpty()) {
                    withoutFullyQualifiedName.add(view.getDescriptionTextOrNid(nid));
                }
                if (language.getRegularDescriptionText(nid).isEmpty()) {
                    withoutRegularName.add();
                }
                if (language.getDefinitionDescriptionText(nid).isPresent()) {
                    withDefinition.add();
                }
                if (!view.getDescriptionTextOrNid(nid).equals(gb.getDescriptionTextOrNid(nid))) {
                    gbDiffers.add();
                }
                if (!view.getDescriptionTextOrNid(nid).equals(fullyQualifiedFirst.getDescriptionTextOrNid(nid))) {
                    fullyQualifiedFirstDiffers.add();
                }
                // Preferred descriptions per (language, type, dialect pattern), from the data.
                Map<String, Integer> preferredCount = new HashMap<>();
                for (int descriptionNid : EntityStore.current().semanticNidsForComponentOfPattern(nid, descriptionPattern)) {
                    Latest<SemanticEntityVersion> description = view.latest(descriptionNid);
                    if (description.isAbsent() || !description.get().active()) {
                        continue;
                    }
                    descriptions.add();
                    var fields = description.get().fieldValues();
                    if (fields.size() != 4 || !(fields.get(0) instanceof EntityFacade languageConcept)
                            || !(fields.get(1) instanceof String) || !(fields.get(3) instanceof EntityFacade type)) {
                        malformed.add(PrimitiveData.textWithNid(descriptionNid));
                        continue;
                    }
                    boolean acceptable = false;
                    for (int acceptabilityNid : EntityStore.current().semanticNidsForComponent(descriptionNid)) {
                        Latest<SemanticEntityVersion> acceptability = view.latest(acceptabilityNid);
                        if (acceptability.isAbsent() || !acceptability.get().active()) {
                            continue;
                        }
                        acceptable = true;
                        int dialectPattern = acceptability.get().entity().patternNid();
                        acceptabilityPatterns.computeIfAbsent(dialectPattern, k -> new Count()).add();
                        var values = acceptability.get().fieldValues();
                        if (!values.isEmpty() && values.get(0) instanceof EntityFacade value && value.nid() == preferred) {
                            preferredCount.merge(languageConcept.nid() + "|" + type.nid() + "|" + dialectPattern, 1, Integer::sum);
                        }
                    }
                    if (!acceptable) {
                        withoutAcceptability.add(fields.get(1) + " (" + PrimitiveData.text(type.nid()) + ") of "
                                + PrimitiveData.text(nid));
                    }
                }
                preferredCount.forEach((key, count) -> {
                    if (count > 1 && key.split("\\|")[1].equals(Integer.toString(fullyQualifiedType))) {
                        twoPreferred.add(PrimitiveData.textWithNid(nid));
                    }
                });
            });

            withoutFullyQualifiedName.store(out, PROBE + "concepts.without.fully.qualified.name");
            twoPreferred.store(out, PROBE + "concepts.with.two.preferred.fully.qualified.names");
            withoutAcceptability.store(out, PROBE + "descriptions.without.acceptability");
            malformed.store(out, PROBE + "descriptions.with.malformed.fields");
            observe(out, "active.descriptions", descriptions.get());
            observe(out, "concepts.without.regular.name", withoutRegularName.get());
            observe(out, "concepts.with.definition", withDefinition.get());
            observe(out, "concepts.named.differently.in.gb", gbDiffers.get());
            observe(out, "concepts.named.differently.fully.qualified.first", fullyQualifiedFirstDiffers.get());
            acceptabilityPatterns.forEach((pattern, count) ->
                    observe(out, "acceptability." + PrimitiveData.text(pattern).replace(' ', '_'), count.get()));
        }

        /** The entity service's enumerations against the store's, and the store's whole and list forms. */
        private static void contract(Properties out, MutableIntSet concepts) {
            EntityService entities = EntityService.get();
            compare(out, "concept.entities", present(EntityStore.current()::forEachConceptNid),
                    collect(consumer -> entities.forEachConceptEntity(e -> consumer.add(e.nid()))));
            compare(out, "pattern.entities", present(EntityStore.current()::forEachPatternNid),
                    collect(consumer -> entities.forEachPatternEntity(e -> consumer.add(e.nid()))));
            compare(out, "stamp.entities", present(EntityStore.current()::forEachStampNid),
                    collect(consumer -> entities.forEachStampEntity(e -> consumer.add(e.nid()))));

            Count ofPattern = new Count();
            EntityStore.current().forEachPatternNid(patternNid -> {
                try {
                    MutableIntSet viaEntities = IntSets.mutable.empty().asSynchronized();
                    entities.forEachSemanticOfPattern(patternNid, semantic -> viaEntities.add(semantic.nid()));
                    MutableIntSet viaStore = IntSets.mutable.with(EntityStore.current().semanticNidsOfPattern(patternNid));
                    if (!viaEntities.equals(viaStore)) {
                        ofPattern.add(PrimitiveData.textWithNid(patternNid) + ": " + viaEntities.size()
                                + " through the entity service, " + viaStore.size() + " from the store");
                    }
                    if (!viaStore.equals(nids(entities.semanticsOfPattern(patternNid)))) {
                        ofPattern.add(PrimitiveData.textWithNid(patternNid) + ": the entity service's stream differs from the store");
                    }
                } catch (RuntimeException e) {
                    ofPattern.add(PrimitiveData.textWithNid(patternNid) + ": " + e);
                }
            });
            ofPattern.store(out, PROBE + "semantics.of.pattern.differences");

            int descriptionPattern = Coordinates.Language.UsEnglishRegularName().descriptionPatternPreferenceNidList().get(0);
            Count forComponent = new Count();
            Count forComponentOfPattern = new Count();
            concepts.forEach(nid -> {
                MutableIntSet viaEntities = IntSets.mutable.empty().asSynchronized();
                entities.forEachSemanticForComponent(nid, semantic -> viaEntities.add(semantic.nid()));
                MutableIntSet viaStore = IntSets.mutable.with(EntityStore.current().semanticNidsForComponent(nid));
                if (!viaEntities.equals(viaStore) || !viaStore.equals(nids(entities.semanticsForComponent(nid)))) {
                    forComponent.add(PrimitiveData.textWithNid(nid));
                }
                MutableIntSet ofPatternViaEntities = IntSets.mutable.empty().asSynchronized();
                entities.forEachSemanticForComponentOfPattern(nid, descriptionPattern,
                        semantic -> ofPatternViaEntities.add(semantic.nid()));
                MutableIntSet ofPatternViaStore = IntSets.mutable.with(
                        EntityStore.current().semanticNidsForComponentOfPattern(nid, descriptionPattern));
                if (!ofPatternViaEntities.equals(ofPatternViaStore)
                        || !ofPatternViaStore.equals(nids(entities.semanticsForComponentOfPattern(nid, descriptionPattern)))) {
                    forComponentOfPattern.add(PrimitiveData.textWithNid(nid));
                }
            });
            forComponent.store(out, PROBE + "semantics.for.component.differences");
            forComponentOfPattern.store(out, PROBE + "semantics.for.component.of.pattern.differences");

            // Every entity, three ways: by kind, through the whole-store scan, and as a list.
            MutableIntSet byKind = IntSets.mutable.empty().asSynchronized();
            byKind.addAll(present(EntityStore.current()::forEachConceptNid));
            byKind.addAll(present(EntityStore.current()::forEachPatternNid));
            byKind.addAll(present(EntityStore.current()::forEachSemanticNid));
            byKind.addAll(present(EntityStore.current()::forEachStampNid));
            MutableIntSet scanned = IntSets.mutable.empty().asSynchronized();
            EntityStore.current().forEach((bytes, nid) -> scanned.add(nid));
            MutableIntSet scannedInParallel = IntSets.mutable.empty().asSynchronized();
            EntityStore.current().forEachParallel((bytes, nid) -> scannedInParallel.add(nid));
            MutableIntSet listed = IntSets.mutable.empty().asSynchronized();
            entities.forEachEntity(byKind.toList().toImmutable(), entity -> listed.add(entity.nid()));
            MutableIntSet everyEntity = IntSets.mutable.empty().asSynchronized();
            entities.forEachEntity(entity -> everyEntity.add(entity.nid()));
            MutableIntSet everyEntityInParallel = IntSets.mutable.empty().asSynchronized();
            entities.forEachEntityParallel(entity -> everyEntityInParallel.add(entity.nid()));
            MutableIntSet semanticsByKind = present(EntityStore.current()::forEachSemanticNid);
            MutableIntSet everySemantic = IntSets.mutable.empty().asSynchronized();
            entities.forEachSemanticEntity(semantic -> everySemantic.add(semantic.nid()));
            compare(out, "store.for.each.parallel", scanned, scannedInParallel);
            compare(out, "entities.of.list", byKind, listed);
            compare(out, "store.for.each", byKind, scanned);
            compare(out, "entities.every", byKind, everyEntity);
            compare(out, "entities.every.parallel", byKind, everyEntityInParallel);
            compare(out, "entities.every.semantic", semanticsByKind, everySemantic);
            Count counted = new Count();
            if (entities.countEntities() != byKind.size()) {
                counted.add("countEntities " + entities.countEntities() + ", by kind " + byKind.size());
            }
            counted.store(out, PROBE + "entities.count.differences");
        }

        private static MutableIntSet nids(java.util.stream.Stream<? extends dev.ikm.tinkar.entity.Entity<?>> entities) {
            MutableIntSet nids = IntSets.mutable.empty();
            entities.forEach(entity -> nids.add(entity.nid()));
            return nids;
        }

        private interface NidSource {
            void forEach(org.eclipse.collections.api.block.procedure.primitive.IntProcedure procedure);
        }

        private interface NidSink {
            void collect(MutableIntSet into);
        }

        // Every collecting set is synchronized: a store may call a procedure from several threads.
        private static MutableIntSet present(NidSource source) {
            MutableIntSet nids = IntSets.mutable.empty().asSynchronized();
            source.forEach(nid -> {
                if (EntityHandle.get(nid).isPresent()) {
                    nids.add(nid);
                }
            });
            return nids;
        }

        private static MutableIntSet collect(NidSink sink) {
            MutableIntSet nids = IntSets.mutable.empty().asSynchronized();
            sink.collect(nids);
            return nids;
        }

        private static void compare(Properties out, String name, MutableIntSet expected, MutableIntSet actual) {
            Count differences = new Count();
            expected.forEach(nid -> {
                if (!actual.contains(nid)) {
                    differences.add("missing " + PrimitiveData.textWithNid(nid));
                }
            });
            actual.forEach(nid -> {
                if (!expected.contains(nid)) {
                    differences.add("extra " + PrimitiveData.textWithNid(nid));
                }
            });
            differences.store(out, PROBE + name + ".differences");
        }

        /** An uncommitted stamp outside a transaction, given to listAndCancelUncommittedStamps, is cancelled. */
        private static void uncommittedWhenListed(Properties out) {
            StampRecord stamp = uncommittedStamp();
            EntityService.get().listAndCancelUncommittedStamps(new int[]{stamp.nid()});
            StampEntity<?> after = EntityHandle.get(stamp.nid()).expectStamp();
            out.setProperty(PROBE + "uncommitted.cancelled.when.listed", after.versions().isEmpty()
                    ? "no version left" : Boolean.toString(after.state() == State.CANCELED));
        }

        /** Writes an uncommitted stamp outside any transaction and returns its identity. */
        static UUID leaveUncommittedStamp() {
            return uncommittedStamp().publicId().asUuidArray()[0];
        }

        private static StampRecord uncommittedStamp() {
            StampRecord stamp = StampRecord.make(UUID.randomUUID(), State.ACTIVE, Long.MAX_VALUE,
                    TinkarTerm.USER.publicId(), TinkarTerm.DEVELOPMENT_MODULE.publicId(), TinkarTerm.DEVELOPMENT_PATH.publicId());
            EntityService.get().putEntity(stamp);
            return stamp;
        }
    }
}
