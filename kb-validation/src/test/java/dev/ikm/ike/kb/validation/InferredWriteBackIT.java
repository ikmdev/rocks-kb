package dev.ikm.ike.kb.validation;

import dev.ikm.tinkar.common.id.PublicId;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.util.uuid.UuidT5Generator;
import dev.ikm.tinkar.coordinate.Calculators;
import dev.ikm.tinkar.coordinate.view.ViewCoordinateRecord;
import dev.ikm.tinkar.coordinate.view.calculator.ViewCalculator;
import dev.ikm.tinkar.entity.ConceptRecord;
import dev.ikm.tinkar.entity.ConceptRecordBuilder;
import dev.ikm.tinkar.entity.ConceptVersionRecord;
import dev.ikm.tinkar.entity.ConceptVersionRecordBuilder;
import dev.ikm.tinkar.entity.EntityHandle;
import dev.ikm.tinkar.entity.EntityService;
import dev.ikm.tinkar.entity.PatternEntity;
import dev.ikm.tinkar.entity.RecordListBuilder;
import dev.ikm.tinkar.entity.SemanticRecord;
import dev.ikm.tinkar.entity.SemanticRecordBuilder;
import dev.ikm.tinkar.entity.SemanticVersionRecord;
import dev.ikm.tinkar.entity.StampEntity;
import dev.ikm.tinkar.entity.graph.adaptor.axiom.LogicalExpressionBuilder;
import dev.ikm.tinkar.entity.load.LoadEntitiesFromProtobufFile;
import dev.ikm.tinkar.entity.transaction.Transaction;
import dev.ikm.tinkar.fixtures.ForkedJvm;
import dev.ikm.tinkar.reasoner.service.ReasonerService;
import dev.ikm.tinkar.terms.State;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.primitive.LongSets;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static dev.ikm.tinkar.common.service.PrimitiveData.SCOPED_PATTERN_PUBLICID_FOR_NID;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A classification written back gives every classified concept an inferred axiom semantic, in
 * every store provider. The IKE starter set is loaded, {@value #ADDED} concepts are added, each
 * stated to be a kind of the logic coordinate's root, and the store is classified and the results
 * written. That many concepts make the writer's new-semantic pass run as twenty chunks at once,
 * each reading its concepts through the store while the others write.
 *
 * <p>On the 64-bit Rocks store, a read in a load phase waited for references other chunks had
 * added after the read's own drain, which nothing wrote until the phase ended: after ten minutes
 * it failed, and the writer dropped the chunk without a word. Classifying SOLOR + GUDID so lost
 * 4.7 million of 5.7 million inferred axiom semantics (IKE-Network/ike-issues#1301).
 */
@Tag("starter-set")
class InferredWriteBackIT {

    /** Under the writer's 100,000-item chunk-size step, so its chunks are 5,000 concepts: twenty of them. */
    static final int ADDED = 99_000;

    private static final String KB_FILE = "kb.file";
    private static final UUID NAMESPACE = UUID.fromString("5c7d6a7e-2f86-4f0e-9d3a-1a8c4e5b9f21");

    @ParameterizedTest
    @EnumSource(Provider.class)
    void everyClassifiedConceptHasAnInferredAxiomSemantic(Provider provider) throws Exception {
        Path work = Path.of("target", "inferred-write-back", provider.name().toLowerCase()).toAbsolutePath();
        SnomedRoundTripIT.deleteTree(work);

        Properties in = new Properties();
        in.setProperty(StoreStage.PROVIDER, provider.name());
        in.setProperty(StoreStage.STORE, work.resolve("store").toString());
        in.setProperty(KB_FILE, StarterSetRoundTripIT.starterSet().toString());
        Properties result = ForkedJvm.run(ClassifyAndWrite.class, in, Duration.ofMinutes(30));

        assertAll(
                () -> assertEquals(ADDED, number(result, "added.classified"),
                        "Added concepts the reasoner classified"),
                () -> assertEquals(0, number(result, "classified.without.inferred"),
                        "Classified concepts without an inferred axiom semantic, of "
                                + result.getProperty("classified") + ": "
                                + result.getProperty("classified.without.inferred.examples")));
    }

    private static long number(Properties properties, String key) {
        return Long.parseLong(properties.getProperty(key, "-1"));
    }

    /** Loads the starter set, adds the concepts, classifies, writes the results and checks them. */
    static class ClassifyAndWrite extends StoreStage {
        @Override
        void work(Properties in, Properties out) throws Exception {
            new LoadEntitiesFromProtobufFile(new File(in.getProperty(KB_FILE))).compute();
            ViewCalculator view = Calculators.View.Default();
            long inferredPattern = view.viewCoordinateRecord().logicCoordinate().inferredAxiomsPatternNid();
            MutableLongSet added = addConcepts(view.viewCoordinateRecord());

            ReasonerService reasoner = Classification.classify(view, out, "classification.");
            reasoner.writeInferredResults();

            AtomicLong without = new AtomicLong();
            List<String> examples = new ArrayList<>();
            reasoner.getReasonerConceptSet().forEach(nid -> {
                if (!hasSemanticOf(nid, inferredPattern)) {
                    if (without.incrementAndGet() <= 10) {
                        examples.add(PrimitiveData.textWithNid(nid));
                    }
                }
            });
            out.setProperty("classified", Integer.toString(reasoner.getReasonerConceptSet().size()));
            out.setProperty("added.classified", Integer.toString(reasoner.getReasonerConceptSet().count(added::contains)));
            out.setProperty("classified.without.inferred", Long.toString(without.get()));
            out.setProperty("classified.without.inferred.examples", String.join("; ", examples));
        }

        private static boolean hasSemanticOf(long componentNid, long patternNid) {
            return EntityService.get().semanticsForComponentOfPattern(componentNid, patternNid).findAny().isPresent();
        }

        /** The concepts, each with a stated axiom semantic saying it is a kind of the root, under one committed stamp. */
        private static MutableLongSet addConcepts(ViewCoordinateRecord coordinate) {
            Transaction transaction = Transaction.make("Concepts to classify");
            StampEntity<?> stamp = transaction.getStamp(State.ACTIVE, coordinate.getAuthorNidForChanges(),
                    coordinate.getDefaultModuleNid(), coordinate.getDefaultPathNid());
            PatternEntity<?> statedPattern = EntityHandle.getPatternOrThrow(coordinate.logicCoordinate().statedAxiomsPatternNid());
            long rootNid = coordinate.logicCoordinate().rootNid();
            MutableLongSet added = LongSets.mutable.withInitialCapacity(ADDED);
            for (int i = 0; i < ADDED; i++) {
                PublicId conceptId = PublicIds.of(UuidT5Generator.get(NAMESPACE, "concept " + i));
                RecordListBuilder<ConceptVersionRecord> conceptVersions = RecordListBuilder.make();
                ConceptRecord bootstrap = ConceptRecord.makeNew(conceptId, conceptVersions);
                conceptVersions.add(ConceptVersionRecordBuilder.builder().chronology(bootstrap).stampNid(stamp.nid()).build());
                ConceptRecord concept = ConceptRecordBuilder.builder(bootstrap).versions(conceptVersions.toImmutable()).build();
                transaction.addComponent(concept);
                EntityService.get().putEntity(concept);

                LogicalExpressionBuilder builder = new LogicalExpressionBuilder();
                builder.NecessarySet(builder.And(builder.ConceptAxiom(rootNid)));
                UUID axiomUuid = UuidT5Generator.singleSemanticUuid(statedPattern.publicId(), conceptId);
                long axiomNid = ScopedValue.where(SCOPED_PATTERN_PUBLICID_FOR_NID, statedPattern.publicId())
                        .call(() -> PrimitiveData.nid(axiomUuid));
                RecordListBuilder<SemanticVersionRecord> axiomVersions = RecordListBuilder.make();
                SemanticRecord axiom = SemanticRecordBuilder.builder()
                        .nid(axiomNid)
                        .leastSignificantBits(axiomUuid.getLeastSignificantBits())
                        .mostSignificantBits(axiomUuid.getMostSignificantBits())
                        .patternNid(statedPattern.nid())
                        .referencedComponentNid(concept.nid())
                        .versions(axiomVersions).build();
                axiomVersions.add(new SemanticVersionRecord(axiom, stamp.nid(), Lists.immutable.of(builder.build().sourceGraph())));
                transaction.addComponent(axiom);
                EntityService.get().putEntity(axiom);
                added.add(concept.nid());
            }
            transaction.commit();
            return added;
        }
    }
}
