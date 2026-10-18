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

import dev.ikm.tinkar.common.id.PublicId;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.entity.EntityHandle;
import dev.ikm.tinkar.entity.EntityService;
import dev.ikm.tinkar.entity.SemanticEntity;
import dev.ikm.tinkar.entity.SemanticEntityVersion;
import dev.ikm.tinkar.entity.graph.DiTreeEntity;
import dev.ikm.tinkar.entity.graph.VertexDerivation;
import dev.ikm.tinkar.entity.graph.adaptor.axiom.LogicalExpressionBuilder;
import dev.ikm.tinkar.entity.load.LoadEntitiesFromProtobufFile;
import dev.ikm.tinkar.entity.transaction.StampedWriter;
import dev.ikm.tinkar.fixtures.ForkedJvm;
import dev.ikm.tinkar.terms.ConceptFacade;
import dev.ikm.tinkar.terms.EntityProxy;
import dev.ikm.tinkar.terms.KernelConcept;
import dev.ikm.tinkar.terms.KernelPattern;
import dev.ikm.tinkar.terms.State;
import network.ike.foundation.ike.bindings.IkeConcept;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Derivation on a store with no reverse identity map (IKE-Network/ike-issues#1310): the legacy
 * 8-bit Rocks layout cannot name a component it does not hold by its public id, and derivation
 * never reads a nid.
 *
 * <p>A definition written through the builder whose values are components the store does not
 * hold, each carried as an {@link EntityProxy} made from its UUIDs, derives its vertex UUIDs
 * from those UUIDs, and so derives the same ones on the legacy layout as on a store with a
 * reverse map. A value that carries only a nid for such a component cannot be named there, and
 * the write is refused.
 */
@Tag("starter-set")
class VertexDerivationWithoutReverseMapIT {

    private static final String TEST = "vertex-derivation-without-reverse-map";
    private static final String KB_FILE = "kb.file";

    private static final PublicId CONCEPT = id("0d7a54e6-55d1-4f0a-bb0b-6a1e8a42d2a1");
    private static final PublicId SEMANTIC = id("6a1fbbd0-6a3c-4a5e-9f0e-3f7f2bc9c8f4");
    private static final PublicId NID_ONLY_CONCEPT = id("a8f7c0a7-2e8e-4c1b-8c55-9a0f7a1d4e63");
    private static final PublicId NID_ONLY_SEMANTIC = id("e2c4b6a9-1f8d-4b6e-a2c3-5d7e9f1a3b5c");
    /** Components no store holds: the definition's parent, its role's type and its role's filler. */
    private static final UUID ABSENT_PARENT = UUID.fromString("3b7e0f0e-0a5c-4c39-8f4e-9c1d2b3a4f50");
    private static final UUID ABSENT_ROLE_TYPE = UUID.fromString("5c9e2a1d-7b3f-4e6a-9d8c-1a2b3c4d5e6f");
    private static final UUID ABSENT_FILLER = UUID.fromString("7e1f3a5b-9c2d-4e8f-a1b3-c5d7e9f1a2b4");
    private static final UUID NID_ONLY_PARENT = UUID.fromString("9a2b4c6d-8e1f-4a3b-b5c7-d9e1f3a5b7c9");

    private static final String DERIVED = "derived";
    private static final String DERIVED_FAILURE = "derived.failure";
    private static final String NID_ONLY_REFUSAL = "nid.only.refusal";

    @Test
    void valuesCarryingPublicIdsDeriveTheSameUuidsWithoutAReverseMap() {
        Properties legacy = run(Provider.ROCKS_LEGACY);
        Properties reverseMap = run(Provider.EPHEMERAL);

        assertAll(
                () -> assertNull(legacy.getProperty(DERIVED_FAILURE), "the write on the legacy layout"),
                () -> assertNull(reverseMap.getProperty(DERIVED_FAILURE), "the write on the ephemeral store"),
                () -> assertNotNull(legacy.getProperty(DERIVED), "the legacy layout's derived UUIDs"),
                () -> assertEquals(reverseMap.getProperty(DERIVED), legacy.getProperty(DERIVED),
                        "vertex UUIDs derived on the legacy layout, against those on a store with a reverse map"),
                () -> assertTrue(legacy.getProperty(DERIVED, "").contains(VertexDerivation.root(SEMANTIC, 0).toString()),
                        "the root's UUID is the one its semantic derives"),
                () -> assertTrue(legacy.getProperty(NID_ONLY_REFUSAL, "").contains("Cannot derive a vertex UUID"),
                        "a nid-only value on the legacy layout is refused: " + legacy.getProperty(NID_ONLY_REFUSAL)),
                () -> assertNull(reverseMap.getProperty(NID_ONLY_REFUSAL),
                        "a nid-only value on a store with a reverse map is named from it"));
    }

    private static Properties run(Provider provider) {
        Properties in = new Properties();
        in.setProperty(StoreStage.PROVIDER, provider.name());
        in.setProperty(StoreStage.STORE, ChangeSets.work(TEST, provider, "store").toString());
        in.setProperty(KB_FILE, StarterSetRoundTripIT.starterSet().toString());
        return ForkedJvm.run(WriteDerived.class, in, Duration.ofMinutes(10));
    }

    /**
     * The starter set loaded into a new store, then the two definitions written through the
     * builder: {@value #DERIVED} the stored tree's vertex UUIDs in vertex order, and
     * {@value #NID_ONLY_REFUSAL} why the nid-only one was refused, when it was.
     */
    static class WriteDerived extends StoreStage {
        @Override
        void work(Properties in, Properties out) {
            new LoadEntitiesFromProtobufFile(new File(in.getProperty(KB_FILE))).compute();

            try {
                LogicalExpressionBuilder builder = new LogicalExpressionBuilder();
                builder.NecessarySet(builder.And(
                        builder.ConceptAxiom(EntityProxy.Concept.make(ABSENT_PARENT.toString(), ABSENT_PARENT)),
                        builder.SomeRole(EntityProxy.Concept.make(ABSENT_ROLE_TYPE.toString(), ABSENT_ROLE_TYPE),
                                builder.ConceptAxiom(EntityProxy.Concept.make(ABSENT_FILLER.toString(), ABSENT_FILLER)))));
                write("derived", 1_800_000_000_000L, CONCEPT, SEMANTIC, (DiTreeEntity) builder.build().sourceGraph());
                out.setProperty(DERIVED, uuids(storedTree(SEMANTIC)));
            } catch (RuntimeException failure) {
                out.setProperty(DERIVED_FAILURE, chain(failure));
            }

            // a nid for a component the store does not hold, with no public id beside it
            long nidOnly = EntityProxy.Concept.make(NID_ONLY_PARENT.toString(), NID_ONLY_PARENT).nid();
            LogicalExpressionBuilder builder = new LogicalExpressionBuilder();
            builder.NecessarySet(builder.And(builder.ConceptAxiom(ConceptFacade.make(nidOnly))));
            try {
                write("nid only", 1_800_000_000_001L, NID_ONLY_CONCEPT, NID_ONLY_SEMANTIC,
                        (DiTreeEntity) builder.build().sourceGraph());
            } catch (RuntimeException refused) {
                out.setProperty(NID_ONLY_REFUSAL, chain(refused));
            }
        }

        private static void write(String name, long time, PublicId concept, PublicId semantic, DiTreeEntity tree) {
            StampedWriter writer = StampedWriter.open(name, State.ACTIVE, time,
                    KernelConcept.USER, IkeConcept.DEVELOPMENT_MODULE, KernelConcept.DEVELOPMENT_PATH);
            try {
                writer.concept(concept);
                writer.semantic(semantic, KernelPattern.EL_PLUS_PLUS_STATED_AXIOMS_PATTERN.publicId(), concept, tree);
                writer.commit();
            } catch (RuntimeException failure) {
                writer.cancel();
                throw failure;
            }
        }

        private static DiTreeEntity storedTree(PublicId semantic) {
            SemanticEntity<SemanticEntityVersion> stored = EntityHandle.get(EntityService.get()
                    .nidForSemantic(KernelPattern.EL_PLUS_PLUS_STATED_AXIOMS_PATTERN.publicId(), semantic)).expectSemantic();
            return (DiTreeEntity) stored.versions().getFirst().fieldValues().getFirst();
        }

        private static String uuids(DiTreeEntity tree) {
            StringJoiner uuids = new StringJoiner(",");
            for (int i = 0; i < tree.vertexMap().size(); i++) {
                uuids.add(tree.vertex(i).asUuidList().getFirst().toString());
            }
            return uuids.toString();
        }

        /** A failure and its causes, each by its type and message. */
        private static String chain(Throwable failure) {
            StringJoiner chain = new StringJoiner(" <- ");
            for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                chain.add(cause.getClass().getSimpleName() + ": " + cause.getMessage());
            }
            return chain.toString();
        }
    }

    private static PublicId id(String uuid) {
        return PublicIds.of(UUID.fromString(uuid));
    }
}
