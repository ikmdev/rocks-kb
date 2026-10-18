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

import dev.ikm.tinkar.common.service.DataActivity;
import dev.ikm.tinkar.entity.Entity;
import dev.ikm.tinkar.entity.EntityService;
import dev.ikm.tinkar.entity.RecordListBuilder;
import dev.ikm.tinkar.entity.SemanticEntity;
import dev.ikm.tinkar.entity.SemanticEntityVersion;
import dev.ikm.tinkar.entity.SemanticRecord;
import dev.ikm.tinkar.entity.SemanticVersionRecord;
import dev.ikm.tinkar.entity.StampEntity;
import dev.ikm.tinkar.entity.graph.DiTreeEntity;
import dev.ikm.tinkar.entity.graph.EntityVertex;
import dev.ikm.tinkar.entity.graph.VertexIdentityWrite;
import dev.ikm.tinkar.fixtures.ForkedJvm;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.primitive.ImmutableIntList;
import org.eclipse.collections.api.map.primitive.MutableIntIntMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntIntMaps;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The vertex-identity history sweep (arch-vertex-identity-verification, "At Write and Over
 * History"; IKE-Network/ike-issues#1310): every semantic holding a logical definition, over all
 * its stored versions, checked against the invariants of arch-vertex-identity-birth and the
 * identity table's shape.
 *
 * <ul>
 *   <li>a UUID is unique within its tree;
 *   <li>a UUID never appears in another semantic's trees;
 *   <li>a kept UUID never changes meaning, nor, as an operand, its identifying properties, from
 *       one version to the next in STAMP order;
 *   <li>every tree's shape is one the identity table admits;
 *   <li>the fixed point: the semantic's versions, stripped of their vertex UUIDs and replayed
 *       through the write path's correlation and derivation, come back with the UUIDs stored.
 * </ul>
 *
 * <p>A violation is a matcher bug or a write that bypassed the write path. Two checks hold only
 * for a store whose identities were derived, and are recorded on every run but asserted only
 * when {@value #DERIVED_PROPERTY} says the store is derived: the fixed point, which a store built
 * before derivation misses through its ordinal minting, and a UUID's one semantic, which such a
 * store breaks where the reasoner copied a property's stated definition, vertex UUIDs and all,
 * into its inferred definition (the write path now derives what a semantic's own history does
 * not hold, so the copy takes UUIDs of its own).
 */
@Tag("starter-set")
class VertexIdentitySweepIT {

    static final String DERIVED_PROPERTY = "vertex.identity.derived";

    private static final String KB_FILE = "kb.file";
    private static final String SWEEP = "sweep.";

    @Test
    void theStarterSetKeepsItsVertexIdentities() throws IOException {
        Path work = Path.of("target", "vertex-identity-sweep").toAbsolutePath();
        SnomedRoundTripIT.deleteTree(work);
        Files.createDirectories(work);
        Properties in = new Properties();
        in.setProperty(StoreStage.PROVIDER, Provider.EPHEMERAL.name());
        in.setProperty(StoreStage.STORE, work.resolve("store").toString());
        in.setProperty(KB_FILE, StarterSetRoundTripIT.starterSet().toString());
        Properties result = ForkedJvm.run(LoadAndSweep.class, in, Duration.ofMinutes(20));

        List<Executable> checks = new ArrayList<>();
        checks.add(() -> assertTrue(number(result, "semantics") > 0, "the store holds logical definitions"));
        zero(checks, result, "crashes", "semantics whose sweep threw");
        zero(checks, result, "uuid.twice.in.a.tree", "trees holding a vertex UUID twice");
        zero(checks, result, "invariant.breaks", "kept UUIDs that changed meaning or an identifying property");
        zero(checks, result, "shape.breaks", "trees whose shape the identity table does not admit");
        boolean derived = Boolean.getBoolean(DERIVED_PROPERTY);
        if (derived) {
            zero(checks, result, "uuid.in.two.semantics", "vertex UUIDs in the trees of more than one semantic");
            zero(checks, result, "fixed.point.misses", "semantics whose replay gives other UUIDs than those stored");
        }
        LoggerFactory.getLogger(VertexIdentitySweepIT.class).info("Vertex identity sweep: {} semantics, {} versions,"
                        + " {} vertices; fixed point missed by {}, UUIDs in two semantics {} ({}asserted: {}={})",
                result.getProperty(SWEEP + "semantics"), result.getProperty(SWEEP + "versions"),
                result.getProperty(SWEEP + "vertices"), result.getProperty(SWEEP + "fixed.point.misses"),
                result.getProperty(SWEEP + "uuid.in.two.semantics"), derived ? "" : "not ", DERIVED_PROPERTY, derived);
        assertAll(checks);
    }

    private static void zero(List<Executable> checks, Properties result, String key, String meaning) {
        checks.add(() -> assertEquals(0, number(result, key),
                meaning + ": " + result.getProperty(SWEEP + key + ".examples")));
    }

    private static long number(Properties result, String key) {
        String value = result.getProperty(SWEEP + key);
        assertTrue(value != null, "No " + key + " was recorded");
        return Long.parseLong(value);
    }

    /** The one stage: the knowledge base loaded into a new store, then swept. */
    static class LoadAndSweep extends StoreStage {
        @Override
        void work(Properties in, Properties out) {
            new dev.ikm.tinkar.entity.load.LoadEntitiesFromProtobufFile(new File(in.getProperty(KB_FILE))).compute();
            sweep(out);
        }
    }

    // ----- the sweep -----

    static void sweep(Properties out) {
        Count semantics = new Count();
        Count versions = new Count();
        Count vertices = new Count();
        Count crashes = new Count();
        Count twice = new Count();
        Count twoSemantics = new Count();
        Count invariants = new Count();
        Count shapes = new Count();
        Count fixedPoint = new Count();
        Map<UUID, Long> owner = new HashMap<>();

        List<SemanticEntity<SemanticEntityVersion>> all = new ArrayList<>();
        EntityService.get().forEachSemanticEntity(all::add);
        for (SemanticEntity<SemanticEntityVersion> semantic : all) {
            if (!holdsLogicalTree(semantic)) {
                continue;
            }
            semantics.add();
            try {
                List<SemanticEntityVersion> ordered = inStampOrder(semantic);
                boolean shapeBroken = false;
                Map<Integer, List<DiTreeEntity>> history = new HashMap<>();
                for (SemanticEntityVersion version : ordered) {
                    versions.add();
                    for (int f = 0; f < version.fieldValues().size(); f++) {
                        if (!(version.fieldValues().get(f) instanceof DiTreeEntity tree) || !VertexIdentityWrite.logical(tree)) {
                            continue;
                        }
                        String where = semantic.publicId().idString() + " field " + f;
                        Set<UUID> inTree = new HashSet<>();
                        for (EntityVertex vertex : tree.vertexMap()) {
                            vertices.add();
                            UUID uuid = vertex.asUuidList().getFirst();
                            if (!inTree.add(uuid)) {
                                twice.add(where + " " + uuid);
                            }
                            Long before = owner.putIfAbsent(uuid, semantic.nid());
                            if (before != null && before != semantic.nid()) {
                                twoSemantics.add(uuid + " in " + described(semantic.nid()) + " and " + described(before));
                            }
                        }
                        List<DiTreeEntity> earlier = history.computeIfAbsent(f, key -> new ArrayList<>());
                        List<String> broken = VertexIdentityWrite.invariantBreaks(earlier, tree);
                        if (!broken.isEmpty()) {
                            invariants.add(where + ": " + broken.getFirst());
                        }
                        String shape = VertexIdentityWrite.shapeBreak(tree);
                        if (shape != null) {
                            shapes.add(where + ": " + shape);
                            shapeBroken = true;
                        }
                        earlier.add(tree);
                    }
                }
                // a shape the write path refuses cannot be replayed through it
                if (!shapeBroken && !fixedPoint(semantic)) {
                    fixedPoint.add(semantic.publicId().idString());
                }
            } catch (RuntimeException failure) {
                crashes.add(semantic.publicId().idString() + ": " + failure);
            }
        }
        semantics.store(out, SWEEP + "semantics");
        versions.store(out, SWEEP + "versions");
        vertices.store(out, SWEEP + "vertices");
        crashes.store(out, SWEEP + "crashes");
        twice.store(out, SWEEP + "uuid.twice.in.a.tree");
        twoSemantics.store(out, SWEEP + "uuid.in.two.semantics");
        invariants.store(out, SWEEP + "invariant.breaks");
        shapes.store(out, SWEEP + "shape.breaks");
        fixedPoint.store(out, SWEEP + "fixed.point.misses");
    }

    /** A semantic as an example names it: its pattern and the component it is about. */
    private static String described(long semanticNid) {
        SemanticEntity<?> semantic = dev.ikm.tinkar.entity.EntityHandle.get(semanticNid).expectSemantic();
        return dev.ikm.tinkar.common.service.PrimitiveData.text(semantic.patternNid()) + " of "
                + dev.ikm.tinkar.common.service.PrimitiveData.text(semantic.referencedComponentNid());
    }

    private static boolean holdsLogicalTree(SemanticEntity<SemanticEntityVersion> semantic) {
        for (SemanticEntityVersion version : semantic.versions()) {
            for (Object field : version.fieldValues()) {
                if (field instanceof DiTreeEntity tree && VertexIdentityWrite.logical(tree)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** The versions in the order the write path reads them: STAMP time, then least STAMP UUID. */
    private static List<SemanticEntityVersion> inStampOrder(SemanticEntity<SemanticEntityVersion> semantic) {
        List<SemanticEntityVersion> ordered = new ArrayList<>(semantic.versions().castToList());
        ordered.sort(Comparator.<SemanticEntityVersion>comparingLong(version -> version.stamp().time())
                .thenComparing(version -> version.stamp().publicId().leastUuid().toString()));
        return ordered;
    }

    /**
     * Whether the semantic's versions, stripped of their vertex UUIDs and written from nothing
     * through the write path's identity step, come back with the UUIDs stored, vertex by vertex.
     */
    private static boolean fixedPoint(SemanticEntity<SemanticEntityVersion> semantic) {
        if (!((Object) semantic instanceof SemanticRecord record)) {
            return true;
        }
        RecordListBuilder<SemanticVersionRecord> strippedVersions = RecordListBuilder.make();
        SemanticRecord stripped = new SemanticRecord(record.mostSignificantBits(), record.leastSignificantBits(),
                record.additionalUuidLongs(), record.nid(), record.patternNid(), record.referencedComponentNid(),
                strippedVersions);
        for (SemanticVersionRecord version : record.versions()) {
            List<Object> fields = new ArrayList<>();
            for (Object field : version.fieldValues()) {
                fields.add(field instanceof DiTreeEntity tree && VertexIdentityWrite.logical(tree) ? strip(tree) : field);
            }
            strippedVersions.add(new SemanticVersionRecord(stripped, version.stampNid(), Lists.immutable.withAll(fields)));
        }
        strippedVersions.build();
        SemanticEntity<?> replayed = VertexIdentityWrite.forWrite(stripped, () -> null, 0,
                Entity::getStamp, DataActivity.SYNCHRONIZABLE_EDIT);
        Map<Long, List<Object>> replayedByStamp = new HashMap<>();
        for (SemanticEntityVersion version : replayed.versions()) {
            replayedByStamp.put(version.stampNid(), version.fieldValues().castToList());
        }
        for (SemanticEntityVersion version : record.versions()) {
            List<Object> again = replayedByStamp.get(version.stampNid());
            for (int f = 0; f < version.fieldValues().size(); f++) {
                if (version.fieldValues().get(f) instanceof DiTreeEntity tree && VertexIdentityWrite.logical(tree)) {
                    DiTreeEntity replayedTree = (DiTreeEntity) again.get(f);
                    for (int i = 0; i < tree.vertexMap().size(); i++) {
                        if (!tree.vertex(i).asUuidList().getFirst().equals(replayedTree.vertex(i).asUuidList().getFirst())) {
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }

    /** The tree with a fresh random UUID on every vertex, at the same indexes in the same order. */
    private static DiTreeEntity strip(DiTreeEntity tree) {
        int size = tree.vertexMap().size();
        EntityVertex[] vertices = new EntityVertex[size];
        MutableIntObjectMap<ImmutableIntList> successors = IntObjectMaps.mutable.empty();
        MutableIntIntMap predecessors = IntIntMaps.mutable.empty();
        for (int i = 0; i < size; i++) {
            EntityVertex original = tree.vertex(i);
            EntityVertex vertex = EntityVertex.make(UUID.randomUUID(), original.getMeaningNid());
            MutableLongObjectMap<Object> properties = LongObjectMaps.mutable.empty();
            original.properties().forEachKeyValue(properties::put);
            vertex.setProperties(properties);
            vertex.setVertexIndex(i);
            vertices[i] = vertex;
            ImmutableIntList children = tree.successors(i);
            if (children != null && !children.isEmpty()) {
                successors.put(i, children);
                int parent = i;
                children.forEach(child -> predecessors.put(child, parent));
            }
        }
        return new DiTreeEntity(vertices[tree.root().vertexIndex()], Lists.immutable.of(vertices),
                successors.toImmutable(), predecessors.toImmutable());
    }
}
