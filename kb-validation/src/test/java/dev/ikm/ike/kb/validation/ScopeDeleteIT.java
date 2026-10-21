package dev.ikm.ike.kb.validation;

import dev.ikm.tinkar.common.id.PublicId;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.service.PrimitiveDataSearchResult;
import dev.ikm.tinkar.common.service.internal.EntityStore;
import dev.ikm.tinkar.entity.EntityHandle;
import dev.ikm.tinkar.entity.EntityService;
import dev.ikm.tinkar.entity.EntityVersion;
import dev.ikm.tinkar.entity.graph.DiTreeEntity;
import dev.ikm.tinkar.entity.graph.adaptor.axiom.LogicalAxiom;
import dev.ikm.tinkar.entity.graph.adaptor.axiom.LogicalExpressionBuilder;
import dev.ikm.tinkar.entity.load.LoadEntitiesFromProtobufFile;
import dev.ikm.tinkar.entity.scope.Scope;
import dev.ikm.tinkar.entity.scope.ScopeOperations;
import dev.ikm.tinkar.entity.transaction.StampedWriter;
import dev.ikm.tinkar.fixtures.ForkedJvm;
import dev.ikm.tinkar.terms.ConceptFacade;
import dev.ikm.tinkar.terms.EntityProxy;
import dev.ikm.tinkar.terms.KernelConcept;
import dev.ikm.tinkar.terms.KernelPattern;
import dev.ikm.tinkar.terms.State;
import org.eclipse.collections.api.factory.primitive.LongLists;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A scope of ephemeral data deleted on every store provider (IKE-Network/ike-issues#1313): two
 * documents written over the starter set, one revised under a STAMP of its own, and its scope
 * deleted. The deleted document's components, public ids, citations and search text are gone,
 * its own STAMP is collected and the shared one kept, and the other document is whole; on a
 * persistent store all of it holds after the store is closed and opened again.
 */
@Tag("starter-set")
class ScopeDeleteIT {

    @ParameterizedTest
    @EnumSource(Provider.class)
    void aDeletedScopeIsGoneAndStaysGone(Provider provider) throws IOException {
        Path work = Path.of("target", "scope-delete", provider.name().toLowerCase()).toAbsolutePath();
        SnomedRoundTripIT.deleteTree(work);
        Files.createDirectories(work);
        Properties in = new Properties();
        in.setProperty(StoreStage.PROVIDER, provider.name());
        in.setProperty(StoreStage.STORE, work.resolve("store").toString());
        in.setProperty(StarterSetProbeIT.KB_FILE, StarterSetRoundTripIT.starterSet().toString());

        Properties written = ForkedJvm.run(Write.class, in, Duration.ofMinutes(10));
        assertEquals("1", written.getProperty("collected"), provider + ": A's own STAMP is collected");
        assertEquals("1", written.getProperty("kept"), provider + ": the STAMP A shares with B is kept");
        assertObserved(provider, "after the delete", written);
        if (provider.persistent) {
            assertObserved(provider, "after the store is opened again", ForkedJvm.run(Reopen.class, written, Duration.ofMinutes(10)));
        }
    }

    private static void assertObserved(Provider provider, String when, Properties out) {
        for (String part : new String[] {"pattern", "root", "line", "cited", "comment", "revision"}) {
            assertEquals("false", out.getProperty("has.a." + part), provider + " " + when + ": A's " + part + " is gone");
        }
        assertEquals("true", out.getProperty("has.b.root"), provider + " " + when + ": B is whole");
        assertEquals("true", out.getProperty("has.b.comment"), provider + " " + when);
        assertEquals("true", out.getProperty("has.shared"), provider + " " + when + ": the shared STAMP stays");
        assertEquals("false", out.getProperty("term.cites.a"), provider + " " + when + ": the term no longer lists A's line");
        assertEquals("true", out.getProperty("term.cites.b"), provider + " " + when);
        assertEquals("false", out.getProperty("search.a"), provider + " " + when + ": A's text is out of the search index");
    }

    /** Loads the starter set, writes two documents, revises one, and deletes its scope. */
    static class Write extends StoreStage {
        private long time = 1_760_000_000_000L;

        @Override
        void work(Properties in, Properties out) throws Exception {
            new LoadEntitiesFromProtobufFile(new File(in.getProperty(StarterSetProbeIT.KB_FILE))).compute();
            ConceptFacade documents = EntityProxy.Concept.make(PublicIds.newRandom());
            ConceptFacade term = EntityProxy.Concept.make(PublicIds.newRandom());
            try (StampedWriter writer = writer(KernelConcept.SOLOR_MODULE, "scope delete knowledge")) {
                module(writer, KernelConcept.EPHEMERAL_MODULE, KernelConcept.SOLOR_MODULE);
                module(writer, documents, KernelConcept.EPHEMERAL_MODULE);
                writer.concept(term.publicId());
                writer.fullyQualifiedName(term.publicId(), "Scope delete term");
                writer.commit();
            }
            PublicId[] a = new PublicId[6];
            PublicId[] b = new PublicId[6];
            try (StampedWriter writer = writer(documents, "scope delete documents")) {
                document(writer, a, term, "zebracornRa");
                document(writer, b, term, "zebracornRb");
                writer.commit();
            }
            try (StampedWriter writer = writer(documents, "scope delete revision")) {
                writer.semantic(a[4], KernelPattern.COMMENT_PATTERN.publicId(), a[2], "zebracornRa revised");
                writer.commit();
            }
            EntityVersion newest = null;
            EntityVersion oldest = null;
            for (Object o : EntityHandle.get(PrimitiveData.nid(a[4])).expectEntity().versions()) {
                EntityVersion v = (EntityVersion) o;
                newest = newest == null || v.time() > newest.time() ? v : newest;
                oldest = oldest == null || v.time() < oldest.time() ? v : oldest;
            }
            a[5] = PrimitiveData.publicId(newest.stampNid());
            String[] parts = {"pattern", "root", "line", "cited", "comment", "revision"};
            for (int i = 0; i < parts.length; i++) {
                out.setProperty("a." + parts[i], a[i].asUuidArray()[0].toString());
            }
            out.setProperty("b.root", b[1].asUuidArray()[0].toString());
            out.setProperty("b.comment", b[4].asUuidArray()[0].toString());
            out.setProperty("b.cited", b[3].asUuidArray()[0].toString());
            out.setProperty("shared", PrimitiveData.publicId(oldest.stampNid()).asUuidArray()[0].toString());
            out.setProperty("term", term.publicId().asUuidArray()[0].toString());
            out.setProperty("a.cited.nid", Long.toString(PrimitiveData.nid(a[3])));
            out.setProperty("a.comment.nid", Long.toString(PrimitiveData.nid(a[4])));

            ScopeOperations.Deleted deleted = ScopeOperations.delete(Scope.of(PrimitiveData.nid(a[0])));
            out.setProperty("collected", Integer.toString(deleted.stamps()));
            out.setProperty("kept", Integer.toString(deleted.stampsKept()));
            observe(out, out);
        }

        private StampedWriter writer(ConceptFacade module, String name) {
            return StampedWriter.open(name, State.ACTIVE, ++time, KernelConcept.USER, module, KernelConcept.DEVELOPMENT_PATH);
        }

        /** A document: its pattern, a root, a line, a line citing the term, and a comment on the line in the shared comment pattern. */
        private static void document(StampedWriter writer, PublicId[] ids, ConceptFacade term, String text) {
            for (int i = 0; i < 5; i++) {
                ids[i] = PublicIds.newRandom();
            }
            writer.pattern(ids[0], KernelConcept.STRING, KernelConcept.STRING,
                    StampedWriter.field(KernelConcept.STRING, KernelConcept.STRING, KernelConcept.STRING));
            writer.fullyQualifiedName(ids[0], "Document " + text);
            EntityService.get().nidForSemantic(ids[0], ids[1]);
            writer.semantic(ids[1], ids[0], ids[0], "root");
            writer.semantic(ids[2], ids[0], ids[1], "a line");
            writer.semantic(ids[3], ids[0], term.publicId(), "a line citing the term");
            writer.semantic(ids[4], KernelPattern.COMMENT_PATTERN.publicId(), ids[2], text + " annotation");
        }

        private static void module(StampedWriter writer, ConceptFacade module, ConceptFacade parent) {
            writer.concept(module.publicId());
            LogicalExpressionBuilder leb = new LogicalExpressionBuilder();
            leb.NecessarySet(leb.And(new LogicalAxiom.Atom[] {leb.ConceptAxiom(parent)}));
            writer.semantic(PublicIds.newRandom(), KernelPattern.EL_PLUS_PLUS_STATED_AXIOMS_PATTERN.publicId(), module.publicId(),
                    (DiTreeEntity) leb.build().sourceGraph());
        }
    }

    /** Opens the store again and reports what it holds. */
    static class Reopen extends StoreStage {
        @Override
        void work(Properties in, Properties out) throws Exception {
            observe(in, out);
        }
    }

    /** What the store holds of the two documents, from the ids a stage recorded. */
    static void observe(Properties in, Properties out) throws Exception {
        for (String part : new String[] {"pattern", "root", "line", "cited", "comment", "revision"}) {
            out.setProperty("has.a." + part, Boolean.toString(PrimitiveData.get().hasPublicId(id(in, "a." + part))));
        }
        out.setProperty("has.b.root", Boolean.toString(PrimitiveData.get().hasPublicId(id(in, "b.root"))));
        out.setProperty("has.b.comment", Boolean.toString(PrimitiveData.get().hasPublicId(id(in, "b.comment"))));
        out.setProperty("has.shared", Boolean.toString(PrimitiveData.get().hasPublicId(id(in, "shared"))));
        long[] citing = EntityStore.current().semanticNidsForComponent(PrimitiveData.nid(id(in, "term")));
        out.setProperty("term.cites.a", Boolean.toString(LongLists.immutable.of(citing).contains(Long.parseLong(in.getProperty("a.cited.nid")))));
        out.setProperty("term.cites.b", Boolean.toString(LongLists.immutable.of(citing).contains(PrimitiveData.nid(id(in, "b.cited")))));
        long aComment = Long.parseLong(in.getProperty("a.comment.nid"));
        PrimitiveDataSearchResult[] results = PrimitiveData.get().search("zebracornRa", 100);
        out.setProperty("search.a", Boolean.toString(results != null && Arrays.stream(results).anyMatch(r -> r.nid() == aComment)));
    }

    private static PublicId id(Properties in, String key) {
        return PublicIds.of(UUID.fromString(in.getProperty(key)));
    }
}
