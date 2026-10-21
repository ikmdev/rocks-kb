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
 *
 * <p>On a store that keeps a data-use index, the deletes run with the read of every component
 * disabled ({@value ScopeOperations#FULL_PASS_PROPERTY}{@code =false}), so the index alone
 * answers them; a store without one reads every component, to the same outcome: a third scope listing a line of the document in a field refuses its delete
 * until that scope is deleted first, and the shared STAMP is kept. A persistent store does the
 * same for a second linked pair after it is opened again, from the index it read back.
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
        assertEquals("EPHEMERAL", written.getProperty("kind.line"), provider + ": a line of the document's own pattern");
        assertEquals("EPHEMERAL", written.getProperty("kind.comment"), provider + ": a comment in the shared comment pattern");
        assertEquals("1", written.getProperty("collected"), provider + ": A's own STAMP is collected");
        assertEquals("1", written.getProperty("kept"), provider + ": the STAMP A shares with B is kept");
        assertIndexed(provider, "as the store is written", written, "a");
        assertObserved(provider, "after the delete", written);
        if (provider.persistent) {
            Properties reopened = ForkedJvm.run(Reopen.class, written, Duration.ofMinutes(10));
            assertObserved(provider, "after the store is opened again", reopened);
            assertIndexed(provider, "after the store is opened again", reopened, "c");
            assertEquals("1", reopened.getProperty("c.collected"), provider + ": C's own STAMP is collected");
            assertEquals("1", reopened.getProperty("c.kept"), provider + ": the STAMP C shares with D is kept");
            assertEquals("true", reopened.getProperty("c.has.shared"), provider + ": the shared STAMP stays");
            assertEquals("true", reopened.getProperty("c.has.other"), provider + ": D is whole");
            if (Boolean.parseBoolean(reopened.getProperty("keepsIndex"))) {
                assertEquals("0", reopened.getProperty("rebuilds"), provider + ": the index was read back, not rebuilt");
            }
        }
    }

    private static void assertIndexed(Provider provider, String when, Properties out, String doc) {
        assertEquals(true, out.getProperty(doc + ".refused", "").contains("lead into it"),
                provider + " " + when + ": a field reference into " + doc + " refuses its delete: " + out.getProperty(doc + ".refused"));
        String keeps = out.getProperty("keepsIndex");
        assertEquals(keeps, out.getProperty(doc + ".link.indexed"), provider + " " + when + ": the index answered the link's delete where kept");
        assertEquals(keeps, out.getProperty(doc + ".indexed"), provider + " " + when + ": the index answered " + doc + "'s delete where kept");
        assertEquals("false", out.getProperty(doc + ".has.root"), provider + " " + when + ": " + doc + " is gone");
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
            fullPassOnlyWithoutAnIndex(out);
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

            // An element of the document's own pattern and one of the shared comment pattern both classify as ephemeral,
            // the first from its pattern (read from the nid where the layout carries it), the second from its own birth.
            out.setProperty("kind.line", dev.ikm.tinkar.entity.boundary.EntityBoundaries.kindOf(PrimitiveData.nid(a[2])).name());
            out.setProperty("kind.comment", dev.ikm.tinkar.entity.boundary.EntityBoundaries.kindOf(PrimitiveData.nid(a[4])).name());
            PublicId linkA = link(documents, a[2]);
            out.setProperty("a.pattern", a[0].asUuidArray()[0].toString());
            out.setProperty("a.link", linkA.asUuidArray()[0].toString());
            ScopeOperations.Deleted deleted = deleteLinked("a", out, out);
            out.setProperty("collected", Integer.toString(deleted.stamps()));
            out.setProperty("kept", Integer.toString(deleted.stampsKept()));
            observe(out, out);

            // A second pair, linked the same way, for the stage that opens the store again.
            PublicId[] c = new PublicId[6];
            PublicId[] d = new PublicId[6];
            try (StampedWriter writer = writer(documents, "scope delete documents C and D")) {
                document(writer, c, term, "zebracornRc");
                document(writer, d, term, "zebracornRd");
                writer.commit();
            }
            try (StampedWriter writer = writer(documents, "scope delete revision of C")) {
                writer.semantic(c[4], KernelPattern.COMMENT_PATTERN.publicId(), c[2], "zebracornRc revised");
                writer.commit();
            }
            out.setProperty("c.pattern", c[0].asUuidArray()[0].toString());
            out.setProperty("c.root", c[1].asUuidArray()[0].toString());
            out.setProperty("c.other", d[1].asUuidArray()[0].toString());
            out.setProperty("c.link", link(documents, c[2]).asUuidArray()[0].toString());
            long sharedC = 0;
            for (Object o : EntityHandle.get(PrimitiveData.nid(c[1])).expectEntity().versions()) {
                sharedC = ((EntityVersion) o).stampNid();
            }
            out.setProperty("c.shared", PrimitiveData.publicId(sharedC).asUuidArray()[0].toString());
        }

        /**
         * A scope whose element lists another document's line in a component-list field: a
         * reference into that document the referencing index does not hold.
         */
        PublicId link(ConceptFacade module, PublicId line) {
            PublicId pattern = PublicIds.newRandom();
            PublicId root = PublicIds.newRandom();
            try (StampedWriter writer = writer(module, "scope delete link")) {
                writer.pattern(pattern, KernelConcept.STRING, KernelConcept.STRING,
                        StampedWriter.field(KernelConcept.STRING, KernelConcept.STRING, KernelConcept.COMPONENT_ID_LIST_FIELD));
                EntityService.get().nidForSemantic(pattern, root);
                writer.semantic(root, pattern, pattern, dev.ikm.tinkar.common.id.LongIds.list.empty());
                writer.semantic(PublicIds.newRandom(), pattern, root,
                        dev.ikm.tinkar.common.id.LongIds.list.of(EntityService.get().nidForPublicId(line)));
                writer.commit();
            }
            return pattern;
        }

        StampedWriter writer(ConceptFacade module, String name) {
            return StampedWriter.open(name, State.ACTIVE, ++time, KernelConcept.USER, module, KernelConcept.DEVELOPMENT_PATH);
        }

        /** A document: its pattern, a root, a line, a line citing the term, and a comment on the line in the shared comment pattern. */
        static void document(StampedWriter writer, PublicId[] ids, ConceptFacade term, String text) {
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

        static void module(StampedWriter writer, ConceptFacade module, ConceptFacade parent) {
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
            fullPassOnlyWithoutAnIndex(out);
            observe(in, out);
            ScopeOperations.Deleted deleted = deleteLinked("c", in, out);
            out.setProperty("c.collected", Integer.toString(deleted.stamps()));
            out.setProperty("c.kept", Integer.toString(deleted.stampsKept()));
            out.setProperty("c.has.shared", Boolean.toString(PrimitiveData.get().hasPublicId(id(in, "c.shared"))));
            out.setProperty("c.has.other", Boolean.toString(PrimitiveData.get().hasPublicId(id(in, "c.other"))));
            if (PrimitiveData.get() instanceof dev.ikm.tinkar.common.service.ScopeErasure erasure) {
                erasure.dataUse().ifPresent(index ->
                        out.setProperty("rebuilds", Long.toString(((dev.ikm.tinkar.entity.scope.DataUseIndex) index).rebuilds())));
            }
        }
    }

    /** Leaves the full pass disabled only on a store that keeps a data-use index, and says which. */
    static void fullPassOnlyWithoutAnIndex(Properties out) {
        boolean keeps = PrimitiveData.get() instanceof dev.ikm.tinkar.common.service.ScopeErasure erasure && erasure.dataUse().isPresent();
        out.setProperty("keepsIndex", Boolean.toString(keeps));
        System.setProperty(ScopeOperations.FULL_PASS_PROPERTY, keeps ? "false" : "true");
    }

    /**
     * Tries to delete a document a link scope references into, which is refused; deletes the link
     * scope; then deletes the document.
     */
    static ScopeOperations.Deleted deleteLinked(String doc, Properties in, Properties out) {
        try {
            ScopeOperations.delete(Scope.of(PrimitiveData.nid(id(in, doc + ".pattern"))));
            out.setProperty(doc + ".refused", "not refused");
        } catch (IllegalStateException refused) {
            out.setProperty(doc + ".refused", refused.getMessage());
        }
        ScopeOperations.Deleted link = ScopeOperations.delete(Scope.of(PrimitiveData.nid(id(in, doc + ".link"))));
        out.setProperty(doc + ".link.indexed", Boolean.toString(link.indexed()));
        ScopeOperations.Deleted deleted = ScopeOperations.delete(Scope.of(PrimitiveData.nid(id(in, doc + ".pattern"))));
        out.setProperty(doc + ".indexed", Boolean.toString(deleted.indexed()));
        out.setProperty(doc + ".has.root", Boolean.toString(PrimitiveData.get().hasPublicId(id(in, doc + ".root"))));
        return deleted;
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
