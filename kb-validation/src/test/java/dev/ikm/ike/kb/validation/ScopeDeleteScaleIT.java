package dev.ikm.ike.kb.validation;

import dev.ikm.tinkar.common.id.PublicId;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.service.ScopeErasure;
import dev.ikm.tinkar.entity.scope.DataUseIndex;
import dev.ikm.tinkar.entity.scope.Scope;
import dev.ikm.tinkar.entity.scope.ScopeOperations;
import dev.ikm.tinkar.entity.transaction.StampedWriter;
import dev.ikm.tinkar.fixtures.ForkedJvm;
import dev.ikm.tinkar.terms.ConceptFacade;
import dev.ikm.tinkar.terms.EntityProxy;
import dev.ikm.tinkar.terms.KernelConcept;
import dev.ikm.tinkar.terms.KernelPattern;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * How long a scope delete takes in a large store, answered by the data-use index and by the read
 * of every component it replaces (IKE-Network/ike-issues#1313; design
 * {@code design-2026-10-20-instance-data}, "The data-use index"). Opt-in: it runs only with
 * {@code -Dscope.scale.store=<a copy of a store>}, which it writes to, and
 * {@code -Dscope.scale.provider} (default {@code ROCKS}). Never point it at a store under
 * {@code ~/Solor}: copy one first.
 *
 * <p>The first stage asks the index (built by one pass when the store has none), writes two
 * linked pairs of ephemeral documents, deletes one pair through the index and the other by
 * reading every component; the second opens the store again, so the index is read from its file,
 * and deletes a third pair through it. Each delete is timed alone.
 */
class ScopeDeleteScaleIT {

    static final String STORE_PROPERTY = "scope.scale.store";
    static final String PROVIDER_PROPERTY = "scope.scale.provider";

    @Test
    void aDeleteThroughTheIndexDoesNotReadTheStore() {
        String store = System.getProperty(STORE_PROPERTY);
        assumeTrue(store != null && !store.isBlank(), "opt-in: -D" + STORE_PROPERTY + "=<a copy of a store>");
        assumeTrue(!store.startsWith(System.getProperty("user.home") + "/Solor"), "never a store under ~/Solor; copy it first");
        Properties in = new Properties();
        in.setProperty(StoreStage.PROVIDER, System.getProperty(PROVIDER_PROPERTY, Provider.ROCKS.name()));
        in.setProperty(StoreStage.STORE, store);

        Properties first = ForkedJvm.runWithoutAgents(Measure.class, in, Duration.ofHours(2));
        first.setProperty("round", "2");
        Properties second = ForkedJvm.runWithoutAgents(Measure.class, first, Duration.ofHours(2));

        System.out.println(report(first, "1") + report(second, "2"));
        for (String round : new String[] {"1", "2"}) {
            Properties out = round.equals("1") ? first : second;
            assertTrue(out.getProperty(round + ".x.refused").contains("lead into it"), "the field reference refuses the delete");
            assertEquals("true", out.getProperty(round + ".x.indexed"));
            assertEquals("1", out.getProperty(round + ".x.kept"), "the shared STAMP is kept");
        }
        assertEquals("false", first.getProperty("1.y.indexed"), "the comparison delete read every component");
        assertEquals("1", first.getProperty("1.y.kept"), "and kept the shared STAMP too");
    }

    private static String report(Properties out, String round) {
        StringBuilder b = new StringBuilder("\nScope delete in ").append(out.getProperty(StoreStage.STORE))
                .append(", round ").append(round).append(":\n");
        for (String key : new String[] {"open", "current", "rebuilds", "x.refused.millis", "x.link.millis", "x.millis",
                "y.link.millis", "y.millis", "index"}) {
            String value = key.equals("open") ? out.getProperty("Measure.open.millis") : out.getProperty(round + "." + key);
            if (value != null) {
                b.append(String.format("  %-18s %s%n", key, value));
            }
        }
        return b.toString();
    }

    /** One round of writes and timed deletes on the store. */
    static class Measure extends StoreStage {
        @Override
        void work(Properties in, Properties out) throws Exception {
            String round = in.getProperty("round", "1");
            DataUseIndex index = (DataUseIndex) ((ScopeErasure) PrimitiveData.get()).dataUse().orElseThrow();
            long start = System.nanoTime();
            assertTrue(index.current());
            out.setProperty(round + ".current", millis(start));
            out.setProperty(round + ".rebuilds", Long.toString(index.rebuilds()));

            ScopeDeleteIT.Write writer = new ScopeDeleteIT.Write();
            ConceptFacade documents = EntityProxy.Concept.make(PublicIds.newRandom());
            ConceptFacade term = EntityProxy.Concept.make(PublicIds.newRandom());
            try (StampedWriter w = writer.writer(KernelConcept.SOLOR_MODULE, "scope scale knowledge")) {
                ScopeDeleteIT.Write.module(w, KernelConcept.EPHEMERAL_MODULE, KernelConcept.SOLOR_MODULE);
                ScopeDeleteIT.Write.module(w, documents, KernelConcept.EPHEMERAL_MODULE);
                w.concept(term.publicId());
                w.fullyQualifiedName(term.publicId(), "Scope scale term " + round);
                w.commit();
            }
            PublicId[] x = pair(writer, documents, term, "x" + round);
            PublicId xLink = writer.link(documents, x[2]);
            PublicId[] y = round.equals("1") ? pair(writer, documents, term, "y" + round) : null;
            PublicId yLink = y == null ? null : writer.link(documents, y[2]);

            System.setProperty(ScopeOperations.FULL_PASS_PROPERTY, "false");
            start = System.nanoTime();
            try {
                ScopeOperations.delete(Scope.of(PrimitiveData.nid(x[0])));
                out.setProperty(round + ".x.refused", "not refused");
            } catch (IllegalStateException refused) {
                out.setProperty(round + ".x.refused", refused.getMessage());
            }
            out.setProperty(round + ".x.refused.millis", millis(start));
            start = System.nanoTime();
            ScopeOperations.delete(Scope.of(PrimitiveData.nid(xLink)));
            out.setProperty(round + ".x.link.millis", millis(start));
            start = System.nanoTime();
            ScopeOperations.Deleted deleted = ScopeOperations.delete(Scope.of(PrimitiveData.nid(x[0])));
            out.setProperty(round + ".x.millis", millis(start));
            out.setProperty(round + ".x.indexed", Boolean.toString(deleted.indexed()));
            out.setProperty(round + ".x.kept", Integer.toString(deleted.stampsKept()));

            if (y != null) {
                System.setProperty(ScopeOperations.FULL_PASS_PROPERTY, "always");
                start = System.nanoTime();
                ScopeOperations.delete(Scope.of(PrimitiveData.nid(yLink)));
                out.setProperty(round + ".y.link.millis", millis(start));
                start = System.nanoTime();
                deleted = ScopeOperations.delete(Scope.of(PrimitiveData.nid(y[0])));
                out.setProperty(round + ".y.millis", millis(start));
                out.setProperty(round + ".y.indexed", Boolean.toString(deleted.indexed()));
                out.setProperty(round + ".y.kept", Integer.toString(deleted.stampsKept()));
            }
            out.setProperty(round + ".index", index.summary());
        }

        /** Two documents under one STAMP, the first revised under a STAMP of its own; returns the first. */
        private static PublicId[] pair(ScopeDeleteIT.Write writer, ConceptFacade documents, ConceptFacade term, String name) {
            PublicId[] a = new PublicId[5];
            PublicId[] b = new PublicId[5];
            try (StampedWriter w = writer.writer(documents, "scope scale documents " + name)) {
                ScopeDeleteIT.Write.document(w, a, term, "zebracorn" + name + "a");
                ScopeDeleteIT.Write.document(w, b, term, "zebracorn" + name + "b");
                w.commit();
            }
            try (StampedWriter w = writer.writer(documents, "scope scale revision " + name)) {
                w.semantic(a[4], KernelPattern.COMMENT_PATTERN.publicId(), a[2], "zebracorn" + name + " revised");
                w.commit();
            }
            return a;
        }

        private static String millis(long start) {
            return Long.toString((System.nanoTime() - start) / 1_000_000);
        }
    }
}
