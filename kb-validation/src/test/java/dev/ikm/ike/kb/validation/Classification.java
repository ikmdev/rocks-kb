package dev.ikm.ike.kb.validation;

import dev.ikm.tinkar.common.service.PluggableService;
import dev.ikm.tinkar.common.service.TrackingCallable;
import dev.ikm.tinkar.coordinate.view.calculator.ViewCalculator;
import dev.ikm.tinkar.entity.graph.adaptor.axiom.LogicalExpression;
import dev.ikm.tinkar.fixtures.StoreDigest;
import dev.ikm.tinkar.reasoner.service.ReasonerService;
import dev.ikm.tinkar.terms.TinkarTerm;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What the reasoner inferred, summarized so that two classifications of the same content
 * compare equal in any store: counts, and a hash of every classified concept's inferred
 * parents, equivalents and necessary normal form, each named by public id.
 */
record Classification(long concepts, long parentEdges, long inEquivalenceSets, String hash) {

    /** Classifies under the view, through the necessary normal form; writes nothing. */
    static ReasonerService classify(ViewCalculator view, Properties out, String prefix) throws Exception {
        ReasonerService reasoner = PluggableService.load(ReasonerService.class).stream()
                .map(ServiceLoader.Provider::get)
                .findFirst().orElseThrow(() -> new IllegalStateException("No ReasonerService is provided"));
        out.setProperty(prefix + "reasoner", reasoner.getName());
        long start = System.currentTimeMillis();
        reasoner.init(view, TinkarTerm.EL_PLUS_PLUS_STATED_AXIOMS_PATTERN, TinkarTerm.EL_PLUS_PLUS_INFERRED_AXIOMS_PATTERN);
        reasoner.extractData(quiet());
        reasoner.loadData(quiet());
        out.setProperty(prefix + "load.millis", Long.toString(System.currentTimeMillis() - start));
        start = System.currentTimeMillis();
        reasoner.computeInferences();
        out.setProperty(prefix + "compute.millis", Long.toString(System.currentTimeMillis() - start));
        start = System.currentTimeMillis();
        reasoner.buildNecessaryNormalForm();
        out.setProperty(prefix + "nnf.millis", Long.toString(System.currentTimeMillis() - start));
        return reasoner;
    }

    static Classification of(ReasonerService reasoner) throws NoSuchAlgorithmException {
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        long[] hash = new long[2];
        AtomicLong parentEdges = new AtomicLong();
        AtomicLong inEquivalenceSets = new AtomicLong();
        reasoner.getReasonerConceptSet().forEach(nid -> {
            TreeSet<String> parents = new TreeSet<>();
            reasoner.getParents(nid).forEach(parent -> parents.add(StoreDigest.ids(parent)));
            TreeSet<String> equivalent = new TreeSet<>();
            reasoner.getEquivalent(nid).forEach(other -> equivalent.add(StoreDigest.ids(other)));
            parentEdges.addAndGet(parents.size());
            if (equivalent.size() > 1) {
                inEquivalenceSets.incrementAndGet();
            }
            LogicalExpression normalForm = reasoner.getNecessaryNormalForm(nid);
            String text = StoreDigest.ids(nid) + " parents " + parents + " equivalent " + equivalent
                    + " form " + (normalForm == null ? "none" : StoreDigest.render(normalForm.sourceGraph()));
            ByteBuffer buffer = ByteBuffer.wrap(sha256.digest(text.getBytes(StandardCharsets.UTF_8)));
            hash[0] += buffer.getLong();
            hash[1] += buffer.getLong();
        });
        return new Classification(reasoner.getReasonerConceptSet().size(), parentEdges.get(), inEquivalenceSets.get(),
                "%016x%016x".formatted(hash[0], hash[1]));
    }

    void store(Properties out, String prefix) {
        out.setProperty(prefix + "classified.concepts", Long.toString(concepts));
        out.setProperty(prefix + "inferred.parent.edges", Long.toString(parentEdges));
        out.setProperty(prefix + "concepts.in.equivalence.sets", Long.toString(inEquivalenceSets));
        out.setProperty(prefix + "inferred.hash", hash);
    }

    static TrackingCallable<Object> quiet() {
        return new TrackingCallable<>() {
            @Override
            protected Object compute() {
                return null;
            }
        };
    }
}
