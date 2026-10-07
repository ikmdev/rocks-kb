package dev.ikm.ike.kb.validation;

import org.eclipse.collections.api.factory.primitive.LongLongMaps;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;

import dev.ikm.elk.snomed.SnomedIds;
import dev.ikm.elk.snomed.SnomedIsa;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.util.uuid.UuidUtil;
import dev.ikm.tinkar.reasoner.service.ReasonerService;
import org.eclipse.collections.api.factory.primitive.IntLongMaps;
import org.eclipse.collections.api.factory.primitive.LongSets;
import org.eclipse.collections.api.factory.primitive.LongSets;
import org.eclipse.collections.api.map.primitive.MutableIntLongMap;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * SNOMED International's own classification at a release date, from the full inferred
 * relationship file, against the reasoner's: for every concept SNOMED classified, its
 * inferred parents in SNOMED and in the knowledge base. Concepts are matched by the UUID
 * the knowledge base derives from the SCTID.
 */
final class SnomedInferred {

    private static final String IS_A = Long.toString(SnomedIds.isa);
    private static final int TYPE_ID = 7;

    private SnomedInferred() {
    }

    /**
     * The IS-A rows of a full relationship file, active and inactive, with its header:
     * all {@link SnomedIsa} reads, at a fraction of the size, so each release date reads
     * the history quickly. A relationship's type never changes, so no history is lost.
     */
    static Path isaHistory(Path fullRelationshipFile, Path isaFile) throws IOException {
        if (Files.isRegularFile(isaFile) && Files.getLastModifiedTime(isaFile).compareTo(Files.getLastModifiedTime(fullRelationshipFile)) > 0) {
            return isaFile;
        }
        Path partial = isaFile.resolveSibling(isaFile.getFileName() + ".partial");
        try (Stream<String> lines = Files.lines(fullRelationshipFile, StandardCharsets.UTF_8);
             BufferedWriter out = Files.newBufferedWriter(partial, StandardCharsets.UTF_8)) {
            boolean[] header = {true};
            for (String line : (Iterable<String>) lines::iterator) {
                if (header[0] || IS_A.equals(field(line, TYPE_ID))) {
                    out.write(line);
                    out.newLine();
                }
                header[0] = false;
            }
        }
        return Files.move(partial, isaFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    private static String field(String line, int index) {
        int start = 0;
        for (int i = 0; i < index; i++) {
            start = line.indexOf('\t', start) + 1;
            if (start == 0) {
                return "";
            }
        }
        int end = line.indexOf('\t', start);
        return end < 0 ? line.substring(start) : line.substring(start, end);
    }

    /** The effective times, as RF2 dates, of the rows of a release file. */
    static java.util.Set<Integer> effectiveTimes(Path releaseFile) throws IOException {
        java.util.Set<Integer> dates = new java.util.TreeSet<>();
        try (Stream<String> lines = Files.lines(releaseFile, StandardCharsets.UTF_8)) {
            lines.skip(1).forEach(line -> dates.add(Integer.parseInt(field(line, 1))));
        }
        return dates;
    }

    /** The full inferred relationship file found under the directory. */
    static Path fullRelationshipFile(Path directory) throws IOException {
        try (Stream<Path> files = Files.walk(directory)) {
            return files.filter(file -> file.getFileName().toString().startsWith("sct2_Relationship_Full_"))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("No sct2_Relationship_Full_ file under " + directory
                            + "; run with -Psnomed"));
        }
    }

    /**
     * Compares and records, under the prefix: SNOMED's concepts; those the store does not
     * hold; those the reasoner did not classify; those whose parents agree and differ; and
     * the reasoner's concepts with no SNOMED counterpart at this date.
     */
    static void compare(ReasonerService reasoner, SnomedIsa snomed, Properties out, String prefix) {
        MutableLongSet classified = LongSets.mutable.withAll(reasoner.getReasonerConceptSet());
        MutableLongLongMap sctidOfNid = LongLongMaps.mutable.empty();
        long notInStore = 0;
        for (long sctid : snomed.getOrderedConcepts().toArray()) {
            UUID uuid = UuidUtil.fromSNOMED(Long.toString(sctid));
            if (PrimitiveData.get().hasUuid(uuid)) {
                sctidOfNid.put(PrimitiveData.nid(uuid), sctid);
            } else {
                notInStore++;
            }
        }
        long notClassified = 0;
        long agree = 0;
        long differ = 0;
        List<String> examples = new ArrayList<>();
        for (long sctid : snomed.getOrderedConcepts().toArray()) {
            if (sctid == SnomedIds.root) {
                continue; // SNOMED's root has no parent; the knowledge base places it under its own root
            }
            UUID uuid = UuidUtil.fromSNOMED(Long.toString(sctid));
            if (!PrimitiveData.get().hasUuid(uuid)) {
                continue;
            }
            long nid = PrimitiveData.nid(uuid);
            if (!classified.contains(nid)) {
                notClassified++;
                continue;
            }
            MutableLongSet ours = LongSets.mutable.empty();
            reasoner.getParents(nid).forEach(parent -> ours.add(sctidOfNid.containsKey(parent) ? sctidOfNid.get(parent) : -parent));
            if (ours.equals(snomed.getParents(sctid))) {
                agree++;
            } else {
                differ++;
                if (examples.size() < 10) {
                    MutableLongSet onlySnomed = LongSets.mutable.withAll(snomed.getParents(sctid));
                    onlySnomed.removeAll(ours);
                    MutableLongSet onlyOurs = LongSets.mutable.withAll(ours);
                    onlyOurs.removeAll(snomed.getParents(sctid));
                    examples.add(sctid + " " + PrimitiveData.text(nid) + ": only SNOMED " + onlySnomed + ", only reasoner " + onlyOurs);
                }
            }
        }
        long withoutCounterpart = classified.count(nid -> !sctidOfNid.containsKey(nid));
        out.setProperty(prefix + "snomed.concepts", Integer.toString(snomed.getOrderedConcepts().size()));
        out.setProperty(prefix + "snomed.not.in.store", Long.toString(notInStore));
        out.setProperty(prefix + "snomed.not.classified", Long.toString(notClassified));
        out.setProperty(prefix + "snomed.parents.agree", Long.toString(agree));
        out.setProperty(prefix + "snomed.parents.differ", Long.toString(differ));
        out.setProperty(prefix + "classified.without.snomed.counterpart", Long.toString(withoutCounterpart));
        out.setProperty("snomed.differences.examples", String.join("\n", examples));
    }
}
