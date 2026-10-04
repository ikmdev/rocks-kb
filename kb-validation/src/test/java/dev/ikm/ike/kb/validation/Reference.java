package dev.ikm.ike.kb.validation;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A recorded reference: what a run observed is written beside its work, then compared,
 * value by value, with the reference resource of the same name. Recording a new
 * reference is the deliberate act of copying the observed file over the resource, after
 * looking at why it changed.
 */
final class Reference {

    private Reference() {
    }

    static void check(SortedMap<String, String> observed, String referenceName, Path workDirectory) throws IOException {
        check(observed, referenceName, workDirectory, false);
    }

    /**
     * As {@link #check(SortedMap, String, Path)}; a partial run, one that observed only some
     * of what the reference records (a limited selection, for development), is compared
     * key by key on what it observed.
     */
    static void check(SortedMap<String, String> observed, String referenceName, Path workDirectory, boolean partial)
            throws IOException {
        Path observedFile = workDirectory.resolve("observed-" + referenceName);
        Properties toWrite = new Properties();
        toWrite.putAll(observed);
        Files.createDirectories(workDirectory);
        try (OutputStream out = Files.newOutputStream(observedFile)) {
            toWrite.store(out, "Observed by this run. Copy over src/test/resources/dev/ikm/ike/kb/validation/"
                    + referenceName + " to record it as the reference.");
        }
        Properties reference = new Properties();
        try (InputStream stream = Reference.class.getResourceAsStream(referenceName)) {
            if (stream == null) {
                fail("There is no reference (" + referenceName + "). This run observed " + observed.size()
                        + " values, written to " + observedFile + "; record them as the reference.");
            }
            reference.load(stream);
        }
        List<String> differences = new ArrayList<>();
        TreeSet<String> keys = new TreeSet<>(observed.keySet());
        if (!partial) {
            keys.addAll(reference.stringPropertyNames());
        }
        for (String key : keys) {
            String expected = reference.getProperty(key);
            String actual = observed.get(key);
            if (expected == null || !expected.equals(actual)) {
                differences.add(key + ": reference " + expected + ", observed " + actual);
            }
        }
        assertEquals(List.of(), differences, "Differences from the recorded reference " + referenceName
                + " (observed values are in " + observedFile + ")");
    }

    /** The entries whose key starts with the prefix, the prefix removed. */
    static void copyPrefixed(Properties from, String prefix, Map<String, String> to) {
        for (String key : from.stringPropertyNames()) {
            if (key.startsWith(prefix)) {
                to.put(key.substring(prefix.length()), from.getProperty(key));
            }
        }
    }
}
