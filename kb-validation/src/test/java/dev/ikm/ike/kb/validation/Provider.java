package dev.ikm.ike.kb.validation;

import java.util.Arrays;
import java.util.List;

/** The store providers the knowledge base is validated against, by controller name. */
enum Provider {
    /** The Rocks provider; a new store is a 64-bit one (IKE-Network/ike-issues#1258). */
    ROCKS("Open Rocks KB", true, null),
    /**
     * The Rocks provider creating its store with the legacy engine, in the 8-bit layout. (The
     * 6-bit layout is not a provider here: it holds at most 62 patterns, fewer than the starter
     * set has, so every provider-parameterized test would fail on it; DexImportIT asks for it
     * by itself.)
     */
    ROCKS_LEGACY("Open Rocks KB", true, "8-bit"),
    SPINED_ARRAY("Open SpinedArrayStore", true, null),
    MV_STORE("Open MV Store", true, null),
    /** The spined array in its ephemeral mode: held in memory only, a store lifetime ends with its JVM. */
    EPHEMERAL("Load Ephemeral Store", false, null);

    final String controllerName;
    /** Whether a store outlives the JVM that wrote it, so it can be closed and reopened. */
    final boolean persistent;
    /** The layout a new store is created in, through {@code rocks.newStoreLayout}; null for the engine's default. */
    final String newStoreLayout;

    Provider(String controllerName, boolean persistent, String newStoreLayout) {
        this.controllerName = controllerName;
        this.persistent = persistent;
        this.newStoreLayout = newStoreLayout;
    }

    /**
     * The system property naming the providers a parameterized store test runs on,
     * comma-separated and case-insensitive, e.g. {@code -Dstore.providers=spined_array,rocks};
     * every provider when it is unset.
     */
    static final String SELECTION = "store.providers";

    /** The providers {@value #SELECTION} selects, in declaration order; every provider when it is unset. */
    static List<Provider> selected() {
        String selection = System.getProperty(SELECTION);
        if (selection == null || selection.isBlank()) {
            return List.of(values());
        }
        List<Provider> selected = Arrays.stream(selection.split(","))
                .map(String::strip).filter(name -> !name.isEmpty())
                .map(name -> valueOf(name.toUpperCase())).toList();
        return Arrays.stream(values()).filter(selected::contains).toList();
    }
}
