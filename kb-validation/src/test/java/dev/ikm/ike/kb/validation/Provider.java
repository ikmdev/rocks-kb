package dev.ikm.ike.kb.validation;

import java.util.Arrays;
import java.util.List;

/** The store providers the knowledge base is validated against, by controller name. */
enum Provider {
    ROCKS("Open Rocks KB", true),
    SPINED_ARRAY("Open SpinedArrayStore", true),
    MV_STORE("Open MV Store", true),
    /** Held in memory only: a store lifetime ends with its JVM. */
    EPHEMERAL("Load Ephemeral Store", false);

    final String controllerName;
    /** Whether a store outlives the JVM that wrote it, so it can be closed and reopened. */
    final boolean persistent;

    Provider(String controllerName, boolean persistent) {
        this.controllerName = controllerName;
        this.persistent = persistent;
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
