package dev.ikm.ike.kb.validation;

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
}
