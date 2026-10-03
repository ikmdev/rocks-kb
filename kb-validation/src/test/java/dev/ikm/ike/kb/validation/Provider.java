package dev.ikm.ike.kb.validation;

/** The store providers the knowledge base is validated against, by controller name. */
enum Provider {
    ROCKS("Open Rocks KB"),
    SPINED_ARRAY("Open SpinedArrayStore");

    final String controllerName;

    Provider(String controllerName) {
        this.controllerName = controllerName;
    }
}
