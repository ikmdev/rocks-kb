package dev.ikm.ike.kb.validation;

import dev.ikm.ds.rocks.RocksProvider;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Makes the Rocks controllers create a new store with the legacy engine for one test class:
 * sets {@link RocksProvider.Controller#NEW_STORE_LAYOUT_PROPERTY} before the provider opens and
 * clears it after. Declare it before {@code @WithKeyValueProvider}, so it runs first.
 */
public final class LegacyStoreLayout implements BeforeAllCallback, AfterAllCallback {

    @Override
    public void beforeAll(ExtensionContext context) {
        System.setProperty(RocksProvider.Controller.NEW_STORE_LAYOUT_PROPERTY, "8-bit");
    }

    @Override
    public void afterAll(ExtensionContext context) {
        System.clearProperty(RocksProvider.Controller.NEW_STORE_LAYOUT_PROPERTY);
    }
}
