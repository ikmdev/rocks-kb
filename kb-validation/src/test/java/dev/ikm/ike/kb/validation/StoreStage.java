package dev.ikm.ike.kb.validation;

import dev.ikm.tinkar.common.service.CachingService;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.service.ServiceKeys;
import dev.ikm.tinkar.common.service.ServiceProperties;
import dev.ikm.tinkar.fixtures.ForkedJvm;

import java.io.File;
import java.util.Properties;

/**
 * A stage with a store: the provider named by {@value #PROVIDER} opened on the directory
 * named by {@link #storeProperty()} before the work, and closed after it; the properties
 * passed through. A directory that does not exist yet becomes a new, empty store.
 */
abstract class StoreStage implements ForkedJvm.Stage {

    static final String PROVIDER = "provider";
    static final String STORE = "store";

    String storeProperty() {
        return STORE;
    }

    abstract void work(Properties in, Properties out) throws Exception;

    /** Sets the service options this stage runs with, after the caches are cleared and before the store opens. */
    void configure(Properties in) {
    }

    @Override
    public final void run(Properties in, Properties out) throws Exception {
        out.putAll(in);
        Provider provider = Provider.valueOf(in.getProperty(PROVIDER));
        File store = new File(in.getProperty(storeProperty()));
        if (!store.isDirectory() && !store.mkdirs()) {
            throw new IllegalStateException("Could not create " + store);
        }
        CachingService.clearAll();
        ServiceProperties.set(ServiceKeys.DATA_STORE_ROOT, store);
        configure(in);
        PrimitiveData.selectControllerByName(provider.controllerName);
        long start = System.currentTimeMillis();
        PrimitiveData.start();
        out.setProperty(getClass().getSimpleName() + ".open.millis", Long.toString(System.currentTimeMillis() - start));
        try {
            work(in, out);
        } finally {
            PrimitiveData.stop();
        }
    }
}
