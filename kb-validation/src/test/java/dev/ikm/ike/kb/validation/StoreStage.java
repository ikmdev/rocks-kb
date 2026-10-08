package dev.ikm.ike.kb.validation;

import dev.ikm.ds.rocks.RocksProvider;
import dev.ikm.tinkar.common.service.CachingService;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.service.ServiceKeys;
import dev.ikm.tinkar.common.service.ServiceProperties;
import dev.ikm.tinkar.fixtures.ForkedJvm;

import jdk.jfr.Configuration;
import jdk.jfr.Recording;

import java.io.File;
import java.nio.file.Path;
import java.util.Properties;

/**
 * A stage with a store: the provider named by {@value #PROVIDER} opened on the directory
 * named by {@link #storeProperty()} before the work, and closed after it; the properties
 * passed through. A directory that does not exist yet becomes a new, empty store.
 *
 * <p>With {@code -D}{@value #JFR_DIRECTORY}{@code =<directory>}, which {@link ForkedJvm}
 * forwards to the stage's JVM, the work runs under a Flight Recorder recording in the
 * {@code profile} configuration, written to {@code <directory>/<stage>-<provider>.jfr}:
 * {@code jfr print --events jdk.ExecutionSample} and {@code jfr view hot-methods} then say
 * where the time went.
 */
abstract class StoreStage implements ForkedJvm.Stage {

    static final String PROVIDER = "provider";
    static final String STORE = "store";
    /** The directory a stage records itself into under JFR, when the property is set. */
    static final String JFR_DIRECTORY = "tinkar.jfr.dir";

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
        if (provider.newStoreLayout == null) {
            System.clearProperty(RocksProvider.Controller.NEW_STORE_LAYOUT_PROPERTY);
        } else {
            System.setProperty(RocksProvider.Controller.NEW_STORE_LAYOUT_PROPERTY, provider.newStoreLayout);
        }
        configure(in);
        PrimitiveData.selectControllerByName(provider.controllerName);
        long start = System.currentTimeMillis();
        PrimitiveData.start();
        out.setProperty(getClass().getSimpleName() + ".open.millis", Long.toString(System.currentTimeMillis() - start));
        try {
            String jfrDirectory = System.getProperty(JFR_DIRECTORY);
            if (jfrDirectory == null || jfrDirectory.isBlank()) {
                work(in, out);
            } else {
                Path recordingFile = Path.of(jfrDirectory, getClass().getSimpleName() + "-" + provider.name().toLowerCase() + ".jfr");
                try (Recording recording = new Recording(Configuration.getConfiguration("profile"))) {
                    recording.setDestination(recordingFile);
                    recording.start();
                    try {
                        work(in, out);
                    } finally {
                        recording.stop();
                        out.setProperty(getClass().getSimpleName() + ".jfr", recordingFile.toString());
                    }
                }
            }
        } finally {
            PrimitiveData.stop();
        }
    }
}
