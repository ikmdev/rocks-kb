package dev.ikm.ds.rocks;

import java.lang.module.Configuration;
import java.lang.module.ModuleFinder;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

/**
 * Run by {@link PluginLayerTest} in a JVM of its own, whose boot layer holds the engine's runtime
 * dependencies as modules: defines the engine in a plugin layer twice, directly and through
 * {@code Layers.createModuleLayer}, and reports whether {@code RocksProvider} loads in each.
 * {@code Layers} is in a package {@code common} does not export, so it is reached by reflection,
 * which the launching JVM allows with {@code --add-exports}.
 */
final class PluginLayerLauncher {

    static final String DIRECT = "direct: ";
    static final String THROUGH_LAYERS = "through Layers: ";
    static final String LOADED = "loaded";

    private PluginLayerLauncher() {
    }

    public static void main(String[] args) throws ReflectiveOperationException {
        Path engine = Path.of(args[0]);
        Configuration configuration = ModuleLayer.boot().configuration()
                .resolve(ModuleFinder.of(engine), ModuleFinder.of(), Set.of("dev.ikm.rocks.engine"));
        ModuleLayer direct = ModuleLayer.defineModulesWithOneLoader(configuration, List.of(ModuleLayer.boot()),
                ClassLoader.getSystemClassLoader()).layer();
        System.out.println(DIRECT + load(direct));
        ModuleLayer throughLayers = (ModuleLayer) Class.forName("dev.ikm.tinkar.common.service.plugin.internal.Layers")
                .getMethod("createModuleLayer", List.class, List.class)
                .invoke(null, List.of(ModuleLayer.boot()), List.of(engine));
        System.out.println(THROUGH_LAYERS + load(throughLayers));
    }

    private static String load(ModuleLayer layer) {
        try {
            Class.forName("dev.ikm.ds.rocks.RocksProvider", false, layer.findLoader("dev.ikm.rocks.engine"));
            return LOADED;
        } catch (Throwable failure) {
            return failure.toString();
        }
    }
}
