package dev.ikm.ds.rocks;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The engine loads as Komet loads it: as a plugin, in a module layer defined after the boot layer
 * (IKE-Network/ike-issues#1247). {@code RocksProvider} implements {@code EntityStore}, from a
 * package {@code common} exports only to named modules, and a qualified export never reaches a
 * module defined in a later layer; {@code Layers.createModuleLayer} grants it. The test suites run
 * on the classpath, where there are no layers, so only a JVM booted on a module path shows this.
 */
class PluginLayerTest {

    @Test
    void theEngineLoadsInAPluginLayerMadeByLayers() throws IOException, InterruptedException {
        List<String> output = launch();
        String direct = line(output, PluginLayerLauncher.DIRECT);
        String throughLayers = line(output, PluginLayerLauncher.THROUGH_LAYERS);

        assertEquals(PluginLayerLauncher.LOADED, throughLayers, String.join("\n", output));
        // The same layer without the grant fails as Komet failed, so the test sees the layering.
        assertTrue(direct.contains("IllegalAccessError") && direct.contains("service.internal"), direct);
    }

    private static List<String> launch() throws IOException, InterruptedException {
        String modulePath = Arrays.stream(Files.readString(Path.of("target", "runtime-module-path.txt"), StandardCharsets.UTF_8)
                        .strip().split(java.io.File.pathSeparator))
                .filter(entry -> entry.endsWith(".jar"))
                .collect(Collectors.joining(java.io.File.pathSeparator));
        String java = ProcessHandle.current().info().command().orElse("java");
        List<String> command = new ArrayList<>(List.of(java, "--enable-preview",
                "--module-path", modulePath,
                "--add-modules", "ALL-MODULE-PATH",
                "--add-exports", "dev.ikm.tinkar.common/dev.ikm.tinkar.common.service.plugin.internal=ALL-UNNAMED",
                "-cp", Path.of("target", "test-classes").toAbsolutePath().toString(),
                PluginLayerLauncher.class.getName(),
                Path.of("target", "classes").toAbsolutePath().toString()));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        List<String> output;
        try (var reader = process.inputReader(StandardCharsets.UTF_8)) {
            output = reader.lines().toList();
        }
        assertTrue(process.waitFor(2, TimeUnit.MINUTES), "the launcher did not finish");
        assertEquals(0, process.exitValue(), String.join("\n", output));
        return output;
    }

    private static String line(List<String> output, String prefix) {
        return output.stream().filter(line -> line.startsWith(prefix)).findFirst()
                .map(line -> line.substring(prefix.length()))
                .orElseThrow(() -> new AssertionError("no \"" + prefix + "\" line in:\n" + String.join("\n", output)));
    }
}
