package dev.ikm.ike.kb.validation;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Leaves a benchmark's figures where the build report reads them:
 * {@code target/measurements/<name>.properties}, one numeric property per figure under the key
 * {@code <name>.<figure>} (IKE-Network/ike-issues#1207). The build-report extension turns every
 * numeric property there into a measure of the session — a line in the receipt, a key in the
 * observations sidecar, and under TeamCity a build statistic charted across builds — so a key,
 * once published, is never renamed. A figure that is not a number, such as the agents a stage
 * ran with, is left out.
 */
final class Measurements {

    /** Where the build report looks, relative to the module. */
    static final Path DIRECTORY = Path.of("target", "measurements");

    private Measurements() {
    }

    /**
     * Writes the numeric figures under a name.
     *
     * @param name    the key prefix and file name, for example {@code bench.starter.rocks}
     * @param figures the figures; non-numeric values are skipped
     * @return the file written
     * @throws IOException when the file cannot be written
     */
    static Path publish(String name, Properties figures) throws IOException {
        Path file = DIRECTORY.resolve(name + ".properties").toAbsolutePath();
        Files.createDirectories(file.getParent());
        Properties out = new Properties();
        for (String key : figures.stringPropertyNames()) {
            String value = figures.getProperty(key).strip();
            try {
                Double.parseDouble(value);
            } catch (NumberFormatException e) {
                continue;
            }
            out.setProperty(name + "." + key, value);
        }
        try (OutputStream stream = Files.newOutputStream(file)) {
            out.store(stream, "Measurements of " + name + ", read by the build report (IKE-Network/ike-issues#1207)");
        }
        return file;
    }
}
