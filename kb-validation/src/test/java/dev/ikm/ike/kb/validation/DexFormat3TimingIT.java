/*
 * Copyright © 2015 Integrated Knowledge Management (support@ikm.dev)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.ikm.ike.kb.validation;

import dev.ikm.tinkar.entity.changeset.ChangeSetFormat;
import dev.ikm.tinkar.fixtures.ForkedJvm;
import dev.ikm.tinkar.fixtures.StoreDigest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.zip.ZipFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Step 6 of the format-3 plan (IKE-Network/ike-issues#1275): DeX, the largest knowledge base
 * in use, loaded from an export into a fresh 64-bit Rocks store, exported again in format 3,
 * and restored into another fresh store, each stage timed in its own JVM. The export is named
 * by {@code -Ddex.export}, a format-1 or format-2 file as Komet writes one; the test is
 * skipped without it. The digests of the exported and the restored store must agree.
 */
@Tag("dex")
class DexFormat3TimingIT {
    private static final Logger LOG = LoggerFactory.getLogger(DexFormat3TimingIT.class);
    private static final String TEST = "dex-format-3";

    @Test
    void theKnowledgeBaseExportsAndRestoresInFormat3() throws IOException {
        String exportProperty = System.getProperty(DexImportIT.EXPORT);
        assumeTrue(exportProperty != null && !exportProperty.isBlank(), "-D" + DexImportIT.EXPORT + " is not set");
        Path source = Path.of(exportProperty);
        assumeTrue(java.nio.file.Files.isRegularFile(source), "No export at " + source);
        Provider provider = Provider.ROCKS;
        Path work = ChangeSets.work(TEST, provider, "timing");
        Properties in = new Properties();
        in.setProperty(StoreStage.PROVIDER, provider.name());
        in.setProperty(StoreStage.STORE, work.resolve("store").toString());
        in.setProperty("restored.store", work.resolve("restored").toString());
        in.setProperty("kb.file", source.toString());
        in.setProperty("export.file", work.resolve("export-pb.zip").toString());
        Properties result = ForkedJvm.run(SnomedRoundTripIT.Load.class, in, Duration.ofHours(2));
        result = ForkedJvm.run(SnomedRoundTripIT.Export.class, result, Duration.ofHours(2));
        result = ForkedJvm.run(SnomedRoundTripIT.Restore.class, result, Duration.ofHours(2));
        StoreDigest exported = StoreDigest.load(result, "exported.");
        StoreDigest restored = StoreDigest.load(result, "restored.");
        File export = new File(result.getProperty("export.file"));
        String version;
        try (ZipFile zip = new ZipFile(export)) {
            version = ChangeSetFormat.manifest(zip).orElseThrow().getMainAttributes().getValue(ChangeSetFormat.VERSION_ATTRIBUTE);
        }
        LOG.info("DeX on {} from {} ({} bytes): load {} ms; export in format {} {} ms, {} records, {} bytes; restore {} ms",
                provider, source.getFileName(), source.toFile().length(), result.getProperty("load.millis"), version,
                result.getProperty("export.millis"), result.getProperty("export.count"), result.getProperty("export.bytes"),
                result.getProperty("restore.millis"));
        assertEquals("3", version, "The export's format version");
        assertEquals(List.of(), restored.differencesFrom(exported),
                "The store restored from the format-3 export, against the store it was exported from");
    }
}
