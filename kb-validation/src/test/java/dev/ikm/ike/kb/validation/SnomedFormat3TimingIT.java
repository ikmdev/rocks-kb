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

/**
 * The SNOMED CT knowledge base loaded from its released export, exported in format 3, and
 * that export restored into a fresh store, each stage timed in its own JVM: the measurement
 * step of {@code notes/design-2026-10-08-changeset-format-3.adoc} (IKE-Network/ike-issues#1275).
 * The restored store must hold what the exported store held.
 */
@Tag("snomed")
class SnomedFormat3TimingIT {
    private static final Logger LOG = LoggerFactory.getLogger(SnomedFormat3TimingIT.class);
    private static final String TEST = "snomed-format-3";

    @ParameterizedTest
    @EnumSource(value = Provider.class, names = {"ROCKS"})
    void theKnowledgeBaseExportsAndRestoresInFormat3(Provider provider) throws IOException {
        Path kb = SnomedRoundTripIT.knowledgeBase();
        Path work = ChangeSets.work(TEST, provider, "timing");
        Properties in = new Properties();
        in.setProperty(StoreStage.PROVIDER, provider.name());
        in.setProperty(StoreStage.STORE, work.resolve("store").toString());
        in.setProperty("restored.store", work.resolve("restored").toString());
        in.setProperty("kb.file", kb.toString());
        in.setProperty("export.file", work.resolve("export-pb.zip").toString());

        Properties result = ForkedJvm.run(SnomedRoundTripIT.Load.class, in, Duration.ofHours(1));
        result = ForkedJvm.run(SnomedRoundTripIT.Export.class, result, Duration.ofHours(1));
        result = ForkedJvm.run(SnomedRoundTripIT.Restore.class, result, Duration.ofHours(1));

        StoreDigest exported = StoreDigest.load(result, "exported.");
        StoreDigest restored = StoreDigest.load(result, "restored.");
        File export = new File(result.getProperty("export.file"));
        String version;
        try (ZipFile zip = new ZipFile(export)) {
            version = ChangeSetFormat.manifest(zip).orElseThrow().getMainAttributes().getValue(ChangeSetFormat.VERSION_ATTRIBUTE);
        }
        LOG.info("SNOMED CT on {}: load of the released export {} ms; export in format {} {} ms, {} records, {} bytes; restore {} ms",
                provider, result.getProperty("load.millis"), version, result.getProperty("export.millis"),
                result.getProperty("export.count"), result.getProperty("export.bytes"), result.getProperty("restore.millis"));
        assertEquals("3", version, "The export's format version");
        assertEquals(List.of(), restored.differencesFrom(exported),
                "The store restored from the format-3 export, against the store it was exported from");
    }
}
