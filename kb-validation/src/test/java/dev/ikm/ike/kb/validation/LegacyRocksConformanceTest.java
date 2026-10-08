package dev.ikm.ike.kb.validation;

import dev.ikm.ds.rocks.RocksProvider;
import dev.ikm.tinkar.fixtures.PrimitiveDataServiceConformance;
import dev.ikm.tinkar.fixtures.WithKeyValueProvider;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * The provider conformance suite against the legacy Rocks engine, on an empty store created in
 * the 8-bit layout: the layout a new store would no longer get, held to the same contract while
 * existing stores are opened by it (IKE-Network/ike-issues#1258).
 */
@ExtendWith(LegacyStoreLayout.class)
@WithKeyValueProvider(controllerClass = RocksProvider.OpenController.class, cleanOnStart = true)
class LegacyRocksConformanceTest extends PrimitiveDataServiceConformance {
}
