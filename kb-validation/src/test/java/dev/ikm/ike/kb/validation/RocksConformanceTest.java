package dev.ikm.ike.kb.validation;

import dev.ikm.ds.rocks.RocksProvider;
import dev.ikm.tinkar.fixtures.PrimitiveDataServiceConformance;
import dev.ikm.tinkar.fixtures.WithKeyValueProvider;

/**
 * The provider conformance suite, from tinkar-core test-fixtures, against an empty RocksDB
 * store: the open controller on an empty directory under {@code target}, since the new-store
 * controller makes its directory under the user's home. An empty directory becomes a 64-bit
 * store, so this is the 64-bit engine's suite; {@link LegacyRocksConformanceTest} is the
 * legacy engine's.
 */
@WithKeyValueProvider(controllerClass = RocksProvider.OpenController.class, cleanOnStart = true)
class RocksConformanceTest extends PrimitiveDataServiceConformance {
}
