package dev.ikm.ike.kb.validation;

import dev.ikm.ds.rocks.RocksProvider;
import dev.ikm.tinkar.fixtures.PrimitiveDataServiceConformance;
import dev.ikm.tinkar.fixtures.WithKeyValueProvider;

/**
 * The provider conformance suite, from tinkar-core test-fixtures, against an empty RocksDB
 * store: the open controller on an empty directory under {@code target}, since the new-store
 * controller makes its directory under the user's home.
 */
@WithKeyValueProvider(controllerClass = RocksProvider.OpenController.class, cleanOnStart = true)
class RocksConformanceTest extends PrimitiveDataServiceConformance {
}
