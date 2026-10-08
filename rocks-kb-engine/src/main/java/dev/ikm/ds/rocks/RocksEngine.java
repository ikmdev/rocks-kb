package dev.ikm.ds.rocks;

import dev.ikm.tinkar.common.service.PrimitiveDataService;
import dev.ikm.tinkar.common.service.internal.EntityStore;

/**
 * What the Rocks provider's controllers hold: an engine over one RocksDB directory. There are
 * two, with no storage code in common (design {@code design-2026-10-07-64-bit-rocks-store}, "One
 * engine or two"): the legacy {@link RocksProvider}, for stores in the 6-bit and 8-bit layouts,
 * and {@link dev.ikm.ds.rocks64.Rocks64Store}, for stores in the 64-bit layout, which every new
 * store is. The controller chooses by the store's format and the caller sees one provider.
 */
public interface RocksEngine extends PrimitiveDataService, EntityStore {

    /** Whether the engine is open and neither closing nor closed. */
    boolean running();

    /** Writes everything held in memory to the database and the database to disk. */
    void save();
}
