package dev.ikm.ds.rocks64;

import dev.ikm.tinkar.common.id.PublicId;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A striped lock table for atomic work on the UUIDs of a {@link PublicId}: each UUID maps to a
 * stripe by hash, and locking an id takes the distinct stripes of its UUIDs in ascending order,
 * so two ids that share a UUID exclude each other and no set of threads can wait in a cycle.
 * The stripes are held strongly for the life of the table (IKE-Network/ike-issues#1140).
 */
final class LockTable {

    static final int STRIPES = 1024;

    private final ReentrantLock[] stripes = new ReentrantLock[STRIPES];

    LockTable() {
        for (int i = 0; i < STRIPES; i++) {
            stripes[i] = new ReentrantLock();
        }
    }

    void lock(PublicId publicId) {
        for (int stripe : stripesFor(publicId.asUuidArray())) {
            stripes[stripe].lock();
        }
    }

    void unlock(PublicId publicId) {
        int[] held = stripesFor(publicId.asUuidArray());
        for (int i = held.length - 1; i >= 0; i--) {
            stripes[held[i]].unlock();
        }
    }

    static int[] stripesFor(UUID[] uuids) {
        return Arrays.stream(uuids).mapToInt(LockTable::stripeOf).distinct().sorted().toArray();
    }

    private static int stripeOf(UUID uuid) {
        int hash = uuid.hashCode();
        hash ^= (hash >>> 16);
        return hash & (STRIPES - 1);
    }
}
