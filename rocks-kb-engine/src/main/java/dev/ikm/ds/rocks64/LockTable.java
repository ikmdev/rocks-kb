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
        UUID[] uuids = publicId.asUuidArray();
        if (uuids.length == 1) {
            stripes[stripeOf(uuids[0])].lock();
            return;
        }
        for (int stripe : stripesFor(uuids)) {
            stripes[stripe].lock();
        }
    }

    void unlock(PublicId publicId) {
        UUID[] uuids = publicId.asUuidArray();
        if (uuids.length == 1) {
            stripes[stripeOf(uuids[0])].unlock();
            return;
        }
        int[] held = stripesFor(uuids);
        for (int i = held.length - 1; i >= 0; i--) {
            stripes[held[i]].unlock();
        }
    }

    /** The distinct stripes of the UUIDs, ascending. A plain sort and squeeze: a stream pipeline here was a fifth of a registration (IKE-Network/ike-issues#1273). */
    static int[] stripesFor(UUID[] uuids) {
        int[] stripes = new int[uuids.length];
        for (int i = 0; i < uuids.length; i++) {
            stripes[i] = stripeOf(uuids[i]);
        }
        Arrays.sort(stripes);
        int distinct = 0;
        for (int i = 0; i < stripes.length; i++) {
            if (i == 0 || stripes[i] != stripes[i - 1]) {
                stripes[distinct++] = stripes[i];
            }
        }
        return distinct == stripes.length ? stripes : Arrays.copyOf(stripes, distinct);
    }

    private static int stripeOf(UUID uuid) {
        int hash = uuid.hashCode();
        hash ^= (hash >>> 16);
        return hash & (STRIPES - 1);
    }
}
