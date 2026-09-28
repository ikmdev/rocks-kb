package dev.ikm.ds.rocks.maps;

import dev.ikm.tinkar.common.id.PublicId;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A striped lock table for atomic operations on the UUIDs of a {@link PublicId}.
 *
 * <p><b>Concurrency strategy:</b> a fixed array of {@link ReentrantLock}s; each UUID
 * maps to one stripe by hash. Locking a public id acquires the distinct stripes of
 * its UUIDs in ascending stripe order and releases them afterwards. Two threads
 * whose ids share any UUID therefore exclude each other, and because every thread
 * acquires stripes in the same global order, no set of threads can wait on each
 * other in a cycle — the table is deadlock-free. Unrelated ids that happen to
 * share a stripe merely serialize, which is correct, only slower.
 *
 * <p>The stripes are held strongly for the life of the table. (An earlier version
 * kept one lock per UUID behind a {@code WeakReference}; a held lock was then
 * reachable only weakly, so a collection could clear it and hand the next thread a
 * fresh lock for the same UUID — silently breaking mutual exclusion;
 * IKE-Network/ike-issues#1140.)
 *
 * <pre>
 *   lockTable.lock(publicId);
 *   try {
 *      // atomic work on every UUID of publicId
 *   } finally {
 *      lockTable.unlock(publicId);
 *   }
 * </pre>
 */
public class MultiUuidLockTable {

    /** Number of stripes; a power of two so a stripe is a mask of the hash. */
    static final int STRIPE_COUNT = 1024;

    private final ReentrantLock[] stripes = new ReentrantLock[STRIPE_COUNT];

    /** Creates a table with {@value #STRIPE_COUNT} stripes. */
    public MultiUuidLockTable() {
        for (int i = 0; i < STRIPE_COUNT; i++) {
            stripes[i] = new ReentrantLock();
        }
    }

    /**
     * Acquires the stripes of every UUID in {@code publicId}, in ascending stripe
     * order. Reentrant: a thread may lock an id it already holds.
     *
     * @param publicId the id whose UUIDs to lock
     */
    public void lock(PublicId publicId) {
        for (int stripe : stripesFor(publicId.asUuidArray())) {
            stripes[stripe].lock();
        }
    }

    /**
     * Releases the stripes acquired by {@link #lock(PublicId)} for the same id.
     *
     * @param publicId the id whose UUIDs to unlock
     */
    public void unlock(PublicId publicId) {
        int[] held = stripesFor(publicId.asUuidArray());
        for (int i = held.length - 1; i >= 0; i--) {
            stripes[held[i]].unlock();
        }
    }

    /**
     * Returns the distinct stripes for {@code uuids}, sorted ascending — the
     * acquisition order.
     *
     * @param uuids the UUIDs
     * @return sorted, distinct stripe indexes
     */
    static int[] stripesFor(UUID[] uuids) {
        return Arrays.stream(uuids).mapToInt(MultiUuidLockTable::stripeOf).distinct().sorted().toArray();
    }

    private static int stripeOf(UUID uuid) {
        int hash = uuid.hashCode();
        hash ^= (hash >>> 16);
        return hash & (STRIPE_COUNT - 1);
    }
}
