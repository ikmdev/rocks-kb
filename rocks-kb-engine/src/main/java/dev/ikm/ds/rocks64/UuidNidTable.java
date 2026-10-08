package dev.ikm.ds.rocks64;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;

/**
 * A UUID to nid table in primitive arrays, for the identities an import holds in memory until
 * they are written: two longs of UUID and one of nid per slot, open addressing with linear
 * probing, about 40 bytes an entry at the load it is kept under, against the hundred of a
 * {@code ConcurrentHashMap<UUID, Long>} (IKE-Network/ike-issues#1273). The table is striped by
 * the first byte of the UUID, each stripe its own arrays under its own monitor, so threads
 * registering different UUIDs rarely wait on one another, and so the stripes in order are the
 * table in key order: a stripe sorted is a run of the column, which is how the table is
 * written as SST files.
 *
 * <p>A read takes no monitor: a slot's nid is written last, with release, and read first, with
 * acquire, so a reader that finds a nid finds the UUID written before it; a reader that finds
 * zero stops, and no entry lies beyond an empty slot in its probe sequence, since nothing is
 * ever removed. A stripe that grows publishes its new arrays whole. A reader may miss an entry
 * another thread is adding at that moment, which is the race the striped lock table above the
 * map settles: a writer looks again under the lock before it adds. Writes take the monitor.
 *
 * <p>A nid of zero marks an empty slot; no 64-bit nid is zero, since every one has a pattern
 * sequence of at least one in its upper half.
 */
final class UuidNidTable {

    static final int STRIPES = 256;
    private static final int INITIAL_CAPACITY = 1024;
    /** A stripe grows when it holds more than this fraction of its slots. */
    private static final double MAX_LOAD = 0.6;

    private final Stripe[] stripes = new Stripe[STRIPES];
    /** The entries in every stripe, kept apart so that a size costs no stripe its monitor. */
    private final LongAdder count = new LongAdder();

    UuidNidTable() {
        for (int i = 0; i < STRIPES; i++) {
            stripes[i] = new Stripe(count);
        }
    }

    /** The stripe of a UUID: the first byte of its key, so the stripes partition the key space in order. */
    private static int stripeOf(long msb) {
        return (int) (msb >>> 56);
    }

    /** The nid mapped from a UUID, or zero if none is. */
    long get(UUID uuid) {
        long msb = uuid.getMostSignificantBits();
        return stripes[stripeOf(msb)].get(msb, uuid.getLeastSignificantBits());
    }

    /** Maps a UUID to a nid unless it is mapped already; the nid it is mapped to afterwards. */
    long putIfAbsent(UUID uuid, long nid) {
        long msb = uuid.getMostSignificantBits();
        return stripes[stripeOf(msb)].putIfAbsent(msb, uuid.getLeastSignificantBits(), nid);
    }

    /** The entries held. */
    long size() {
        return count.sum();
    }

    boolean isEmpty() {
        return size() == 0;
    }

    /** An entry, visited in key order. */
    @FunctionalInterface
    interface EntryConsumer {
        void accept(long msb, long lsb, long nid);
    }

    /**
     * The entries of a range of stripes, in key order: the first stripe sorted, then the
     * next, and so on. The table must not be written to meanwhile.
     */
    void forEachSorted(int fromStripe, int toStripe, EntryConsumer consumer) {
        for (int stripe = fromStripe; stripe < toStripe; stripe++) {
            stripes[stripe].forEachSorted(consumer);
        }
    }

    /** The entries of a range of stripes. */
    long sizeOf(int fromStripe, int toStripe) {
        long size = 0;
        for (int stripe = fromStripe; stripe < toStripe; stripe++) {
            size += stripes[stripe].size();
        }
        return size;
    }

    /** Sorts every stripe, several at a time, so that {@link #forEachSorted} afterwards only reads. */
    void sort() {
        IntStream.range(0, STRIPES).parallel().forEach(stripe -> stripes[stripe].sorted());
    }

    /** One stripe: its own arrays, its own monitor for writes, and the order of its entries once sorted. */
    private static final class Stripe {
        private static final VarHandle NID = MethodHandles.arrayElementVarHandle(long[].class);

        /** The arrays of a stripe, replaced whole when it grows. */
        private record Slots(long[] msb, long[] lsb, long[] nid) {
            Slots(int capacity) {
                this(new long[capacity], new long[capacity], new long[capacity]);
            }

            int mask() {
                return nid.length - 1;
            }
        }

        private volatile Slots slots = new Slots(INITIAL_CAPACITY);
        private int size;
        /** The occupied slots in key order, computed once the stripe stops changing; null until then. */
        private int[] order;
        private final LongAdder count;

        Stripe(LongAdder count) {
            this.count = count;
        }

        synchronized int size() {
            return size;
        }

        long get(long m, long l) {
            Slots current = slots;
            int mask = current.mask();
            int slot = slotOf(m, l) & mask;
            while (true) {
                long value = (long) NID.getAcquire(current.nid(), slot);
                if (value == 0) {
                    return 0;
                }
                if (current.msb()[slot] == m && current.lsb()[slot] == l) {
                    return value;
                }
                slot = (slot + 1) & mask;
            }
        }

        synchronized long putIfAbsent(long m, long l, long value) {
            if (value == 0) {
                throw new IllegalArgumentException("A nid of zero marks an empty slot");
            }
            if (size + 1 > slots.nid().length * MAX_LOAD) {
                grow();
            }
            Slots current = slots;
            int mask = current.mask();
            int slot = slotOf(m, l) & mask;
            while (current.nid()[slot] != 0) {
                if (current.msb()[slot] == m && current.lsb()[slot] == l) {
                    return current.nid()[slot];
                }
                slot = (slot + 1) & mask;
            }
            current.msb()[slot] = m;
            current.lsb()[slot] = l;
            NID.setRelease(current.nid(), slot, value);
            size++;
            count.increment();
            order = null;
            return value;
        }

        private void grow() {
            Slots old = slots;
            Slots grown = new Slots(old.nid().length * 2);
            int mask = grown.mask();
            for (int i = 0; i < old.nid().length; i++) {
                if (old.nid()[i] != 0) {
                    int slot = slotOf(old.msb()[i], old.lsb()[i]) & mask;
                    while (grown.nid()[slot] != 0) {
                        slot = (slot + 1) & mask;
                    }
                    grown.msb()[slot] = old.msb()[i];
                    grown.lsb()[slot] = old.lsb()[i];
                    grown.nid()[slot] = old.nid()[i];
                }
            }
            slots = grown;
        }

        private static int slotOf(long m, long l) {
            long h = m ^ l;
            h ^= h >>> 32;
            h *= 0x9E3779B97F4A7C15L;
            h ^= h >>> 29;
            return (int) h;
        }

        synchronized int[] sorted() {
            if (order == null) {
                long[] nid = slots.nid();
                int[] occupied = new int[size];
                int n = 0;
                for (int i = 0; i < nid.length; i++) {
                    if (nid[i] != 0) {
                        occupied[n++] = i;
                    }
                }
                sort(occupied, 0, occupied.length - 1);
                order = occupied;
            }
            return order;
        }

        synchronized void forEachSorted(EntryConsumer consumer) {
            Slots current = slots;
            for (int slot : sorted()) {
                consumer.accept(current.msb()[slot], current.lsb()[slot], current.nid()[slot]);
            }
        }

        /** The key order of the column: the sixteen bytes compared unsigned, so the two longs are, high first. */
        private int compare(int a, int b) {
            Slots current = slots;
            int byMsb = Long.compareUnsigned(current.msb()[a], current.msb()[b]);
            return byMsb != 0 ? byMsb : Long.compareUnsigned(current.lsb()[a], current.lsb()[b]);
        }

        /** Quicksort of slot indices by their keys, median-of-three pivots, insertion sort for short runs. */
        private void sort(int[] slots, int low, int high) {
            while (high - low > 16) {
                int mid = (low + high) >>> 1;
                if (compare(slots[mid], slots[low]) < 0) {
                    swap(slots, mid, low);
                }
                if (compare(slots[high], slots[low]) < 0) {
                    swap(slots, high, low);
                }
                if (compare(slots[high], slots[mid]) < 0) {
                    swap(slots, high, mid);
                }
                int pivot = slots[mid];
                int i = low;
                int j = high;
                while (i <= j) {
                    while (compare(slots[i], pivot) < 0) {
                        i++;
                    }
                    while (compare(slots[j], pivot) > 0) {
                        j--;
                    }
                    if (i <= j) {
                        swap(slots, i, j);
                        i++;
                        j--;
                    }
                }
                // Recurse into the shorter side, loop on the longer: bounded stack depth.
                if (j - low < high - i) {
                    sort(slots, low, j);
                    low = i;
                } else {
                    sort(slots, i, high);
                    high = j;
                }
            }
            for (int i = low + 1; i <= high; i++) {
                int slot = slots[i];
                int j = i - 1;
                while (j >= low && compare(slots[j], slot) > 0) {
                    slots[j + 1] = slots[j];
                    j--;
                }
                slots[j + 1] = slot;
            }
        }

        private static void swap(int[] slots, int a, int b) {
            int t = slots[a];
            slots[a] = slots[b];
            slots[b] = t;
        }
    }
}
