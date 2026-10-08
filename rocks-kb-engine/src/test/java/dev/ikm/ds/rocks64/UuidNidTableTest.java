package dev.ikm.ds.rocks64;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UuidNidTableTest {

    @Test
    void mapsAUuidOnceAndAnswersIt() {
        UuidNidTable table = new UuidNidTable();
        UUID uuid = UUID.randomUUID();
        assertEquals(0, table.get(uuid));
        assertEquals(7L << 32, table.putIfAbsent(uuid, 7L << 32));
        assertEquals(7L << 32, table.putIfAbsent(uuid, 8L << 32), "the first mapping stands");
        assertEquals(7L << 32, table.get(uuid));
        assertEquals(1, table.size());
        assertThrows(IllegalArgumentException.class, () -> table.putIfAbsent(UUID.randomUUID(), 0));
    }

    @Test
    void growsPastItsInitialCapacityInEveryStripe() {
        UuidNidTable table = new UuidNidTable();
        Random random = new Random(20261008L);
        int count = 400_000;
        UUID[] uuids = new UUID[count];
        for (int i = 0; i < count; i++) {
            uuids[i] = new UUID(random.nextLong(), random.nextLong());
            assertEquals(i + 1L, table.putIfAbsent(uuids[i], i + 1L));
        }
        assertEquals(count, table.size());
        for (int i = 0; i < count; i++) {
            assertEquals(i + 1L, table.get(uuids[i]));
        }
        assertEquals(0, table.get(new UUID(random.nextLong(), random.nextLong())));
    }

    @Test
    void visitsTheEntriesInTheColumnsKeyOrder() {
        UuidNidTable table = new UuidNidTable();
        Random random = new Random(20261008L);
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0; i < 50_000; i++) {
            UUID uuid = new UUID(random.nextLong(), random.nextLong());
            table.putIfAbsent(uuid, i + 1L);
            keys.add(Keys.of(uuid));
        }
        // Keys with every sign of both halves, so that the unsigned order is tested.
        for (long half : new long[]{Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE}) {
            UUID uuid = new UUID(half, half);
            table.putIfAbsent(uuid, 99L);
            keys.add(Keys.of(uuid));
        }
        keys.sort(Arrays::compareUnsigned);

        table.sort();
        List<byte[]> visited = new ArrayList<>();
        table.forEachSorted(0, UuidNidTable.STRIPES, (msb, lsb, nid) ->
                visited.add(ByteBuffer.allocate(16).putLong(msb).putLong(lsb).array()));
        assertEquals(keys.size(), visited.size());
        for (int i = 0; i < keys.size(); i++) {
            assertArrayEquals(keys.get(i), visited.get(i), "entry " + i);
        }
        assertEquals(keys.size(), table.sizeOf(0, UuidNidTable.STRIPES));
    }
}
