package dev.ikm.ds.rocks.maps;

import dev.ikm.tinkar.common.id.EntityKey;
import dev.ikm.tinkar.common.id.PublicId;
import dev.ikm.tinkar.common.id.PublicIds;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Concurrent entity-key allocation in {@link UuidEntityKeyMap} against a real
 * RocksDB instance (IKE-Network/ike-issues#1140).
 *
 * <p>The defect: allocation ran inside {@code ConcurrentHashMap.computeIfAbsent},
 * and for a multi-UUID id the mapping function wrote the id's other UUIDs back
 * into the same map. A thread allocating for {@code [u1, u2]} held u1's bin and
 * blocked writing u2, while a thread allocating for {@code [u2, u1]} held u2's
 * bin and waited for the first thread's UUID locks — a cycle that hung imports.
 */
class UuidEntityKeyMapConcurrencyTest {

    private static final int ENTITIES = 2_000;
    private static final int REQUESTS_PER_ENTITY = 4;

    @TempDir
    Path tempDir;

    private DBOptions dbOptions;
    private RocksDB db;
    private List<ColumnFamilyHandle> handles;
    private UuidEntityKeyMap keyMap;

    @BeforeEach
    void openDb() throws RocksDBException {
        RocksDB.loadLibrary();
        List<ColumnFamilyDescriptor> descriptors = List.of(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor("UuidEntityKeyMap".getBytes(StandardCharsets.UTF_8),
                        new ColumnFamilyOptions()));
        handles = new ArrayList<>();
        dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        db = RocksDB.open(dbOptions, tempDir.resolve("rocks").toString(), descriptors, handles);
        SequenceMap sequenceMap = new SequenceMap(db, handles.get(0));
        keyMap = new UuidEntityKeyMap(db, handles.get(1), sequenceMap);
    }

    @AfterEach
    void closeDb() {
        for (ColumnFamilyHandle handle : handles) {
            handle.close();
        }
        db.close();
        dbOptions.close();
    }

    @Test
    void concurrentMultiUuidAllocation_completes_andEachIdGetsExactlyOneKey() {
        PublicId conceptPattern = PublicIds.of(SequenceMap.conceptPatternUUID);
        List<UUID[]> entities = new ArrayList<>();
        for (int i = 0; i < ENTITIES; i++) {
            entities.add(new UUID[]{UUID.randomUUID(), UUID.randomUUID()});
        }
        ConcurrentHashMap<Integer, Set<EntityKey>> keysByEntity = new ConcurrentHashMap<>();

        assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
            try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
                List<Future<?>> futures = new ArrayList<>();
                for (int request = 0; request < REQUESTS_PER_ENTITY; request++) {
                    boolean reversed = request % 2 == 1;
                    for (int i = 0; i < ENTITIES; i++) {
                        int entity = i;
                        UUID[] uuids = entities.get(i);
                        // Half the requests name the UUIDs in the opposite order — the
                        // interleaving that deadlocked.
                        PublicId id = reversed ? PublicIds.of(uuids[1], uuids[0]) : PublicIds.of(uuids[0], uuids[1]);
                        futures.add(executor.submit(() -> keysByEntity
                                .computeIfAbsent(entity, k -> ConcurrentHashMap.newKeySet())
                                .add(keyMap.getEntityKey(conceptPattern, id))));
                    }
                }
                for (Future<?> future : futures) {
                    future.get();
                }
            }
        }, "concurrent multi-UUID key allocation must not deadlock");

        Set<EntityKey> allKeys = new HashSet<>();
        for (int i = 0; i < ENTITIES; i++) {
            Set<EntityKey> keys = keysByEntity.get(i);
            assertEquals(1, keys.size(), "entity " + i + " got more than one key: " + keys);
            EntityKey key = keys.iterator().next();
            for (UUID uuid : entities.get(i)) {
                assertEquals(key, keyMap.getEntityKey(uuid).orElseThrow(), "every UUID maps to the entity's key");
            }
            allKeys.add(key);
        }
        assertEquals(ENTITIES, allKeys.size(), "distinct entities never share a key");
    }

    @Test
    void lockTable_stripesAreDistinctAndAscending() {
        UUID a = UUID.randomUUID();
        int[] stripes = MultiUuidLockTable.stripesFor(new UUID[]{a, a, UUID.randomUUID(), UUID.randomUUID()});
        int[] sortedDistinct = java.util.Arrays.stream(stripes).distinct().sorted().toArray();
        assertArrayEquals(sortedDistinct, stripes);
    }

    @Test
    void lockTable_isReentrantForTheSameId() {
        MultiUuidLockTable table = new MultiUuidLockTable();
        PublicId id = PublicIds.of(UUID.randomUUID(), UUID.randomUUID());
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            table.lock(id);
            table.lock(id);
            table.unlock(id);
            table.unlock(id);
        });
    }
}
