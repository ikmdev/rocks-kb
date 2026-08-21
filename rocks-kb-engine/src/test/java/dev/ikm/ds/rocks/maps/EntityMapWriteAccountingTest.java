package dev.ikm.ds.rocks.maps;

import dev.ikm.tinkar.common.id.impl.KeyUtil;
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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static dev.ikm.tinkar.entity.EntityRecordFactory.ENTITY_FORMAT_VERSION;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the {@link EntityMap} write pipeline against a real RocksDB instance in a
 * temporary directory: flush accounting under concurrent producers
 * (IKE-Network/ike-issues#1058), flush accounting after a rejected put
 * (IKE-Network/ike-issues#1059), and writer-thread termination after repeated
 * identical puts (IKE-Network/ike-issues#1060).
 *
 * <p>All entities are chronicle-only (no version parts), which keeps the writer's key
 * construction independent of the {@code RocksProvider} singleton that version-part
 * keys require.</p>
 */
class EntityMapWriteAccountingTest {

    /**
     * Generous ceiling for operations that must not hit the flush wait's 120-second
     * deadline. A healthy pipeline finishes these in well under a second; only the
     * wedged accounting the fixed defects produced would exceed this.
     */
    private static final long PROMPT_SECONDS = 60;

    @TempDir
    Path tempDir;

    private DBOptions dbOptions;
    private RocksDB db;
    private ColumnFamilyHandle entityHandle;
    private List<ColumnFamilyHandle> handles;
    private EntityMap entityMap;

    @BeforeEach
    void openDb() throws RocksDBException {
        RocksDB.loadLibrary();
        List<ColumnFamilyDescriptor> descriptors = List.of(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()),
                new ColumnFamilyDescriptor("entityMap".getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions()));
        handles = new ArrayList<>();
        dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);
        db = RocksDB.open(dbOptions, tempDir.resolve("rocks").toString(), descriptors, handles);
        entityHandle = handles.get(1);
        entityMap = new EntityMap(db, entityHandle, null);
    }

    @AfterEach
    void closeDb() {
        entityMap.closeMap();
        for (ColumnFamilyHandle handle : handles) {
            handle.close();
        }
        db.close();
        dbOptions.close();
    }

    /**
     * Builds a serialized chronicle-only entity in the format
     * {@code extractVersionParts} expects: part count, chronicle part size, format
     * version byte, chronicle bytes, version count (zero).
     */
    private static byte[] chronicleOnlyEntity(byte[] chronicle) {
        ByteBuffer buf = ByteBuffer.allocate(4 + 4 + 1 + chronicle.length + 4);
        buf.putInt(1);
        buf.putInt(chronicle.length + 5);
        buf.put(ENTITY_FORMAT_VERSION);
        buf.put(chronicle);
        buf.putInt(0);
        return buf.array();
    }

    /**
     * Builds a distinct chronicle for a key. The first byte (1) is a non-stamp data
     * type token, so the writer never consults stamp sequences for these entities.
     */
    private static byte[] chronicleFor(long key, byte marker) {
        ByteBuffer buf = ByteBuffer.allocate(1 + 8 + 1);
        buf.put((byte) 1);
        buf.putLong(key);
        buf.put(marker);
        return buf.array();
    }

    private void assertDurable(long key, byte[] chronicle) throws RocksDBException {
        byte[] stored = db.get(entityHandle, KeyUtil.longToByteArray(key));
        assertNotNull(stored, "chronicle for key " + key + " missing from RocksDB");
        assertArrayEquals(chronicle, stored, "chronicle for key " + key + " differs in RocksDB");
    }

    @Test
    void putIsDurableAndPromptlyFlushed() throws RocksDBException {
        byte[][] chronicles = new byte[500][];
        for (int i = 0; i < chronicles.length; i++) {
            chronicles[i] = chronicleFor(i, (byte) 0);
            entityMap.put(1000L + i, chronicleOnlyEntity(chronicles[i]));
        }

        long start = System.nanoTime();
        entityMap.save();
        long elapsedSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
        assertTrue(elapsedSeconds < PROMPT_SECONDS,
                "save() took " + elapsedSeconds + " s — flush accounting is wedged");

        for (int i = 0; i < chronicles.length; i++) {
            assertArrayEquals(chronicleOnlyEntity(chronicles[i]), entityMap.get(1000L + i));
            assertDurable(1000L + i, chronicles[i]);
        }
    }

    @Test
    void concurrentPutStormFlushesPromptly() throws RocksDBException, InterruptedException {
        final int threadCount = 8;
        final int putsPerThread = 1000;
        List<Thread> producers = new ArrayList<>(threadCount);
        for (int t = 0; t < threadCount; t++) {
            final int threadIndex = t;
            producers.add(Thread.ofPlatform().start(() -> {
                for (int i = 0; i < putsPerThread; i++) {
                    long key = (long) threadIndex * putsPerThread + i;
                    entityMap.put(key, chronicleOnlyEntity(chronicleFor(key, (byte) 0)));
                }
            }));
        }
        for (Thread producer : producers) {
            producer.join();
        }

        long start = System.nanoTime();
        entityMap.save();
        long elapsedSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
        assertTrue(elapsedSeconds < PROMPT_SECONDS,
                "save() after concurrent puts took " + elapsedSeconds
                        + " s — the flush marker regressed (ike-issues#1058)");

        // A quiesced pipeline must satisfy a second flush wait immediately.
        start = System.nanoTime();
        entityMap.save();
        long secondSaveSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
        assertTrue(secondSaveSeconds < 10,
                "save() on a quiesced pipeline took " + secondSaveSeconds + " s");

        for (int t = 0; t < threadCount; t++) {
            long key = (long) t * putsPerThread;
            assertDurable(key, chronicleFor(key, (byte) 0));
        }
    }

    @Test
    void failedPutDoesNotWedgeLaterFlushes() throws RocksDBException {
        // A large entity occupies the writer for milliseconds, so the conflicting put
        // microseconds later deterministically finds the first record still pending.
        byte[] giantChronicle = new byte[32 * 1024 * 1024];
        giantChronicle[0] = 1;
        entityMap.put(1L, chronicleOnlyEntity(giantChronicle));

        long conflictedKey = 42L;
        byte[] originalChronicle = chronicleFor(conflictedKey, (byte) 0);
        entityMap.put(conflictedKey, chronicleOnlyEntity(originalChronicle));
        IllegalStateException rejected = null;
        try {
            entityMap.put(conflictedKey, chronicleOnlyEntity(chronicleFor(conflictedKey, (byte) 9)));
        } catch (IllegalStateException e) {
            rejected = e;
        }
        assertNotNull(rejected, "a put with a different chronicle for the same pending key must be rejected");

        long survivorKey = 43L;
        byte[] survivorChronicle = chronicleFor(survivorKey, (byte) 0);
        entityMap.put(survivorKey, chronicleOnlyEntity(survivorChronicle));

        long start = System.nanoTime();
        entityMap.save();
        long elapsedSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
        assertTrue(elapsedSeconds < PROMPT_SECONDS,
                "save() after a rejected put took " + elapsedSeconds
                        + " s — the rejected put stranded the flush target (ike-issues#1059)");

        assertDurable(conflictedKey, originalChronicle);
        assertDurable(survivorKey, survivorChronicle);
    }

    @Test
    void repeatedIdenticalPutsCloseCleanly() throws RocksDBException {
        long key = 7L;
        byte[] chronicle = chronicleFor(key, (byte) 0);
        byte[] entity = chronicleOnlyEntity(chronicle);
        // Re-putting identical content races the writer into the unchanged-merge path
        // many times over.
        for (int i = 0; i < 2_000; i++) {
            entityMap.put(key, entity);
        }
        for (int i = 0; i < 100; i++) {
            entityMap.put(200_000L + i, chronicleOnlyEntity(chronicleFor(200_000L + i, (byte) 0)));
        }

        long start = System.nanoTime();
        entityMap.closeMap();
        long elapsedSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
        assertTrue(elapsedSeconds < 20,
                "closeMap() took " + elapsedSeconds
                        + " s — the writer could not drain the pending map (ike-issues#1060)");
        assertFalse(entityMap.writeThread.isAlive(),
                "EntityMap-Writer is still alive after closeMap() (ike-issues#1060)");

        entityMap.save();
        assertDurable(key, chronicle);
        assertArrayEquals(entity, entityMap.get(key));
    }

    @Test
    void readYourWritesBeforeAndAfterFlush() throws RocksDBException {
        long key = 99L;
        byte[] chronicle = chronicleFor(key, (byte) 0);
        byte[] entity = chronicleOnlyEntity(chronicle);

        entityMap.put(key, entity);
        assertArrayEquals(entity, entityMap.get(key), "pending write must be readable before flush");

        entityMap.save();
        assertArrayEquals(entity, entityMap.get(key), "write must be readable after flush");
        assertDurable(key, chronicle);
    }
}
