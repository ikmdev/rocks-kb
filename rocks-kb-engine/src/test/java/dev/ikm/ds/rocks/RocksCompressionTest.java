package dev.ikm.ds.rocks;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TableProperties;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The store reads a store whose blocks are compressed. Its column families write uncompressed
 * blocks (compression was measured and deferred: design {@code design-2026-09-30-64-bit-nids},
 * "RocksDB compression"; IKE-Network/ike-issues#1251), but RocksDB records the codec in each
 * block, so a store compressed by another build, LZ4 and ZSTD alike, opens with the store's own
 * options and reads every value, and new files are written uncompressed beside the compressed
 * ones. Checked at the RocksDB level, with the options the store opens its column families with.
 */
class RocksCompressionTest {

    private static final RocksProvider.ColumnFamily FAMILY = RocksProvider.ColumnFamily.ENTITY_MAP;
    private static final int KEYS = 20_000;

    @TempDir
    Path directory;

    @Test
    void aStoreWithCompressedBlocksOpensAndReadsWithTheStoresOptions() throws RocksDBException {
        // A store written by a build that compresses: LZ4, and ZSTD once compacted to the bottom.
        try (Store store = new Store(directory, RocksCompressionTest::compressed)) {
            store.write(0, KEYS);
            store.write(KEYS, 2 * KEYS);
            try (CompactRangeOptions force = new CompactRangeOptions()
                    .setBottommostLevelCompaction(CompactRangeOptions.BottommostLevelCompaction.kForce)) {
                store.db.compactRange(store.family, null, null, force);
            }
            store.write(2 * KEYS, 3 * KEYS);
            assertEquals(Set.of("LZ4", "ZSTD"), store.compressionNames());
        }

        // The store's own options read it, and write new files uncompressed beside it.
        try (Store store = new Store(directory, RocksCompressionTest::storeOptions)) {
            store.assertReads(0, 3 * KEYS);
            store.write(3 * KEYS, 4 * KEYS);
            assertEquals(Set.of("LZ4", "NoCompression", "ZSTD"), store.compressionNames());
            store.assertReads(0, 4 * KEYS);
        }
    }

    @Test
    void theStoreWritesUncompressedBlocks() throws RocksDBException {
        try (Store store = new Store(directory, RocksCompressionTest::storeOptions)) {
            store.write(0, KEYS);
            assertEquals(Set.of("NoCompression"), store.compressionNames());
        }
    }

    private static ColumnFamilyOptions storeOptions() {
        return RocksProvider.columnFamilyOptions(FAMILY, new BlockBasedTableConfig());
    }

    /** The store's options with compression, as a build that compresses would write. */
    private static ColumnFamilyOptions compressed() {
        ColumnFamilyOptions options = storeOptions();
        options.setCompressionType(CompressionType.LZ4_COMPRESSION);
        options.setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION);
        return options;
    }

    private static byte[] key(int index) {
        return String.format("%016d", index).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] value(int index) {
        return ("entity " + index + " with a description that repeats, as entity bytes do: "
                + "concept, pattern, semantic, stamp, concept, pattern, semantic, stamp")
                .getBytes(StandardCharsets.UTF_8);
    }

    /** A database with the default column family and one store column family, opened with given options. */
    private static final class Store implements AutoCloseable {
        private final DBOptions dbOptions;
        private final ColumnFamilyOptions defaultOptions = new ColumnFamilyOptions();
        private final ColumnFamilyOptions familyOptions;
        private final List<ColumnFamilyHandle> handles = new ArrayList<>();
        private final RocksDB db;
        private final ColumnFamilyHandle family;

        Store(Path directory, Supplier<ColumnFamilyOptions> options) throws RocksDBException {
            RocksDB.loadLibrary();
            this.familyOptions = options.get();
            this.dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
            this.db = RocksDB.open(dbOptions, directory.resolve("rocks").toString(), List.of(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultOptions),
                    new ColumnFamilyDescriptor(FAMILY.getValue(), familyOptions)), handles);
            this.family = handles.get(1);
        }

        void write(int from, int to) throws RocksDBException {
            for (int index = from; index < to; index++) {
                db.put(family, key(index), value(index));
            }
            try (FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
                db.flush(flush, family);
            }
        }

        void assertReads(int from, int to) throws RocksDBException {
            for (int index = from; index < to; index++) {
                assertArrayEquals(value(index), db.get(family, key(index)), "value " + index);
            }
        }

        /** The compression of each table file of the column family. */
        Set<String> compressionNames() throws RocksDBException {
            Map<String, TableProperties> tables = db.getPropertiesOfAllTables(family);
            assertTrue(!tables.isEmpty(), "no table files");
            Set<String> names = new TreeSet<>();
            tables.values().forEach(table -> names.add(table.getCompressionName()));
            return names;
        }

        @Override
        public void close() {
            handles.forEach(ColumnFamilyHandle::close);
            db.close();
            familyOptions.close();
            defaultOptions.close();
            dbOptions.close();
        }
    }
}
