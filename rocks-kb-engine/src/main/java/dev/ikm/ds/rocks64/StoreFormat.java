package dev.ikm.ds.rocks64;

import dev.ikm.tinkar.common.id.impl.NidLayout;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

import java.io.File;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The column family that names a 64-bit store's format: its layout, its entity format, and the
 * build that created it. RocksDB refuses to open a database whose column family the opener did
 * not list, so every build before this one stops at open without reading or writing a 64-bit
 * store, and this build tells a 64-bit store from a legacy one by the family's presence
 * (design {@code design-2026-10-07-64-bit-rocks-store}).
 */
public final class StoreFormat {

    static final String NAME = "StoreFormat";
    static final byte[] COLUMN_FAMILY = NAME.getBytes(UTF_8);

    static final String LAYOUT = "layout";
    static final String ENTITY_FORMAT = "entityFormat";
    static final String CREATED_BY = "createdBy";
    static final String CREATED_AT = "createdAt";

    private StoreFormat() {
    }

    /** Whether a RocksDB directory holds a 64-bit store: it has the format column family. */
    public static boolean isSixtyFourBit(File rocksDirectory) {
        if (!rocksDirectory.isDirectory() || !new File(rocksDirectory, "CURRENT").exists()) {
            return false;
        }
        try (Options options = new Options()) {
            List<byte[]> families = RocksDB.listColumnFamilies(options, rocksDirectory.getAbsolutePath());
            return families.stream().anyMatch(name -> new String(name, UTF_8).equals(NAME));
        } catch (RocksDBException e) {
            throw new IllegalStateException("Cannot list the column families of " + rocksDirectory, e);
        }
    }

    /** Whether a directory holds any RocksDB database at all. */
    public static boolean holdsADatabase(File rocksDirectory) {
        return rocksDirectory.isDirectory() && new File(rocksDirectory, "CURRENT").exists();
    }

    /** Writes the descriptor of a store this build creates. */
    static void write(WriteBatch batch, ColumnFamilyHandle handle) throws RocksDBException {
        batch.put(handle, LAYOUT.getBytes(UTF_8), NidLayout.SIXTY_FOUR_BIT.displayName().getBytes(UTF_8));
        batch.put(handle, ENTITY_FORMAT.getBytes(UTF_8), Integer.toString(NidLayout.ENTITY_FORMAT_2).getBytes(UTF_8));
        batch.put(handle, CREATED_BY.getBytes(UTF_8), build().getBytes(UTF_8));
        batch.put(handle, CREATED_AT.getBytes(UTF_8), Instant.now().toString().getBytes(UTF_8));
    }

    /** The descriptor as stored. */
    static Map<String, String> read(RocksDB db, ColumnFamilyHandle handle) {
        Map<String, String> descriptor = new TreeMap<>();
        try (RocksIterator it = db.newIterator(handle)) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                descriptor.put(new String(it.key(), UTF_8), new String(it.value(), UTF_8));
            }
        }
        return descriptor;
    }

    /** Refuses a store whose descriptor names a layout or format this build does not read. */
    static void verify(Map<String, String> descriptor, File root) {
        String layout = descriptor.get(LAYOUT);
        String format = descriptor.get(ENTITY_FORMAT);
        if (!NidLayout.SIXTY_FOUR_BIT.displayName().equals(layout)
                || !Integer.toString(NidLayout.ENTITY_FORMAT_2).equals(format)) {
            throw new IllegalStateException(root + " names layout " + layout + " and entity format " + format
                    + ", which this build does not read (it reads " + NidLayout.SIXTY_FOUR_BIT.displayName()
                    + ", format " + NidLayout.ENTITY_FORMAT_2 + "); created by " + descriptor.get(CREATED_BY));
        }
    }

    private static String build() {
        String version = StoreFormat.class.getPackage().getImplementationVersion();
        return "rocks-kb " + (version == null ? "development build" : version);
    }
}
