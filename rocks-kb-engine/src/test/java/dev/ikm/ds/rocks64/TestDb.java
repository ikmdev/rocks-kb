package dev.ikm.ds.rocks64;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** A RocksDB with the engine's column families, plainly configured, for tests of the maps. */
final class TestDb implements AutoCloseable {

    final RocksDB db;
    private final DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
    private final List<ColumnFamilyOptions> familyOptions = new ArrayList<>();
    private final List<ColumnFamilyHandle> handles = new ArrayList<>();

    TestDb(File directory) {
        RocksDB.loadLibrary();
        List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
        for (Rocks64Store.Family family : Rocks64Store.Family.values()) {
            ColumnFamilyOptions cf = new ColumnFamilyOptions();
            if (family.prefixBytes > 0) {
                cf.useFixedLengthPrefixExtractor(family.prefixBytes);
            }
            familyOptions.add(cf);
            descriptors.add(new ColumnFamilyDescriptor(family.name, cf));
        }
        try {
            db = RocksDB.open(options, directory.getAbsolutePath(), descriptors, handles);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    ColumnFamilyHandle handle(Rocks64Store.Family family) {
        return handles.get(family.ordinal());
    }

    void putRecord(long nid, byte[] record) {
        try {
            db.put(handle(Rocks64Store.Family.ENTITIES), Keys.of(nid), record);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    void putCounter(int pattern, int next) {
        try {
            db.put(handle(Rocks64Store.Family.DEFAULT), ByteBuffer.allocate(4).putInt(pattern).array(),
                    ByteBuffer.allocate(4).putInt(next).array());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        handles.forEach(ColumnFamilyHandle::close);
        db.close();
        familyOptions.forEach(ColumnFamilyOptions::close);
        options.close();
    }
}
