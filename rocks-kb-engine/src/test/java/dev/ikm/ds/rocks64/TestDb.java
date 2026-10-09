package dev.ikm.ds.rocks64;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
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
    private final List<Options> ingestOptions = new ArrayList<>();
    private final File directory;

    TestDb(File directory) {
        this.directory = directory;
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

    /** How the identity map writes SST files into this database, from the given number of entries. */
    IdentityMap.SstIngest identityIngest(long threshold) {
        Options options = new Options(this.options, familyOptions.get(Rocks64Store.Family.IDENTITIES.ordinal()));
        ingestOptions.add(options);
        return new IdentityMap.SstIngest(options, new File(directory, "ingest"), threshold);
    }

    /** Load-phase ingestion for a record map, with the entity and reference columns' options. */
    RecordMap.Ingest recordIngest() {
        return recordIngest(RecordMap.RUN_BYTES, RecordMap.CHUNK_PAIRS);
    }

    RecordMap.Ingest recordIngest(long runBytes, int chunkPairs) {
        Options entities = new Options(this.options, familyOptions.get(Rocks64Store.Family.ENTITIES.ordinal()));
        Options references = new Options(this.options, familyOptions.get(Rocks64Store.Family.REFERENCES.ordinal()));
        ingestOptions.add(entities);
        ingestOptions.add(references);
        return new RecordMap.Ingest(entities, references, new File(directory, "ingest"), runBytes, chunkPairs);
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
        ingestOptions.forEach(Options::close);
        familyOptions.forEach(ColumnFamilyOptions::close);
        options.close();
    }
}
