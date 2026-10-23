package dev.ikm.ds.rocks64;

import dev.ikm.tinkar.entity.EntityFormat2;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * The records of a store created in entity format 2, the first 64-bit format, read and written
 * as format 3 (IKE-Network/ike-issues#1313).
 *
 * <p>Such a store opens as it is. A record is read in format 3 whichever format it was stored
 * in ({@link #current}): every place the store reads a record turns a format 2 record into the
 * same entity's format 3 record, so nothing above the store sees format 2. A record is always
 * written in format 3, so a store that is used becomes format 3 one written record at a time.
 * Format 3 kept format 2's nids and dropped only what a nid already says, so the conversion
 * changes no nid, identity or value.
 *
 * <p>The upgrade is optional. {@code -D}{@value #UPGRADE_PROPERTY}{@code =true} makes the store
 * rewrite every record still in format 2 as it opens ({@link #run}), then record that it holds
 * format 3 alone. The pass skips a record already in format 3, so one that stopped part way
 * finishes when the store next opens with the property.
 */
public final class FormatUpgrade {

    private static final Logger LOG = LoggerFactory.getLogger(FormatUpgrade.class);

    /** The system property that upgrades a format 2 store as it opens. */
    public static final String UPGRADE_PROPERTY = "rocks.upgradeEntityFormat";

    /** Records converted and written per batch. */
    private static final int BATCH = 20_000;

    private FormatUpgrade() {
    }

    /**
     * A stored record as the store reads it: in format 3, converted from format 2 if it was stored
     * in that format; as stored otherwise. Null stays null.
     */
    static byte[] current(byte[] stored) {
        return EntityFormat2.isFormat2(stored) ? EntityFormat2.upgrade(stored) : stored;
    }

    /** Whether the store should be upgraded as it opens. */
    static boolean requested() {
        return Boolean.getBoolean(UPGRADE_PROPERTY);
    }

    /**
     * Rewrites every format 2 record in format 3, then records that the store holds format 3
     * alone.
     *
     * @param db       the open database
     * @param entities the entities column family
     * @param format   the store-format column family
     * @param name     the store's name, for the log
     * @throws IllegalStateException naming the nid of a record format 3 refuses, the store's
     *                               descriptor left naming format 2
     */
    static void run(RocksDB db, ColumnFamilyHandle entities, ColumnFamilyHandle format, String name)
            throws RocksDBException {
        LOG.info("Upgrading the records of {} from entity format 2 to format 3 ({}=true)", name, UPGRADE_PROPERTY);
        long started = System.nanoTime();
        long upgraded = 0;
        long current = 0;
        List<byte[]> keys = new ArrayList<>(BATCH);
        List<byte[]> records = new ArrayList<>(BATCH);
        try (ReadOptions ro = new ReadOptions().setFillCache(false).setTotalOrderSeek(true);
             RocksIterator it = db.newIterator(entities, ro);
             WriteOptions wo = new WriteOptions()) {
            for (it.seekToFirst(); it.isValid(); it.next()) {
                byte[] record = it.value();
                if (!EntityFormat2.isFormat2(record)) {
                    current++;
                    continue;
                }
                keys.add(it.key());
                records.add(record);
                if (keys.size() == BATCH) {
                    upgraded += write(db, entities, wo, keys, records);
                    if (upgraded % 1_000_000 < BATCH) {
                        LOG.info("{}: {} records upgraded", name, String.format("%,d", upgraded));
                    }
                    keys.clear();
                    records.clear();
                }
            }
            upgraded += write(db, entities, wo, keys, records);
            try (WriteBatch batch = new WriteBatch()) {
                StoreFormat.writeUpgraded(batch, format);
                db.write(wo, batch);
            }
        }
        LOG.info("Upgraded {} to entity format 3 in {} s: {} records rewritten, {} already in format 3", name,
                String.format("%.1f", (System.nanoTime() - started) / 1e9), String.format("%,d", upgraded),
                String.format("%,d", current));
    }

    /** Converts a batch in parallel and writes it in one write; returns how many records it wrote. */
    private static int write(RocksDB db, ColumnFamilyHandle entities, WriteOptions wo, List<byte[]> keys,
                             List<byte[]> records) throws RocksDBException {
        if (keys.isEmpty()) {
            return 0;
        }
        byte[][] upgraded = IntStream.range(0, records.size()).parallel()
                .mapToObj(i -> convert(keys.get(i), records.get(i)))
                .toArray(byte[][]::new);
        try (WriteBatch batch = new WriteBatch()) {
            for (int i = 0; i < keys.size(); i++) {
                batch.put(entities, keys.get(i), upgraded[i]);
            }
            db.write(wo, batch);
        }
        return keys.size();
    }

    private static byte[] convert(byte[] key, byte[] record) {
        try {
            return EntityFormat2.upgrade(record);
        } catch (RuntimeException refused) {
            throw new IllegalStateException("The format 2 record of nid " + Keys.nid(key)
                    + " cannot be written in format 3: " + refused.getMessage(), refused);
        }
    }
}
