package dev.ikm.ds.rocks.maps;

import dev.ikm.tinkar.common.id.EntityKey;
import dev.ikm.tinkar.common.id.impl.KeyUtil;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.MutableList;
import org.rocksdb.*;

public class EntityReferencingSemanticMap
        extends RocksDbMap<RocksDB> {

    private static final byte[] emptyValue = new byte[0];

    public EntityReferencingSemanticMap(RocksDB db, ColumnFamilyHandle mapHandle) {
        super(db, mapHandle);

        try (final ColumnFamilyOptions cfo = new ColumnFamilyOptions()) {
            cfo.useFixedLengthPrefixExtractor(8); // first 8 bytes as prefix

            final BlockBasedTableConfig t = new BlockBasedTableConfig()
                    .setFilterPolicy(new BloomFilter(10, false))
                    .setWholeKeyFiltering(false)
                    .setCacheIndexAndFilterBlocks(true);

            cfo.setTableFormatConfig(t);
            cfo.setMemtablePrefixBloomSizeRatio(0.125);
            // use cfo when creating/opening the column family
        }
    }

    @Override
    protected void writeMemoryToDb() {
        // Nothing buffered, all direct writes to the DB. So no need to write before the flush.
    }

    @Override
    protected void closeMap() {
        // Nothing buffered, all direct writes to the DB. So no need to write before the flush.
    }

    public void add(EntityKey entityKey, EntityKey referencingEntityKey) {
        byte[] compoundKey = KeyUtil.entityReferencingSemanticKey(entityKey, referencingEntityKey);
        put(compoundKey, emptyValue);
    }

    public ImmutableList<EntityKey> getReferencingEntityKeys(EntityKey entityKey) {
        return getReferencingEntityKeys(entityKey.longKey());
    }

    public ImmutableList<EntityKey> getReferencingEntityKeysOfPattern(EntityKey entityKey, EntityKey patternEntityKey) {
        return getReferencingEntityKeysOfPattern(entityKey.longKey(), patternEntityKey.patternSequence());
    }

    /**
     * The semantics of one pattern that reference an entity. A compound key is the entity's
     * long key then the semantic's, whose first two bytes are its pattern sequence, so the
     * semantics of one pattern lie together under the entity's key and that sequence: the read
     * seeks straight to them rather than reading every reference and keeping some.
     */
    public ImmutableList<EntityKey> getReferencingEntityKeysOfPattern(long entityKey, int patternSequence) {
        byte[] prefix = new byte[10];
        System.arraycopy(KeyUtil.longToByteArray(entityKey), 0, prefix, 0, 8);
        prefix[8] = (byte) (patternSequence >>> 8);
        prefix[9] = (byte) patternSequence;
        return referencingEntityKeys(prefix);
    }

    public ImmutableList<EntityKey> getReferencingEntityKeys(long longKey) {
        return referencingEntityKeys(KeyUtil.longToByteArray(longKey));
    }

    private ImmutableList<EntityKey> referencingEntityKeys(byte[] prefix) {
        MutableList<EntityKey> results = Lists.mutable.empty();
        long epoch = readEpoch();
        RocksIterator it = borrowIterator(epoch);
        try {
            for (it.seek(prefix); it.isValid(); it.next()) {
                byte[] key = it.key();
                if (!startsWith(key, prefix)) {
                    break;
                }
                results.add(KeyUtil.referencingEntityKeyFromEntityReferencingSemanticKey(key));
            }
        } finally {
            returnIterator(it, epoch);
        }
        return results.toImmutable();
    }


    private static boolean startsWith(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) return false;
        }
        return true;
    }
}
