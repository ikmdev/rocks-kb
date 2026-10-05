package dev.ikm.ds.rocks.maps;


import dev.ikm.ds.rocks.spliterator.LongSpliteratorOfPattern;
import dev.ikm.ds.rocks.internal.Get;
import dev.ikm.tinkar.common.id.EntityKey;
import dev.ikm.tinkar.common.id.impl.KeyUtil;
import dev.ikm.tinkar.common.service.PrimitiveDataService;
import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.primitive.ByteLists;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.list.primitive.ImmutableByteList;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongConsumer;
import java.util.function.ObjIntConsumer;

import static dev.ikm.tinkar.entity.EntityRecordFactory.ENTITY_FORMAT_VERSION;

public class EntityMap
        extends RocksDbMap<RocksDB> {

    private static final Logger LOG = LoggerFactory.getLogger(EntityMap.class);

    private final UuidEntityKeyMap uuidEntityKeyMap;

    // Flush accounting: enqueuedCount is incremented by producers immediately before a
    // record enters pendingWrites (nothing throwable in between, so a count always has a
    // record behind it); flushedCount is incremented only by the single writer thread
    // after a batch commits. Both are monotonic, so a flush wait can never be wedged by
    // out-of-order batches or a put() that failed validation (ike-issues#1058, #1059).
    private final AtomicLong enqueuedCount = new AtomicLong(0);
    final AtomicLong flushedCount = new AtomicLong(0);
    final ReentrantLock flushLock = new ReentrantLock();
    final Condition flushed = flushLock.newCondition();
    // running flag to control writer lifecycle
    private final AtomicBoolean running = new AtomicBoolean(true);

    private record WriteRecord(long key, ImmutableList<ImmutableByteList> entityParts) {}

    private final ConcurrentHashMap<Long, WriteRecord> pendingWritesMap = new ConcurrentHashMap<>();


    private final LinkedBlockingDeque<WriteRecord> pendingWrites = new LinkedBlockingDeque<>();

    final Thread writeThread = new Thread(() -> {
        // Drain remaining writes even after running=false
        while (running.get() || !pendingWritesMap.isEmpty()) {
            WriteRecord firstRecord = null;
            try {
                // BLOCKING WAIT: Wait for work to arrive.
                // Using poll with timeout allows checking the 'running' flag periodically.
                firstRecord = pendingWrites.poll(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            if (firstRecord == null) {
                continue;
            }

            try (WriteBatch batch = new WriteBatch();
                 WriteOptions writeOptions = new WriteOptions()
                         .setDisableWAL(true)
                         .setSync(false)
                         .setNoSlowdown(true)) {

                // Initialize list with the item we already retrieved
                MutableList<WriteRecord> writeRecords = Lists.mutable.with(firstRecord);

                // Add the first record immediately
                addToBatch(batch, firstRecord);

                int batchCount = 1;

                // Batch up additional records if immediately available
                // We don't need to wait (poll with timeout) here; we want to batch what's ready
                while (batchCount++ < 16384) {
                    WriteRecord writeRecord = pendingWrites.poll(); // Non-blocking poll for subsequent items
                    if (writeRecord == null) {
                        break;
                    }
                    addToBatch(batch, writeRecord);
                    writeRecords.add(writeRecord);
                }

                long backoffMillis = 1;
                final int maxAttempts = 8;

                for (int attempt = 0; attempt <= maxAttempts; attempt++) {
                    try {
                        db.write(writeOptions, batch);
                        break; // Success, exit loop
                    } catch (RocksDBException e) {
                        org.rocksdb.Status st = e.getStatus();
                        boolean retryable = st != null &&
                                (st.getCode() == org.rocksdb.Status.Code.Busy ||
                                 st.getCode() == org.rocksdb.Status.Code.Incomplete);

                        // If this was the last attempt, or error is not retryable, fail hard
                        if (!retryable || attempt == maxAttempts) {
                            throw e;
                        }

                        try {
                            Thread.sleep(backoffMillis);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            // If interrupted during backoff, abort operations
                            throw new RuntimeException("Write thread interrupted during retry", ie);
                        }
                        backoffMillis = Math.min(200, backoffMillis * 2);
                    }
                }

                // Before any record leaves pendingWritesMap, so a reader that no longer finds
                // a record pending reads with an iterator that sees it.
                advanceWriteEpoch();

                // A record superseded by a later merge fails this conditional remove;
                // the superseding record is itself queued and clears the entry when it lands.
                for (WriteRecord writeRecord : writeRecords) {
                    pendingWritesMap.remove(writeRecord.key, writeRecord);
                }

                flushedCount.addAndGet(writeRecords.size());
                flushLock.lock();
                try {
                    flushed.signalAll();
                } finally {
                    flushLock.unlock();
                }
            } catch (RocksDBException e) {
                LOG.error("Failed to write batch to RocksDB", e);
                throw new RuntimeException(e);
            }
        }
    }, "EntityMap-Writer");

    // Helper method to reduce code duplication
    private void addToBatch(WriteBatch batch, WriteRecord writeRecord) throws RocksDBException {
        boolean isStamp = isStamp(writeRecord.entityParts.get(0));
        for (int i = 0; i < writeRecord.entityParts.size(); i++) {
            byte[] key = makeKey(writeRecord.key, isStamp, i, writeRecord.entityParts);
            byte[] partBytes = writeRecord.entityParts.get(i).toArray();
            if (partBytes == null || partBytes.length == 0) {
                throw new IllegalStateException("writeThread: entityParts is empty");
            }
            batch.put(mapHandle, key, partBytes);
        }
    }

    public EntityMap(RocksDB db, ColumnFamilyHandle mapHandle, UuidEntityKeyMap uuidEntityKeyMap) {
        super(db, mapHandle);
        this.uuidEntityKeyMap = uuidEntityKeyMap;
        writeThread.setDaemon(true);
        writeThread.start();
    }

    /**
     * Gracefully stops the writer and flushes pending data. Call this before closing the DB.
     */
    public final void closeMap() {
        // First, ensure all pending writes are flushed while the writer is still running
        writeMemoryToDb();
        // Then signal shutdown and wait for the writer to drain any small tail
        running.set(false);
        try {
            writeThread.join(30_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (writeThread.isAlive()) {
            LOG.warn("EntityMap-Writer did not exit within 30 s of shutdown");
        }
    }


    /**
     * Waits until every record handed to the writer before this call has been written
     * to RocksDB, bounded by a two-minute deadline. Records enqueued by puts that are
     * still in flight when this method starts are not covered by the wait.
     */
    @Override
    protected void writeMemoryToDb() {
        long target = enqueuedCount.get();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120); // 2m bound; adjust as needed
        flushLock.lock();
        try {
            while (flushedCount.get() < target) {
                long remainingNanos = deadline - System.nanoTime();
                if (remainingNanos <= 0) {
                    LOG.warn("Timed out waiting for flush. flushed={} target={}",
                            flushedCount.get(), target);
                    break;
                }
                try {
                    flushed.awaitNanos(Math.min(TimeUnit.MILLISECONDS.toNanos(200), remainingNanos));
                } catch (InterruptedException e) {
                    // preserve interrupt, keep waiting within bound
                    Thread.currentThread().interrupt();
                }
            }
        } finally {
            flushLock.unlock();
        }
    }

    public byte[] get(EntityKey key) {
        return get(key.longKey());
    }

    /**
     * The entity's bytes, or {@code null} if the store holds no entity under the key. One seek
     * answers both: the chronology part lies under the entity's own key and its versions under
     * keys that extend it, so a seek that lands elsewhere means there is no entity.
     */
    public byte[] get(long longKey) {
        WriteRecord pendingWrite = pendingWritesMap.get(longKey);
        if (pendingWrite != null) {
            return mergeParts(pendingWrite.entityParts);
        }
        byte[] entityPrefix = KeyUtil.longToByteArray(longKey);
        // Read after the pending check: the epoch then covers any record that has left it.
        long epoch = readEpoch();
        RocksIterator iterator = borrowIterator(epoch);
        try {
            iterator.seek(entityPrefix);
            MutableList<byte[]> parts = readParts(iterator, entityPrefix);
            return parts == null ? null : assemble(parts);
        } finally {
            returnIterator(iterator, epoch);
        }
    }


    /**
     * Reads the parts of the entity whose key the iterator is on, if it is on that entity's
     * chronology part, leaving the iterator after the entity's last version.
     *
     * @return the parts, chronology first, or {@code null} if the iterator is not on the entity
     */
    private static MutableList<byte[]> readParts(RocksIterator iterator, byte[] entityPrefix) {
        if (!iterator.isValid() || !startsWith(iterator.key(), entityPrefix)) {
            return null;
        }
        MutableList<byte[]> parts = Lists.mutable.ofInitialCapacity(4);
        parts.add(iterator.value());
        for (iterator.next(); iterator.isValid() && startsWith(iterator.key(), entityPrefix); iterator.next()) {
            parts.add(iterator.value());
        }
        return parts;
    }

    /**
     * The entity's bytes, from its parts as stored, in the layout {@link #mergeParts} writes,
     * copied once into an array of the exact size.
     */
    private static byte[] assemble(MutableList<byte[]> parts) {
        int size = 4 + 4 + 1 + parts.get(0).length + 4;
        for (int i = 1; i < parts.size(); i++) {
            size += 4 + parts.get(i).length;
        }
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(size);
        buf.putInt(parts.size());
        byte[] chronology = parts.get(0);
        buf.putInt(chronology.length + 4 + 1); // the chronology, its 4 byte size and the 1 byte format version
        buf.put(ENTITY_FORMAT_VERSION);
        buf.put(chronology);
        buf.putInt(parts.size() - 1);
        for (int i = 1; i < parts.size(); i++) {
            buf.putInt(parts.get(i).length);
            buf.put(parts.get(i));
        }
        return buf.array();
    }

    private static int nidOf(byte[] chronologyPart) {
        if (chronologyPart.length < 5) {
            throw new IllegalArgumentException("chronology part is too small to contain nid at [1..4]: "
                    + java.util.Arrays.toString(chronologyPart));
        }
        return ((chronologyPart[1] & 0xFF) << 24) |
                ((chronologyPart[2] & 0xFF) << 16) |
                ((chronologyPart[3] & 0xFF) << 8)  |
                (chronologyPart[4] & 0xFF);
    }

    private static byte[] mergeParts(ImmutableList<ImmutableByteList> entityParts) {
        int size = entityParts.stream().mapToInt(b -> b.size()).sum() + entityParts.size() * 16;
        if (size == 0) {
            LOG.error("mergeParts: entityParts is empty");
            return null;
        }
        ByteBuf buf = ByteBufPool.allocate(size);
        buf.writeInt(entityParts.size());
        for (int i = 0; i < entityParts.size(); i++) {
            if (i == 0) {
                buf.writeInt(entityParts.get(i).size() + 4 + 1); // number of bytes in the first array + 4 byte size + 1 byte format version.
                buf.writeByte(ENTITY_FORMAT_VERSION);
                buf.put(entityParts.get(i).toArray());
                buf.writeInt(entityParts.size() -1);
            } else {
                buf.writeInt(entityParts.get(i).size()); // Number of bytes in the version part
                buf.put(entityParts.get(i).toArray());
            }
        }
        byte[] results = new byte[buf.readRemaining()];
        buf.read(results, 0, results.length);
        return results;
    }

    private static boolean startsWith(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) return false;
        }
        return true;
    }


    public void put(EntityKey entityKey, byte[] value) {
        if (entityKey instanceof EntityKey.EntityVersionKey) {
            throw new IllegalArgumentException("EntityVersionKey should not be used for put, only the EntityKey.");
        }
        put(entityKey.longKey(), value);
    }

    /**
     * Accumulates the entity's version parts into the pending record for {@code longKey}
     * and hands the result to the writer thread.
     *
     * @param longKey the entity's long key
     * @param value   the serialized entity chronology; its version parts are merged with
     *                any parts already pending for the key
     * @throws IllegalStateException if the chronicle part of {@code value} differs from
     *                               the chronicle part already pending for the key
     */
    public void put(long longKey, byte[] value) {
        // a possibly empty existing part list.
        ImmutableList<ImmutableByteList> newParts = extractVersionParts(value).toImmutable();
        WriteRecord newRecord = new WriteRecord(longKey, newParts);

        // Captures the record an unchanged merge left in place, so the enqueue decision
        // below can tell it apart from a merged superset (ike-issues#1060).
        WriteRecord[] unchangedRecord = new WriteRecord[1];

        WriteRecord recordToWrite = pendingWritesMap.merge(longKey, newRecord, (oldRecord, incomingRecord) -> {
            if (!incomingRecord.entityParts.get(0).equals(oldRecord.entityParts.get(0))) {
                throw new IllegalStateException("Entity parts[0] must be the same for the same longKey.");
            }
            MutableList<ImmutableByteList> mergedParts = oldRecord.entityParts.toList();
            for (int i = 1; i < incomingRecord.entityParts.size(); i++) {
                if (!mergedParts.contains(incomingRecord.entityParts.get(i))) {
                    mergedParts.add(incomingRecord.entityParts.get(i));
                }
            }
            boolean changed = mergedParts.size() != oldRecord.entityParts.size();
            if (changed) {
                return new WriteRecord(longKey, mergedParts.toImmutable());
            }
            unchangedRecord[0] = oldRecord;
            return oldRecord;
        });

        if (recordToWrite != unchangedRecord[0]) {
            // Fresh insert or merged superset: queue exactly the record the map now holds.
            // An unchanged merge queues nothing — the record already holding the key was
            // queued when it became the map value.
            enqueuedCount.incrementAndGet();
            pendingWrites.add(recordToWrite);
        }
    }

    private static byte[] makeKey(long longKey, boolean isStamp, int partIndex, ImmutableList<ImmutableByteList> chronologyParts) {
        if (partIndex == 0) {
            return KeyUtil.longToByteArray(longKey);
        }
        if (isStamp) {
            return KeyUtil.stampVersionKey(longKey, Get.stampSequenceForStampNid(getStampNid(chronologyParts.get(partIndex))), (byte) partIndex);
        }
        return KeyUtil.elementVersionKey(longKey, Get.stampSequenceForStampNid(getStampNid(chronologyParts.get(partIndex))));
    }

    /**
     * See dev.ikm.tinkar.entity.EntityRecordFactory.getBytes() for the format of the byte array.
     *
     * @param bytes
     * @throws IOException
     */
    private static MutableList<ImmutableByteList> extractVersionParts(byte[] bytes) {
        ByteBuf readBuf = ByteBuf.wrapForReading(bytes);
        final int partCount = readBuf.readInt();
        MutableList<ImmutableByteList> result = Lists.mutable.ofInitialCapacity(partCount);
        for (int i = 0; i < partCount; i++) {
            int partSize = readBuf.readInt();
            if (i == 0) {
                byte localEntityFormat = readBuf.readByte();
                if (localEntityFormat != ENTITY_FORMAT_VERSION) {
                    throw new IllegalStateException("All entities should be the same format. Found: " + ENTITY_FORMAT_VERSION + " != " + localEntityFormat);
                }
                // The first array is the chronicle and has a field for the number of versions...
                // Add one for the entityFormat token.
                byte[] tmpPart = new byte[partSize - 5];
                readBuf.read(tmpPart);
                result.add(ByteLists.immutable.of(tmpPart));
                int versionCount = readBuf.readInt();
                if (versionCount != partCount - 1) {
                    throw new IllegalStateException("Malformed data. versionCount: " +
                            versionCount + " arrayCount: " + partCount);
                }
                // Version count is not included as the version count may change as a result of merge.
                // It must be added back in after sorting unique versions.
            } else {
                byte[] tmpPart = new byte[partSize];
                readBuf.read(tmpPart);
                result.add(ByteLists.immutable.of(tmpPart));
            }
        }
        return result;
    }
    private static int getStampNid(ImmutableByteList result) {
        int stampNid = ((result.get(1) & 0xFF) << 24) |
                ((result.get(2) & 0xFF) << 16) |
                ((result.get(3) & 0xFF) << 8) |
                ((result.get(4) & 0xFF) << 0);
        return stampNid;
    }

    private static boolean isStamp(ImmutableByteList chronologyPart) {
        return chronologyPart.get(0) == PrimitiveDataService.STAMP_DATA_TYPE;
    }

    public void forEach(ObjIntConsumer<byte[]> entityHandler) {
        final int allowedErrors = 5;
        int errors = 0;
        int count = 0;
        try (final Snapshot s = db.getSnapshot();
             final ReadOptions ro =
                     new ReadOptions().setPrefixSameAsStart(false) // 2 byte Column Family prefix, and 8 byte key prefix
                             .setTotalOrderSeek(false)
                             .setSnapshot(s);
             RocksIterator it = rocksIterator(ro)) {
            for (it.seekToFirst(); it.isValid(); ) {
                // The iterator is on an entity's chronology part; read it and its versions.
                MutableList<byte[]> parts = readParts(it, it.key());
                entityHandler.accept(assemble(parts), nidOf(parts.get(0)));
            }
        }
    }


    public void scanEntitiesInRange(LongSpliteratorOfPattern spliterator,
                                    ObjIntConsumer<byte[]> entityHandler) {
        Objects.requireNonNull(entityHandler, "entityHandler");

        try (final Snapshot s = db.getSnapshot();
             final ReadOptions ro =
                     new ReadOptions().setPrefixSameAsStart(false) // 2 byte Column Family prefix, and 8 byte key prefix
                                      .setTotalOrderSeek(false)
                                      .setSnapshot(s);
             final RocksIterator it = rocksIterator(ro)) {
            byte[] firstPrefix = KeyUtil.longToByteArray(spliterator.peek());
            it.seek(firstPrefix);

            if (it.isValid()) {
                while (spliterator.tryAdvance((LongConsumer) longKey -> {
                    byte[] entityPrefix = KeyUtil.longToByteArray(longKey);
                    // Ensure we are positioned at or after this entity
                    if (!it.isValid() || !startsWith(it.key(), entityPrefix)) {
                        it.seek(entityPrefix);
                    }

                    // If entity exists, consume its chronology and versions
                    MutableList<byte[]> parts = readParts(it, entityPrefix);
                    if (parts != null) {
                        entityHandler.accept(assemble(parts), nidOf(parts.get(0)));
                    }
                }));
            }
        }
    }

}
