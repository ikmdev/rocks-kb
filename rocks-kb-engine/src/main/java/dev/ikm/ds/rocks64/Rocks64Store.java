package dev.ikm.ds.rocks64;

import dev.ikm.ds.rocks.RocksEngine;
import dev.ikm.tinkar.common.id.EntityKey;
import dev.ikm.tinkar.common.id.Nid;
import dev.ikm.tinkar.common.id.PublicId;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.common.id.impl.NidLayout;
import dev.ikm.tinkar.common.service.DataActivity;
import dev.ikm.tinkar.common.service.EntityRecordFormat2;
import dev.ikm.tinkar.common.service.NidGenerator;
import dev.ikm.tinkar.common.service.PluggableService;
import dev.ikm.tinkar.common.service.PrimitiveDataSearchResult;
import dev.ikm.tinkar.common.service.SearchService;
import dev.ikm.tinkar.common.service.ServiceKeys;
import dev.ikm.tinkar.common.service.ServiceLifecycleManager;
import dev.ikm.tinkar.common.service.ServiceProperties;
import dev.ikm.tinkar.common.util.SetOnce;
import dev.ikm.tinkar.common.util.time.Stopwatch;
import dev.ikm.tinkar.entity.ChangeSetWriterService;
import dev.ikm.tinkar.entity.Entity;
import dev.ikm.tinkar.entity.EntityService;
import dev.ikm.tinkar.entity.SemanticEntity;
import org.eclipse.collections.api.block.procedure.primitive.LongProcedure;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.list.primitive.ImmutableLongList;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ChecksumType;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.IndexType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.ObjLongConsumer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The 64-bit Rocks engine: a store whose keys are 64-bit nids, a pattern sequence over an
 * element sequence, holding one format 2 record per entity (design
 * {@code design-2026-10-07-64-bit-rocks-store}; IKE-Network/ike-issues#1258). It shares no
 * storage code with the legacy {@link dev.ikm.ds.rocks.RocksProvider}; the provider's
 * controllers choose between the two by the store's format, which this engine names in its
 * {@link StoreFormat} column family.
 *
 * <p>Four column families beside the default, which holds the counters: {@code Entities}, nid to
 * record; {@code References}, referenced nid then referencing nid, to nothing, so the semantics
 * referencing a component are a prefix range; {@code Identities}, UUID to nid; and
 * {@code StoreFormat}. Every nid is minted under a pattern ({@link IdentityMap}), records are
 * written behind by one thread as unions ({@link RecordMap}), and scans read one snapshot in
 * byte-sized ranges ({@link Scanner}).
 */
public final class Rocks64Store implements RocksEngine, NidGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(Rocks64Store.class);

    /** The block cache's size in bytes: {@code rocks.blockCache.bytes}, else 1 GB. */
    static final long BLOCK_CACHE_BYTES = Long.getLong("rocks.blockCache.bytes", 1L << 30);
    static final int BLOOM_BITS_PER_KEY = 10;
    static final File DEFAULT_DATA_DIRECTORY = new File(new File(System.getProperty("user.home"), "Solor"), "rocksdb");
    private static final int ENUMERATION_CHUNK = 65_536;
    private static final int POINT_READ_CHUNK = 4_096;

    /** The column families, in the order their handles are kept. */
    enum Family {
        DEFAULT(RocksDB.DEFAULT_COLUMN_FAMILY, 4L << 20, -1, true),
        ENTITIES("Entities".getBytes(UTF_8), 256L << 20, -1, true),
        REFERENCES("References".getBytes(UTF_8), 64L << 20, 8, false),
        IDENTITIES("Identities".getBytes(UTF_8), 64L << 20, -1, true),
        FORMAT(StoreFormat.COLUMN_FAMILY, 1L << 20, -1, true);

        final byte[] name;
        final long writeBufferBytes;
        final int prefixBytes;
        final boolean wholeKeyFiltering;

        Family(byte[] name, long writeBufferBytes, int prefixBytes, boolean wholeKeyFiltering) {
            this.name = name;
            this.writeBufferBytes = writeBufferBytes;
            this.prefixBytes = prefixBytes;
            this.wholeKeyFiltering = wholeKeyFiltering;
        }
    }

    private final String name;
    private final File root;
    private final RocksDB db;
    private final DBOptions dbOptions;
    private final Cache blockCache;
    private final List<ColumnFamilyDescriptor> descriptors;
    private final List<ColumnFamilyHandle> handles = new ArrayList<>();
    private final List<BloomFilter> filters = new ArrayList<>();
    private final Counters counters;
    private final IdentityMap identityMap;
    /** The identity column's options as a whole, for the SST files the identity map writes. */
    private final Options identityFileOptions;
    /** The entity and reference columns' options as a whole, for the SST files a load phase ingests. */
    private final Options entityFileOptions;
    private final Options referenceFileOptions;
    private final RecordMap recordMap;
    private final Scanner scanner;

    private final LongAdder writeSequence = new LongAdder();
    private final Semaphore startupShutdown = new Semaphore(1);
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final SetOnce<ImmutableList<ChangeSetWriterService>> changeSetWriters = new SetOnce<>();
    /** Shared by every reference lookup: an iterator confined to the 8-byte prefix it seeks. Made once the library is loaded. */
    private final ReadOptions referenceReadOptions;
    /**
     * Reference iterators kept for reuse, each stamped with the epoch of the references column
     * when it was made: one made before a batch of references landed cannot see them, so it is
     * closed rather than reused once the epoch moves. Creating an iterator costs more than the
     * seek and the few keys a lookup reads, as the legacy engine's pool found.
     */
    private final ConcurrentLinkedQueue<PooledIterator> referenceIterators = new ConcurrentLinkedQueue<>();
    private final AtomicInteger pooledReferenceIterators = new AtomicInteger();
    private static final int MAX_POOLED_REFERENCE_ITERATORS = 64;

    private record PooledIterator(RocksIterator iterator, long epoch) {
    }
    private final SetOnce<SearchService> searchService = new SetOnce<>();
    private volatile boolean loadPhase = false;

    /**
     * Opens the store at the configured data root ({@link ServiceKeys#DATA_STORE_ROOT}), creating
     * it if the root holds no database.
     *
     * @return the open store
     * @throws IllegalStateException if the root holds a database in another format, or was
     *                               expected empty and is not
     */
    public static Rocks64Store open() {
        File root = ServiceProperties.get(ServiceKeys.DATA_STORE_ROOT, DEFAULT_DATA_DIRECTORY);
        boolean expectEmpty = ServiceProperties.get(ServiceKeys.DATA_STORE_EXPECT_EMPTY, Boolean.FALSE);
        if (expectEmpty) {
            assertEmpty(root);
            ServiceProperties.set(ServiceKeys.DATA_STORE_EXPECT_EMPTY, Boolean.FALSE);
        }
        ServiceProperties.set(ServiceKeys.DATA_STORE_ROOT, root);
        return new Rocks64Store(root);
    }

    private static void assertEmpty(File root) {
        if (!root.exists()) {
            return;
        }
        if (!root.isDirectory()) {
            throw new IllegalStateException("Configured DATA_STORE_ROOT is not a directory: " + root.getAbsolutePath());
        }
        String[] entries = root.list();
        if (entries != null && entries.length > 0) {
            throw new IllegalStateException("Expected empty DATA_STORE_ROOT but found contents: " + root.getAbsolutePath());
        }
    }

    private Rocks64Store(File root) {
        startupShutdown.acquireUninterruptibly();
        try {
            Stopwatch stopwatch = new Stopwatch();
            this.root = root;
            this.name = root.getName();
            File rocks = new File(root, "rocks");
            File logs = new File(root, "rocks-logs");
            for (File directory : List.of(root, rocks, logs)) {
                if (!directory.isDirectory() && !directory.mkdirs()) {
                    throw new IllegalStateException("Could not create " + directory.getAbsolutePath());
                }
            }
            boolean creating = !StoreFormat.holdsADatabase(rocks);
            if (!creating && !StoreFormat.isSixtyFourBit(rocks)) {
                throw new IllegalStateException(rocks + " holds a store in a legacy layout, which the 64-bit engine does not open");
            }
            LOG.info("{} the 64-bit Rocks store at {}", creating ? "Creating" : "Opening", root.getAbsolutePath());

            RocksDB.loadLibrary();
            this.referenceReadOptions = new ReadOptions().setPrefixSameAsStart(true);
            this.blockCache = new LRUCache(BLOCK_CACHE_BYTES);
            this.descriptors = Arrays.stream(Family.values())
                    .map(family -> new ColumnFamilyDescriptor(family.name, columnFamilyOptions(family)))
                    .toList();
            this.dbOptions = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true)
                    .setIncreaseParallelism(Runtime.getRuntime().availableProcessors())
                    .setMaxBackgroundJobs(Math.max(8, Runtime.getRuntime().availableProcessors()))
                    .setAllowConcurrentMemtableWrite(true)
                    .setEnablePipelinedWrite(true)
                    .setBytesPerSync(8L << 20)
                    .setWalBytesPerSync(8L << 20)
                    .setDbLogDir(logs.getAbsolutePath())
                    .setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
            RocksDB opened;
            try {
                opened = RocksDB.open(dbOptions, rocks.getAbsolutePath(), descriptors, handles);
            } catch (RocksDBException e) {
                closeNative();
                throw new RuntimeException("Could not open " + rocks, e);
            }
            this.db = opened;
            try {
                NidLayout.activate(NidLayout.SIXTY_FOUR_BIT);
                this.counters = Counters.load(db, handle(Family.DEFAULT), handle(Family.ENTITIES));
                this.identityFileOptions = new Options(dbOptions, descriptors.get(Family.IDENTITIES.ordinal()).getOptions());
                this.identityMap = new IdentityMap(db, handle(Family.IDENTITIES), counters,
                        new IdentityMap.SstIngest(identityFileOptions, new File(root, "rocks-ingest"), IdentityMap.SST_THRESHOLD));
                if (creating) {
                    identityMap.bootstrap();
                    try (WriteBatch batch = new WriteBatch(); WriteOptions options = new WriteOptions()) {
                        StoreFormat.write(batch, handle(Family.FORMAT));
                        counters.save(batch, handle(Family.DEFAULT));
                        db.write(options, batch);
                    }
                } else {
                    StoreFormat.verify(StoreFormat.read(db, handle(Family.FORMAT)), rocks);
                    identityMap.verifyBindings();
                }
                this.entityFileOptions = new Options(dbOptions, descriptors.get(Family.ENTITIES.ordinal()).getOptions());
                this.referenceFileOptions = new Options(dbOptions, descriptors.get(Family.REFERENCES.ordinal()).getOptions());
                this.recordMap = new RecordMap(db, handle(Family.ENTITIES), handle(Family.REFERENCES), this::isCanceledStampNid,
                        new RecordMap.Ingest(entityFileOptions, referenceFileOptions, new File(root, "rocks-ingest")));
                this.scanner = new Scanner(db, handle(Family.ENTITIES), counters);
            } catch (RocksDBException | RuntimeException e) {
                closeDatabaseQuietly();
                throw e instanceof RuntimeException runtime ? runtime : new RuntimeException(e);
            }
            stopwatch.stop();
            LOG.info("Opened the 64-bit Rocks store {} in {}; counters {}", name, stopwatch.durationString(), counters.report());
        } finally {
            startupShutdown.release();
        }
    }

    /**
     * The options of a column family: uncompressed 16 KB blocks (compression measured and set
     * aside, IKE-Network/ike-issues#1251), a binary-search index and a bloom filter per file,
     * both cached and pinned, and a prefix extractor where the family's keys are compound.
     */
    private ColumnFamilyOptions columnFamilyOptions(Family family) {
        BloomFilter filter = new BloomFilter(BLOOM_BITS_PER_KEY, false);
        filters.add(filter);
        BlockBasedTableConfig table = new BlockBasedTableConfig()
                .setBlockCache(blockCache)
                .setFilterPolicy(filter)
                .setWholeKeyFiltering(family.wholeKeyFiltering)
                .setCacheIndexAndFilterBlocks(true)
                .setPinL0FilterAndIndexBlocksInCache(true)
                .setBlockSize(16 * 1024)
                .setChecksumType(ChecksumType.kXXH3)
                .setFormatVersion(5)
                // One binary-search index and one bloom filter per file, cached and pinned: a
                // point read is one index lookup. The two-level index with partitioned filters
                // measured 1.5 times slower on ordered point reads of SNOMED CT (2026-10-08).
                .setIndexType(IndexType.kBinarySearch)
                .setPartitionFilters(false);
        ColumnFamilyOptions options = new ColumnFamilyOptions()
                .setCompressionType(CompressionType.NO_COMPRESSION)
                .setTableFormatConfig(table)
                .setWriteBufferSize(family.writeBufferBytes);
        if (family.prefixBytes > 0) {
            options.useFixedLengthPrefixExtractor(family.prefixBytes);
            options.setMemtablePrefixBloomSizeRatio(0.1);
        }
        return options;
    }

    private ColumnFamilyHandle handle(Family family) {
        return handles.get(family.ordinal());
    }

    RocksDB db() {
        return db;
    }

    Counters counters() {
        return counters;
    }

    Scanner scanner() {
        return scanner;
    }

    RecordMap recordMap() {
        return recordMap;
    }

    IdentityMap identityMap() {
        return identityMap;
    }

    private void checkOpen() {
        if (closed.get()) {
            throw new IllegalStateException("The 64-bit Rocks store " + name + " is closed");
        }
    }

    // ---------- lifecycle ----------

    @Override
    public boolean running() {
        return !closed.get() && !closing.get();
    }

    /**
     * Writes what is held in memory, the identities and the records the writer has not yet
     * written, and the counters, then flushes every column family to disk. The records are
     * written without the write-ahead log, so this is what makes them durable.
     */
    @Override
    public void save() {
        if (!running()) {
            LOG.debug("The store is closing or closed; nothing to save");
            return;
        }
        identityMap.flush();
        recordMap.awaitPendingWrites();
        flushToDisk();
    }

    private void flushToDisk() {
        try (WriteBatch batch = new WriteBatch(); WriteOptions options = new WriteOptions()) {
            counters.save(batch, handle(Family.DEFAULT));
            db.write(options, batch);
        } catch (RocksDBException e) {
            throw new RuntimeException("Could not save the counters", e);
        }
        try (FlushOptions options = new FlushOptions().setWaitForFlush(true)) {
            db.flush(options, handles);
        } catch (RocksDBException e) {
            throw new RuntimeException("Could not flush " + name, e);
        }
    }

    @Override
    public void close() {
        startupShutdown.acquireUninterruptibly();
        try {
            if (closed.get() || !closing.compareAndSet(false, true)) {
                LOG.info("The 64-bit Rocks store {} is already closing or closed", name);
                return;
            }
            LOG.info("Closing the 64-bit Rocks store {}", name);
            try {
                identityMap.flush();
                recordMap.close();
                flushToDisk();
            } catch (RuntimeException e) {
                LOG.error("Error while saving {} during close", name, e);
            }
            closeDatabaseQuietly();
            closed.set(true);
            LOG.info("Closed the 64-bit Rocks store {}", name);
        } finally {
            startupShutdown.release();
        }
    }

    private void closeDatabaseQuietly() {
        closePooledReferenceIterators();
        for (ColumnFamilyHandle handle : handles) {
            try {
                handle.close();
            } catch (RuntimeException e) {
                LOG.debug("Error closing a column family handle", e);
            }
        }
        handles.clear();
        try {
            if (db != null) {
                db.close();
            }
        } catch (RuntimeException e) {
            LOG.warn("Error closing RocksDB", e);
        }
        closeNative();
    }

    private void closeNative() {
        if (descriptors != null) {
            for (ColumnFamilyDescriptor descriptor : descriptors) {
                try {
                    descriptor.getOptions().close();
                } catch (RuntimeException e) {
                    LOG.debug("Error closing column family options", e);
                }
            }
        }
        // The bloom filters belong to the options and are freed with them; closing them here
        // too crashed the native layer at shutdown in the legacy engine.
        filters.clear();
        if (entityFileOptions != null) {
            entityFileOptions.close();
        }
        if (referenceFileOptions != null) {
            referenceFileOptions.close();
        }
        if (identityFileOptions != null) {
            try {
                identityFileOptions.close();
            } catch (RuntimeException e) {
                LOG.debug("Error closing the identity file options", e);
            }
        }
        try {
            referenceReadOptions.close();
        } catch (RuntimeException e) {
            LOG.debug("Error closing the reference read options", e);
        }
        if (blockCache != null) {
            try {
                blockCache.close();
            } catch (RuntimeException e) {
                LOG.debug("Error closing the block cache", e);
            }
        }
        if (dbOptions != null) {
            try {
                dbOptions.close();
            } catch (RuntimeException e) {
                LOG.debug("Error closing DBOptions", e);
            }
        }
    }

    @Override
    public String name() {
        return name;
    }

    /** The data root the store was opened on. */
    public File root() {
        return root;
    }

    @Override
    public long writeSequence() {
        return writeSequence.sum();
    }

    /** Pattern sequences are discovered before elements are keyed under them, so an import takes two passes. */
    @Override
    public boolean requiresMultiPassImport() {
        return true;
    }

    /** The counters, one {@code pattern=next} per pattern. */
    public String sequenceReport() {
        return counters.report();
    }

    // ---------- identity ----------

    /** A 64-bit store mints nids under a pattern only: see {@link #getEntityKey(PublicId, PublicId)}. */
    @Override
    public long newNid() {
        throw new UnsupportedOperationException("A 64-bit store mints a nid under the entity's pattern: "
                + "ask for the key by pattern and public id");
    }

    @Override
    public long nidForUuids(UUID... uuids) {
        checkOpen();
        return identityMap.nidForUuids(uuids);
    }

    @Override
    public long nidForUuids(ImmutableList<UUID> uuidList) {
        return nidForUuids(uuidList.toArray(new UUID[0]));
    }

    @Override
    public boolean hasUuid(UUID uuid) {
        checkOpen();
        return identityMap.knows(uuid);
    }

    @Override
    public boolean hasPublicId(PublicId publicId) {
        for (UUID uuid : publicId.asUuidArray()) {
            if (hasUuid(uuid)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public EntityKey getEntityKey(PublicId patternId, PublicId entityId) {
        checkOpen();
        return EntityKey.ofNid(identityMap.nidFor(patternId, entityId));
    }

    @Override
    public Optional<EntityKey> getEntityKey(UUID uuid) {
        checkOpen();
        return identityMap.nid(uuid).map(EntityKey::ofNid);
    }

    /**
     * The UUIDs of the entity stored under a nid, from its record; for a nid minted and never
     * written, from the identity map, by a scan.
     */
    @Override
    public PublicId publicIdForNid(long nid) {
        byte[] record = getBytes(nid);
        if (record != null) {
            return PublicIds.of(EntityRecordFormat2.uuids(record).toArray(new UUID[0]));
        }
        return identityMap.publicIdOf(nid).orElseThrow(() ->
                new IllegalStateException("No entity is stored for nid " + nid + " in " + name + ", and no public id was minted for it"));
    }

    // ---------- records ----------

    @Override
    public byte[] getBytes(long nid) {
        checkOpen();
        return recordMap.get(nid);
    }

    @Override
    public byte[] merge(long nid, long patternNid, long referencedComponentNid, byte[] value, Object sourceObject,
                        DataActivity activity) {
        checkOpen();
        Nid.validate64(nid);
        if (!EntityRecordFormat2.isFormat2(value)) {
            throw new IllegalArgumentException("A 64-bit store holds format 2 records; nid " + nid + " was given "
                    + (value == null || value.length == 0 ? "no record" : "a format " + value[0] + " record"));
        }
        if (sourceObject instanceof SemanticEntity semantic) {
            recordMap.addReference(semantic.referencedComponentNid(), nid);
        }
        // A record whose nid was minted in this load phase has nothing stored: it joins its
        // pattern's run, ingested in nid order, instead of the writer's queue.
        recordMap.put(nid, value, loadPhase && identityMap.fresh(nid));
        byte[] merged = recordMap.get(nid);
        if (merged == null) {
            throw new IllegalStateException("Record " + nid + " read as absent right after its put: " + recordMap.whereIs(nid));
        }
        writeSequence.increment();

        ImmutableList<ChangeSetWriterService> writers = changeSetWriters.orElseSet(this::loadChangeSetWriters);
        writers.forEach(writer -> writer.writeToChangeSet((Entity) sourceObject, activity));

        // Live indexing, within the load phase policy, as the legacy engine does.
        if (!loadPhase || EntityService.get().loadPhaseSearchPolicy().shouldIndexLive()) {
            try {
                getSearchService().index(sourceObject);
            } catch (Exception e) {
                LOG.debug("SearchService not available for real-time indexing; will rely on later index rebuild. Entity={}",
                        sourceObject, e);
            }
        }
        return merged;
    }

    private ImmutableList<ChangeSetWriterService> loadChangeSetWriters() {
        ServiceLoader<ChangeSetWriterService> loader = PluggableService.load(ChangeSetWriterService.class);
        MutableList<ChangeSetWriterService> writers = Lists.mutable.empty();
        loader.stream().forEach(provider -> writers.add(provider.get()));
        return writers.toImmutable();
    }

    // ---------- scans ----------

    @Override
    public void forEach(ObjLongConsumer<byte[]> action) {
        checkOpen();
        recordMap.awaitPendingWrites();
        scanner.forEach(action);
    }

    @Override
    public void forEachParallel(ObjLongConsumer<byte[]> action) {
        checkOpen();
        recordMap.awaitPendingWrites();
        scanner.forEachParallel(action);
    }

    /**
     * Visits the records of these nids, in nid order: the nids sorted and cut into chunks, a
     * chunk whose nids fill at least a quarter of its key span read as one bounded scan, a
     * sparser one as one multi-get. Records the writer still holds are written first, so the
     * scans see them.
     */
    @Override
    public void forEach(ImmutableLongList nids, ObjLongConsumer<byte[]> action) {
        checkOpen();
        recordMap.awaitPendingWrites();
        long[] sorted = nids.toSortedArray();
        for (int from = 0; from < sorted.length; from += POINT_READ_CHUNK) {
            readChunk(Arrays.copyOfRange(sorted, from, Math.min(sorted.length, from + POINT_READ_CHUNK)), action);
        }
    }

    /** As {@link #forEach(ImmutableLongList, ObjLongConsumer)}, the chunks on several threads at once. */
    @Override
    public void forEachParallel(ImmutableLongList nids, ObjLongConsumer<byte[]> action) {
        checkOpen();
        recordMap.awaitPendingWrites();
        long[] sorted = nids.toSortedArray();
        List<Runnable> chunks = new ArrayList<>();
        for (int from = 0; from < sorted.length; from += POINT_READ_CHUNK) {
            long[] chunk = Arrays.copyOfRange(sorted, from, Math.min(sorted.length, from + POINT_READ_CHUNK));
            chunks.add(() -> readChunk(chunk, action));
        }
        scanner.parallel(chunks);
    }

    private void readChunk(long[] nids, ObjLongConsumer<byte[]> action) {
        if (nids.length == 0) {
            return;
        }
        long first = nids[0];
        long last = nids[nids.length - 1];
        boolean onePattern = Nid.patternSequence64(first) == Nid.patternSequence64(last);
        if (onePattern && last - first + 1 <= 4L * nids.length) {
            scanChunk(nids, action);
        } else {
            byte[][] records = recordMap.getAll(nids);
            for (int i = 0; i < nids.length; i++) {
                if (records[i] != null) {
                    action.accept(records[i], nids[i]);
                }
            }
        }
    }

    /** One bounded scan from the chunk's first nid through its last, keeping the keys the chunk names. */
    private void scanChunk(long[] nids, ObjLongConsumer<byte[]> action) {
        byte[] lower = Keys.of(nids[0]);
        byte[] upper = Keys.of(nids[nids.length - 1] + 1);
        try (Slice lowerSlice = new Slice(lower); Slice upperSlice = new Slice(upper);
             ReadOptions ro = new ReadOptions().setIterateLowerBound(lowerSlice).setIterateUpperBound(upperSlice).setTotalOrderSeek(true);
             RocksIterator it = db.newIterator(handle(Family.ENTITIES), ro)) {
            for (it.seek(lower); it.isValid(); it.next()) {
                long nid = Keys.nid(it.key());
                if (Arrays.binarySearch(nids, nid) >= 0) {
                    action.accept(it.value(), nid);
                }
            }
        }
    }

    // ---------- enumerations ----------

    /**
     * Visits the semantics of a pattern, by its counter: every element sequence the pattern has
     * issued. The three fixed patterns key concepts, stamps and patterns, not semantics, so they
     * have none here, as in every other provider.
     *
     * @throws IllegalStateException if the nid is not a pattern's
     */
    @Override
    public void forEachSemanticNidOfPattern(long patternNid, LongProcedure procedure) {
        checkOpen();
        int pattern = patternSequenceOf(patternNid);
        if (pattern < Counters.FIRST_SEMANTIC_PATTERN) {
            return;
        }
        forEachElementOf(pattern, procedure);
    }

    private static int patternSequenceOf(long patternNid) {
        if (!Nid.isValid64(patternNid) || Nid.patternSequence64(patternNid) != Counters.PATTERN_OF_PATTERNS) {
            throw new IllegalStateException("Trying to iterate elements for entity that is not a pattern: " + patternNid);
        }
        return Nid.elementSequence64(patternNid);
    }

    private void forEachElementOf(int pattern, LongProcedure procedure) {
        int next = counters.next(pattern);
        for (int element = Counters.FIRST_ELEMENT; element < next; element++) {
            procedure.accept(Nid.compose64(pattern, element));
        }
    }

    /** Visits every pattern an entity stands behind: a sequence issued and never written is not a pattern of the store. */
    @Override
    public void forEachPatternNid(LongProcedure procedure) {
        checkOpen();
        int next = counters.next(Counters.PATTERN_OF_PATTERNS);
        for (int element = Counters.FIRST_ELEMENT; element < next; element++) {
            long nid = Nid.compose64(Counters.PATTERN_OF_PATTERNS, element);
            if (recordMap.get(nid) != null) {
                procedure.accept(nid);
            }
        }
    }

    @Override
    public void forEachConceptNid(LongProcedure procedure) {
        checkOpen();
        forEachElementOf(Counters.CONCEPT_PATTERN, procedure);
    }

    @Override
    public void forEachStampNid(LongProcedure procedure) {
        checkOpen();
        forEachElementOf(Counters.STAMP_PATTERN, procedure);
    }

    /**
     * Visits every semantic nid: every element of every pattern after the three fixed ones, in
     * chunks on several threads at once, so the procedure must be safe to call concurrently.
     */
    @Override
    public void forEachSemanticNid(LongProcedure procedure) {
        checkOpen();
        List<Runnable> chunks = new ArrayList<>();
        counters.patternSequences().filter(pattern -> pattern >= Counters.FIRST_SEMANTIC_PATTERN).forEach(pattern -> {
            int next = counters.next(pattern);
            for (int first = Counters.FIRST_ELEMENT; first < next; first += ENUMERATION_CHUNK) {
                int from = first;
                int to = Math.min(next, first + ENUMERATION_CHUNK);
                chunks.add(() -> {
                    for (int element = from; element < to; element++) {
                        procedure.accept(Nid.compose64(pattern, element));
                    }
                });
            }
        });
        scanner.parallel(chunks);
    }

    /** The references pending are read before the stored ones: the writer moves a reference from the one to the other. */
    @Override
    public void forEachSemanticNidForComponent(long componentNid, LongProcedure procedure) {
        checkOpen();
        TreeSet<Long> referencing = new TreeSet<>(recordMap.pendingReferencesTo(componentNid));
        referencesTo(Keys.referencesTo(componentNid), referencing);
        referencing.forEach(procedure::accept);
    }

    @Override
    public void forEachSemanticNidForComponentOfPattern(long componentNid, long patternNid, LongProcedure procedure) {
        checkOpen();
        int pattern = patternSequenceOf(patternNid);
        TreeSet<Long> referencing = new TreeSet<>();
        for (long nid : recordMap.pendingReferencesTo(componentNid)) {
            if (Nid.patternSequence64(nid) == pattern) {
                referencing.add(nid);
            }
        }
        referencesTo(Keys.referencesTo(componentNid, pattern), referencing);
        referencing.forEach(procedure::accept);
    }

    /**
     * Adds the referencing nids of the reference keys under a prefix: the 8 bytes of the
     * referenced nid, or those and the referencing pattern's 4. The iterator, borrowed from the
     * pool, seeks with the family's prefix bloom and stays within the 8-byte prefix; the 12-byte
     * one ends the read at the first key outside it.
     */
    private void referencesTo(byte[] prefix, TreeSet<Long> referencing) {
        PooledIterator pooled = borrowReferenceIterator();
        try {
            RocksIterator it = pooled.iterator();
            for (it.seek(prefix); it.isValid(); it.next()) {
                byte[] key = it.key();
                if (!Keys.startsWith(key, prefix)) {
                    break;
                }
                referencing.add(Keys.referencingNid(key));
            }
        } finally {
            returnReferenceIterator(pooled);
        }
    }

    private PooledIterator borrowReferenceIterator() {
        long epoch = recordMap.referenceEpoch();
        PooledIterator pooled;
        while ((pooled = referenceIterators.poll()) != null) {
            pooledReferenceIterators.decrementAndGet();
            if (pooled.epoch() == epoch) {
                return pooled;
            }
            pooled.iterator().close();
        }
        return new PooledIterator(db.newIterator(handle(Family.REFERENCES), referenceReadOptions), epoch);
    }

    private void returnReferenceIterator(PooledIterator pooled) {
        if (running() && pooled.epoch() == recordMap.referenceEpoch()
                && pooledReferenceIterators.incrementAndGet() <= MAX_POOLED_REFERENCE_ITERATORS) {
            referenceIterators.add(pooled);
        } else {
            if (pooled.epoch() == recordMap.referenceEpoch() && running()) {
                pooledReferenceIterators.decrementAndGet();
            }
            pooled.iterator().close();
        }
    }

    private void closePooledReferenceIterators() {
        PooledIterator pooled;
        while ((pooled = referenceIterators.poll()) != null) {
            pooled.iterator().close();
        }
        pooledReferenceIterators.set(0);
    }

    // ---------- search ----------

    private SearchService getSearchService() {
        return searchService.orElseSet(() -> ServiceLifecycleManager.get()
                .getRunningService(SearchService.class)
                .orElseThrow(() -> new IllegalStateException("SearchService not available - ensure services are started")));
    }

    @Override
    public PrimitiveDataSearchResult[] search(String query, int maxResultSize) throws Exception {
        return getSearchService().search(query, maxResultSize);
    }

    @Override
    public String highlight(String query, String text) throws Exception {
        return getSearchService().highlight(query, text);
    }

    /** A load phase holds every identity in memory; leaving it writes them to the column once ({@link IdentityMap}). */
    @Override
    public void setLoadPhase(boolean loadPhase) {
        if (loadPhase) {
            this.loadPhase = true;
            identityMap.setLoadPhase(true);
            recordMap.setLoadPhase(true);
            return;
        }
        // Ending: the runs and the chunk first, while the fresh nids are still known; then the identities.
        boolean wasLoading = this.loadPhase;
        recordMap.setLoadPhase(false);
        this.loadPhase = false;
        identityMap.setLoadPhase(false);
        if (wasLoading) {
            LOG.info("Load phase ended: {} record(s) and {} reference(s) ingested as SST files",
                    String.format("%,d", recordMap.recordsIngested()), String.format("%,d", recordMap.referencesIngested()));
        }
    }

    @Override
    public CompletableFuture<Void> recreateLuceneIndex() {
        return getSearchService().recreateIndex();
    }
}
