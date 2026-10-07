package dev.ikm.ds.rocks;

import dev.ikm.tinkar.common.service.internal.EntityStore;
import dev.ikm.tinkar.common.util.thread.StructuredScopes;
import dev.ikm.tinkar.common.util.thread.SubtaskFailedException;
import dev.ikm.tinkar.common.util.SetOnce;
import dev.ikm.ds.rocks.maps.*;
import dev.ikm.ds.rocks.spliterator.LongSpliteratorOfPattern;
import dev.ikm.ds.rocks.spliterator.SortedLongArraySpliteratorOfPattern;
import dev.ikm.ds.rocks.spliterator.SpliteratorForRocksKeyOfPattern;
import dev.ikm.tinkar.common.id.EntityKey;
import dev.ikm.tinkar.common.id.PublicId;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.common.id.impl.KeyUtil;
import dev.ikm.tinkar.common.id.impl.NidLayout;
import dev.ikm.tinkar.common.service.*;
import dev.ikm.tinkar.provider.search.DataStoreLockProbe;
import dev.ikm.tinkar.common.util.time.Stopwatch;
import dev.ikm.tinkar.common.validation.ValidationRecord;
import dev.ikm.tinkar.common.validation.ValidationSeverity;
import dev.ikm.tinkar.entity.*;
import dev.ikm.tinkar.common.service.SearchService;
import dev.ikm.tinkar.terms.EntityBinding;
import dev.ikm.tinkar.terms.EntityProxy;
import org.eclipse.collections.api.block.procedure.primitive.IntProcedure;
import org.eclipse.collections.api.collection.primitive.MutableLongCollection;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.factory.primitive.LongLists;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.list.primitive.ImmutableIntList;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.api.map.MutableMap;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongConsumer;
import java.util.function.ObjIntConsumer;

import static dev.ikm.tinkar.common.service.PrimitiveData.SCOPED_PATTERN_PUBLICID_FOR_NID;

public class RocksProvider implements PrimitiveDataService, EntityStore, NidGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(RocksProvider.class);
    public static final long defaultCacheSize = 256L * 1024 * 1024; // 256 MB cache
    public static final int defaultBloomFilterBitsPerKey = 10;

    protected static final File defaultDataDirectory = new File(new File(System.getProperty("user.home"), "Solor"), "rocksdb");

    private LongAdder writeSequence = new LongAdder();

    final Semaphore startupShutdownSemaphore = new Semaphore(1);
    final String name;

    final SetOnce<ImmutableList<ChangeSetWriterService>> changeSetWriterServices = new SetOnce<>();
    final SetOnce<SearchService> searchService = new SetOnce<>();
    private volatile boolean loadPhase = false;

    private final RocksDB db;
    private final EntityMap entityMap;
    private final EntityReferencingSemanticMap entityReferencingSemanticMap;
    private final SequenceMap sequenceMap;
    private final UuidEntityKeyMap uuidEntityKeyMap;

    private final List<ColumnFamilyHandle> columnHandles = new ArrayList<>();

    public void scanEntitiesInRange(LongSpliteratorOfPattern range, ObjIntConsumer<byte[]> entityHandler) {
        entityMap.scanEntitiesInRange(range, entityHandler);
    }

    public RocksDB getDb() {
        return db;
    }

    /**
     * The options of a column family. Blocks are written uncompressed: on the SNOMED CT baseline
     * (Graphlet, 2026-10-07), LZ4 took 39% off the store and ZSTD at the bottommost level 51%, but
     * scans were 8% and 27% slower and the import 2% and 15% slower, and a store that fits in
     * memory on a fast local disk gains nothing from being smaller (design
     * {@code design-2026-09-30-64-bit-nids}, "RocksDB compression", deferred;
     * IKE-Network/ike-issues#1251). RocksDB records the codec in each block, so this
     * configuration still reads a store whose blocks are compressed.
     *
     * @param cf       the column family
     * @param tableCfg its table configuration
     * @return the options, which the caller closes
     */
    static ColumnFamilyOptions columnFamilyOptions(ColumnFamily cf, BlockBasedTableConfig tableCfg) {
        ColumnFamilyOptions cfo = new ColumnFamilyOptions();
        cfo.setCompressionType(CompressionType.NO_COMPRESSION);
        cfo.setTableFormatConfig(tableCfg);
        cfo.setWriteBufferSize(cf.writeBufferSize);
        if (cf.keyPrefixBytes >= 0) {
            cfo.useFixedLengthPrefixExtractor(cf.keyPrefixBytes);
        }
        return cfo;
    }

    public enum ColumnFamily {
        DEFAULT(RocksDB.DEFAULT_COLUMN_FAMILY, -1, 0, 1024 * 1024),
        ENTITY_MAP("EntityMap", 2, defaultBloomFilterBitsPerKey, 512L * 1024 * 1024),
        ENTITY_REFERENCING_SEMANTIC_MAP("EntityReferencingSemanticMap", 16, defaultBloomFilterBitsPerKey, 10L * 1024 * 1024),
        UUID_ENTITY_KEY_MAP("UuidEntityKeyMap", -1, defaultBloomFilterBitsPerKey, 10L * 1024 * 1024);

        private final byte[] value;
        private final int keyPrefixBytes; // For compound keys. -1 if no prefix;
        private final int bloomFilterBitsPerKey;
        private final long writeBufferSize;

        ColumnFamily(byte[] value, int keyPrefixBytes,
                     int bloomFilterBitsPerKey, long writeBufferSize ) {
            this.value = value;
            this.keyPrefixBytes = keyPrefixBytes;
            this.bloomFilterBitsPerKey = bloomFilterBitsPerKey;
            this.writeBufferSize = writeBufferSize;
        }

        ColumnFamily(String stringValue, int keyPrefixBytes,
                     int bloomFilterBitsPerKey, long writeBufferSize) {
            this(stringValue.getBytes(), keyPrefixBytes, bloomFilterBitsPerKey, writeBufferSize);
        }

        public byte[] getValue() {
            return value;
        }
    }

    private List<ColumnFamilyDescriptor> columnDescriptors;

    private ImmutableList changeSetWriterServicesList() {
        ServiceLoader<ChangeSetWriterService> changeSetServiceLoader = PluggableService.load(ChangeSetWriterService.class);
        MutableList<ChangeSetWriterService> changeSetWriters = Lists.mutable.empty();
        changeSetServiceLoader.stream().forEach(changeSetProvider -> {
            changeSetWriters.add(changeSetProvider.get());
        });
        return changeSetWriters.toImmutable();
    }


    public static RocksProvider get() {
        try {
            return stableProvider.orElseSet(RocksProvider::new);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    private static final SetOnce<RocksProvider> stableProvider = new SetOnce<>();

    private final Cache blockCache;
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    // Add these fields to track native resources that need cleanup
    private DBOptions dbOptions;
    private final List<Filter> bloomFilters = new ArrayList<>();
    private final List<BlockBasedTableConfig> tableConfigs = new ArrayList<>();

    private RocksProvider() {
        startupShutdownSemaphore.acquireUninterruptibly();
        try {
            Stopwatch stopwatch = new Stopwatch();
            LOG.info("Opening " + this.getClass().getSimpleName());
            File configuredRoot = ServiceProperties.get(ServiceKeys.DATA_STORE_ROOT, defaultDataDirectory);
            boolean expectEmpty = ServiceProperties.get(ServiceKeys.DATA_STORE_EXPECT_EMPTY, Boolean.FALSE);
            if (expectEmpty) {
                assertEmptyDataRoot(configuredRoot);
                ServiceProperties.set(ServiceKeys.DATA_STORE_EXPECT_EMPTY, Boolean.FALSE);
            }
            // Ensure DATA_STORE_ROOT is set in ServiceProperties for other services (like SearchProvider)
            ServiceProperties.set(ServiceKeys.DATA_STORE_ROOT, configuredRoot);
            this.name = configuredRoot.getName();

            // Create directory structure - ensure parent exists before creating subdirectories
            if (!configuredRoot.exists() && !configuredRoot.mkdirs()) {
                throw new RuntimeException("Failed to create data store root directory: " + configuredRoot.getAbsolutePath());
            }
            LOG.info("Datastore root: " + configuredRoot.getAbsolutePath());

            File rockFiles = new File(configuredRoot, "rocks");
            boolean kbExists = rockFiles.exists();
            if (!rockFiles.exists() && !rockFiles.mkdirs()) {
                throw new RuntimeException("Failed to create rocks directory: " + rockFiles.getAbsolutePath());
            }

            File rocksLogsDir = new File(configuredRoot, "rocks-logs");
            if (!rocksLogsDir.exists() && !rocksLogsDir.mkdirs()) {
                throw new RuntimeException("Failed to create rocks-logs directory: " + rocksLogsDir.getAbsolutePath());
            }

            RocksDB.loadLibrary();

            this.blockCache = new LRUCache(defaultCacheSize);
            this.columnDescriptors = Arrays.stream(ColumnFamily.values())
                    .map(cf -> {

                        BloomFilter bloomFilter = new BloomFilter(cf.bloomFilterBitsPerKey, /*useBlockBasedBuilder*/ false);
                        bloomFilters.add(bloomFilter); // Track for cleanup
                        
                        BlockBasedTableConfig tableCfg = new BlockBasedTableConfig()
                                .setBlockCache(this.blockCache)
                                .setFilterPolicy(bloomFilter)
                                .setWholeKeyFiltering(false)                    // favor prefix Bloom (when prefix extractor is configured)
                                .setCacheIndexAndFilterBlocks(true)            // keep index/filter hot
                                .setPinL0FilterAndIndexBlocksInCache(true)     // optional
                                .setBlockSize(16 * 1024)                       // 16 KB data blocks (tune for your workload)
                                .setChecksumType(ChecksumType.kXXH3)           // modern checksum
                                .setFormatVersion(5)                           // stable table format
                                .setPartitionFilters(true);                    // scalable filters for large data
                        
                        tableConfigs.add(tableCfg); // Track for cleanup

                        return new ColumnFamilyDescriptor(cf.getValue(), columnFamilyOptions(cf, tableCfg));
                    }).toList();

            // Don't use try-with-resources - we need to keep DBOptions alive
            this.dbOptions = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true)
                    .setIncreaseParallelism(Runtime.getRuntime().availableProcessors())
                    .setMaxBackgroundJobs(Math.max(8, Runtime.getRuntime().availableProcessors()))
                    .setAllowConcurrentMemtableWrite(true)
                    .setEnablePipelinedWrite(true)
                    .setBytesPerSync(8 * 1024 * 1024)
                    .setWalBytesPerSync(8 * 1024 * 1024)
                    // Configure RocksDB log directory (creates LOG, LOG.old.* here)
                    .setDbLogDir(rocksLogsDir.getAbsolutePath())
                    // Optional: adjust verbosity of RocksDB's internal logging
                    .setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
                    
            try {
                db = RocksDB.open(dbOptions, rockFiles.getAbsolutePath(), columnDescriptors, columnHandles);
                entityReferencingSemanticMap = new EntityReferencingSemanticMap(db, getHandle(ColumnFamily.ENTITY_REFERENCING_SEMANTIC_MAP));
                sequenceMap = new SequenceMap(db, getHandle(ColumnFamily.DEFAULT));
                uuidEntityKeyMap = new UuidEntityKeyMap(db, getHandle(ColumnFamily.UUID_ENTITY_KEY_MAP), sequenceMap);
                entityMap = new EntityMap(db, getHandle(ColumnFamily.ENTITY_MAP), uuidEntityKeyMap);
            } catch (RocksDBException e) {
                // Close ColumnFamilyOptions/Table configs if open failed
                safeCloseColumnFamilyDescriptors();
                safeCloseNativeResources();
                throw new RuntimeException(e);
            } catch (RuntimeException e) {
                // A map failed while opening the database. Release it without closing
                // the maps: a map's close saves its state, and a database that failed
                // to open must stay unmodified.
                closeRefusedDatabase();
                throw e;
            }

            stopwatch.stop();
            LOG.info("Opened RocksProvider in: " + stopwatch.durationString());
        } finally {
            startupShutdownSemaphore.release();
        }
    }

    private static void assertEmptyDataRoot(File configuredRoot) {
        if (!configuredRoot.exists()) {
            return;
        }
        if (!configuredRoot.isDirectory()) {
            throw new IllegalStateException("Configured DATA_STORE_ROOT is not a directory: " + configuredRoot.getAbsolutePath());
        }
        String[] entries = configuredRoot.list();
        if (entries != null && entries.length > 0) {
            throw new IllegalStateException("Expected empty DATA_STORE_ROOT but found contents: " + configuredRoot.getAbsolutePath());
        }
    }

    private void safeCloseColumnFamilyDescriptors() {
        if (this.columnDescriptors != null) {
            for (ColumnFamilyDescriptor d : this.columnDescriptors) {
                try {
                    ColumnFamilyOptions opts = d.getOptions();
                    if (opts != null) {
                        // Don't close filters here - we'll close them separately in safeCloseNativeResources
                        // to avoid double-close
                        opts.close();
                    }
                } catch (Throwable t) {
                    LOG.debug("Error closing ColumnFamilyOptions", t);
                }
            }
        }
    }

    private void checkOpen() {
        if (closed.get()) {
            throw new IllegalStateException("RocksProvider is closed");
        }
    }

    public ImmutableList<SpliteratorForRocksKeyOfPattern> allPatternSpliterators() {
        return this.sequenceMap.allPatternSpliterators();
    }

    public ColumnFamilyHandle getHandle(ColumnFamily columnFamily) {
        return columnHandles.get(columnFamily.ordinal());
    }

    public void save() {
        if (closed.get() || closing.get()) {
            LOG.debug("RocksProvider is closing/closed, skipping save");
            return;
        }
        entityMap.save();
        entityReferencingSemanticMap.save();
        sequenceMap.save();
        uuidEntityKeyMap.save();
        try (FlushOptions flushOptions = new FlushOptions()) {
            db.flush(flushOptions);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean running() {
        return !closed.get() && !closing.get();
    }

        @Override
        public void close() {
            startupShutdownSemaphore.acquireUninterruptibly();
            try {
                if (closed.get()) {
                    LOG.info("Called close() but RocksProvider already closed.");
                    return;
                }
                
                if (!closing.compareAndSet(false, true)) {
                    LOG.info("Close already in progress");
                    return;
                }
                
                LOG.info("Closing RocksProvider...");

                // 1. Flush and sync DB first
                try {
                    save();
                } catch (Exception e) {
                    LOG.warn("Error while saving during close", e);
                }

                // 2. Close maps (they will close their column family handles)
                try {
                    if (this.entityMap != null) {
                        this.entityMap.close();
                    }
                } catch (Exception e) {
                    LOG.warn("Error closing entityMap", e);
                }
                try {
                    if (this.entityReferencingSemanticMap != null) {
                        this.entityReferencingSemanticMap.close();
                    }
                } catch (Exception e) {
                    LOG.warn("Error closing entityReferencingSemanticMap", e);
                }
                try {
                    if (this.uuidEntityKeyMap != null) {
                        this.uuidEntityKeyMap.close();
                    }
                } catch (Exception e) {
                    LOG.warn("Error closing uuidEntityKeyNidMap", e);
                }
                try {
                    if (this.sequenceMap != null) {
                        this.sequenceMap.close();
                    }
                } catch (Exception e) {
                    LOG.warn("Error closing sequenceMap", e);
                }

                // 3. Clear the column handles list (already closed by maps above)
                columnHandles.clear();

                // 4. Close DB after all column family handles are closed
                try {
                    if (this.db != null) {
                        this.db.close();
                    }
                } catch (Exception e) {
                    LOG.warn("Error closing RocksDB", e);
                }

                // 5. Close ColumnFamilyOptions (without closing filters inside them)
                try {
                    safeCloseColumnFamilyDescriptors();
                } catch (Exception e) {
                    LOG.warn("Error closing ColumnFamilyOptions", e);
                }

                // 6. Close cache
                try {
                    if (this.blockCache != null) {
                        this.blockCache.close();
                    }
                } catch (Exception e) {
                    LOG.warn("Error closing blockCache", e);
                }
                
                // 7. Close native resources (BloomFilters, DBOptions) LAST
                try {
                    safeCloseNativeResources();
                } catch (Exception e) {
                    LOG.warn("Error closing native resources", e);
                }

                closed.set(true);
                LOG.info("RocksProvider closed.");
            } finally {
                startupShutdownSemaphore.release();
            }
        }

    /**
     * Release a database that was opened but refused during map construction,
     * writing nothing: column handles, then the database, then the options,
     * cache, and native resources.
     */
    private void closeRefusedDatabase() {
        for (ColumnFamilyHandle handle : columnHandles) {
            try {
                handle.close();
            } catch (Exception e) {
                LOG.debug("Error closing column family handle", e);
            }
        }
        columnHandles.clear();
        try {
            if (this.db != null) {
                this.db.close();
            }
        } catch (Exception e) {
            LOG.warn("Error closing refused RocksDB", e);
        }
        safeCloseColumnFamilyDescriptors();
        try {
            if (this.blockCache != null) {
                this.blockCache.close();
            }
        } catch (Exception e) {
            LOG.debug("Error closing blockCache", e);
        }
        safeCloseNativeResources();
    }

    private void safeCloseNativeResources() {
        // Close bloom filters - these DO have native resources
        /*
Claude: The crash is almost certainly in the native RocksDB layer during shutdown,
specifically around BloomFilter or cache cleanup. The NULL pointer dereference
suggests RocksDB's C++ code tried to access a freed resource.

Fix: Do NOT manually close BloomFilters that are owned by ColumnFamilyOptions.
Remove the explicit BloomFilter closing from safeCloseNativeResources() or
ensure they're not already freed when ColumnFamilyOptions closes.
         */
//        for (Filter f : this.bloomFilters) {
//            try {
//                if (f != null) {
//                    f.close();
//                }
//            } catch (Throwable t) {
//                LOG.debug("Error closing BloomFilter", t);
//            }
//        }
        this.bloomFilters.clear();

        // BlockBasedTableConfig doesn't need explicit closing - it's just a config holder
        // The native resources it represents are owned by ColumnFamilyOptions
        this.tableConfigs.clear();

        // Close DB options
        if (this.dbOptions != null) {
            try {
                this.dbOptions.close();
            } catch (Throwable t) {
                LOG.debug("Error closing DBOptions", t);
            }
            this.dbOptions = null;
        }
    }
    
    public EntityKey getEntityKey(PublicId patternId, PublicId entityId) {
        return uuidEntityKeyMap.getEntityKey(patternId, entityId);
    }

    public Optional<EntityKey> getEntityKey(UUID uuid) {
        return uuidEntityKeyMap.getEntityKey(uuid);
    }

    public String sequenceReport() {
        return sequenceMap.sequenceReport();
    }

    @Override
    public int newNid() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long writeSequence() {
        return writeSequence.sum();
    }

    @Override
    public boolean requiresMultiPassImport() {
        return true;
    }

    public long elementSequenceForNid(int nid) {
        return NidLayout.active().decodeElementSequence(nid);
    }
    public int patternSequenceForNid(int nid) {
        return NidLayout.active().decodePatternSequence(nid);
    }

    public long rocksKeyForNid(int nid) {
        return NidLayout.active().rocksKeyForNid(nid);
    }

    public int stampSequenceForStampNid(int nid) {
        return (int) NidLayout.active().decodeElementSequence(nid);
    }

    @Override
    public int nidForUuids(UUID... uuids) {
        // Diagnostic for ikmdev/komet-desktop#12 (closed): which path produces the nid for a
        // new pattern UUID. Debug only: this runs for every nid lookup, millions of times in a
        // large import.
        boolean debug = LOG.isDebugEnabled();
        if (debug) {
            boolean scopedBound = SCOPED_PATTERN_PUBLICID_FOR_NID.isBound();
            LOG.debug("nidForUuids: uuids={}, scopedPatternBound={}, scopedPatternPublicId={}",
                    Arrays.toString(uuids),
                    scopedBound,
                    scopedBound ? SCOPED_PATTERN_PUBLICID_FOR_NID.get() : null);
        }
        // When more than one is known, the least known UUID decides, whatever order the caller
        // listed them in; UUIDs known for different components are reported.
        UUID[] ordered = uuids;
        if (uuids.length > 1) {
            ordered = uuids.clone();
            Arrays.sort(ordered);
            java.util.TreeSet<Integer> nids = new java.util.TreeSet<>();
            for (UUID uuid : ordered) {
                uuidEntityKeyMap.getEntityKey(uuid).ifPresent(key -> nids.add(key.nid()));
            }
            if (nids.size() > 1) {
                IdentityAdvisories.componentsShareUuids(List.of(ordered), nids);
            }
        }
        for (UUID uuid: ordered) {
            Optional<EntityKey> optionalKey = uuidEntityKeyMap.getEntityKey(uuid);
            if (optionalKey.isPresent()) {
                int nid = optionalKey.get().nid();
                if (debug) {
                    LOG.debug("nidForUuids: existing match for uuid={} -> nid={} (patternSeq={}, elementSeq={})",
                            uuid, nid, NidLayout.active().decodePatternSequence(nid), NidLayout.active().decodeElementSequence(nid));
                }
                return nid;
            }
        }
        if (SCOPED_PATTERN_PUBLICID_FOR_NID.isBound()) {
            PublicId patternPublicId = SCOPED_PATTERN_PUBLICID_FOR_NID.get();
            EntityKey stampEntityKey = uuidEntityKeyMap.getEntityKey(patternPublicId, PublicIds.of(uuids));
            int nid = stampEntityKey.nid();
            if (debug) {
                LOG.debug("nidForUuids: allocated via scoped pattern {} -> nid={} (patternSeq={}, elementSeq={})",
                        patternPublicId, nid, NidLayout.active().decodePatternSequence(nid), NidLayout.active().decodeElementSequence(nid));
            }
            return nid;
        }

        // Log detailed information before throwing exception
        LOG.error("Failed to find entity key for UUIDs: {}", Arrays.toString(uuids));
        LOG.error("Database location: {}", this.name);
        LOG.error("This likely indicates starter data has not been imported yet, or the database is empty.");
        LOG.error("For new databases, ensure DataLoadController is registered and running in DATA_LOAD phase.");

        throw new IllegalStateException("No entity key found for UUIDs: " + Arrays.toString(uuids));
    }

    @Override
    public int nidForUuids(ImmutableList<UUID> uuidList) {
        return nidForUuids(uuidList.toArray(new UUID[uuidList.size()]));
    }

    @Override
    public boolean hasUuid(UUID uuid) {
        // Memory first, then the column in caching mode: a UUID assigned this session is
        // not in the column until the next flush (IKE-Network/ike-issues#1141).
        return this.uuidEntityKeyMap.getEntityKey(uuid).isPresent();
    }

    @Override
    public boolean hasPublicId(PublicId publicId) {
        for (UUID uuid: publicId.asUuidArray()) {
            if (hasUuid(uuid)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void forEach(ObjIntConsumer<byte[]> action) {
        entityMap.forEach(action);
    }

    @Override
    public void forEachParallel(ObjIntConsumer<byte[]> action) {
        // 1) Build a list of SpliteratorForRocksKeyOfPattern ranges from the all-entity spliterator
        Spliterator.OfLong allEntityKeys = this.sequenceMap.allEntityRocksKeySpliterator();
        forEachParallel(action, allEntityKeys);
    }

    private void forEachParallel(ObjIntConsumer<byte[]> action, Spliterator.OfLong entityKeys) {
        MutableList<SpliteratorForRocksKeyOfPattern> ranges = Lists.mutable.empty();

        // Keep splitting until we can’t split anymore; collect per-pattern ranges
        ArrayDeque<Spliterator.OfLong> queue = new ArrayDeque<>();
        queue.add(entityKeys);

        while (!queue.isEmpty()) {
            Spliterator.OfLong s = queue.pollFirst();
            Spliterator.OfLong split = s.trySplit();
            if (split != null) {
                // Recurse on both halves
                queue.addFirst(s);
                queue.addFirst(split);
                continue;
            }
            // No further split: if this is a per-pattern range, collect it
            if (s instanceof SpliteratorForRocksKeyOfPattern sp) {
                ranges.add(sp);
            } else {
                // Fallback: if we encounter a non-per-pattern spliterator that can’t split,
                // try one more split attempt loop (defensive); otherwise, it should be tiny.
                // In practice, SpliteratorForEntityKeys.trySplit() hands out per-pattern spliterators,
                // so we should almost always end up here as SpliteratorForRocksKeyOfPattern.
                // If this happens, we can drain it sequentially as a very small tail:
                s.forEachRemaining((LongConsumer) (rocksKey) -> {
                    // Create a 1-element range to reuse scanEntitiesInRange
                    int pattern = (int) ((rocksKey >>> 48) & 0xFFFF);
                    long elementSeq = (rocksKey & 0xFFFFFFFFFFFFL);
                    SpliteratorForRocksKeyOfPattern singleton =
                            new SpliteratorForRocksKeyOfPattern(pattern, elementSeq, elementSeq + 1);
                    this.entityMap.scanEntitiesInRange(singleton, action);
                });
            }
        }

        LOG.info("Found {} EntityKey ranges to process.", ranges.size());

        // 2) Run each range in parallel with structured concurrency
        try (StructuredTaskScope<Object, Void, SubtaskFailedException> scope = StructuredScopes.open()) {
            for (SpliteratorForRocksKeyOfPattern range : ranges) {
                scope.fork(() -> {
                    this.entityMap.scanEntitiesInRange((LongSpliteratorOfPattern) range, action);
                    return null;
                });
            }
            scope.join();
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void forEachParallel(ImmutableIntList nids, ObjIntConsumer<byte[]> action) {
        // Collect directly to array
        long[] keys = new long[nids.size()];
        nids.forEachWithIndex((nid, index) -> keys[index] = NidLayout.active().rocksKeyForNid(nid));

        // Sort in place (no extra allocation)
        Arrays.parallelSort(keys);

        ImmutableList<LongSpliteratorOfPattern> parts = SortedLongArraySpliteratorOfPattern.of(keys);

        MutableList<LongSpliteratorOfPattern> subSpliterators = Lists.mutable.empty();
        int targetChunkSize = PrimitiveData.calculateOptimalChunkSize(nids.size());;

        for (LongSpliteratorOfPattern part : parts) {
            ArrayDeque<LongSpliteratorOfPattern> queue = new ArrayDeque<>();
            queue.add(part);

            while (!queue.isEmpty()) {
                LongSpliteratorOfPattern current = queue.poll();
                if (current.estimateSize() > targetChunkSize) {
                    LongSpliteratorOfPattern split = (LongSpliteratorOfPattern) current.trySplit();
                    if (split != null) {
                        queue.add(split);
                        queue.add(current);
                        continue;
                    }
                }
                subSpliterators.add(current);
            }
        }

        try (StructuredTaskScope<Object, Void, SubtaskFailedException> scope = StructuredScopes.open()) {
            for (LongSpliteratorOfPattern part : subSpliterators) {
                scope.fork(() -> {
                    this.entityMap.scanEntitiesInRange(part, action);
                    return null;
                });
            }
            scope.join();
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void forEach(ImmutableIntList nids, ObjIntConsumer<byte[]> action) {
        MutableLongCollection longCollection = LongLists.mutable.withInitialCapacity(nids.size());
        nids.collectLong(nid -> NidLayout.active().rocksKeyForNid(nid), longCollection);
        longCollection = longCollection.toSortedList();
        long[] keys = longCollection.toArray();
        // Pass false to disable parallel execution (splitting)
        ImmutableList<LongSpliteratorOfPattern> parts = SortedLongArraySpliteratorOfPattern.of(keys, false);
        for (LongSpliteratorOfPattern part : parts) {
            this.entityMap.scanEntitiesInRange(part, action);
        }    }

    @Override
    public byte[] getBytes(int nid) {
        checkOpen();
        return this.entityMap.get(rocksKeyForNid(nid));
    }

    @Override
    public byte[] merge(int nid, int patternNid, int referencedComponentNid, byte[] value, final Object sourceObject, final DataActivity activity) {
        checkOpen();
        if (nid == Integer.MIN_VALUE) {
            LOG.error("NID should not be Integer.MIN_VALUE");
            throw new IllegalStateException("NID should not be Integer.MIN_VALUE");
        }
        if (nid == 0) {
            LOG.error("NID should not be 0");
            throw new IllegalStateException("NID should not be 0");
        }
        if (nid == Integer.MAX_VALUE) {
            LOG.error("NID should not be Integer.MAX_VALUE");
            throw new IllegalStateException("NID should not be Integer.MAX_VALUE");
        }
        switch (sourceObject) {
            case SemanticEntity semanticEntity -> entityReferencingSemanticMap.add(EntityKey.ofNid(semanticEntity.referencedComponentNid()), EntityKey.ofNid(semanticEntity.nid()));
            default -> {}
        }
        // The put operation does its own merge in a simpler way...
        this.entityMap.put(rocksKeyForNid(nid), value);
        byte[] mergedBytes = this.entityMap.get(rocksKeyForNid(nid));
        if (mergedBytes == null) {
            throw new IllegalStateException("Merged bytes should not be null");
        }

        this.writeSequence.increment();

        ImmutableList<ChangeSetWriterService> changeSetWriterServices = this.changeSetWriterServices.orElseSet(this::changeSetWriterServicesList);
        changeSetWriterServices.forEach(writerService -> writerService.writeToChangeSet((Entity) sourceObject, activity));

        // Delegate indexing to SearchProvider.
        //
        // TODO(temp): During loadPhase, live-index up to LoadPhaseSearchPolicy's
        // threshold; once exceeded, skip the rest and fall back to a full
        // recreate at endLoadPhase. Replace this two-mode shim with touched-nid
        // notification + per-nid catch-up when the proper design lands.
        // See LoadPhaseSearchPolicy javadoc for the full picture.
        if (!loadPhase
                || dev.ikm.tinkar.entity.EntityService.get().loadPhaseSearchPolicy().shouldIndexLive()) {
            try {
                SearchService searchService = getSearchService();
                searchService.index(sourceObject);
            } catch (Exception e) {
                // SearchService may not be available yet during startup; log at debug with the entity info.
                LOG.debug("SearchService not available for real-time indexing; will rely on later index rebuild. Entity={}", sourceObject, e);
            }
        }

        return mergedBytes;
    }

    private SearchService getSearchService() {
        return searchService.orElseSet(() -> {
            LOG.info("RocksProvider.getSearchService() - Looking up SearchService via ServiceLifecycleManager");
            return ServiceLifecycleManager.get()
                .getRunningService(SearchService.class)
                .orElseThrow(() -> {
                    LOG.error("RocksProvider.getSearchService() - SearchService not found");
                    return new IllegalStateException("SearchService not available - ensure services are started");
                });
        });
    }

    @Override
    public PrimitiveDataSearchResult[] search(String query, int maxResultSize) throws Exception {
        LOG.debug("RocksProvider.search() called with query='{}', maxResultSize={}", query, maxResultSize);
        SearchService service = getSearchService();
        LOG.debug("RocksProvider.search() - Got SearchService, delegating search");
        PrimitiveDataSearchResult[] results = service.search(query, maxResultSize);
        LOG.debug("RocksProvider.search() - Search returned {} results", results != null ? results.length : 0);
        return results;
    }

    @Override
    public String highlight(String query, String text) throws Exception {
        return getSearchService().highlight(query, text);
    }

    @Override
    public void setLoadPhase(boolean loadPhase) {
        this.loadPhase = loadPhase;
    }

    @Override
    public CompletableFuture<Void> recreateLuceneIndex() {
        return getSearchService().recreateIndex();
    }

    /**
     * Visits the semantics of a pattern. The concept, stamp and pattern-of-patterns bindings key
     * concepts, stamps and patterns under their sequences, not semantics, so they have none here,
     * as in every other provider.
     *
     * @throws IllegalStateException if the nid is not a pattern
     */
    @Override
    public void forEachSemanticNidOfPattern(int patternNid, IntProcedure procedure) {
        if (patternNid == EntityBinding.Concept.pattern().nid()
                || patternNid == EntityBinding.Stamp.pattern().nid()
                || patternNid == EntityBinding.Pattern.pattern().nid()) {
            return;
        }
        EntityHandle.get(patternNid).expectPattern("Trying to iterate elements for entity that is not a pattern: ");
        forEachElementOfPattern(patternNid, procedure);
    }

    /** Visits every element keyed under a pattern's sequence, whatever kind of entity it is. */
    private void forEachElementOfPattern(int patternNid, IntProcedure procedure) {
        int patternSequence = (int) NidLayout.active().decodeElementSequence(patternNid);
        LongSpliteratorOfPattern spliteratorOfPattern = this.sequenceMap.spliteratorOfPattern(patternSequence);
        spliteratorOfPattern.forEachRemaining((LongConsumer) rocksKey -> procedure.accept(NidLayout.active().nidForRocksKey(rocksKey)));
    }

    @Override
    public void forEachPatternNid(IntProcedure procedure) {
        // Diagnostic for ikmdev/komet-desktop#12 (closed): what the pattern navigator's reload sees.
        long counterValue = sequenceMap.nextSequenceMap.get(dev.ikm.ds.rocks.maps.SequenceMap.patternPatternSequence()).get();
        java.util.concurrent.atomic.AtomicInteger visitedCount = new java.util.concurrent.atomic.AtomicInteger(0);
        LOG.debug("forEachPatternNid: iterating PATTERN_PATTERN_SEQUENCE={} element range [1, {})",
                dev.ikm.ds.rocks.maps.SequenceMap.patternPatternSequence(), counterValue);
        sequenceMap.spliteratorOfPatterns().forEachRemaining((LongConsumer) rocksKey -> {
            int nid = NidLayout.active().nidForRocksKey(rocksKey);
            // The binding patterns hold a sequence here whether or not a pattern was written
            // for them; a pattern no entity stands behind is not a pattern of this store.
            if (getBytes(nid) != null) {
                visitedCount.incrementAndGet();
                procedure.accept(nid);
            }
        });
        LOG.debug("forEachPatternNid: visited {} pattern nid(s)", visitedCount.get());
    }

    @Override
    public void forEachConceptNid(IntProcedure procedure) {
        forEachElementOfBindingPattern(EntityBinding.Concept.pattern(), procedure);
    }

    @Override
    public void forEachStampNid(IntProcedure procedure) {
        forEachElementOfBindingPattern(EntityBinding.Stamp.pattern(), procedure);
    }

    /**
     * Visits the elements keyed under a binding pattern. A store nothing has been written to
     * has no key for the pattern yet, and so no elements, rather than a nid to fail to find.
     */
    private void forEachElementOfBindingPattern(EntityProxy.Pattern bindingPattern, IntProcedure procedure) {
        bindingPatternKey(bindingPattern).ifPresent(patternKey -> forEachElementOfPattern(patternKey.nid(), procedure));
    }

    private Optional<EntityKey> bindingPatternKey(EntityProxy.Pattern bindingPattern) {
        for (UUID uuid : bindingPattern.asUuidArray()) {
            Optional<EntityKey> key = getEntityKey(uuid);
            if (key.isPresent()) {
                return key;
            }
        }
        return Optional.empty();
    }

    @Override
    public void forEachSemanticNid(IntProcedure procedure) {
        BitSet excludedPatternSequences = new BitSet();
        for (EntityProxy.Pattern bindingPattern : List.of(EntityBinding.Concept.pattern(),
                EntityBinding.Stamp.pattern(), EntityBinding.Pattern.pattern())) {
            bindingPatternKey(bindingPattern).ifPresent(key ->
                    excludedPatternSequences.set((int) NidLayout.active().decodeElementSequence(key.nid())));
        }

        ImmutableList<SpliteratorForRocksKeyOfPattern> semanticSpliterators = this.sequenceMap.allPatternSpliterators().select(spliterator -> !excludedPatternSequences.get(spliterator.patternSequence()));

        // Create a list to collect all the sub-spliterators
        MutableList<Spliterator.OfLong> subSpliterators = Lists.mutable.empty();

        // Process each semantic spliterator
        for (SpliteratorForRocksKeyOfPattern semanticSpliterator : semanticSpliterators) {
            // Keep splitting until we get chunks of the appropriate size
            Deque<Spliterator.OfLong> queue = new ArrayDeque<>();
            queue.add(semanticSpliterator);

            while (!queue.isEmpty()) {
                Spliterator.OfLong current = queue.poll();
                long estimatedSize = current.estimateSize();

                // Target chunk size - adjust this value based on your needs
                int targetChunkSize = 10000;

                if (estimatedSize > targetChunkSize) {
                    Spliterator.OfLong split = current.trySplit();
                    if (split != null) {
                        queue.add(split);
                        queue.add(current);
                        continue;
                    }
                }

                subSpliterators.add(current);
            }
        }

        LOG.info("Split into {} sub-tasks for parallel processing", subSpliterators.size());

        // Process chunks in parallel using structured concurrency
        try (StructuredTaskScope<Object, Void, SubtaskFailedException> scope = StructuredScopes.open()) {
            for (Spliterator.OfLong subSpliterator : subSpliterators) {
                scope.fork(() -> {
                    subSpliterator.forEachRemaining((LongConsumer) rocksKey ->
                            procedure.accept(NidLayout.active().nidForRocksKey(rocksKey)));
                    return null;
                });
            }
            scope.join();
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void forEachSemanticNidForComponent(int componentNid, IntProcedure procedure) {
        ImmutableList<EntityKey> referencingEntityKeys = this.entityReferencingSemanticMap.getReferencingEntityKeys(NidLayout.active().rocksKeyForNid(componentNid));
        referencingEntityKeys.forEach(entityKey -> procedure.accept(entityKey.nid()));
    }

    @Override
    public void forEachSemanticNidForComponentOfPattern(int componentNid, int patternNid, IntProcedure procedure) {
        ImmutableList<EntityKey> referencingEntityKeys = this.entityReferencingSemanticMap.getReferencingEntityKeysOfPattern(NidLayout.active().rocksKeyForNid(componentNid), (int) NidLayout.active().decodeElementSequence(patternNid));
        referencingEntityKeys.forEach(entityKey -> procedure.accept(entityKey.nid()));
    }

    @Override
    public String name() {
        return this.name;
    }

    /**
     * Base Controller for RocksProvider lifecycle management.
     * <p>     * Handles heavyweight initialization including data loading and indexing.
     */
    public abstract static class Controller extends ProviderController<RocksProvider>
            implements DataServiceController<PrimitiveDataService> {

        @Override
        protected RocksProvider createProvider() throws Exception {
            return RocksProvider.get();
        }

        @Override
        protected void startProvider(RocksProvider provider) {
            // Provider starts itself during get()
        }

        @Override
        protected void stopProvider(RocksProvider provider) {
            provider.close();
        }

        @Override
        protected void cleanupProvider(RocksProvider provider) throws Exception {
            // save() is already called in close(), no need to call it again
        }

        @Override
        protected String getProviderName() {
            return "RocksProvider";
        }

        @Override
        public ServiceLifecyclePhase getLifecyclePhase() {
            return ServiceLifecyclePhase.DATA_STORAGE;
        }

        @Override
        public Optional<ServiceExclusionGroup> getMutualExclusionGroup() {
            return Optional.of(ServiceExclusionGroup.DATA_PROVIDER);
        }

        @Override
        public Optional<String> openConflict(DataUriOption option) {
            return option == null ? Optional.empty()
                    : DataStoreLockProbe.openConflict(option.toFile());
        }

        // ========== DataServiceController Implementation ==========

        @Override
        public ImmutableList<Class<?>> serviceClasses() {
            // RocksProvider (the generic type parameter P) implements PrimitiveDataService
            // This establishes the contract: ProviderController<RocksProvider> provides PrimitiveDataService
            return Lists.immutable.of(PrimitiveDataService.class);
        }

        @Override
        public boolean running() {
            RocksProvider provider = getProvider();
            return provider != null && provider.running();
        }

        @Override
        public void start() {
            startup();
        }

        @Override
        public void stop() {
            shutdown();
        }

        @Override
        public void save() {
            RocksProvider provider = getProvider();
            if (provider != null) {
                provider.save();
            }
        }

        @Override
        public void reload() {
            throw new UnsupportedOperationException("Reload not yet supported");
        }

        // Note: provider() method is inherited from ProviderController base class
    }

    /**
     * Controller for opening an existing Rocks database.
     */
    public static class OpenController extends Controller {
        public static final String CONTROLLER_NAME = "Open Rocks KB";

        @Override
        public void setDataUriOption(DataUriOption option) {
            super.setDataUriOption(option);
            if (option != null) {
                ServiceProperties.set(ServiceKeys.DATA_STORE_ROOT, option.toFile());
                ServiceProperties.set(ServiceKeys.DATA_STORE_EXPECT_EMPTY, Boolean.FALSE);
            }
        }

        @Override
        public boolean isValidDataLocation(String name) {
            // Check if it's a directory that contains rocks database files
            File rootFolder = new File(System.getProperty("user.home"), "Solor");
            File checkDir = new File(rootFolder, name);
            if (checkDir.exists() && checkDir.isDirectory()) {
                File rocksDir = new File(checkDir, "rocks");
                return rocksDir.exists() && rocksDir.isDirectory();
            }
            return false;
        }

        @Override
        public String controllerName() {
            return CONTROLLER_NAME;
        }

        @Override
        public int getSubPriority() {
            return 30; // After MVStore
        }

        @Override
        public List<DataUriOption> providerOptions() {
            List<DataUriOption> dataUriOptions = new ArrayList<>();
            File rootFolder = new File(System.getProperty("user.home"), "Solor");
            if (!rootFolder.exists()) {
                rootFolder.mkdirs();
            }
            File[] files = rootFolder.listFiles();
            if (files != null) {
                for (File f : files) {
                    if (f.isDirectory() && isValidDataLocation(f.getName())) {
                        dataUriOptions.add(new DataUriOption(f.getName(), f.toURI()));
                    }
                }
            }
            return dataUriOptions;
        }
    }

    /**
     * Controller for creating a new Rocks database and importing data.
     */
    public static class NewController extends Controller {
        public static final String CONTROLLER_NAME = "New Rocks KB";
        public static final DataServiceProperty NEW_FOLDER_PROPERTY =
                new DataServiceProperty("New folder name", false, true);

        private final MutableMap<DataServiceProperty, String> providerProperties = Maps.mutable.empty();
        private String importDataFileString;
        private final AtomicBoolean loading = new AtomicBoolean(false);

        public NewController() {
            providerProperties.put(NEW_FOLDER_PROPERTY, null);
        }

        @Override
        public ImmutableMap<DataServiceProperty, String> providerProperties() {
            return providerProperties.toImmutable();
        }

        @Override
        public void setDataServiceProperty(DataServiceProperty key, String value) {
            providerProperties.replace(key, value);
        }

        @Override
        public ValidationRecord[] validate(DataServiceProperty dataServiceProperty, Object value, Object target) {
            if (NEW_FOLDER_PROPERTY.equals(dataServiceProperty)) {
                File rootFolder = new File(System.getProperty("user.home"), "Solor");
                if (value instanceof String fileName) {
                    if (fileName.isBlank()) {
                        return new ValidationRecord[]{new ValidationRecord(ValidationSeverity.ERROR,
                                "Directory name cannot be blank", target)};
                    } else {
                        File possibleFile = new File(rootFolder, fileName);
                        if (possibleFile.exists() && !isEmptyDirectory(possibleFile)) {
                            return new ValidationRecord[]{new ValidationRecord(ValidationSeverity.ERROR,
                                    "Directory exists and is not empty", target)};
                        }
                    }
                }
            }
            return new ValidationRecord[]{};
        }

        @Override
        public List<DataUriOption> providerOptions() {
            List<DataUriOption> dataUriOptions = new ArrayList<>();
            File rootFolder = new File(System.getProperty("user.home"), "Solor");
            if (!rootFolder.exists()) {
                rootFolder.mkdirs();
            }
            File[] files = rootFolder.listFiles();
            if (files != null) {
                for (File f : files) {
                    if (isValidDataLocation(f.getName())) {
                        dataUriOptions.add(new DataUriOption(f.getName(), f.toURI()));
                    }
                }
            }
            return dataUriOptions;
        }

        @Override
        public void setDataUriOption(DataUriOption option) {
            super.setDataUriOption(option);
            if (option != null) {
                // toFile() decodes the URI; URL.getFile() would keep %20 for a space (ike-issues#1156).
                importDataFileString = option.toFile().getAbsolutePath();
            }
        }

        @Override
        protected RocksProvider createProvider() throws Exception {
            // Ensure DATA_STORE_ROOT is set before RocksProvider is constructed.
            File rootFolder = new File(System.getProperty("user.home"), "Solor");
            String folderName = providerProperties.get(NEW_FOLDER_PROPERTY);
            if (folderName == null || folderName.isBlank()) {
                throw new IllegalStateException("New folder name not set for New Rocks KB");
            }
            File dataDirectory = new File(rootFolder, folderName);
            ServiceProperties.set(ServiceKeys.DATA_STORE_EXPECT_EMPTY, Boolean.TRUE);
            assertNewDataDirectory(dataDirectory);
            ServiceProperties.set(ServiceKeys.DATA_STORE_ROOT, dataDirectory);
            return RocksProvider.get();
        }

        @Override
        protected void initializeProvider(RocksProvider provider) throws Exception {
            try {
                loading.set(true);
                // Set up the data directory from properties
                File rootFolder = new File(System.getProperty("user.home"), "Solor");
                File dataDirectory = new File(rootFolder, providerProperties.get(NEW_FOLDER_PROPERTY));
                ServiceProperties.set(ServiceKeys.DATA_STORE_ROOT, dataDirectory);

                // Queue data file for loading in DATA_LOAD phase (don't load it now)
                if (importDataFileString != null) {
                    File importFile = new File(importDataFileString);
                    LOG.info("═══════════════════════════════════════════════════════════");
                    LOG.info("RocksProvider.NewController: Queueing starter data for deferred import");
                    LOG.info("  Import file: {}", importFile.getName());
                    LOG.info("  Full path: {}", importFile.getAbsolutePath());
                    LOG.info("  File size: {} MB", importFile.length() / (1024.0 * 1024.0));
                    LOG.info("  Data directory: {}", dataDirectory.getAbsolutePath());
                    LOG.info("  Loading phase: DATA_LOAD (runs after INDEXING phase)");
                    LOG.info("  This ensures all required services are ready before import");
                    LOG.info("═══════════════════════════════════════════════════════════");

                    // Get DataLoadProvider singleton and queue the file
                    // DataLoadController will load this during DATA_LOAD phase after all services are ready
                    dev.ikm.tinkar.entity.load.DataLoadProvider dataLoadService =
                            dev.ikm.tinkar.entity.load.DataLoadProvider.get();
                    dataLoadService.addFile(importFile);

                    LOG.info("Import file successfully queued - DataLoadController will process in DATA_LOAD phase");
                } else {
                    LOG.warn("No import file specified - creating empty database");
                    LOG.warn("Database will not contain starter data (DESCRIPTION_PATTERN, etc.)");
                }
            } finally {
                loading.set(false);
            }
        }

        @Override
        public boolean isValidDataLocation(String name) {
            return name.toLowerCase().endsWith("pb.zip") ||
                    (name.toLowerCase().endsWith(".zip") && name.toLowerCase().contains("tink"));
        }

        @Override
        public String controllerName() {
            return CONTROLLER_NAME;
        }

        @Override
        public int getSubPriority() {
            return 31; // After Open
        }

        @Override
        public boolean loading() {
            return loading.get();
        }

        private static void assertNewDataDirectory(File dir) {
            if (dir.exists()) {
                if (!dir.isDirectory()) {
                    throw new IllegalStateException("New Rocks KB path is not a directory: " + dir.getAbsolutePath());
                }
                if (!isEmptyDirectory(dir)) {
                    throw new IllegalStateException("New Rocks KB directory is not empty: " + dir.getAbsolutePath());
                }
            }
        }

        private static boolean isEmptyDirectory(File dir) {
            if (!dir.exists()) {
                return true;
            }
            if (!dir.isDirectory()) {
                return false;
            }
            String[] entries = dir.list();
            return entries == null || entries.length == 0;
        }
    }
}
