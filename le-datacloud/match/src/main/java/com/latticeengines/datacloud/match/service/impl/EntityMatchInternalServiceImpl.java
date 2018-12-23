package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.common.exposed.util.ValidationUtils.checkNotNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.EntityLookupEntryService;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.datacloud.match.service.EntityMatchInternalService;
import com.latticeengines.datacloud.match.service.EntityRawSeedService;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("entityMatchInternalService")
public class EntityMatchInternalServiceImpl implements EntityMatchInternalService {
    private static final Logger log = LoggerFactory.getLogger(EntityMatchInternalServiceImpl.class);

    private static final String LOOKUP_BACKGROUND_THREAD_NAME = "entity-match-internal";
    private static final int ENTITY_ID_LENGTH = 16;
    private static final int MAX_ID_ALLOCATION_ATTEMPTS = 50;
    // make the probability even for all characters since we want to have case insensitive ID
    private static final char[] ENTITY_ID_CHARS = "0123456789abcdefghijklmnopqrstuvwxyz".toCharArray();

    // TODO move this to property file or config service after the mechanism is finalized
    private static final int NUM_LOOKUP_BACKGROUND_THREADS = 3;
    private static final int LOOKUP_BATCH_SIZE = 25;
    private static final long LOOKUP_POLLING_TIMEOUT_IN_MILLIS = 5000;
    private static final long LOOKUP_POPULATE_SLEEP_IN_MILLIS = 200;
    private static final long TERMINATION_TIMEOUT_IN_MILLIS = 30000;

    private final EntityLookupEntryService entityLookupEntryService;
    private final EntityRawSeedService entityRawSeedService;
    private final EntityMatchConfigurationService entityMatchConfigurationService;

    // flag to indicate whether background workers should keep running
    private volatile boolean shouldTerminate = false;

    /*
     * TODO set this value at startup time (find a way to know whether we are in which env)
     * isRealTimeMode = true means (a) no allocation, (b) cache seed, (c) only lookup from cache & serving table
     * isRealTimeMode = false means
     *   (a) has allocation, (b) not caching seed, (c) lookup from cache, staging and serving table
     */
    private boolean isRealTimeMode = false;
    // [ tenant ID, lookup entry ] => seed ID
    private volatile Cache<Pair<String, EntityLookupEntry>, String> lookupCache;
    // [ [ tenant ID, entity ], seed ID ] => raw seed
    private volatile Cache<Pair<Pair<String, String>, String>, EntityRawSeed> seedCache;

    private BlockingQueue<Triple<Tenant, EntityLookupEntry, String>> lookupQueue = new LinkedBlockingQueue<>();
    private AtomicLong nProcessingLookupEntries = new AtomicLong(0L);
    private volatile ExecutorService lookupExecutorService; // thread pool for populating lookup staging table

    @Inject
    public EntityMatchInternalServiceImpl(
            EntityLookupEntryService entityLookupEntryService, EntityRawSeedService entityRawSeedService,
            EntityMatchConfigurationService entityMatchConfigurationService) {
        this.entityLookupEntryService = entityLookupEntryService;
        this.entityRawSeedService = entityRawSeedService;
        this.entityMatchConfigurationService = entityMatchConfigurationService;
    }

    @Override
    public String getId(@NotNull Tenant tenant, @NotNull EntityLookupEntry lookupEntry) {
        checkNotNull(tenant, lookupEntry);
        List<String> ids = getIds(tenant, Collections.singletonList(lookupEntry));
        Preconditions.checkNotNull(ids);
        Preconditions.checkArgument(ids.size() == 1);
        return ids.get(0);
    }

    @Override
    public List<String> getIds(@NotNull Tenant tenant, @NotNull List<EntityLookupEntry> lookupEntries) {
        check(tenant, lookupEntries);
        if (lookupEntries.isEmpty()) {
            return Collections.emptyList();
        }

        // make sure required service are loaded properly
        lazyInitServices();

        return getIdsInternal(tenant, lookupEntries);
    }

    @Override
    public EntityRawSeed get(@NotNull Tenant tenant, @NotNull String entity, @NotNull String seedId) {
        checkNotNull(tenant, entity, seedId);
        List<EntityRawSeed> seeds = get(tenant, entity, Collections.singletonList(seedId));
        Preconditions.checkNotNull(seeds);
        Preconditions.checkArgument(seeds.size() == 1);
        return seeds.get(0);
    }

    @Override
    public List<EntityRawSeed> get(@NotNull Tenant tenant, @NotNull String entity, @NotNull List<String> seedIds) {
        check(tenant, seedIds);
        checkNotNull(entity);
        if (seedIds.isEmpty()) {
            return Collections.emptyList();
        }

        // make sure required service are loaded properly
        lazyInitServices();

        return getSeedsInternal(tenant, entity, seedIds);
    }

    @Override
    public String allocateId(@NotNull Tenant tenant, @NotNull String entity) {
        checkNotNull(tenant, entity);
        if (isRealTimeMode) {
            throw new UnsupportedOperationException("Not allowed to allocate ID in realtime mode");
        }
        // [ idx, seedId ]
        Optional<Pair<Integer, String>> allocatedId = IntStream
                .range(0, MAX_ID_ALLOCATION_ATTEMPTS)
                .mapToObj(idx -> {
                    String id = newId();
                    // use serving as the single source of truth
                    boolean created = entityRawSeedService
                            .createIfNotExists(EntityMatchEnvironment.SERVING, tenant, entity, id);
                    return created ? Pair.of(idx, id) : null;
                })
                .findFirst();
        if (!allocatedId.isPresent()) {
            // fail to allocate
            log.error("Failed to allocate ID for entity = {} in tenant ID = {} after {} attempts",
                    entity, tenant.getId(), MAX_ID_ALLOCATION_ATTEMPTS);
            throw new IllegalStateException("Failed to allocate entity ID");
        }
        int nAttempts = allocatedId.get().getKey();
        if (nAttempts > 1) {
            log.info("Encounter conflict when allocating ID, succeeded after {} attempts", nAttempts);
        }
        return allocatedId.get().getValue();
    }

    @Override
    public Triple<EntityRawSeed, List<EntityLookupEntry>, List<EntityLookupEntry>> associate(
            @NotNull Tenant tenant, @NotNull EntityRawSeed seed) {
        EntityMatchEnvironment env = EntityMatchEnvironment.STAGING; // only change staging seed
        checkNotNull(tenant, seed);
        if (isRealTimeMode) {
            throw new UnsupportedOperationException("Not allowed to associate entity in realtime mode");
        }

        // update seed & lookup table and get all entries that cannot update
        EntityRawSeed seedBeforeUpdate = entityRawSeedService.updateIfNotSet(env, tenant, seed);
        Map<Pair<EntityLookupEntry.Type, String>, Set<String>> existingLookupPairs =
                getExistingLookupPairs(seedBeforeUpdate);
        Set<EntityLookupEntry> entriesFailedToAssociate = getLookupEntriesFailedToAssociate(existingLookupPairs, seed);
        List<EntityLookupEntry> entriesFailedToSetLookup =
                mapLookupEntriesToSeed(env, tenant, existingLookupPairs, seed);

        // clear one to one entries in seed that we failed to set in the lookup table
        List<EntityLookupEntry> entriesToClear = entriesFailedToSetLookup
                .stream()
                .filter(entry -> entry.getType().mapping == EntityLookupEntry.Mapping.ONE_TO_ONE)
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(entriesToClear)) {
            EntityRawSeed seedToClear = new EntityRawSeed(seed.getId(), seed.getEntity(), entriesToClear, null);
            entityRawSeedService.clear(env, tenant, seedToClear);
        }

        return Triple.of(seedBeforeUpdate, new ArrayList<>(entriesFailedToAssociate), entriesFailedToSetLookup);
    }

    @VisibleForTesting
    boolean isRealTimeMode() {
        return isRealTimeMode;
    }

    @VisibleForTesting
    void setRealTimeMode(boolean realTimeMode) {
        isRealTimeMode = realTimeMode;
    }

    @VisibleForTesting
    Cache<Pair<String, EntityLookupEntry>, String> getLookupCache() {
        return lookupCache;
    }

    @VisibleForTesting
    Cache<Pair<Pair<String, String>, String>, EntityRawSeed> getSeedCache() {
        return seedCache;
    }

    @VisibleForTesting
    long getProcessingLookupEntriesCount() {
        return nProcessingLookupEntries.get();
    }

    /*
     * shutdown all workers and block until all background threads finishes
     */
    @VisibleForTesting
    synchronized void shutdownAndAwaitTermination() throws Exception {
        shouldTerminate = true;
        if (lookupExecutorService != null) {
            lookupExecutorService.shutdown();
            lookupExecutorService.awaitTermination(TERMINATION_TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS);
        }
    }

    /*
     * Map: [ type, serializedKey ] => Set(serializedValue)
     * NOTE use set for all lookup entries to make code shorter, even though one to one should have at most one value
     */
    @VisibleForTesting
    protected Map<Pair<EntityLookupEntry.Type, String>, Set<String>> getExistingLookupPairs(
            EntityRawSeed seedBeforeUpdate) {
        if (seedBeforeUpdate == null) {
            return Collections.emptyMap();
        }

        return seedBeforeUpdate
                .getLookupEntries()
                .stream()
                .map(entry -> {
                    Pair<EntityLookupEntry.Type, String> key = Pair.of(entry.getType(), entry.getSerializedKeys());
                    return Pair.of(key, entry.getSerializedValues());
                })
                // group by the key and collect the values to a set
                .collect(groupingBy(Pair::getKey, mapping(Pair::getValue, toSet())));
    }

    /*
     * get all entries that (a) is X to one and (b) already has a different value in current seed
     */
    @VisibleForTesting
    protected Set<EntityLookupEntry> getLookupEntriesFailedToAssociate(
            @NotNull Map<Pair<EntityLookupEntry.Type, String>, Set<String>> existingLookupPairs,
            @NotNull EntityRawSeed seed) {
        return seed
                .getLookupEntries()
                .stream()
                // only X to one are possible fail to update seed
                .filter(entry -> entry.getType().mapping == EntityLookupEntry.Mapping.ONE_TO_ONE
                        || entry.getType().mapping == EntityLookupEntry.Mapping.MANY_TO_ONE)
                .filter(entry -> {
                    Pair<EntityLookupEntry.Type, String> key = Pair.of(entry.getType(), entry.getSerializedKeys());
                    // not already have value or have value but not equals
                    return existingLookupPairs.containsKey(key)
                            && !existingLookupPairs.get(key).contains(entry.getSerializedValues());
                })
                .collect(toSet());
    }

    /*
     * update lookup entries that need to be mapped to the seed and
     * return all entries that already mapped to another seed
     */
    @VisibleForTesting
    protected List<EntityLookupEntry> mapLookupEntriesToSeed(
            @NotNull EntityMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull Map<Pair<EntityLookupEntry.Type, String>, Set<String>> existingLookupPairs,
            @NotNull EntityRawSeed seed) {
        return seed.getLookupEntries()
                .stream()
                .filter(entry -> {
                    // get all lookup entry that need to mapped to seed ID
                    Pair<EntityLookupEntry.Type, String> key = Pair.of(entry.getType(), entry.getSerializedKeys());
                    EntityLookupEntry.Mapping mapping = entry.getType().mapping;
                    if (mapping == EntityLookupEntry.Mapping.ONE_TO_ONE
                            || mapping == EntityLookupEntry.Mapping.MANY_TO_ONE) {
                        // if mapping is x to 1, when key is in existing seed, either
                        //  (a) already have other value, cannot update
                        //  (b) have the same value, no need to update
                        return !existingLookupPairs.containsKey(key);
                    } else {
                        // if mapping is many to many, only need to update if we don't have the exact lookup entry
                        return !existingLookupPairs.containsKey(key)
                                || !existingLookupPairs.get(key).contains(entry.getSerializedValues());
                    }
                })
                // try to map the lookup entry to seed and return if seed is mapped successfully
                // NOTE use setIfEquals because two threads might be mapping the same entry to the same seed
                //      need to consider this case as success in both threads
                .map(entry -> Pair.of(entry, entityLookupEntryService.setIfEquals(env, tenant, entry, seed.getId())))
                .filter(pair -> !pair.getValue()) // only get the ones failed to set
                .map(Pair::getKey)
                .collect(Collectors.toList());
    }

    /*
     * Retrieve list of seed IDs using input list of lookup entries.
     * Start from local cache -> staging table -> serving table and perform synchronization between layers.
     */
    private List<String> getIdsInternal(@NotNull Tenant tenant, @NotNull List<EntityLookupEntry> lookupEntries) {
        String tenantId = tenant.getId();
        Set<EntityLookupEntry> uniqueEntries = new HashSet<>(lookupEntries);
        // retrieve seed IDs from cache
        Map<EntityLookupEntry, String> results =  getPresentCacheValues(
                tenantId, lookupEntries, lookupCache);
        if (results.size() == uniqueEntries.size()) {
            // have all the seed IDs
            return generateResult(lookupEntries, results);
        }

        // get lookup entries that are not present in local cache and try staging layer
        Set<EntityLookupEntry> missingEntries = getMissingKeys(uniqueEntries, results);
        Map<EntityLookupEntry, String> missingResults = getIdsStaging(tenant, missingEntries);

        // add to results
        results.putAll(missingResults);
        // populate in-memory cache
        putInCache(tenantId, missingResults, lookupCache);

        return generateResult(lookupEntries, results);
    }

    /*
     * Retrieve seed IDs, starting at staging layer.
     */
    private Map<EntityLookupEntry, String> getIdsStaging(@NotNull Tenant tenant, @NotNull Set<EntityLookupEntry> keys) {
        if (isRealTimeMode) {
            // in real time mode, skip staging layer
            return getIdsServing(tenant, keys);
        }

        // now in bulk mode
        Map<EntityLookupEntry, String> results = getIdsInEnvironment(tenant, EntityMatchEnvironment.STAGING, keys);
        if (results.size() == keys.size()) {
            return results;
        }

        // get lookup entries that are not present in staging table and try serving layer
        Map<EntityLookupEntry, String> missingResults = getIdsServing(tenant, getMissingKeys(keys, results));
        results.putAll(missingResults);

        // NOTE we can publish lookup entry asynchronously because we will not try to update
        //      lookup entry that is already in serving

        // increase # of processing entries
        nProcessingLookupEntries.addAndGet(missingResults.size());
        // populate missing seed IDs to staging layer (async, batch)
        missingResults
                .entrySet()
                .stream()
                .map(entry -> Triple.of(tenant, entry.getKey(), entry.getValue()))
                .forEach(lookupQueue::offer);

        return results;
    }

    /*
     * Retrieve seed IDs in serving layer
     */
    private Map<EntityLookupEntry, String> getIdsServing(
            @NotNull Tenant tenant, @NotNull Set<EntityLookupEntry> uniqueEntries) {
        return getIdsInEnvironment(tenant, EntityMatchEnvironment.SERVING, uniqueEntries);
    }

    /*
     * Retrieve seed IDs in the given environment and returns a map from lookup entry to seed ID
     */
    private Map<EntityLookupEntry, String> getIdsInEnvironment(
            @NotNull Tenant tenant, @NotNull EntityMatchEnvironment env, @NotNull Set<EntityLookupEntry> uniqueEntries) {
        List<EntityLookupEntry> entries = new ArrayList<>(uniqueEntries);
        List<String> seedIds = entityLookupEntryService
                .get(env, tenant, entries);
        return listToMap(entries, seedIds);
    }

    /*
     * Retrieve list of raw seeds using input list of seed IDs.
     * Start from local cache -> staging table -> serving table and perform synchronization between layers.
     */
    private List<EntityRawSeed> getSeedsInternal(
            @NotNull Tenant tenant, @NotNull String entity, @NotNull List<String> seedIds) {
        // need entity here because seed ID does not contain this info (unlike lookup entry)
        String tenantId = tenant.getId();
        Pair<String, String> prefix = Pair.of(tenantId, entity);
        Set<String> uniqueSeedIds = new HashSet<>(seedIds);
        if (!isRealTimeMode) {
            // in bulk mode, does not cache seed in-memory because seed will be updated and invalidating cache
            // for multiple processes will be difficult to do.
            return generateResult(seedIds, getSeedsStaging(tenant, entity, uniqueSeedIds));
        }

        // NOTE in real time mode

        // results from cache
        Map<String, EntityRawSeed> results =  getPresentCacheValues(prefix, seedIds, seedCache);

        Set<String> missingSeedIds = getMissingKeys(uniqueSeedIds, results);
        // real time mode goes directly to serving table (skip staging)
        Map<String, EntityRawSeed> missingResults = getSeedsServing(tenant, entity, missingSeedIds);

        // add to results
        results.putAll(missingResults);
        // populate in-memory cache
        putInCache(prefix, missingResults, seedCache);

        return generateResult(seedIds, results);
    }

    /*
     * Retrieve raw seeds, starting from staging
     */
    private Map<String, EntityRawSeed> getSeedsStaging(
            @NotNull Tenant tenant, @NotNull String entity, @NotNull Set<String> seedIds) {
        if (isRealTimeMode) {
            throw new IllegalStateException("Should not reach here in real time mode.");
        }

        // bulk mode
        Map<String, EntityRawSeed> results = getSeedsInEnvironment(tenant, EntityMatchEnvironment.STAGING, entity, seedIds);
        if (results.size() == seedIds.size()) {
            return results;
        }

        Map<String, EntityRawSeed> missingResults = getSeedsServing(tenant, entity, getMissingKeys(seedIds, results));
        results.putAll(missingResults);

        // populate staging table
        // NOTE we need to update seed synchronously because we will use update expression to update each lookup entry
        //      in the seed. therefore we need to make sure the same seed is in staging before we returns anything
        missingResults
                .values()
                .forEach(seed -> entityRawSeedService.setIfNotExists(EntityMatchEnvironment.STAGING, tenant, seed));

        return results;
    }

    /*
     * Retrieve raw seeds in serving table
     */
    private Map<String, EntityRawSeed> getSeedsServing(
            @NotNull Tenant tenant, @NotNull String entity, @NotNull Set<String> seedIds) {
        return getSeedsInEnvironment(tenant, EntityMatchEnvironment.SERVING, entity, seedIds);
    }

    /*
     * Retrieve seeds in the given environment and returns a map from seed ID to seed
     */
    private Map<String, EntityRawSeed> getSeedsInEnvironment(
            @NotNull Tenant tenant, @NotNull EntityMatchEnvironment env,
            @NotNull String entity, @NotNull Set<String> uniqueSeedIds) {
        List<String> seedIds = new ArrayList<>(uniqueSeedIds);
        List<EntityRawSeed> seeds = entityRawSeedService.get(env, tenant, entity, seedIds);
        return listToMap(seedIds, seeds);
    }

    private <K, V> Map<K, V> listToMap(List<K> keys, List<V> values) {
        Preconditions.checkArgument(keys.size() == values.size());
        return IntStream.range(0, keys.size())
                .filter(idx -> values.get(idx) != null)
                .mapToObj(idx -> Pair.of(keys.get(idx), values.get(idx)))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    /*
     * Populate given results into in-memory cache. Prefix will be added to the key of result as cache key.
     */
    private <K, T, V> void putInCache(@NotNull T prefix, Map<K, V> results, Cache<Pair<T, K>, V> cache) {
        cache.putAll(results
                .entrySet()
                .stream()
                .map(entry -> Triple.of(prefix, entry.getKey(), entry.getValue()))
                // [ prefix, result.key ] => result.value
                .collect(Collectors.toMap(triple -> Pair.of(triple.getLeft(), triple.getMiddle()), Triple::getRight)));
    }

    /*
     * Build cache key from input list of keys and retrieve all cached values.
     */
    private <K, T, V> Map<K, V> getPresentCacheValues(
            @NotNull T prefix, List<K> keys, Cache<Pair<T, K>, V> cache) {
        Set<Pair<T, K>> keysForCache = keys
                .stream()
                .map(entry -> Pair.of(prefix, entry))
                .collect(toSet());
        return cache
                .getAllPresent(keysForCache)
                .entrySet()
                .stream()
                .map(entry -> Pair.of(entry.getKey().getValue(), entry.getValue()))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    /*
     * Helper to return a set of keys that does not exist in result map.
     */
    private <K, V> Set<K> getMissingKeys(@NotNull Set<K> keys, @NotNull Map<K, V> foundValues) {
        return keys.stream().filter(key -> !foundValues.containsKey(key)).collect(toSet());
    }

    /*
     * Generate list of values where each item will be the value associated to the same item in input key list.
     * Null will be added to indicate no value is associated with respective key.
     */
    private <K, V> List<V> generateResult(@NotNull List<K> originalInputs, @NotNull Map<K, V> foundValues) {
        return originalInputs.stream().map(foundValues::get).collect(Collectors.toList());
    }

    /*
     * random an entity ID (return in lowercase)
     */
    private String newId() {
        return RandomStringUtils.random(ENTITY_ID_LENGTH, ENTITY_ID_CHARS);
    }

    private void check(@NotNull Tenant tenant, @NotNull List<?> list) {
        checkNotNull(tenant, list);
        list.forEach(Preconditions::checkNotNull);
    }

    /*
     * Lazily instantiate all internal services (e.g., thread pools).
     */
    private void lazyInitServices() {
        if (shouldTerminate) {
            return;
        }
        initStagingWorkers();
        initSeedCache();
        initLookupCache();
    }

    /*
     * Lazily instantiate seed cache
     */
    private void initSeedCache() {
        if (seedCache != null) {
            return;
        }

        synchronized (this) {
            if (seedCache == null) {
                log.info("Instantiating raw seed cache, maxWeight = {}, maxIdleDuration = {}",
                        getMaxSeedCacheWeight(), entityMatchConfigurationService.getMaxSeedCacheIdleDuration());
                seedCache = Caffeine
                        .newBuilder()
                        // use weight instead of number of entry because the number of lookup entries in seed can vary
                        .weigher(this::getWeight)
                        .maximumWeight(getMaxSeedCacheWeight())
                        // expire after idle for a certain amount of time
                        .expireAfterAccess(entityMatchConfigurationService.getMaxSeedCacheIdleDuration())
                        .build();
            }
        }
    }

    /*
     * NOTE this is a rough estimate which the method is described below.
     *
     * 1. Seed used for testing is an seed with empty lookup entries and extra attributes (not used atm)
     * 2. Using ObjectSizeCalculator#getObjectSize we get around 230 bytes for empty seed
     * 3. Cache key ([ [ tenantId, entity ], seedId ] takes around 200 bytes
     * 4. Insert 1M ~ 10M seeds to memory cache and calculate the memory usage using
     *    RunTime#totalMemory - RunTime#freeMemory
     * 5. Average of 4 is around 290 bytes
     * 6. Decide to use 400 bytes per entry for now (easier to calculate). Therefore 1MiB => 2500 entries
     */
    @VisibleForTesting
    protected long getMaxSeedCacheWeight() {
        return entityMatchConfigurationService.getMaxSeedCacheMemoryInMB() * 2500;
    }

    /*
     * Memory used by one raw seed cache and one lookup cache is pretty close.
     * Currently, to make things simpler, use 1 (seed) + # of lookup entries as weight
     */
    private int getWeight(Pair<Pair<String, String>, String> key, EntityRawSeed val) {
        return 1 + (val == null ? 0 : val.getLookupEntries().size());
    }

    /*
     * Lazily instantiate lookup entry cache
     */
    private void initLookupCache() {
        if (lookupCache != null) {
            return;
        }

        synchronized (this) {
            if (lookupCache == null) {
                log.info("Instantiating lookup entry cache, maxSize = {}, maxIdleDuration = {}",
                        getMaxLookupCacheSize(), entityMatchConfigurationService.getMaxLookupCacheIdleDuration());
                lookupCache = Caffeine
                        .newBuilder()
                        // max number of lookup entries allowed in cache
                        .maximumSize(getMaxLookupCacheSize())
                        // expire after idle for a certain amount of time
                        .expireAfterAccess(entityMatchConfigurationService.getMaxLookupCacheIdleDuration())
                        .build();
            }
        }
    }

    /*
     * NOTE this is a rough estimate which the method is described below.
     *
     * 1. Lookup entry used for testing is a domain/country entry, with domain length = 16 & country length = 10
     *    values are random & seedID is random as well
     * 2. Using ObjectSizeCalculator#getObjectSize we get around 400 bytes for lookup entry
     * 3. Seed ID mapped by the lookup entry takes 16 bytes
     * 4. Insert 1M ~ 10M random entries to memory cache and calculate the memory usage using
     *    RunTime#totalMemory - RunTime#freeMemory
     * 5. Average of 4 is around 300 bytes
     * 6. Decide to use 400 bytes per entry for now. Therefore 1MiB => 2500 entries
     */
    @VisibleForTesting
    protected long getMaxLookupCacheSize() {
        return entityMatchConfigurationService.getMaxLookupCacheMemoryInMB() * 2500;
    }

    /*
     * Lazily instantiate thread pools and runnables for populating staging seed/lookup table
     */
    private void initStagingWorkers() {
        if (isRealTimeMode) {
            // NOTE no need to populate staging table in real time mode
            return;
        }
        // TODO tune all pool size

        if (lookupExecutorService == null) {
            synchronized (this) {
                if (lookupExecutorService == null) {
                    log.info("Instantiating staging lookup entry publishers");
                    lookupExecutorService = ThreadPoolUtils
                            .getFixedSizeThreadPool(LOOKUP_BACKGROUND_THREAD_NAME, NUM_LOOKUP_BACKGROUND_THREADS);
                    IntStream
                            .range(0, NUM_LOOKUP_BACKGROUND_THREADS)
                            .forEach((idx) -> lookupExecutorService.submit(new StagingLookupEntryPublisher()));
                }
            }
        }
    }

    /*
     * Worker that poll from a queue and populate lookup entries to staging table
     *
     * NOTE the populating will be done in batch. while some entry might be written multiple times (if multiple thread
     * read the same entry at the same time while the entry is not in staging), this should be very rare and
     * the batching efficiency improvement should outweigh the duplicate writes penalty.
     */
    private class StagingLookupEntryPublisher implements Runnable {
        @Override
        public void run() {
            // TODO add info log for reporting total populated entries (report every X entries populated)
            // TODO tune the polling mechanism
            int total = 0;
            Map<String, List<Triple<Tenant, EntityLookupEntry, String>>> batches = new HashMap<>();
            while (!shouldTerminate) {
                try {
                    Triple<Tenant, EntityLookupEntry, String> triple = lookupQueue.poll(
                            LOOKUP_POLLING_TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS);
                    if (triple != null) {
                        String tenantId = triple.getLeft().getId(); // should not be null
                        batches.putIfAbsent(tenantId, new ArrayList<>());
                        batches.get(tenantId).add(triple); // add to local list
                        total++;
                    }
                    // triple == null means timeout
                    if (triple == null || total >= LOOKUP_BATCH_SIZE) {
                        populate(batches);
                        // finishes total entries, decrease the same amount in processing entries counter
                        nProcessingLookupEntries.addAndGet(-total);
                        // clear populated batches
                        total = 0;
                        batches.clear();
                        Thread.sleep(LOOKUP_POPULATE_SLEEP_IN_MILLIS);
                    }
                } catch (InterruptedException e) {
                    if (!shouldTerminate) {
                        log.error("Staging lookup entry publisher (in background) is interrupted", e);
                    }
                } catch (Exception e) {
                    log.error("Encounter an error (in background) in staging lookup entry publisher", e);
                }
            }
        }

        private void populate(Map<String, List<Triple<Tenant, EntityLookupEntry, String>>> batches) {
            // since bulk mode should only have one tenant, map is probably not required, use map just in case
            batches.values().forEach(list -> {
                if (CollectionUtils.isEmpty(list)) {
                    return;
                }
                List<Pair<EntityLookupEntry, String>> pairs = list
                        .stream()
                        .map(triple -> Pair.of(triple.getMiddle(), triple.getRight()))
                        .collect(Collectors.toList());
                Tenant tenant = list.get(0).getLeft(); // tenant in the list should all be the same
                entityLookupEntryService.set(EntityMatchEnvironment.STAGING, tenant, pairs);
            });
        }
    }
}
