package com.latticeengines.datacloud.match.service.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.CDLEntityMatchInternalService;
import com.latticeengines.datacloud.match.service.CDLLookupEntryService;
import com.latticeengines.datacloud.match.service.CDLRawSeedService;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.cdl.CDLRawSeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.latticeengines.common.exposed.util.ValidationUtils.checkNotNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

@Component("cdlEntityMatchInternalService")
public class CDLEntityMatchInternalServiceImpl implements CDLEntityMatchInternalService {
    private static final Logger log = LoggerFactory.getLogger(CDLEntityMatchInternalServiceImpl.class);

    /*
     * TODO unit test intermediate functions
     */

    private static final String LOOKUP_BACKGROUND_THREAD_NAME = "cdl-match-lookup-internal";
    private static final int ENTITY_ID_LENGTH = 16;
    private static final int MAX_ID_ALLOCATION_ATTEMPTS = 50;

    // TODO move this to property file or config service after the mechanism is finalized
    private static final int NUM_LOOKUP_BACKGROUND_THREADS = 3;
    private static final int LOOKUP_BATCH_SIZE = 25;
    private static final long LOOKUP_POLLING_TIMEOUT_IN_MILLIS = 5000;
    private static final long LOOKUP_POPULATE_SLEEP_IN_MILLIS = 200;
    private static final long TERMINATION_TIMEOUT_IN_MILLIS = 30000;

    private final CDLLookupEntryService cdlLookupEntryService;
    private final CDLRawSeedService cdlRawSeedService;

    // flag to indicate whether background workers should keep running
    private volatile boolean shouldTerminate = false;

    /*
     * TODO set this value at startup time (find a way to know whether we are in which env)
     * isRealTimeMode = true means (a) no allocation, (b) cache seed, (c) only lookup from cache & serving table
     * isRealTimeMode = false means
     *   (a) has allocation, (b) not caching seed, (c) lookup from cache, staging and serving table
     */
    private boolean isRealTimeMode = false;
    // [ tenant PID, lookup entry ] => seed ID
    private volatile Cache<Pair<Long, CDLLookupEntry>, String> lookupCache;
    // [ tenant PID, seed ID ] => raw seed
    private volatile Cache<Pair<Pair<Long, BusinessEntity>, String>, CDLRawSeed> seedCache;

    private BlockingQueue<Triple<Tenant, CDLLookupEntry, String>> lookupQueue = new LinkedBlockingQueue<>();
    private volatile ExecutorService lookupExecutorService; // thread pool for populating lookup staging table

    @Inject
    public CDLEntityMatchInternalServiceImpl(
            CDLLookupEntryService cdlLookupEntryService, CDLRawSeedService cdlRawSeedService) {
        this.cdlLookupEntryService = cdlLookupEntryService;
        this.cdlRawSeedService = cdlRawSeedService;
    }

    @Override
    public String getId(@NotNull Tenant tenant, @NotNull CDLLookupEntry lookupEntry) {
        checkNotNull(tenant, lookupEntry);
        List<String> ids = getIds(tenant, Collections.singletonList(lookupEntry));
        Preconditions.checkNotNull(ids);
        Preconditions.checkArgument(ids.size() == 1);
        return ids.get(0);
    }

    @Override
    public List<String> getIds(@NotNull Tenant tenant, @NotNull List<CDLLookupEntry> lookupEntries) {
        check(tenant, lookupEntries);
        if (lookupEntries.isEmpty()) {
            return Collections.emptyList();
        }

        // make sure required service are loaded properly
        lazyInitServices();

        return getIdsInternal(tenant, lookupEntries);
    }

    @Override
    public CDLRawSeed get(@NotNull Tenant tenant, @NotNull BusinessEntity entity, @NotNull String seedId) {
        checkNotNull(tenant, entity, seedId);
        List<CDLRawSeed> seeds = get(tenant, entity, Collections.singletonList(seedId));
        Preconditions.checkNotNull(seeds);
        Preconditions.checkArgument(seeds.size() == 1);
        return seeds.get(0);
    }

    @Override
    public List<CDLRawSeed> get(@NotNull Tenant tenant, @NotNull BusinessEntity entity, @NotNull List<String> seedIds) {
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
    public String allocateId(@NotNull Tenant tenant, @NotNull BusinessEntity entity) {
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
                    boolean created = cdlRawSeedService
                            .createIfNotExists(CDLMatchEnvironment.SERVING, tenant, entity, id);
                    return created ? Pair.of(idx, id) : null;
                })
                .findFirst();
        if (!allocatedId.isPresent()) {
            // fail to allocate
            log.error("Failed to allocate ID for entity = {} in tenant PID = {} after {} attempts",
                    entity, tenant.getPid(), MAX_ID_ALLOCATION_ATTEMPTS);
            throw new IllegalStateException("Failed to allocate entity ID");
        }
        int nAttempts = allocatedId.get().getKey();
        if (nAttempts > 1) {
            log.info("Encounter conflict when allocating ID, succeeded after {} attempts", nAttempts);
        }
        return allocatedId.get().getValue();
    }

    @Override
    public Triple<CDLRawSeed, List<CDLLookupEntry>, List<CDLLookupEntry>> associate(
            @NotNull Tenant tenant, @NotNull CDLRawSeed seed) {
        CDLMatchEnvironment env = CDLMatchEnvironment.STAGING; // only change staging seed
        checkNotNull(tenant, seed);
        if (isRealTimeMode) {
            throw new UnsupportedOperationException("Not allowed to associate entity in realtime mode");
        }

        // update seed & lookup table and get all entries that cannot update
        CDLRawSeed seedBeforeUpdate = cdlRawSeedService.updateIfNotSet(env, tenant, seed);
        Map<Pair<CDLLookupEntry.Type, String>, Set<String>> existingLookupPairs =
                getExistingLookupPairs(seedBeforeUpdate);
        Set<CDLLookupEntry> entriesFailedToAssociate = getLookupEntriesFailedToAssociate(existingLookupPairs, seed);
        List<CDLLookupEntry> entriesFailedToSetLookup = mapLookupEntriesToSeed(env, tenant, existingLookupPairs, seed);

        // clear one to one entries in seed that we failed to set in the lookup table
        List<CDLLookupEntry> entriesToClear = entriesFailedToSetLookup
                .stream()
                .filter(entry -> entry.getType().mapping == CDLLookupEntry.Mapping.ONE_TO_ONE)
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(entriesToClear)) {
            CDLRawSeed seedToClear = new CDLRawSeed(seed.getId(), seed.getEntity(), entriesToClear, null);
            cdlRawSeedService.clear(env, tenant, seedToClear);
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
    Cache<Pair<Long, CDLLookupEntry>, String> getLookupCache() {
        return lookupCache;
    }

    @VisibleForTesting
    Cache<Pair<Pair<Long, BusinessEntity>, String>, CDLRawSeed> getSeedCache() {
        return seedCache;
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
     * TODO put this in some util class
     * Map: [ type, serializedKey ] => Set(serializedValue)
     * NOTE use set for all lookup entries to make code shorter, even though one to one should have at most one value
     */
    private Map<Pair<CDLLookupEntry.Type, String>, Set<String>> getExistingLookupPairs(CDLRawSeed seedBeforeUpdate) {
        if (seedBeforeUpdate == null) {
            return Collections.emptyMap();
        }

        return seedBeforeUpdate
                .getLookupEntries()
                .stream()
                .map(entry -> {
                    Pair<CDLLookupEntry.Type, String> key = Pair.of(entry.getType(), entry.getSerializedKeys());
                    return Pair.of(key, entry.getSerializedValues());
                })
                // group by the key and collect the values to a set
                .collect(groupingBy(Pair::getKey, mapping(Pair::getValue, toSet())));
    }

    /*
     * TODO put into some util class
     * get all entries that (a) is one to one and (b) already has a different value in current seed
     */
    private Set<CDLLookupEntry> getLookupEntriesFailedToAssociate(
            @NotNull Map<Pair<CDLLookupEntry.Type, String>, Set<String>> existingLookupPairs,
            @NotNull CDLRawSeed seed) {
        return seed
                .getLookupEntries()
                .stream()
                // only one to one are possible fail to update
                .filter(entry -> entry.getType().mapping == CDLLookupEntry.Mapping.ONE_TO_ONE)
                .filter(entry -> {
                    Pair<CDLLookupEntry.Type, String> key = Pair.of(entry.getType(), entry.getSerializedKeys());
                    // not already have value or have value but not equals
                    return existingLookupPairs.containsKey(key)
                            && !existingLookupPairs.get(key).contains(entry.getSerializedValues());
                })
                .collect(Collectors.toSet());
    }

    /*
     * TODO unit test this
     * update lookup entries that need to be mapped to the seed and
     * return all entries that already mapped to another seed
     */
    private List<CDLLookupEntry> mapLookupEntriesToSeed(
            @NotNull CDLMatchEnvironment env, @NotNull Tenant tenant,
            @NotNull Map<Pair<CDLLookupEntry.Type, String>, Set<String>> existingLookupPairs,
            @NotNull CDLRawSeed seed) {
        return seed.getLookupEntries()
                .stream()
                .filter(entry -> {
                    // get all lookup entry that need to mapped to seed ID
                    Pair<CDLLookupEntry.Type, String> key = Pair.of(entry.getType(), entry.getSerializedKeys());
                    CDLLookupEntry.Mapping mapping = entry.getType().mapping;
                    if (mapping == CDLLookupEntry.Mapping.ONE_TO_ONE) {
                        // if mapping is 1 to 1, when key is in existing seed, either
                        //  (a) already have other value, cannot update
                        //  (b) have the same value, no need to update
                        return !existingLookupPairs.containsKey(key);
                    } else {
                        // if mapping is many to X, only need to update if we don't have the exact lookup entry
                        return !existingLookupPairs.containsKey(key)
                                || !existingLookupPairs.get(key).contains(entry.getSerializedValues());
                    }
                })
                // try to map the lookup entry to seed and return if seed is mapped successfully
                // NOTE use setIfEquals because two threads might be mapping the same entry to the same seed
                //      need to consider this case as success in both threads
                .map(entry -> Pair.of(entry, cdlLookupEntryService.setIfEquals(env, tenant, entry, seed.getId())))
                .filter(pair -> !pair.getValue()) // only get the ones failed to set
                .map(Pair::getKey)
                .collect(Collectors.toList());
    }

    /*
     * Retrieve list of seed IDs using input list of lookup entries.
     * Start from local cache -> staging table -> serving table and perform synchronization between layers.
     */
    private List<String> getIdsInternal(@NotNull Tenant tenant, @NotNull List<CDLLookupEntry> lookupEntries) {
        Set<CDLLookupEntry> uniqueEntries = new HashSet<>(lookupEntries);
        // retrieve seed IDs from cache
        Map<CDLLookupEntry, String> results =  getPresentCacheValues(tenant.getPid(), lookupEntries, lookupCache);
        if (results.size() == uniqueEntries.size()) {
            // have all the seed IDs
            return generateResult(lookupEntries, results);
        }

        // get lookup entries that are not present in local cache and try staging layer
        Set<CDLLookupEntry> missingEntries = getMissingKeys(uniqueEntries, results);
        Map<CDLLookupEntry, String> missingResults = getIdsStaging(tenant, missingEntries);

        // add to results
        results.putAll(missingResults);
        // populate in-memory cache
        putInCache(tenant.getPid(), missingResults, lookupCache);

        return generateResult(lookupEntries, results);
    }

    /*
     * Retrieve seed IDs, starting at staging layer.
     */
    private Map<CDLLookupEntry, String> getIdsStaging(@NotNull Tenant tenant, @NotNull Set<CDLLookupEntry> keys) {
        if (isRealTimeMode) {
            // in real time mode, skip staging layer
            return getIdsServing(tenant, keys);
        }

        // now in bulk mode
        Map<CDLLookupEntry, String> results = getIdsInEnvironment(tenant, CDLMatchEnvironment.STAGING, keys);
        if (results.size() == keys.size()) {
            return results;
        }

        // get lookup entries that are not present in staging table and try serving layer
        Map<CDLLookupEntry, String> missingResults = getIdsServing(tenant, getMissingKeys(keys, results));
        results.putAll(missingResults);

        // NOTE we can publish lookup entry asynchronously because we will not try to update
        //      lookup entry that is already in serving

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
    private Map<CDLLookupEntry, String> getIdsServing(
            @NotNull Tenant tenant, @NotNull Set<CDLLookupEntry> uniqueEntries) {
        return getIdsInEnvironment(tenant, CDLMatchEnvironment.SERVING, uniqueEntries);
    }

    /*
     * Retrieve seed IDs in the given environment and returns a map from lookup entry to seed ID
     */
    private Map<CDLLookupEntry, String> getIdsInEnvironment(
            @NotNull Tenant tenant, @NotNull CDLMatchEnvironment env, @NotNull Set<CDLLookupEntry> uniqueEntries) {
        List<CDLLookupEntry> entries = new ArrayList<>(uniqueEntries);
        List<String> seedIds = cdlLookupEntryService
                .get(env, tenant, entries);
        return IntStream
                .range(0, entries.size())
                .filter(idx -> seedIds.get(idx) != null)
                .mapToObj(idx -> Pair.of(entries.get(idx), seedIds.get(idx)))
                .collect(Collectors.toMap(Pair::getKey, Pair::getRight));
    }

    /*
     * Retrieve list of raw seeds using input list of seed IDs.
     * Start from local cache -> staging table -> serving table and perform synchronization between layers.
     */
    private List<CDLRawSeed> getSeedsInternal(
            @NotNull Tenant tenant, @NotNull BusinessEntity entity, @NotNull List<String> seedIds) {
        // need entity here because seed ID does not contain this info (unlike lookup entry)
        Pair<Long, BusinessEntity> prefix = Pair.of(tenant.getPid(), entity);
        Set<String> uniqueSeedIds = new HashSet<>(seedIds);
        if (!isRealTimeMode) {
            // in bulk mode, does not cache seed in-memory because seed will be updated and invalidating cache
            // for multiple processes will be difficult to do.
            return generateResult(seedIds, getSeedsStaging(tenant, entity, uniqueSeedIds));
        }

        // NOTE in real time mode

        // results from cache
        Map<String, CDLRawSeed> results =  getPresentCacheValues(prefix, seedIds, seedCache);

        Set<String> missingSeedIds = getMissingKeys(uniqueSeedIds, results);
        // real time mode goes directly to serving table (skip staging)
        Map<String, CDLRawSeed> missingResults = getSeedsServing(tenant, entity, missingSeedIds);

        // add to results
        results.putAll(missingResults);
        // populate in-memory cache
        putInCache(prefix, missingResults, seedCache);

        return generateResult(seedIds, results);
    }

    /*
     * Retrieve raw seeds, starting from staging
     */
    private Map<String, CDLRawSeed> getSeedsStaging(
            @NotNull Tenant tenant, @NotNull BusinessEntity entity, @NotNull Set<String> seedIds) {
        if (isRealTimeMode) {
            throw new IllegalStateException("Should not reach here in real time mode.");
        }

        // bulk mode
        Map<String, CDLRawSeed> results = getSeedsInEnvironment(tenant, CDLMatchEnvironment.STAGING, entity, seedIds);
        if (results.size() == seedIds.size()) {
            return results;
        }

        Map<String, CDLRawSeed> missingResults = getSeedsServing(tenant, entity, getMissingKeys(seedIds, results));
        results.putAll(missingResults);

        // populate staging table
        // NOTE we need to update seed synchronously because we will use update expression to update each lookup entry
        //      in the seed. therefore we need to make sure the same seed is in staging before we returns anything
        missingResults
                .values()
                .forEach(seed -> cdlRawSeedService.setIfNotExists(CDLMatchEnvironment.STAGING, tenant, seed));

        return results;
    }

    /*
     * Retrieve raw seeds in serving table
     */
    private Map<String, CDLRawSeed> getSeedsServing(
            @NotNull Tenant tenant, @NotNull BusinessEntity entity, @NotNull Set<String> seedIds) {
        return getSeedsInEnvironment(tenant, CDLMatchEnvironment.SERVING, entity, seedIds);
    }

    /*
     * Retrieve seeds in the given environment and returns a map from seed ID to seed
     */
    private Map<String, CDLRawSeed> getSeedsInEnvironment(
            @NotNull Tenant tenant, @NotNull CDLMatchEnvironment env,
            @NotNull BusinessEntity entity, @NotNull Set<String> uniqueSeedIds) {
        List<String> seedIds = new ArrayList<>(uniqueSeedIds);
        List<CDLRawSeed> seeds = cdlRawSeedService.get(env, tenant, entity, seedIds);
        return IntStream
                .range(0, seedIds.size())
                .filter(idx -> seeds.get(idx) != null)
                .mapToObj(idx -> Pair.of(seedIds.get(idx), seeds.get(idx)))
                .collect(Collectors.toMap(Pair::getKey, Pair::getRight));
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
                .collect(Collectors.toSet());
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
        return keys.stream().filter(key -> !foundValues.containsKey(key)).collect(Collectors.toSet());
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
        // TODO make the probability even for all characters
        return RandomStringUtils.randomAlphanumeric(ENTITY_ID_LENGTH).toLowerCase();
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
                log.info("Instantiating raw seed cache");
                // TODO tune the cache setting
                seedCache = Caffeine.newBuilder().build();
            }
        }
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
                log.info("Instantiating lookup entry cache");
                // TODO tune the cache setting
                lookupCache = Caffeine.newBuilder().build();
            }
        }
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
            Map<Long, List<Triple<Tenant, CDLLookupEntry, String>>> batches = new HashMap<>();
            while (!shouldTerminate) {
                try {
                    Triple<Tenant, CDLLookupEntry, String> triple = lookupQueue.poll(
                            LOOKUP_POLLING_TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS);
                    if (triple != null) {
                        long pid = triple.getLeft().getPid(); // should not be null
                        batches.putIfAbsent(pid, new ArrayList<>());
                        batches.get(pid).add(triple); // add to local list
                        total++;
                    }
                    // triple == null means timeout
                    if (triple == null || total >= LOOKUP_BATCH_SIZE) {
                        populate(batches);
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

        private void populate(Map<Long, List<Triple<Tenant, CDLLookupEntry, String>>> batches) {
            // since bulk mode should only have one tenant, map is probably not required, use map just in case
            batches.values().forEach(list -> {
                if (CollectionUtils.isEmpty(list)) {
                    return;
                }
                List<Pair<CDLLookupEntry, String>> pairs = list
                        .stream()
                        .map(triple -> Pair.of(triple.getMiddle(), triple.getRight()))
                        .collect(Collectors.toList());
                Tenant tenant = list.get(0).getLeft(); // tenant in the list should all be the same
                cdlLookupEntryService.set(CDLMatchEnvironment.STAGING, tenant, pairs);
            });
        }
    }
}
