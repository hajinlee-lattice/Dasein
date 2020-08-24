package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.common.exposed.util.ValidationUtils.checkNotNull;
import static com.latticeengines.datacloud.match.util.EntityMatchUtils.shouldSetTTL;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ENTITY_ANONYMOUS_ID;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.TERMINATE_EXECUTOR_TIMEOUT_MS;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment.SERVING;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment.STAGING;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
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
import com.latticeengines.datacloud.match.service.EntityMatchMetricService;
import com.latticeengines.datacloud.match.service.EntityMatchVersionService;
import com.latticeengines.datacloud.match.service.EntityRawSeedService;
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishStatistics;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityTransactUpdateResult;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("entityMatchInternalService")
public class EntityMatchInternalServiceImpl implements EntityMatchInternalService {
    private static final Logger log = LoggerFactory.getLogger(EntityMatchInternalServiceImpl.class);

    private static final String LOOKUP_BACKGROUND_THREAD_NAME = "entity-match-internal";
    private static final int ENTITY_ID_LENGTH = 16;
    private static final int MAX_ID_ALLOCATION_ATTEMPTS = 50;
    private static final int DYNAMO_TXN_LIMIT = 25; // max number of records involved in one dynamo txn
    // make the probability even for all characters since we want to have case insensitive ID
    private static final char[] ENTITY_ID_CHARS = "0123456789abcdefghijklmnopqrstuvwxyz".toCharArray();

    // params for populating lookup entry to staging table in background
    @Value("${datacloud.match.entity.lookup.populate.num.threads}")
    private int nLookupPopulateThreads;
    @Value("${datacloud.match.entity.lookup.populate.batch.size:25}")
    private int lookupPopulateBatchSize;
    @Value("${datacloud.match.entity.lookup.populate.poll.interval.ms:5000}")
    private long lookupPopulatePollIntervalMillis;
    @Value("${datacloud.match.entity.lookup.populate.sleep.ms:200}")
    private long lookupPopulateSleepMillis;

    private final EntityLookupEntryService entityLookupEntryService;
    private final EntityRawSeedService entityRawSeedService;
    private final EntityMatchConfigurationService entityMatchConfigurationService;
    private final EntityMatchMetricService entityMatchMetricService;
    private final EntityMatchVersionService entityMatchVersionService;

    // flag to indicate whether background workers should keep running
    private volatile boolean shouldTerminate = false;

    /*-
     * Use serving version across the board cuz
     * (a) in lookup mode we use serving version
     * (b) in allocate mode cache is used by single job and not shared, using serving is more consistent
     */
    // [ tenant ID, serving version, lookup entry ] => seed ID
    private volatile Cache<Triple<String, Integer, EntityLookupEntry>, String> lookupCache;
    // [ [ tenant ID, entity ], serving version, seed ID ] => raw seed
    private volatile Cache<Triple<Pair<String, String>, Integer, String>, EntityRawSeed> seedCache;
    // cache for anonymous entities
    // [ tenant ID, entity, serving version ] => raw seed
    private volatile Cache<Triple<String, String, Integer>, EntityRawSeed> anonymousSeedCache;

    private BlockingQueue<StagingLookupPublishRequest> lookupQueue = new LinkedBlockingQueue<>();
    private AtomicLong nProcessingLookupEntries = new AtomicLong(0L);
    private volatile ExecutorService lookupExecutorService; // thread pool for populating lookup staging table

    @Inject
    public EntityMatchInternalServiceImpl(
            EntityLookupEntryService entityLookupEntryService, EntityRawSeedService entityRawSeedService,
            EntityMatchConfigurationService entityMatchConfigurationService,
            EntityMatchVersionService entityMatchVersionService,
            @Lazy EntityMatchMetricService entityMatchMetricService) {
        this.entityLookupEntryService = entityLookupEntryService;
        this.entityRawSeedService = entityRawSeedService;
        this.entityMatchVersionService = entityMatchVersionService;
        this.entityMatchConfigurationService = entityMatchConfigurationService;
        this.entityMatchMetricService = entityMatchMetricService;
    }

    @Override
    public String getId(@NotNull Tenant tenant, @NotNull EntityLookupEntry lookupEntry,
            Map<EntityMatchEnvironment, Integer> versionMap) {
        checkNotNull(tenant, lookupEntry);
        List<String> ids = getIds(tenant, Collections.singletonList(lookupEntry), versionMap);
        Preconditions.checkNotNull(ids);
        Preconditions.checkArgument(ids.size() == 1);
        return ids.get(0);
    }

    @Override
    public List<String> getIds(@NotNull Tenant tenant, @NotNull List<EntityLookupEntry> lookupEntries,
            Map<EntityMatchEnvironment, Integer> versionMap) {
        checkNotNull(tenant, lookupEntries);
        if (lookupEntries.isEmpty()) {
            return Collections.emptyList();
        }

        // make sure required service are loaded properly
        lazyInitServices();

        Map<EntityLookupEntry, String> seedIdMap = getIdsInternal(tenant,
                lookupEntries.stream().filter(Objects::nonNull).collect(Collectors.toList()), versionMap);
        return generateResult(lookupEntries, seedIdMap);
    }

    @Override
    public EntityRawSeed get(@NotNull Tenant tenant, @NotNull String entity, @NotNull String seedId,
            Map<EntityMatchEnvironment, Integer> versionMap) {
        checkNotNull(tenant, entity, seedId);
        List<EntityRawSeed> seeds = get(tenant, entity, Collections.singletonList(seedId), versionMap);
        Preconditions.checkNotNull(seeds);
        Preconditions.checkArgument(seeds.size() == 1);
        return seeds.get(0);
    }

    @Override
    public List<EntityRawSeed> get(@NotNull Tenant tenant, @NotNull String entity, @NotNull List<String> seedIds,
            Map<EntityMatchEnvironment, Integer> versionMap) {
        checkNotNull(tenant, seedIds);
        checkNotNull(entity);
        if (seedIds.isEmpty()) {
            return Collections.emptyList();
        }

        // make sure required service are loaded properly
        lazyInitServices();

        Map<String, EntityRawSeed> resultMap = getSeedsInternal(tenant, entity,
                seedIds.stream().filter(Objects::nonNull).collect(Collectors.toList()), versionMap);
        return generateResult(seedIds, resultMap);
    }

    @Override
    public EntityRawSeed getOrCreateAnonymousSeed(@NotNull Tenant tenant, @NotNull String entity,
            Map<EntityMatchEnvironment, Integer> versionMap) {
        checkNotNull(tenant, entity);
        if (!isAllocateMode()) {
            throw new UnsupportedOperationException("Not allowed to create anonymous seed in lookup mode");
        }

        // make sure required service are loaded properly
        lazyInitServices();

        int servingVersion = getMatchVersion(SERVING, tenant, versionMap);
        Triple<String, String, Integer> cacheKey = Triple.of(tenant.getId(), entity, servingVersion);
        EntityRawSeed seed = anonymousSeedCache.getIfPresent(cacheKey);
        if (seed != null) {
            // already have anonymous in cache
            return seed;
        }

        seed = get(tenant, entity, ENTITY_ANONYMOUS_ID, versionMap);
        boolean isNewSeed = false;
        if (seed == null) {
            // no anonymous entity in staging & serving, create one
            seed = new EntityRawSeed(ENTITY_ANONYMOUS_ID, entity, false);

            // is considered new if we set the staging seed successfully
            isNewSeed = entityRawSeedService.setIfNotExists(STAGING, tenant, seed, shouldSetTTL(STAGING),
                    getMatchVersion(STAGING, tenant, versionMap));
        }

        // populate cache (isNewlyAllocated=false since it's already created for the
        // following calls)
        anonymousSeedCache.put(cacheKey, seed);
        // return seed with isNewlyAllocated=true if anonymous entity is just created
        return isNewSeed ? new EntityRawSeed(ENTITY_ANONYMOUS_ID, entity, true) : seed;
    }

    @Override
    public String allocateId(@NotNull Tenant tenant, @NotNull String entity, String preferredId,
            Map<EntityMatchEnvironment, Integer> versionMap) {
        checkNotNull(tenant, entity);
        if (!isAllocateMode()) {
            throw new UnsupportedOperationException("Not allowed to allocate ID in lookup mode");
        }
        EntityMatchEnvironment env = EntityMatchEnvironment.SERVING;
        // try to allocate preferred ID
        if (StringUtils.isNotBlank(preferredId)) {
            boolean created = entityRawSeedService.createIfNotExists(env, tenant, entity, preferredId,
                    shouldSetTTL(env), getMatchVersion(env, tenant, versionMap));
            if (created) {
                // preferredId is not taken
                return preferredId;
            }
        }

        // allocate random ID, format = [ idx, seedId ]
        Optional<Pair<Integer, String>> allocatedId = IntStream
                .range(0, MAX_ID_ALLOCATION_ATTEMPTS)
                .mapToObj(idx -> {
                    String id = newId();
                    // use serving as the single source of truth
                    boolean created = entityRawSeedService
                            .createIfNotExists(env, tenant, entity, id, shouldSetTTL(env),
                                    getMatchVersion(env, tenant, versionMap));
                    return created ? Pair.of(idx, id) : null;
                })
                .filter(Objects::nonNull) //
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
            @NotNull Tenant tenant, @NotNull EntityRawSeed seed, boolean clearAllFailedLookupEntries,
            Set<EntityLookupEntry> entriesMapToOtherSeed, Map<EntityMatchEnvironment, Integer> versionMap) {
        EntityMatchEnvironment env = EntityMatchEnvironment.STAGING; // only change staging seed
        checkNotNull(tenant, seed);
        if (!isAllocateMode()) {
            throw new UnsupportedOperationException("Not allowed to associate entity in lookup mode");
        }

        // update seed & lookup table and get all entries that cannot update
        EntityRawSeed seedBeforeUpdate = entityRawSeedService.updateIfNotSet(env, tenant, seed, shouldSetTTL(env),
                getMatchVersion(env, tenant, versionMap));
        Map<Pair<EntityLookupEntry.Type, String>, Set<String>> existingLookupPairs =
                getExistingLookupPairs(seedBeforeUpdate);
        Set<EntityLookupEntry> entriesFailedToAssociate = getLookupEntriesFailedToAssociate(existingLookupPairs, seed);
        List<EntityLookupEntry> entriesFailedToSetLookup =
                mapLookupEntriesToSeed(env, tenant, existingLookupPairs, seed, clearAllFailedLookupEntries,
                        entriesMapToOtherSeed, versionMap);

        // clear one to one entries in seed that we failed to set in the lookup table
        List<EntityLookupEntry> entriesToClear = entriesFailedToSetLookup
                .stream()
                .filter(entry -> clearAllFailedLookupEntries
                        || entry.getType().mapping == EntityLookupEntry.Mapping.ONE_TO_ONE)
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(entriesToClear)) {
            EntityRawSeed seedToClear = new EntityRawSeed(seed.getId(), seed.getEntity(), entriesToClear, null);
            entityRawSeedService.clear(env, tenant, seedToClear, getMatchVersion(env, tenant, versionMap));
        }

        return Triple.of(seedBeforeUpdate, new ArrayList<>(entriesFailedToAssociate), entriesFailedToSetLookup);
    }

    @Override
    public EntityTransactUpdateResult transactAssociate(@NotNull Tenant tenant, @NotNull EntityRawSeed seed,
            Set<EntityLookupEntry> entriesMapToOtherSeed, Map<EntityMatchEnvironment, Integer> versionMap) {
        EntityMatchEnvironment env = EntityMatchEnvironment.STAGING; // only change staging seed
        checkNotNull(tenant, seed);
        if (!isAllocateMode()) {
            throw new UnsupportedOperationException("Not allowed to associate entity in lookup mode");
        }
        int version = getMatchVersion(env, tenant, versionMap);

        // first txn, if this fail then consider association failure
        int startIdx = 0;
        EntityRawSeed seedToUpdate = seedForUpdate(seed, startIdx, true);
        Preconditions.checkNotNull(seedToUpdate,
                String.format("Should have either lookup entries or attributes for association. seed = %s", seed));
        EntityTransactUpdateResult result = entityRawSeedService.transactUpdate(env, tenant, seedToUpdate,
                seedToUpdate.getLookupEntries().stream() //
                        .filter(entry -> !CollectionUtils.emptyIfNull(entriesMapToOtherSeed).contains(entry)) //
                        .collect(Collectors.toList()),
                shouldSetTTL(env), version);
        if (!result.isSucceeded()) {
            // just return when first txn failed, not handling un-processed batch for now,
            // can optimize later if needed
            return result;
        }
        startIdx += seedToUpdate.getLookupEntries().size();

        // since first txn succeeded, shouldn't have conflict
        Map<EntityLookupEntry, String> entriesConflictInLookup = new HashMap<>();
        // handle following batches, ignore any conflict and accept that entries will
        // not be updated for now (since we are far from reaching 25 items, this part
        // shouldn't be executed at all)
        for (; (seedToUpdate = seedForUpdate(seed, startIdx, false)) != null; startIdx += seedToUpdate
                .getLookupEntries().size()) {
            result = entityRawSeedService.transactUpdate(env, tenant, seedToUpdate,
                    seedToUpdate.getLookupEntries().stream() //
                            .filter(entry -> !CollectionUtils.emptyIfNull(entriesMapToOtherSeed).contains(entry)) //
                            .collect(Collectors.toList()),
                    shouldSetTTL(env), version);
            if (!result.isSucceeded()) {
                // TODO handle non-first txn conflicts and merge seed into the final result
                entriesConflictInLookup.putAll(result.getEntriesMapToOtherSeeds());
            }
        }

        return new EntityTransactUpdateResult(true, result.getSeed(), entriesConflictInLookup);
    }

    @Override
    public void cleanupOrphanSeed(@NotNull Tenant tenant, @NotNull String entity, @NotNull String seedId,
            Map<EntityMatchEnvironment, Integer> versionMap) {
        checkNotNull(tenant, entity, seedId);
        // TODO batch the seedIds and cleanup async

        // cleanup staging first so the seed ID cannot be allocated before we cleanup
        // all environments
        entityRawSeedService.delete(STAGING, tenant, entity, seedId, getMatchVersion(STAGING, tenant, versionMap));
        entityRawSeedService.delete(EntityMatchEnvironment.SERVING, tenant, entity, seedId,
                getMatchVersion(SERVING, tenant, versionMap));
    }

    @Override
    public EntityPublishStatistics publishEntity(@NotNull String entity, @NotNull Tenant sourceTenant,
            @NotNull Tenant destTenant, @NotNull EntityMatchEnvironment destEnv, Boolean destTTLEnabled,
            Integer srcStagingVersion, Integer destEnvVersion) {
        sourceTenant = EntityMatchUtils.newStandardizedTenant(sourceTenant);
        destTenant = EntityMatchUtils.newStandardizedTenant(destTenant);
        // try to get the latest version
        entityMatchVersionService.invalidateCache(sourceTenant);
        entityMatchVersionService.invalidateCache(destTenant);

        EntityMatchEnvironment sourceEnv = STAGING;
        if (sourceTenant.getId().equals(destTenant.getId()) && sourceEnv == destEnv) {
            // return with default publish count as 0
            return new EntityPublishStatistics();
        }
        if (destTTLEnabled == null) {
            destTTLEnabled = EntityMatchUtils.shouldSetTTL(destEnv);
        }

        int seedCount = 0;
        int lookupCount = 0;
        int nNotInStaging = 0;
        List<String> getSeedIds = new ArrayList<>();
        List<EntityRawSeed> scanSeeds = new ArrayList<>();
        do {
            Map<Integer, List<EntityRawSeed>> seeds = entityRawSeedService.scan(sourceEnv, sourceTenant, entity,
                    getSeedIds, 1000, getMatchVersion(sourceEnv, sourceTenant, srcStagingVersion));
            getSeedIds.clear();
            if (MapUtils.isNotEmpty(seeds)) {
                for (Map.Entry<Integer, List<EntityRawSeed>> entry : seeds.entrySet()) {
                    getSeedIds.add(entry.getValue().get(entry.getValue().size() - 1).getId());
                    scanSeeds.addAll(entry.getValue());
                }
                List<Pair<EntityLookupEntry, String>> pairs = new ArrayList<>();
                for (EntityRawSeed seed : scanSeeds) {
                    List<String> seedIds = entityLookupEntryService.get(sourceEnv, sourceTenant,
                            seed.getLookupEntries(), getMatchVersion(sourceEnv, sourceTenant, srcStagingVersion));
                    for (int i = 0; i < seedIds.size(); i++) {
                        if (seedIds.get(i) == null) {
                            nNotInStaging++;
                            continue;
                        }
                        if (seedIds.get(i).equals(seed.getId())) {
                            pairs.add(Pair.of(seed.getLookupEntries().get(i), seedIds.get(i)));
                        }
                    }

                }
                entityRawSeedService.batchCreate(destEnv, destTenant, scanSeeds, destTTLEnabled,
                        getMatchVersion(destEnv, destTenant, destEnvVersion));
                entityLookupEntryService.set(destEnv, destTenant, pairs, destTTLEnabled,
                        getMatchVersion(destEnv, destTenant, destEnvVersion));
                seedCount += scanSeeds.size();
                lookupCount += pairs.size();
            }
            scanSeeds.clear();
        } while (CollectionUtils.isNotEmpty(getSeedIds));
        return new EntityPublishStatistics(seedCount, lookupCount, nNotInStaging);
    }

    /**
     * Create a new seed contains specified part of lookup entries from original
     * seed & attributes for update
     *
     * @param seed
     *            original seed for update
     * @param startIdx
     *            start index to retrieve lookup entries from original seed
     * @param includeAttributes
     *            whether to include attributes from original seed
     * @return partial seed for update, {@code null} if nothing left
     */
    private EntityRawSeed seedForUpdate(@NotNull EntityRawSeed seed, int startIdx, boolean includeAttributes) {
        int maxNumLookupEntries = DYNAMO_TXN_LIMIT - 1; // reserve one for seed update
        if (startIdx == 0 && maxNumLookupEntries >= CollectionUtils.size(seed.getLookupEntries())
                && (includeAttributes || MapUtils.isEmpty(seed.getAttributes()))) {
            // range includes entire seed
            return seed;
        }
        if (startIdx >= CollectionUtils.size(seed.getLookupEntries())
                && (!includeAttributes || MapUtils.isEmpty(seed.getAttributes()))) {
            // no lookup entries and no need to update attributes
            return null;
        }

        List<EntityLookupEntry> lookupEntries = seed.getLookupEntries();
        List<EntityLookupEntry> entries = lookupEntries.subList(startIdx,
                Math.min(lookupEntries.size(), startIdx + maxNumLookupEntries));
        return new EntityRawSeed(seed.getId(), seed.getEntity(), entries,
                includeAttributes ? seed.getAttributes() : null);
    }

    @VisibleForTesting
    public Cache<Triple<String, Integer, EntityLookupEntry>, String> getLookupCache() {
        return lookupCache;
    }

    @VisibleForTesting
    public Cache<Triple<Pair<String, String>, Integer, String>, EntityRawSeed> getSeedCache() {
        return seedCache;
    }

    @VisibleForTesting
    long getProcessingLookupEntriesCount() {
        return nProcessingLookupEntries.get();
    }

    /*
     * shutdown all workers and block until all background threads finishes
     */
    @PreDestroy
    @VisibleForTesting
    synchronized void predestroy() {
        try {
            if (shouldTerminate) {
                return;
            }
            log.info("Shutting down staging lookup entry publishers");
            shouldTerminate = true;
            if (lookupExecutorService != null) {
                lookupExecutorService.shutdownNow();
                lookupExecutorService.awaitTermination(TERMINATE_EXECUTOR_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }
            log.info("Completed shutting down of staging lookup entry publishers");
        } catch (Exception e) {
            log.error("Fail to finish all pre-destroy actions", e);
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
                    // already have value and not equals the one we try to associate
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
            @NotNull EntityRawSeed seed, boolean clearAllFailedLookupEntries,
            Set<EntityLookupEntry> entriesMapToOtherSeed, Map<EntityMatchEnvironment, Integer> versionMap) {
        return seed.getLookupEntries()
                .stream()
                .filter(entry -> {
                    // get all lookup entry that need to mapped to seed ID
                    Pair<EntityLookupEntry.Type, String> key = Pair.of(entry.getType(), entry.getSerializedKeys());
                    EntityLookupEntry.Mapping mapping = entry.getType().mapping;
                    if (mapping == EntityLookupEntry.Mapping.ONE_TO_ONE
                            || mapping == EntityLookupEntry.Mapping.MANY_TO_ONE) {
                        if (clearAllFailedLookupEntries && existingLookupPairs.containsKey(key)
                                && existingLookupPairs.get(key).contains(entry.getSerializedValues())) {
                            // to handle an very rare case, try to set lookup if the entry is exactly the
                            // same as the
                            // one in seed, worst case is to waste one request that does nothing. best case
                            // can caught
                            // false positive on setting highest priority key successfully.
                            return true;
                        }

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
                .map(entry -> {
                    if (CollectionUtils.isNotEmpty(entriesMapToOtherSeed) && entriesMapToOtherSeed.contains(entry)) {
                        // we already know this entry map to other seed, not bother trying to set
                        log.debug(
                                "Lookup entry {} already map to another seed, skip setting lookup mapping. Target seed ID = {}",
                                entry, seed.getId());
                        return Pair.of(entry, false);
                    }
                    boolean setSucceeded = entityLookupEntryService.setIfEquals(env, tenant, entry, seed.getId(),
                            shouldSetTTL(env), getMatchVersion(env, tenant, versionMap));
                    // NOTE for debugging concurrency issue.
                    log.debug("Map lookup entry {} to seed(ID={}), success={}", entry, seed.getId(), setSucceeded);
                    return Pair.of(entry, setSucceeded);
                })
                .filter(pair -> !pair.getValue()) // only get the ones failed to set
                .map(Pair::getKey)
                .collect(Collectors.toList());
    }

    /*
     * Retrieve list of seed IDs using input list of lookup entries.
     * Start from local cache -> staging table -> serving table and perform synchronization between layers.
     */
    private Map<EntityLookupEntry, String> getIdsInternal(@NotNull Tenant tenant,
            @NotNull List<EntityLookupEntry> lookupEntries, Map<EntityMatchEnvironment, Integer> versionMap) {
        String tenantId = tenant.getId();
        Set<EntityLookupEntry> uniqueEntries = new HashSet<>(lookupEntries);
        // always use serving version for lookup entry, if it is allocate mode, cache
        // will only exist in one match job and won't be shared. so no problem when
        // staging data get discarded in rollback
        int servingVersion = getMatchVersion(SERVING, tenant, versionMap);
        // retrieve seed IDs from cache
        Map<EntityLookupEntry, String> results =  getPresentCacheValues(
                tenantId, lookupEntries, lookupCache, servingVersion);
        if (results.size() == uniqueEntries.size()) {
            // have all the seed IDs
            return results;
        }

        // get lookup entries that are not present in local cache and try staging layer
        Set<EntityLookupEntry> missingEntries = getMissingKeys(uniqueEntries, results);
        Map<EntityLookupEntry, String> missingResults = getIdsStaging(tenant, missingEntries, versionMap);

        // add to results
        results.putAll(missingResults);
        // populate in-memory cache
        putInCache(tenantId, missingResults, lookupCache, servingVersion);

        return results;
    }

    /*
     * Retrieve seed IDs, starting at staging layer.
     */
    private Map<EntityLookupEntry, String> getIdsStaging(@NotNull Tenant tenant, @NotNull Set<EntityLookupEntry> keys,
            Map<EntityMatchEnvironment, Integer> versionMap) {
        if (!isAllocateMode()) {
            // in lookup mode, skip staging layer
            return getIdsServing(tenant, keys, versionMap);
        }

        // now in allocate mode
        int stagingVersion = getMatchVersion(STAGING, tenant, versionMap);
        Map<EntityLookupEntry, String> results = getIdsInEnvironment(tenant, STAGING, keys, versionMap);
        if (results.size() == keys.size()) {
            return results;
        }

        // get lookup entries that are not present in staging table and try serving layer
        Map<EntityLookupEntry, String> missingResults = getIdsServing(tenant, getMissingKeys(keys, results),
                versionMap);
        results.putAll(missingResults);

        // NOTE we can publish lookup entry asynchronously because we will not try to update
        //      lookup entry that is already in serving

        // increase # of processing entries
        nProcessingLookupEntries.addAndGet(missingResults.size());
        // populate missing seed IDs to staging layer (async, batch)
        missingResults
                .entrySet()
                .stream()
                .map(entry -> new StagingLookupPublishRequest(tenant, entry.getKey(), entry.getValue(), stagingVersion))
                .forEach(lookupQueue::offer);

        return results;
    }

    /*
     * Retrieve seed IDs in serving layer
     */
    private Map<EntityLookupEntry, String> getIdsServing(
            @NotNull Tenant tenant, @NotNull Set<EntityLookupEntry> uniqueEntries,
            Map<EntityMatchEnvironment, Integer> versionMap) {
        return getIdsInEnvironment(tenant, EntityMatchEnvironment.SERVING, uniqueEntries, versionMap);
    }

    /*
     * Retrieve seed IDs in the given environment and returns a map from lookup entry to seed ID
     */
    private Map<EntityLookupEntry, String> getIdsInEnvironment(
            @NotNull Tenant tenant, @NotNull EntityMatchEnvironment env, @NotNull Set<EntityLookupEntry> uniqueEntries,
            Map<EntityMatchEnvironment, Integer> versionMap) {
        List<EntityLookupEntry> entries = new ArrayList<>(uniqueEntries);
        List<String> seedIds = entityLookupEntryService
                .get(env, tenant, entries, getMatchVersion(env, tenant, versionMap));
        return listToMap(entries, seedIds);
    }

    /*
     * Retrieve list of raw seeds using input list of seed IDs.
     * Start from local cache -> staging table -> serving table and perform synchronization between layers.
     */
    private Map<String, EntityRawSeed> getSeedsInternal(
            @NotNull Tenant tenant, @NotNull String entity, @NotNull List<String> seedIds,
            Map<EntityMatchEnvironment, Integer> versionMap) {
        // need entity here because seed ID does not contain this info (unlike lookup entry)
        String tenantId = tenant.getId();
        Pair<String, String> prefix = Pair.of(tenantId, entity);
        Set<String> uniqueSeedIds = new HashSet<>(seedIds);
        // only allow to cache seed in serving
        int servingVersion = getMatchVersion(SERVING, tenant, versionMap);
        if (isAllocateMode()) {
            // in allocate mode, does not cache seed in-memory because seed will be updated and invalidating cache
            // for multiple processes will be difficult to do.
            return getSeedsStaging(tenant, entity, uniqueSeedIds, versionMap);
        }

        // NOTE in lookup mode

        // results from cache
        Map<String, EntityRawSeed> results = getPresentCacheValues(prefix, seedIds, seedCache, servingVersion);

        Set<String> missingSeedIds = getMissingKeys(uniqueSeedIds, results);
        // lookup mode goes directly to serving table (skip staging)
        Map<String, EntityRawSeed> missingResults = getSeedsServing(tenant, entity, missingSeedIds, versionMap);

        // add to results
        results.putAll(missingResults);
        // populate in-memory cache
        putInCache(prefix, missingResults, seedCache, servingVersion);

        return results;
    }

    /*
     * Retrieve raw seeds, starting from staging
     */
    private Map<String, EntityRawSeed> getSeedsStaging(
            @NotNull Tenant tenant, @NotNull String entity, @NotNull Set<String> seedIds,
            Map<EntityMatchEnvironment, Integer> versionMap) {
        if (!isAllocateMode()) {
            throw new IllegalStateException("Should not reach here in lookup mode.");
        }

        EntityMatchEnvironment env = STAGING;
        // allocate mode
        Map<String, EntityRawSeed> results = getSeedsInEnvironment(tenant, env, entity, seedIds, versionMap);
        if (results.size() == seedIds.size()) {
            return results;
        }

        Map<String, EntityRawSeed> missingResults = getSeedsServing(tenant, entity, getMissingKeys(seedIds, results),
                versionMap);
        results.putAll(missingResults);

        // populate staging table
        // NOTE we need to update seed synchronously because we will use update expression to update each lookup entry
        //      in the seed. therefore we need to make sure the same seed is in staging before we returns anything
        missingResults
                .values()
                .forEach(seed -> entityRawSeedService.setIfNotExists(env, tenant, seed, shouldSetTTL(env),
                        getMatchVersion(env, tenant, versionMap)));

        return results;
    }

    /*
     * Retrieve raw seeds in serving table
     */
    private Map<String, EntityRawSeed> getSeedsServing(
            @NotNull Tenant tenant, @NotNull String entity, @NotNull Set<String> seedIds,
            Map<EntityMatchEnvironment, Integer> versionMap) {
        return getSeedsInEnvironment(tenant, EntityMatchEnvironment.SERVING, entity, seedIds, versionMap);
    }

    /*
     * Retrieve seeds in the given environment and returns a map from seed ID to seed
     */
    private Map<String, EntityRawSeed> getSeedsInEnvironment(
            @NotNull Tenant tenant, @NotNull EntityMatchEnvironment env,
            @NotNull String entity, @NotNull Set<String> uniqueSeedIds,
            Map<EntityMatchEnvironment, Integer> versionMap) {
        List<String> seedIds = new ArrayList<>(uniqueSeedIds);
        List<EntityRawSeed> seeds = entityRawSeedService.get(env, tenant, entity, seedIds,
                getMatchVersion(env, tenant, versionMap));
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
    private <K, T, V> void putInCache(@NotNull T prefix, Map<K, V> results, Cache<Triple<T, Integer, K>, V> cache,
            int version) {
        cache.putAll(results
                .entrySet()
                .stream()
                .map(entry -> Triple.of(prefix, entry.getKey(), entry.getValue()))
                // [ prefix, result.key ] => result.value
                .collect(Collectors.toMap(triple -> Triple.of(triple.getLeft(), version, triple.getMiddle()),
                        Triple::getRight)));
    }

    /*
     * Build cache key from input list of keys and retrieve all cached values.
     */
    private <K, T, V> Map<K, V> getPresentCacheValues(
            @NotNull T prefix, List<K> keys, Cache<Triple<T, Integer, K>, V> cache, int version) {
        Set<Triple<T, Integer, K>> keysForCache = keys
                .stream()
                .map(entry -> Triple.of(prefix, version, entry))
                .collect(toSet());
        return cache
                .getAllPresent(keysForCache)
                .entrySet()
                .stream()
                .map(entry -> Pair.of(entry.getKey().getRight(), entry.getValue()))
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
        return originalInputs.stream().map(input -> input == null ? null : foundValues.get(input))
                .collect(Collectors.toList());
    }

    /*
     * random an entity ID (return in lowercase)
     */
    private String newId() {
        return RandomStringUtils.random(ENTITY_ID_LENGTH, ENTITY_ID_CHARS);
    }

    private int getMatchVersion(@NotNull EntityMatchEnvironment env, @NotNull Tenant tenant, Integer version) {
        return version == null ? entityMatchVersionService.getCurrentVersion(env, tenant) : version;
    }

    private int getMatchVersion(@NotNull EntityMatchEnvironment env, @NotNull Tenant tenant,
            Map<EntityMatchEnvironment, Integer> versionMap) {
        if (versionMap == null || versionMap.get(env) == null) {
            return entityMatchVersionService.getCurrentVersion(env, tenant);
        }
        return versionMap.get(env);
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
        initAnonymousSeedCache();
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
                        .recordStats() //
                        .build();
                entityMatchMetricService.registerSeedCache(seedCache);
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
     * Lazily instantiate anonymous seed cache
     */
    private void initAnonymousSeedCache() {
        if (anonymousSeedCache != null) {
            return;
        }

        synchronized (this) {
            if (anonymousSeedCache == null) {
                log.info("Instantiating anonymous seed cache, maxSize = {}, maxIdleDuration = {}",
                        getMaxSeedCacheWeight(),
                        entityMatchConfigurationService.getMaxAnonymousSeedCacheIdleDuration());
                anonymousSeedCache = Caffeine.newBuilder()
                        // max number of lookup entries allowed in cache
                        .maximumSize(getMaxAnonymousSeedCacheWeight())
                        // expire after idle for a certain amount of time
                        .expireAfterAccess(entityMatchConfigurationService.getMaxAnonymousSeedCacheIdleDuration())
                        .recordStats() //
                        .build();
            }
        }
    }

    /*
     * use the same estimation as seed
     */
    protected long getMaxAnonymousSeedCacheWeight() {
        return entityMatchConfigurationService.getMaxAnonymousSeedCacheInMB() * 2500;
    }

    /*
     * Memory used by one raw seed cache and one lookup cache is pretty close.
     * Currently, to make things simpler, use 1 (seed) + # of lookup entries as weight
     */
    private int getWeight(Triple<Pair<String, String>, Integer, String> key, EntityRawSeed val) {
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
                        .recordStats() //
                        .build();
                entityMatchMetricService.registerLookupCache(lookupCache,
                        entityMatchConfigurationService.isAllocateMode());
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
        if (!isAllocateMode()) {
            // NOTE only need to populate staging table in allocate mode
            return;
        }
        // TODO tune all pool size

        if (lookupExecutorService == null) {
            synchronized (this) {
                if (lookupExecutorService == null) {
                    log.info("Instantiating staging lookup entry publishers");
                    lookupExecutorService = ThreadPoolUtils
                            .getFixedSizeThreadPool(LOOKUP_BACKGROUND_THREAD_NAME, nLookupPopulateThreads);
                    IntStream
                            .range(0, nLookupPopulateThreads)
                            .forEach((idx) -> lookupExecutorService.submit(new StagingLookupEntryPublisher()));
                }
            }
        }
    }

    /*
     * isAllocateMode = false means (a) no allocation, (b) cache seed, (c) only lookup from cache & serving table
     * isAllocateMode = true means
     *   (a) has allocation, (b) not caching seed, (c) lookup from cache, staging and serving table
     */
    private boolean isAllocateMode() {
        return entityMatchConfigurationService.isAllocateMode();
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
            Map<String, List<StagingLookupPublishRequest>> batches = new HashMap<>();
            while (!shouldTerminate) {
                try {
                    StagingLookupPublishRequest request = lookupQueue.poll(
                            lookupPopulatePollIntervalMillis, TimeUnit.MILLISECONDS);
                    if (request != null) {
                        String tenantId = request.tenant.getId(); // should not be null
                        batches.putIfAbsent(tenantId, new ArrayList<>());
                        batches.get(tenantId).add(request); // add to local list
                        total++;
                    }
                    // triple == null means timeout
                    if (request == null || total >= lookupPopulateBatchSize) {
                        populate(batches);
                        // finishes total entries, decrease the same amount in processing entries counter
                        nProcessingLookupEntries.addAndGet(-total);
                        // clear populated batches
                        total = 0;
                        batches.clear();
                        Thread.sleep(lookupPopulateSleepMillis);
                    }
                } catch (InterruptedException e) {
                    if (!shouldTerminate) {
                        log.warn("Staging lookup entry publisher (in background) is interrupted");
                    }
                    int numUnpublished = batches.values() //
                            .stream() //
                            .filter(Objects::nonNull) //
                            .mapToInt(List::size) //
                            .sum();
                    log.info("There are {} lookup entries not published yet", numUnpublished);
                } catch (Exception e) {
                    log.error("Encounter an error (in background) in staging lookup entry publisher", e);
                }
            }
        }

        private void populate(Map<String, List<StagingLookupPublishRequest>> batches) {
            EntityMatchEnvironment env = STAGING;
            // since allocate mode should only have one tenant, map is probably not required, use map just in case
            batches.values().forEach(list -> {
                if (CollectionUtils.isEmpty(list)) {
                    return;
                }
                List<Pair<EntityLookupEntry, String>> pairs = list
                        .stream()
                        .map(req -> Pair.of(req.entry, req.entityId))
                        .collect(Collectors.toList());
                Tenant tenant = list.get(0).tenant; // tenant in the list should all be the same
                /*-
                 * we can assume staging version will also be the same since they are in a single job.
                 * TODO extend to multiple version later if necessary
                 */
                int stagingVersion = list.get(0).stagingVersion;
                entityLookupEntryService.set(env, tenant, pairs, shouldSetTTL(env), stagingVersion);
            });
        }
    }

    private class StagingLookupPublishRequest {
        final Tenant tenant;
        final EntityLookupEntry entry;
        final String entityId;
        final int stagingVersion;

        StagingLookupPublishRequest(Tenant tenant, EntityLookupEntry entry, String entityId, int stagingVersion) {
            this.tenant = tenant;
            this.entry = entry;
            this.entityId = entityId;
            this.stagingVersion = stagingVersion;
        }
    }
}
