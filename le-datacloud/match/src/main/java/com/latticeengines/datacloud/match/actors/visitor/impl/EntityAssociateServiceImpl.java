package com.latticeengines.datacloud.match.actors.visitor.impl;

import static com.latticeengines.datacloud.match.util.EntityMatchUtils.mergeSeed;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Mapping.MANY_TO_MANY;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Mapping.ONE_TO_ONE;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.datacloud.match.service.EntityMatchInternalService;
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityAssociationRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityAssociationResponse;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityTransactUpdateResult;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Associate all the lookup entry for a single record to one entity.
 * The entity to associate to is decide by the result of lookups. If no entity found or there are conflicts
 * between the entity seed and lookup entries, a new entity will be created (and a new ID allocated).
 *
 * NOTE should only get request here if {@link EntityMatchConfigurationService#isAllocateMode()} is true.
 *
 * Input data: type={@link EntityAssociationRequest}
 * Output: {@link EntityAssociationResponse}
 */
@Component("entityAssociateService")
public class EntityAssociateServiceImpl extends DataSourceMicroBatchLookupServiceBase {

    private static final String ANONYMOUS_ENTITY_ID = DataCloudConstants.ENTITY_ANONYMOUS_ID;
    private static final String THREAD_POOL_NAME = "entity-associate-worker";

    private static final Logger log = LoggerFactory.getLogger(EntityAssociateServiceImpl.class);

    @Inject
    private EntityMatchInternalService entityMatchInternalService;

    @Value("${datacloud.match.dynamo.fetchers.num}")
    private Integer nWorkers;

    @Value("${datacloud.match.num.dynamo.fetchers.batch.num}")
    private Integer nBatchWorkers;

    @Value("${datacloud.match.dynamo.fetchers.chunk.size}")
    private Integer chunkSize;

    @Override
    protected String getThreadPoolName() {
        return THREAD_POOL_NAME;
    }

    @Override
    protected int getChunkSize() {
        return chunkSize;
    }

    @Override
    protected int getThreadCount() {
        return isBatchMode() ? nBatchWorkers : nWorkers;
    }

    @Override
    protected void handleRequests(List<String> requestIds) {
        Map<String, List<Pair<String, EntityAssociationRequest>>> params = requestIds
                .stream()
                .map(id -> Pair.of(id, getReq(id)))
                .map(pair -> Pair.of(pair.getKey(), (EntityAssociationRequest) pair.getValue().getInputData()))
                // group by tenant ID, put all lookupRequests in this tenant into a list
                .collect(groupingBy(pair -> {
                    String tenantId = pair.getValue().getTenant().getId();
                    // use both tenant & version info as key
                    return String.format("%s_%s", tenantId,
                            EntityMatchUtils.serialize(pair.getValue().getVersionMap()));
                }, mapping(pair -> pair, toList())));
        params.values().forEach(this::handleRequestsForTenant);
    }

    @Override
    protected EntityAssociationResponse lookupFromService(String lookupRequestId, DataSourceLookupRequest request) {
        EntityAssociationRequest associationReq = (EntityAssociationRequest) request.getInputData();
        Map<EntityMatchEnvironment, Integer> versionMap = associationReq.getVersionMap();
        associationReq = lookupNotMappedEntries(associationReq, versionMap);
        EntityRawSeed targetSeed = getOrAllocate(associationReq, pickTargetEntity(associationReq), versionMap);
        if (targetSeed == null) {
            // no target seed, just return
            return new EntityAssociationResponse(associationReq.getTenant(), associationReq.getEntity(), null, null,
                    null, false);
        }

        // TODO make it configurable which association implementation to use
        return transactAssociate(lookupRequestId, associationReq, targetSeed, versionMap);
    }

    /*
     * Process all requests that belong to a single tenant/version pair
     */
    private void handleRequestsForTenant(List<Pair<String, EntityAssociationRequest>> pairs) {
        if (CollectionUtils.isEmpty(pairs)) {
            return;
        }

        // should all have the same tenant/version
        Tenant tenant = getTenant(pairs);
        Map<EntityMatchEnvironment, Integer> versionMap = getVersionMap(pairs);

        if (tenant == null) {
            return;
        }
        Preconditions.checkNotNull(tenant.getId());
        String tenantId = tenant.getId();

        try {
            List<Pair<String, EntityAssociationRequest>> updatedPairs = pairs.stream() //
                    .map(pair -> Pair.of(pair.getKey(), lookupNotMappedEntries(pair.getValue(), versionMap))) //
                    .collect(toList());
            List<String> targetSeedIds = updatedPairs.stream() //
                    .map(Pair::getValue) //
                    .map(this::pickTargetEntity) //
                    .collect(Collectors.toList());
            List<EntityAssociationRequest> requests = updatedPairs.stream().map(Pair::getValue).collect(toList());
            List<EntityRawSeed> targetSeeds = getOrAllocate(tenant, requests, targetSeedIds, versionMap);
            IntStream.range(0, updatedPairs.size())
                    .forEach(idx ->
                    associateAsync(updatedPairs.get(idx).getKey(), updatedPairs.get(idx).getValue(),
                            targetSeeds.get(idx),
                            versionMap));
            log.debug("Handled {} requests for tenant (ID={})", updatedPairs.size(), tenantId);
        } catch (Exception e) {
            log.error("Failed to handle {} requests for tenant (ID={})", pairs.size(), tenantId);
            // fail all requests
            sendFailureResponses(pairs.stream().map(Pair::getKey).collect(toList()), e);
        }
    }

    /*
     * Return the entity ID found by the highest priority key, return null if not entity found
     */
    private String pickTargetEntity(@NotNull EntityAssociationRequest request) {
        return request.getLookupResults()
                .stream()
                .map(Pair::getValue)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    /*
     * Retrieve seed with given ID or allocate a new one if null seedId is provided
     */
    private EntityRawSeed getOrAllocate(@NotNull EntityAssociationRequest request, String seedId,
            Map<EntityMatchEnvironment, Integer> versionMap) {
        Tenant tenant = request.getTenant();
        String entity = request.getEntity();
        if (StringUtils.isNotBlank(seedId)) {
            EntityRawSeed seed = entityMatchInternalService.get(tenant, entity, seedId, versionMap);
            if (!conflictInHighestPriorityEntry(request, seed)) {
                // has target seed (found with some lookup entry) and no conflict with highest
                // priority entry in target seed
                return seed;
            }
        }

        return anonymousOrNewEntity(request, versionMap);
    }

    /*
     * Retrieve seeds with given IDs or allocate new ones if null seedId is provided in the list
     */
    @VisibleForTesting
    protected List<EntityRawSeed> getOrAllocate(
            @NotNull Tenant tenant, @NotNull List<EntityAssociationRequest> requests, @NotNull List<String> seedIds,
            Map<EntityMatchEnvironment, Integer> versionMap) {
        Preconditions.checkArgument(requests.size() == seedIds.size());

        // entity => List<EntitySeedId>
        Map<String, List<String>> entitySeedIdMap = IntStream.range(0, requests.size())
                .filter(idx -> seedIds.get(idx) != null)
                .mapToObj(idx -> Pair.of(requests.get(idx).getEntity(), seedIds.get(idx)))
                .collect(groupingBy(Pair::getKey, mapping(Pair::getValue, toList())));

        // retrieve list of seeds for each entity
        // entity => seed ID => seed
        Map<String, Map<String, EntityRawSeed>> entitySeedMap = entitySeedIdMap
                .entrySet()
                .stream()
                // entry -> Pair<Entity, Map<ID, Seed>>>
                .map(entry -> Pair.of(
                        entry.getKey(),
                        entityMatchInternalService
                                .get(tenant, entry.getKey(), entry.getValue(), versionMap)
                                .stream()
                                .collect(Collectors.toMap(EntityRawSeed::getId, seed -> seed, (s1, s2) -> s1))))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        return IntStream.range(0, requests.size())
                .mapToObj(idx -> {
                    String entity = requests.get(idx).getEntity();
                    String seedId = seedIds.get(idx);
                    if (StringUtils.isNotBlank(seedId) && !conflictInHighestPriorityEntry(requests.get(idx),
                            entitySeedMap.get(entity).get(seedId))) {
                        // has target seed (found with some lookup entry) and no conflict with highest
                        // priority entry in target seed
                        return entitySeedMap.get(entity).get(seedId);
                    } else {
                        return anonymousOrNewEntity(requests.get(idx), versionMap);
                    }
                })
                .collect(Collectors.toList());
    }

    /*
     * determine if the highest priority lookup entry (we try to associate) has
     * conflict with target seed
     */
    @VisibleForTesting
    protected boolean conflictInHighestPriorityEntry(@NotNull EntityAssociationRequest request,
            @NotNull EntityRawSeed targetSeed) {
        // lookup result should not be empty here
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(request.getLookupResults()));
        EntityLookupEntry maxPriorityEntry = getEntry(request.getEntity(), request.getLookupResults().get(0).getKey());
        EntityLookupEntry.Mapping mapping = maxPriorityEntry.getType().mapping;
        if (mapping == MANY_TO_MANY) {
            // no way of causing conflict with many to many entry
            return false;
        }

        return targetSeed.getLookupEntries().stream().anyMatch(entry -> {
            if (entry.getType() != maxPriorityEntry.getType()
                    || !Objects.equals(entry.getSerializedKeys(), maxPriorityEntry.getSerializedKeys())) {
                // not the same type/serialized key
                return false;
            }

            // has conflict only if the serialized values are not equals (e.g., want to
            // associate DUNS=123 but target seed already has DUNS=456)
            return !Objects.equals(entry.getSerializedValues(), maxPriorityEntry.getSerializedValues());
        });
    }

    /*
     * Return anonymous ID if no match keys are provided, allocate a new ID if allowed in the request
     */
    private EntityRawSeed anonymousOrNewEntity(@NotNull EntityAssociationRequest request,
            Map<EntityMatchEnvironment, Integer> versionMap) {
        Tenant tenant = request.getTenant();
        String entity = request.getEntity();
        if (CollectionUtils.isEmpty(request.getLookupResults())) {
            // no lookup entry in the request, associate to anonymous entity
            return entityMatchInternalService.getOrCreateAnonymousSeed(tenant, entity, versionMap);
        } else {
            // allocate new seed
            String seedId = entityMatchInternalService.allocateId(tenant, entity, request.getPreferredEntityId(),
                    versionMap);
            return new EntityRawSeed(seedId, entity, true);
        }
    }

    /*
     * helper to associate single request and send response based on the result, guarding against exceptions
     */
    private void associateAsync(
            @NotNull String requestId, @NotNull EntityAssociationRequest request,
            @NotNull EntityRawSeed targetEntitySeed, Map<EntityMatchEnvironment, Integer> versionMap) {
        try {
            // TODO make it configurable which association implementation to use
            EntityAssociationResponse response = transactAssociate(requestId, request, targetEntitySeed, versionMap);
            // inject failure only for testing purpose
            injectFailure(getReq(requestId));
            // send successful response
            String returnAddress = getReqReturnAddr(requestId);
            removeReq(requestId);
            sendResponse(requestId, response, returnAddress);
        } catch (Exception e) {
            log.error("Failed to associate request (ID={}) to target ({}), error = {}",
                    requestId, targetEntitySeed, e.getMessage());
            // fail this request only
            DataSourceLookupRequest lookupRequest = getReq(requestId);
            removeReq(requestId);
            sendFailureResponse(lookupRequest, e);
        }
    }

    /*
     * Associate a single request to a target seed and generate a non-null response.
     */
    @VisibleForTesting
    protected EntityAssociationResponse associate(
            @NotNull String requestId, @NotNull EntityAssociationRequest request,
            @NotNull EntityRawSeed targetEntitySeed, Map<EntityMatchEnvironment, Integer> versionMap) {
        Tenant tenant = request.getTenant();
        String tenantId = tenant.getId();
        EntityAssociationResponse response = associateAnonymousOrAttributeOnly(requestId, request, targetEntitySeed,
                versionMap);
        if (response != null) {
            return response;
        }

        // handling highest priority lookup entry
        String entity = request.getEntity();
        EntityLookupEntry maxPriorityEntry = getEntry(entity, request.getLookupResults().get(0).getKey());
        String maxPrioritySeedId = request.getLookupResults().get(0).getValue();
        // only update the max priority if it is not mapped to target entity ID at the moment
        if (!targetEntitySeed.getId().equals(maxPrioritySeedId)) {
            // try to associate with the highest priority entry, if fail, return as match failure
            Triple<EntityRawSeed, List<EntityLookupEntry>, List<EntityLookupEntry>> maxPriorityResult =
                    entityMatchInternalService.associate(tenant, prepareSeedToAssociate(
                            targetEntitySeed, Collections.singletonList(maxPriorityEntry), null), true, null,
                            versionMap);
            if (hasAssociationError(maxPriorityResult)) {
                log.debug("Failed to associate highest priority lookup entry {} to target entity (ID={})," +
                        " requestId={}, tenant (ID={}), entity={}",
                        maxPriorityEntry, targetEntitySeed.getId(), requestId, tenantId, request.getEntity());
                // NOTE even conflict on lookup mapping for many to X entry is considered
                // failure for highest priority key
                if (maxPriorityResult.getLeft() == null) {
                    log.debug("Cleanup orphan seed, entity={} entityId={}, tenant (ID={})", tenantId,
                            request.getEntity(), targetEntitySeed.getId());
                    // the target entity is newly allocated, cleanup orphan seed
                    entityMatchInternalService.cleanupOrphanSeed(tenant, entity, targetEntitySeed.getId(), versionMap);
                }
                // fail to associate the highest priority entry
                return getResponse(
                        request, null, null, null, false, Collections.singleton(maxPriorityEntry),
                        getConflictMessages(targetEntitySeed.getId(), request, maxPriorityResult, null, null));
            }

            log.debug(
                    "Associate highest priority lookup entry ({}) successfully to target entity (ID={}),"
                            + " requestId={}, tenant (ID={}), entity={}",
                    maxPriorityEntry, targetEntitySeed.getId(), requestId, tenantId, request.getEntity());
        } else {
            log.debug("Highest priority lookup entry {} already maps to target entity (ID={})," +
                    " requestId={}, tenant (ID={}), entity={}",
                    maxPriorityEntry, targetEntitySeed.getId(), requestId, tenantId, request.getEntity());
        }

        List<Pair<EntityLookupEntry, String>> mappingConflictEntries =
                getLookupConflictsWithTarget(request, targetEntitySeed);
        Set<EntityLookupEntry> seedConflictEntries = getSeedConflictEntries(
                request, targetEntitySeed, mappingConflictEntries);
        // handling the remaining lookup entries
        if (needAssociation(request, targetEntitySeed, seedConflictEntries, true, false)) {
            // has more things to associate (excluding max highest priority entry
            // NOTE we update every thing that does NOT already mapped to another seed
            //      and rely on associate method of internal service to handle other conflict for code simplicity.
            //      Even though we can filter out some of them (e.g., one to one lookup entry that is already in target)
            //      , it does not actually save anything (except for a few bytes sent over network).
            EntityRawSeed seedToUpdate = prepareSeedToAssociate(request, targetEntitySeed, seedConflictEntries, false);
            Set<EntityLookupEntry> entriesMapToOtherSeed = getEntriesMapToOtherSeed(request, targetEntitySeed.getId());
            Triple<EntityRawSeed, List<EntityLookupEntry>, List<EntityLookupEntry>> result =
                    entityMatchInternalService.associate(tenant, seedToUpdate, false, entriesMapToOtherSeed,
                            versionMap);
            Preconditions.checkNotNull(result);

            log.debug("Association result = {}, mapping conflict entries = {}," +
                    " seed conflict entries = {}, requestId = {}",
                    result, mappingConflictEntries, seedConflictEntries, requestId);
            Set<EntityLookupEntry> conflictEntries = getConflictEntries(mappingConflictEntries, seedConflictEntries,
                    result.getMiddle(), result.getRight());
            return getResponse(
                    request, targetEntitySeed.getId(),
                    targetEntitySeed,
                    // generate updated seed
                    mergeSeed(targetEntitySeed, seedToUpdate, conflictEntries),
                    targetEntitySeed.isNewlyAllocated(),
                    conflictEntries,
                    getConflictMessages(targetEntitySeed.getId(), request, result, mappingConflictEntries,
                            seedConflictEntries));
        }

        // no conflict during association
        log.debug("No need for additional association. Mapping conflict entries = {}," +
                " seed conflict entries = {}, requestId = {}",
                mappingConflictEntries, seedConflictEntries, requestId);
        Set<EntityLookupEntry> conflictEntries = getConflictEntries(mappingConflictEntries, seedConflictEntries, null,
                null);
        return getResponse(
                request, targetEntitySeed.getId(), //
                targetEntitySeed, //
                merge(targetEntitySeed, maxPriorityEntry, conflictEntries), //
                targetEntitySeed.isNewlyAllocated(), //
                conflictEntries, //
                getConflictMessages(targetEntitySeed.getId(), request, null, mappingConflictEntries,
                        seedConflictEntries));
    }

    /*
     * Associate (using dynamo txn) a single request to a target seed and generate a
     * non-null response.
     */
    @VisibleForTesting
    protected EntityAssociationResponse transactAssociate(@NotNull String requestId,
            @NotNull EntityAssociationRequest request, @NotNull EntityRawSeed targetEntitySeed,
            Map<EntityMatchEnvironment, Integer> versionMap) {
        Tenant tenant = request.getTenant();
        String tenantId = tenant.getId();
        EntityAssociationResponse response = associateAnonymousOrAttributeOnly(requestId, request, targetEntitySeed,
                versionMap);
        if (response != null) {
            return response;
        }

        Set<EntityLookupEntry> seedConflictEntries = new HashSet<>(getSeedConflictEntries(request, targetEntitySeed,
                getLookupConflictsWithTarget(request, targetEntitySeed)));
        Set<EntityLookupEntry> entriesMapToOtherSeed = new HashSet<>(
                getEntriesMapToOtherSeed(request, targetEntitySeed.getId()));
        Set<EntityLookupEntry> conflictEntries = getConflictEntries(null, seedConflictEntries, null,
                entriesMapToOtherSeed);
        EntityRawSeed seedAfterUpdate = targetEntitySeed;
        if (needAssociation(request, targetEntitySeed, seedConflictEntries, false, true)) {
            EntityRawSeed seedToUpdate = prepareSeedToAssociate(request, targetEntitySeed, seedConflictEntries, true);
            EntityTransactUpdateResult result = entityMatchInternalService.transactAssociate(tenant, seedToUpdate,
                    entriesMapToOtherSeed, versionMap);

            Preconditions.checkNotNull(result);
            seedConflictEntries.addAll(getSeedConflictEntries(result, seedToUpdate));

            if (!result.isSucceeded()) {
                if (targetEntitySeed.isNewlyAllocated()) {
                    log.debug("Cleanup orphan seed, entity={} entityId={}, tenant (ID={})", tenantId,
                            request.getEntity(), targetEntitySeed.getId());
                    // the target entity is newly allocated, cleanup orphan seed
                    entityMatchInternalService.cleanupOrphanSeed(tenant, request.getEntity(), targetEntitySeed.getId(),
                            versionMap);
                }

                // TODO retry if this is not a newly allocated entity
                // txn update failure (concurrent update), no need to surface conflict to
                // outside since this is not the final result TODO change to debug level
                log.info("Conflict detected due to concurrent association, result = {}, update seed = {}", result,
                        seedToUpdate);
                return getResponse(request, null, null, null, false, conflictEntries, Collections.emptyList());
            }

            seedAfterUpdate = mergeSeed(seedAfterUpdate, seedToUpdate, conflictEntries);
        }

        // associate dummy entries
        EntityRawSeed dummies = seedWithDummyEntries(request, targetEntitySeed);
        if (dummies != null) {
            // conflict for dummy entries doesn't matter
            Triple<EntityRawSeed, List<EntityLookupEntry>, List<EntityLookupEntry>> result = entityMatchInternalService
                    .associate(tenant, dummies, false, Collections.emptySet(), versionMap);
            conflictEntries.addAll(CollectionUtils.emptyIfNull(result.getMiddle()));
            conflictEntries.addAll(CollectionUtils.emptyIfNull(result.getRight()));
            // TODO maybe optimize merging better
            seedAfterUpdate = mergeSeed(seedAfterUpdate, dummies, conflictEntries);
        }

        // no association is performed, all conflict is found at lookup time
        log.debug(
                "Association succeeded. Mapping conflict entries = {}," + " seed conflict entries = {}, requestId = {}",
                entriesMapToOtherSeed, seedConflictEntries, requestId);
        // TODO generate better conflict messages
        return getResponse(request, targetEntitySeed.getId(), //
                targetEntitySeed, seedAfterUpdate, //
                targetEntitySeed.isNewlyAllocated(), //
                conflictEntries, //
                Collections.emptyList());
    }

    private Set<EntityLookupEntry> getSeedConflictEntries(@NotNull EntityTransactUpdateResult result,
            @NotNull EntityRawSeed seedToUpdate) {
        if (result.getSeed() == null) {
            return Collections.emptySet();
        }
        return seedToUpdate.getLookupEntries() //
                .stream() //
                .filter(entry -> EntityMatchUtils.hasConflictInSeed(result.getSeed(), entry)) //
                .collect(Collectors.toSet());
    }

    /*
     * Include all dummy lookup entries in the resulting seed that can be used for
     * association. return null if no dummy entries.
     */
    private EntityRawSeed seedWithDummyEntries(@NotNull EntityAssociationRequest request,
            @NotNull EntityRawSeed target) {
        if (CollectionUtils.isEmpty(request.getDummyLookupResultIndices())) {
            return null;
        }

        List<EntityLookupEntry> dummyEntries = IntStream.range(0, request.getLookupResults().size()) //
                .filter(request.getDummyLookupResultIndices()::contains) //
                .mapToObj(request.getLookupResults()::get) //
                .map(pair -> {
                    EntityLookupEntry entry = getEntry(request.getEntity(), pair.getKey());
                    return Pair.of(entry, pair.getValue());
                }) //
                .map(Pair::getLeft) //
                .collect(toList());
        return prepareSeedToAssociate(target, dummyEntries, null);
    }

    /*
     * get entries that map to other seeds (need to map to something)
     */
    private Set<EntityLookupEntry> getEntriesMapToOtherSeed(@NotNull EntityAssociationRequest request,
            @NotNull String seedId) {
        return request.getLookupResults() //
                .stream() //
                .filter(pair -> StringUtils.isNotBlank(pair.getRight())) //
                .filter(pair -> !pair.getRight().equals(seedId)) // entries map to other seed
                .map(Pair::getKey) //
                .map(tuple -> getEntry(request.getEntity(), tuple)) //
                .collect(Collectors.toSet());
    }

    private EntityAssociationResponse associateAnonymousOrAttributeOnly(@NotNull String requestId,
            @NotNull EntityAssociationRequest request, @NotNull EntityRawSeed targetEntitySeed,
            Map<EntityMatchEnvironment, Integer> versionMap) {
        Tenant tenant = request.getTenant();
        String tenantId = tenant.getId();
        if (ANONYMOUS_ENTITY_ID.equals(targetEntitySeed.getId())) {
            // not creating anonymous seed entry
            return getResponse(request, targetEntitySeed.getId(), targetEntitySeed, targetEntitySeed,
                    targetEntitySeed.isNewlyAllocated());
        }
        if (CollectionUtils.isEmpty(request.getLookupResults())) {
            log.debug(
                    "No lookup entry for request (ID={}), attributes={}, tenant (ID={}),"
                            + " entity={}, target entity ID={}",
                    requestId, request.getExtraAttributes(), tenantId, request.getEntity(), targetEntitySeed.getId());
            EntityRawSeed seedToUpdate = null;
            if (hasExtraAttributes(targetEntitySeed, request.getExtraAttributes())) {
                seedToUpdate = new EntityRawSeed(targetEntitySeed.getId(), targetEntitySeed.getEntity(),
                        Collections.emptyList(), request.getExtraAttributes());
                // ignore result as attribute update won't fail
                entityMatchInternalService.associate(request.getTenant(), seedToUpdate, false, null, versionMap);
            }
            return getResponse(request, targetEntitySeed.getId(), targetEntitySeed,
                    mergeSeed(targetEntitySeed, seedToUpdate, null), targetEntitySeed.isNewlyAllocated());
        }

        return null;
    }

    /*-
     * redundant read to lower the chance of splitting entity
     */
    private EntityAssociationRequest lookupNotMappedEntries(@NotNull EntityAssociationRequest request,
            Map<EntityMatchEnvironment, Integer> versionMap) {
        int size = request.getLookupResults().size();
        // find all entries not mapped to any entity
        Map<Integer, EntityLookupEntry> notMappedResults = IntStream.range(0, size) //
                .filter(idx -> request.getLookupResults().get(idx) != null
                        && request.getLookupResults().get(idx).getValue() == null) //
                .filter(idx -> !request.getDummyLookupResultIndices().contains(idx)) // not re-lookup artificial result
                .mapToObj(idx -> Pair.of(getEntry(request.getEntity(), request.getLookupResults().get(idx).getKey()),
                        idx)) //
                .collect(toMap(Pair::getValue, Pair::getKey));
        if (MapUtils.isEmpty(notMappedResults)) {
            return request;
        }

        // lookup again for less stale data
        List<EntityLookupEntry> notMappedEntries = new ArrayList<>(notMappedResults.values());
        List<String> ids = entityMatchInternalService.getIds(request.getTenant(), notMappedEntries, versionMap);
        if (ids.stream().anyMatch(StringUtils::isNotBlank)) {
            // some entries got mapped
            List<Pair<MatchKeyTuple, String>> newLookupResults = IntStream.range(0, size).mapToObj(idx -> {
                if (!notMappedResults.containsKey(idx)) {
                    return request.getLookupResults().get(idx);
                } else {
                    int idIdx = notMappedEntries.indexOf(notMappedResults.get(idx));
                    // get original tuple and new ID
                    return Pair.of(request.getLookupResults().get(idx).getKey(), ids.get(idIdx));
                }
            }).collect(toList());
            // copy request with new result
            return new EntityAssociationRequest(request.getTenant(), request.getEntity(), request.getVersionMap(),
                    request.getPreferredEntityId(), newLookupResults, request.getExtraAttributes());
        } else {
            return request;
        }
    }

    /*
     * helper to merge highest priority match key given known conflict match keys
     */
    private EntityRawSeed merge(@NotNull EntityRawSeed target, @NotNull EntityLookupEntry maxPriorityEntry,
            Set<EntityLookupEntry> conflictEntries) {
        return mergeSeed(
                target, new EntityRawSeed(target.getId(), target.getEntity(),
                        Collections.singletonList(maxPriorityEntry), null),
                conflictEntries);
    }

    /*
     * helper to combine all conflict match keys (known conflicts before update and
     * conflicts occurs during update)
     */
    private Set<EntityLookupEntry> getConflictEntries(List<Pair<EntityLookupEntry, String>> mappingConflicts,
            Set<EntityLookupEntry> seedConflicts, Collection<EntityLookupEntry> seedUpdateConflicts,
            Collection<EntityLookupEntry> mappingUpdateConflicts) {
        Set<EntityLookupEntry> entries = new HashSet<>();
        if (CollectionUtils.isNotEmpty(mappingConflicts)) {
            mappingConflicts.forEach(pair -> entries.add(pair.getLeft()));
        }
        if (CollectionUtils.isNotEmpty(seedConflicts)) {
            entries.addAll(seedConflicts);
        }
        if (CollectionUtils.isNotEmpty(seedUpdateConflicts)) {
            entries.addAll(seedUpdateConflicts);
        }
        if (CollectionUtils.isNotEmpty(mappingUpdateConflicts)) {
            entries.addAll(mappingUpdateConflicts);
        }
        return entries;
    }

    private Set<EntityLookupEntry> getSeedConflictEntries(
            @NotNull EntityAssociationRequest request, @NotNull EntityRawSeed target,
            @NotNull List<Pair<EntityLookupEntry, String>> mappingConflictEntries) {
        Set<EntityLookupEntry> mappingConflicts = mappingConflictEntries
                .stream()
                .map(Pair::getKey)
                .collect(toSet());
        return request.getLookupResults()
                .stream()
                .map(pair -> getEntry(request.getEntity(), pair.getKey()))
                .filter(entry -> !mappingConflicts.contains(entry))
                .filter(entry -> EntityMatchUtils.hasConflictInSeed(target, entry))
                .collect(toSet());
    }

    /*
     * Check if the input association request contains any lookup entries or extra attributes that requires association
     * to target, need to be in sync with this#prepareSeedToAssociate
     */
    private boolean needAssociation(
            @NotNull EntityAssociationRequest request, @NotNull EntityRawSeed target,
            @NotNull Set<EntityLookupEntry> seedConflictEntries, boolean skipFirst, boolean skipDummy) {
        Optional<Pair<EntityLookupEntry, String>> entryNeedUpdate = IntStream
                .range(0, request.getLookupResults().size()) //
                // skip the highest priority one if skipFirst is true
                .skip(skipFirst ? 1L : 0L).mapToObj(idx -> {
                    Pair<MatchKeyTuple, String> pair = request.getLookupResults().get(idx);
                    EntityLookupEntry entry = getEntry(request.getEntity(), pair.getKey());
                    return Triple.of(idx, entry, pair.getValue());
                })
                // entries not mapped to target (null or diff ID)
                .filter(triple -> !target.getId().equals(triple.getRight())).filter(this::needAssociation) //
                // entry does not have conflict in seed (e.g., SFDC_ID already have a different value)
                .filter(triple -> !seedConflictEntries.contains(triple.getMiddle())) //
                .filter(triple -> !skipDummy || !request.getDummyLookupResultIndices().contains(triple.getLeft())) //
                .map(triple -> Pair.of(triple.getMiddle(), triple.getRight())) //
                .findFirst();
        return entryNeedUpdate.isPresent() || hasExtraAttributes(target, request.getExtraAttributes());
    }

    private boolean hasExtraAttributes(@NotNull EntityRawSeed target, Map<String, String> extraAttributes) {
        // need to update if attrs in request is not a subset of current attrs
        return MapUtils.isNotEmpty(extraAttributes) &&
                !target.getAttributes().entrySet().containsAll(extraAttributes.entrySet());
    }

    /*
     * Create a raw seed that contains all lookup entries that need association,
     * need to be in sync with this#needAssociation
     */
    private EntityRawSeed prepareSeedToAssociate(
            @NotNull EntityAssociationRequest request, @NotNull EntityRawSeed target,
            @NotNull Set<EntityLookupEntry> seedConflictEntries, boolean skipDummy) {
        List<EntityLookupEntry> entries = IntStream.range(0, request.getLookupResults().size()) //
                .mapToObj(idx -> {
                    Pair<MatchKeyTuple, String> pair = request.getLookupResults().get(idx);
                    EntityLookupEntry entry = getEntry(request.getEntity(), pair.getKey());
                    return Triple.of(idx, entry, pair.getValue());
                })
                // entries not mapped to target (null or diff ID)
                .filter(triple -> !target.getId().equals(triple.getRight()))
                .filter(this::needAssociation)
                // entry does not have conflict in seed (e.g., SFDC_ID already have a different value)
                .filter(triple -> !seedConflictEntries.contains(triple.getMiddle())) //
                /*-
                 * when skipDummy is true, only use lookup result that's not dummy (idx not in given list)
                 */
                .filter(triple -> !skipDummy || !request.getDummyLookupResultIndices().contains(triple.getLeft())) //
                .map(Triple::getMiddle) //
                .collect(toList());
        return prepareSeedToAssociate(target, entries, request.getExtraAttributes());
    }

    /*
     * NOTE if entry is many to x and already mapped to another seed, technically we don't need to update lookup entry
     *      but it is hard to pass in this info to associate method since associate method is fixed.
     * TODO Modify associate interface later if there is performance problem due to this.
     */
    private boolean needAssociation(@NotNull Triple<Integer, EntityLookupEntry, String> triple) {
        EntityLookupEntry.Mapping mapping = triple.getMiddle().getType().mapping;
        // we need to update either
        // (a) this entry is many to x or
        // (b) this entry is one to one but not mapped to any entity at the moment
        return mapping != ONE_TO_ONE || StringUtils.isBlank(triple.getRight());
    }

    private EntityRawSeed prepareSeedToAssociate(
            @NotNull EntityRawSeed target, @NotNull List<EntityLookupEntry> entries,
            Map<String, String> extraAttributes) {
        return new EntityRawSeed(target.getId(), target.getEntity(), entries, extraAttributes);
    }

    private boolean hasAssociationError(
            Triple<EntityRawSeed, List<EntityLookupEntry>, List<EntityLookupEntry>> result) {
        if (result == null) {
            return false;
        }
        return CollectionUtils.isNotEmpty(result.getMiddle()) || CollectionUtils.isNotEmpty(result.getRight());
    }

    /*
     * generate conflict messages
     */
    private List<String> getConflictMessages(@NotNull String targetEntityId, @NotNull EntityAssociationRequest request,
            Triple<EntityRawSeed, List<EntityLookupEntry>, List<EntityLookupEntry>> result,
            List<Pair<EntityLookupEntry, String>> mappingConflictEntries,
            Set<EntityLookupEntry> seedConflictEntries) {
        String entity = request.getEntity();
        List<String> seedConflicts = new ArrayList<>();
        List<String> lookupConflicts = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(mappingConflictEntries)) {
            lookupConflicts.addAll(prettify(mappingConflictEntries.stream().map(Pair::getKey)));
        }
        if (CollectionUtils.isNotEmpty(seedConflictEntries)) {
            seedConflicts.addAll(prettify(seedConflictEntries.stream()));
        }
        if (result != null) {
            if (CollectionUtils.isNotEmpty(result.getRight())) {
                lookupConflicts.addAll(prettify(result.getRight().stream()));
            }
            if (CollectionUtils.isNotEmpty(result.getMiddle())) {
                seedConflicts.addAll(prettify(result.getMiddle().stream()));
            }
        }

        List<String> errors = new ArrayList<>();
        if (!lookupConflicts.isEmpty()) {
            errors.add(String.format("Match keys %s already map to another %s, cannot be mapped to matched %s(ID=%s)",
                    lookupConflicts, entity, entity, targetEntityId));
        }
        if (!seedConflicts.isEmpty()) {
            errors.add(String.format("Match keys %s already exist in matched %s(ID=%s) with different values",
                    seedConflicts, entity, targetEntityId));
        }

        return errors;
    }

    private List<String> prettify(@NotNull Stream<EntityLookupEntry> entries) {
        return entries.map(EntityMatchUtils::prettyToString).collect(toList());
    }

    /*
     * Get all lookup results that has mapping conflict with the target entity
     *  - mapped to a different entity AND
     *  - lookup entry has one to one mapping to entity
     */
    private List<Pair<EntityLookupEntry, String>> getLookupConflictsWithTarget(
            @NotNull EntityAssociationRequest request, @NotNull EntityRawSeed target) {
        return request.getLookupResults()
                .stream()
                .map(pair -> {
                    EntityLookupEntry entry = getEntry(request.getEntity(), pair.getKey());
                    return Pair.of(entry, pair.getValue());
                })
                .filter(pair -> {
                    EntityLookupEntry.Mapping mapping = pair.getKey().getType().mapping;
                    String id = pair.getValue();
                    // one to one and mapped to another entity (exclude those not mapped to any entity)
                    return mapping == ONE_TO_ONE && StringUtils.isNotBlank(id) && !id.equals(target.getId());
                })
                .collect(toList());
    }

    /*
     * Each tuple in EntityAssociationRequest should be transformed to a list of exactly ONE lookup entry.
     */
    private EntityLookupEntry getEntry(@NotNull String entity, @NotNull MatchKeyTuple tuple) {
        List<EntityLookupEntry> entries = EntityLookupEntryConverter
                .fromMatchKeyTuple(entity, tuple);
        Preconditions.checkArgument(entries.size() == 1);
        return entries.get(0);
    }

    private EntityAssociationResponse getResponse(
            @NotNull EntityAssociationRequest request, String entitySeedId, EntityRawSeed targetSeedBeforeUpdate,
            EntityRawSeed targetSeedAfterUpdate, boolean isNewlyAllocated, Set<EntityLookupEntry> conflictEntries,
            @NotNull List<String> associationErrors) {
        return new EntityAssociationResponse(request.getTenant(), request.getEntity(), isNewlyAllocated, entitySeedId,
                targetSeedBeforeUpdate, targetSeedAfterUpdate, conflictEntries, associationErrors);
    }

    private EntityAssociationResponse getResponse(@NotNull EntityAssociationRequest request, String entitySeedId,
            EntityRawSeed targetSeedBeforeUpdate, EntityRawSeed targetSeedAfterUpdate, boolean isNewlyAllocated) {
        return new EntityAssociationResponse(request.getTenant(), request.getEntity(), entitySeedId,
                targetSeedBeforeUpdate, targetSeedAfterUpdate, isNewlyAllocated);
    }

    /*
     * helper to retrieve the first tenant in a list of requests (which are supposed to have the same tenants).
     * return null is cannot retrieve tenant or tenant does not have ID
     */
    private Tenant getTenant(@NotNull List<Pair<String, EntityAssociationRequest>> pairs) {
        if (pairs.get(0) == null || pairs.get(0).getRight() == null) {
            return null;
        }

        Tenant tenant = pairs.get(0).getRight().getTenant();
        return tenant != null && tenant.getId() != null ? tenant : null;
    }

    /*-
     * generate version map from list of requests, all request should have the same tenant/version pair
     */
    private Map<EntityMatchEnvironment, Integer> getVersionMap(
            @NotNull List<Pair<String, EntityAssociationRequest>> pairs) {
        if (pairs.get(0) == null || pairs.get(0).getRight() == null) {
            return null;
        }

        // TODO cache map reference for reuse
        return pairs.get(0).getRight().getVersionMap();
    }
}
