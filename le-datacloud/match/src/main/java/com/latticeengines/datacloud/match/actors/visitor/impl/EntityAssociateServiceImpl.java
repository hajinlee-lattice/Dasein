package com.latticeengines.datacloud.match.actors.visitor.impl;

import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Mapping.MANY_TO_MANY;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Mapping.ONE_TO_ONE;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.util.ArrayList;
import java.util.Collections;
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
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
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
                .collect(groupingBy(pair -> pair.getValue().getTenant().getId(), mapping(pair -> pair, toList())));
        params.values().forEach(this::handleRequestsForTenant);
    }

    @Override
    protected EntityAssociationResponse lookupFromService(String lookupRequestId, DataSourceLookupRequest request) {
        EntityAssociationRequest associationReq = (EntityAssociationRequest) request.getInputData();
        EntityRawSeed targetSeed = getOrAllocate(associationReq, pickTargetEntity(associationReq));
        if (targetSeed == null) {
            // no target seed, just return
            return new EntityAssociationResponse(associationReq.getTenant(), associationReq.getEntity(), null, null,
                    false);
        }

        return associate(lookupRequestId, associationReq, targetSeed);
    }

    /*
     * Process all requests that belong to a single tenant
     */
    private void handleRequestsForTenant(List<Pair<String, EntityAssociationRequest>> pairs) {
        if (CollectionUtils.isEmpty(pairs)) {
            return;
        }

        Tenant tenant = getTenant(pairs); // should all have the same tenant
        if (tenant == null) {
            return;
        }
        Preconditions.checkNotNull(tenant.getId());
        String tenantId = tenant.getId();

        try {
            List<String> targetSeedIds = pairs
                    .stream()
                    .map(Pair::getValue)
                    .map(this::pickTargetEntity)
                    .collect(Collectors.toList());
            List<EntityAssociationRequest> requests = pairs.stream().map(Pair::getValue).collect(toList());
            List<EntityRawSeed> targetSeeds = getOrAllocate(tenant, requests, targetSeedIds);
            IntStream.range(0, pairs.size())
                    .forEach(idx ->
                            associateAsync(pairs.get(idx).getKey(), pairs.get(idx).getValue(), targetSeeds.get(idx)));
            log.debug("Handled {} requests for tenant (ID={})", pairs.size(), tenantId);
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
    private EntityRawSeed getOrAllocate(@NotNull EntityAssociationRequest request, String seedId) {
        Tenant tenant = request.getTenant();
        String entity = request.getEntity();
        if (StringUtils.isNotBlank(seedId)) {
            EntityRawSeed seed = entityMatchInternalService.get(tenant, entity, seedId);
            if (!conflictInHighestPriorityEntry(request, seed)) {
                // has target seed (found with some lookup entry) and no conflict with highest
                // priority entry in target seed
                return seed;
            }
        }

        return anonymousOrNewEntity(request);
    }

    /*
     * Retrieve seeds with given IDs or allocate new ones if null seedId is provided in the list
     */
    @VisibleForTesting
    protected List<EntityRawSeed> getOrAllocate(
            @NotNull Tenant tenant, @NotNull List<EntityAssociationRequest> requests, @NotNull List<String> seedIds) {
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
                                .get(tenant, entry.getKey(), entry.getValue())
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
                        return anonymousOrNewEntity(requests.get(idx));
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
    private EntityRawSeed anonymousOrNewEntity(@NotNull EntityAssociationRequest request) {
        Tenant tenant = request.getTenant();
        String entity = request.getEntity();
        if (CollectionUtils.isEmpty(request.getLookupResults())) {
            // no lookup entry in the request, associate to anonymous entity
            return new EntityRawSeed(ANONYMOUS_ENTITY_ID, entity, Collections.emptyList(), null);
        } else {
            // allocate new seed
            String seedId = entityMatchInternalService.allocateId(tenant, entity);
            return new EntityRawSeed(seedId, entity, true);
        }
    }

    /*
     * helper to associate single request and send response based on the result, guarding against exceptions
     */
    private void associateAsync(
            @NotNull String requestId, @NotNull EntityAssociationRequest request,
            @NotNull EntityRawSeed targetEntitySeed) {
        try {
            EntityAssociationResponse response = associate(requestId, request, targetEntitySeed);
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
            @NotNull EntityRawSeed targetEntitySeed) {
        Tenant tenant = request.getTenant();
        String tenantId = tenant.getId();
        if (CollectionUtils.isEmpty(request.getLookupResults())) {
            log.debug("No lookup entry for request (ID={}), attributes={}, tenant (ID={})," +
                    " entity={}, target entity ID={}", requestId, request.getExtraAttributes(),
                    tenantId, request.getEntity(), targetEntitySeed.getId());
            if (hasExtraAttributes(targetEntitySeed, request.getExtraAttributes())) {
                EntityRawSeed seedToUpdate = new EntityRawSeed(
                        targetEntitySeed.getId(), targetEntitySeed.getEntity(),
                        Collections.emptyList(), request.getExtraAttributes());
                // ignore result as attribute update won't fail
                entityMatchInternalService.associate(request.getTenant(), seedToUpdate, false);
            }
            return getResponse(request, targetEntitySeed.getId(), targetEntitySeed.isNewlyAllocated(),
                    targetEntitySeed);
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
                            targetEntitySeed, Collections.singletonList(maxPriorityEntry), null), true);
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
                    entityMatchInternalService.cleanupOrphanSeed(tenant, entity, targetEntitySeed.getId());
                }
                // fail to associate the highest priority entry
                return getResponse(
                        request, null, null, false,
                        getConflictMessages(targetEntitySeed.getId(), request, maxPriorityResult, null, null));
            }

            log.debug("Associate highest priority lookup entry successfully to target entity (ID={})," +
                    " requestId={}, tenant (ID={}), entity={}",
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
        if (needAdditionalAssociation(request, targetEntitySeed, seedConflictEntries)) {
            // has more things to associate (excluding max highest priority entry
            // NOTE we update every thing that does NOT already mapped to another seed
            //      and rely on associate method of internal service to handle other conflict for code simplicity.
            //      Even though we can filter out some of them (e.g., one to one lookup entry that is already in target)
            //      , it does not actually save anything (except for a few bytes sent over network).
            EntityRawSeed seedToUpdate = prepareSeedToAssociate(request, targetEntitySeed, seedConflictEntries);
            Triple<EntityRawSeed, List<EntityLookupEntry>, List<EntityLookupEntry>> result =
                    entityMatchInternalService.associate(tenant, seedToUpdate, false);
            Preconditions.checkNotNull(result);

            log.debug("Association result = {}, mapping conflict entries = {}," +
                    " seed conflict entries = {}, requestId = {}",
                    result, mappingConflictEntries, seedConflictEntries, requestId);
            return getResponse(
                    request, targetEntitySeed.getId(), targetEntitySeed, targetEntitySeed.isNewlyAllocated(),
                    getConflictMessages(targetEntitySeed.getId(), request, result, mappingConflictEntries,
                            seedConflictEntries));
        }

        // no conflict during association
        log.debug("No need for additional association. Mapping conflict entries = {}," +
                " seed conflict entries = {}, requestId = {}",
                mappingConflictEntries, seedConflictEntries, requestId);
        return getResponse(
                request, targetEntitySeed.getId(), targetEntitySeed, targetEntitySeed.isNewlyAllocated(),
                getConflictMessages(targetEntitySeed.getId(), request, null, mappingConflictEntries,
                        seedConflictEntries));
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
    private boolean needAdditionalAssociation(
            @NotNull EntityAssociationRequest request, @NotNull EntityRawSeed target,
            @NotNull Set<EntityLookupEntry> seedConflictEntries) {
        Optional<Pair<EntityLookupEntry, String>> entryNeedUpdate = request.getLookupResults()
                .stream()
                // skip the highest priority one
                .skip(1L)
                .map(pair -> {
                    EntityLookupEntry entry = getEntry(request.getEntity(), pair.getKey());
                    return Pair.of(entry, pair.getValue());
                })
                // entries not mapped to target (null or diff ID)
                .filter(pair -> !target.getId().equals(pair.getValue()))
                .filter(this::needAssociation)
                // entry does not have conflict in seed (e.g., SFDC_ID already have a different value)
                .filter(pair -> !seedConflictEntries.contains(pair.getKey()))
                .findFirst();
        return entryNeedUpdate.isPresent() || hasExtraAttributes(target, request.getExtraAttributes());
    }

    private boolean hasExtraAttributes(@NotNull EntityRawSeed target, Map<String, String> extraAttributes) {
        // need to update if attrs in request is not a subset of current attrs
        return MapUtils.isNotEmpty(extraAttributes) &&
                !target.getAttributes().entrySet().containsAll(extraAttributes.entrySet());
    }

    /*
     * Create a raw seed that contains all lookup entries that need association, need to be in sync with
     * this#needAdditionalAssociation
     */
    private EntityRawSeed prepareSeedToAssociate(
            @NotNull EntityAssociationRequest request, @NotNull EntityRawSeed target,
            @NotNull Set<EntityLookupEntry> seedConflictEntries) {
        List<EntityLookupEntry> entries = request
                .getLookupResults()
                .stream()
                .map(pair -> {
                    EntityLookupEntry entry = getEntry(request.getEntity(), pair.getKey());
                    return Pair.of(entry, pair.getValue());
                })
                // entries not mapped to target (null or diff ID)
                .filter(pair -> !target.getId().equals(pair.getValue()))
                .filter(this::needAssociation)
                // entry does not have conflict in seed (e.g., SFDC_ID already have a different value)
                .filter(pair -> !seedConflictEntries.contains(pair.getKey()))
                .map(Pair::getLeft)
                .collect(toList());
        return prepareSeedToAssociate(target, entries, request.getExtraAttributes());
    }

    /*
     * NOTE if entry is many to x and already mapped to another seed, technically we don't need to update lookup entry
     *      but it is hard to pass in this info to associate method since associate method is fixed.
     * TODO Modify associate interface later if there is performance problem due to this.
     */
    private boolean needAssociation(@NotNull Pair<EntityLookupEntry, String> pair) {
        EntityLookupEntry.Mapping mapping = pair.getKey().getType().mapping;
        // we need to update either
        // (a) this entry is many to x or
        // (b) this entry is one to one but not mapped to any entity at the moment
        return mapping != ONE_TO_ONE || StringUtils.isBlank(pair.getValue());
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

    // TODO using pre-update seed for now since we only need pre-update accountId
    // attr for email-only match keys, perform artificial update later

    private EntityAssociationResponse getResponse(
            @NotNull EntityAssociationRequest request, String entitySeedId, EntityRawSeed targetEntitySeed,
            boolean isNewlyAllocated, @NotNull List<String> associationErrors) {
        return new EntityAssociationResponse(request.getTenant(), request.getEntity(), isNewlyAllocated, entitySeedId,
                targetEntitySeed,
                associationErrors);
    }

    private EntityAssociationResponse getResponse(@NotNull EntityAssociationRequest request, String entitySeedId,
            boolean isNewlyAllocated, EntityRawSeed targetEntitySeed) {
        return new EntityAssociationResponse(request.getTenant(), request.getEntity(), entitySeedId, targetEntitySeed,
                isNewlyAllocated);
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
}
