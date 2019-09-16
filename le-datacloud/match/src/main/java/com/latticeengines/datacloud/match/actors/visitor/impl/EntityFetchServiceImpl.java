package com.latticeengines.datacloud.match.actors.visitor.impl;

import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment.SERVING;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.service.EntityMatchInternalService;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityFetchRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityFetchResponse;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Fetch seed object for target entity. Handle request batching and async lookup
 * at this service.
 *
 * Input data: type={@link EntityFetchRequest} Output:
 * type={@link EntityFetchResponse}
 */
@Component("entityFetchService")
public class EntityFetchServiceImpl extends DataSourceMicroBatchLookupServiceBase {

    private static final String THREAD_POOL_NAME = "entity-seed-fetcher";

    private static final Logger log = LoggerFactory.getLogger(EntityFetchServiceImpl.class);

    @Inject
    private EntityMatchInternalService entityMatchInternalService;

    @Value("${datacloud.match.dynamo.fetchers.num}")
    private Integer nFetcher;

    @Value("${datacloud.match.num.dynamo.fetchers.batch.num}")
    private Integer nBatchFetcher;

    @Value("${datacloud.match.dynamo.fetchers.chunk.size}")
    private Integer chunkSize;

    @Override
    protected void handleRequests(List<String> requestIds) {
        // Retrieve requests and group them by tenant. Handle all requests for the same
        // tenant afterwards.
        Map<String, List<Pair<String, EntityFetchRequest>>> params = requestIds.stream() //
                .map(id -> Pair.of(id, getReq(id))) //
                .filter(pair -> pair.getValue() != null) //
                .map(pair -> Pair.of(pair.getKey(), (EntityFetchRequest) pair.getValue().getInputData()))
                // group by tenant ID, put all lookupRequests in this tenant into a list
                .collect(groupingBy(pair -> {
                    String tenantId = pair.getValue().getTenant().getId();
                    Integer servingVersion = pair.getValue().getServingVersion();
                    return String.format("%s_%d", tenantId, servingVersion); // use both tenant & version as key
                }, mapping(pair -> pair, toList())));
        params.values().forEach(this::handleRequestForTenant);
    }

    @Override
    protected Object lookupFromService(String lookupRequestId, DataSourceLookupRequest request) {
        EntityFetchRequest fetchRequest = (EntityFetchRequest) request.getInputData();
        log.info("Fetching seed for request={}", fetchRequest);
        Map<EntityMatchEnvironment, Integer> versionMap = fetchRequest.getServingVersion() == null ? null
                : Collections.singletonMap(SERVING, fetchRequest.getServingVersion());
        return entityMatchInternalService.get(fetchRequest.getTenant(), fetchRequest.getEntity(),
                fetchRequest.getEntityId(), versionMap);
    }

    /*
     * list(pair(requestId, request)) => request should not be null and should have
     * entity
     */
    private void handleRequestForTenant(List<Pair<String, EntityFetchRequest>> pairs) {
        if (CollectionUtils.isEmpty(pairs)) {
            return;
        }

        // should all have the same tenant and serving version
        Tenant tenant = pairs.get(0).getRight().getTenant();
        Map<EntityMatchEnvironment, Integer> versionMap = getVersionMap(pairs);
        String tenantId = tenant.getId();
        Map<String, List<Pair<String, EntityFetchRequest>>> requestMap = pairs.stream()
                .collect(groupingBy(pair -> pair.getValue().getEntity(), mapping(pair -> pair, toList())));
        requestMap.forEach((entity, pairList) -> {
            try {
                // get unique entity IDs
                List<String> entityIds = pairList.stream() //
                        .map(Pair::getValue) //
                        .filter(Objects::nonNull) //
                        .map(EntityFetchRequest::getEntityId) //
                        .distinct() //
                        .collect(toList());
                log.info("Fetching seeds for tenant(ID={}), entity={}, unique IDs={}", tenantId, entity,
                        entityIds.size());
                // entityId -> seed
                Map<String, EntityRawSeed> seedMap = entityMatchInternalService
                        .get(tenant, entity, entityIds, versionMap) //
                        .stream() //
                        .collect(Collectors.toMap(EntityRawSeed::getId, seed -> seed, (s1, s2) -> s1));
                // send response for each request
                pairList.forEach(pair -> {
                    String requestId = pair.getKey();
                    String entityId = pair.getValue().getEntityId();
                    EntityFetchResponse fetchResponse = new EntityFetchResponse(tenant, seedMap.get(entityId));
                    // inject failure only for testing purpose
                    injectFailure(getReq(requestId));
                    // remove request and send response
                    String returnAddress = getReqReturnAddr(requestId);
                    removeReq(requestId);
                    sendResponse(requestId, fetchResponse, returnAddress);
                });
            } catch (Exception e) {
                String msg = String.format("Failed to retrieve seeds for tenant(ID=%s), entity=%s", tenantId, entity);
                log.error(msg, e);
                // consider all requests failed
                sendFailureResponses(pairList.stream().map(Pair::getKey).collect(toList()), e);
            }
        });
    }

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
        return isBatchMode() ? nBatchFetcher : nFetcher;
    }

    /*-
     * generate version map from list of requests, all request should have the same tenant/version pair
     */
    private Map<EntityMatchEnvironment, Integer> getVersionMap(@NotNull List<Pair<String, EntityFetchRequest>> pairs) {
        if (pairs.get(0) == null || pairs.get(0).getRight() == null) {
            return null;
        }

        // TODO cache map reference for reuse
        Integer servingVersion = pairs.get(0).getRight().getServingVersion();
        return servingVersion == null ? null : Collections.singletonMap(EntityMatchEnvironment.SERVING, servingVersion);
    }
}
