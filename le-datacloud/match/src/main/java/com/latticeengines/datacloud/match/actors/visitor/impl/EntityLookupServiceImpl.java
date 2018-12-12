package com.latticeengines.datacloud.match.actors.visitor.impl;

import com.google.common.base.Preconditions;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.service.EntityMatchInternalService;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupResponse;
import com.latticeengines.domain.exposed.security.Tenant;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

/**
 * Lookup entity ID with given {@link com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple}.
 * Handle request batching and async lookup at this service.
 *
 * Input data: type={@link EntityLookupRequest}
 * Output: type={@link com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupResponse}
 */
@Component("entityLookupService")
public class EntityLookupServiceImpl extends DataSourceMicroBatchLookupServiceBase {

    private static final String THREAD_POOL_NAME = "entity-lookup-fetcher";

    private static final Logger log = LoggerFactory.getLogger(EntityLookupServiceImpl.class);

    @Inject
    private EntityMatchInternalService entityMatchInternalService;

    @Value("${datacloud.match.dynamo.fetchers.num}")
    private Integer nFetcher;

    @Value("${datacloud.match.num.dynamo.fetchers.batch.num}")
    private Integer nBatchFetcher;

    @Value("${datacloud.match.dynamo.fetchers.chunk.size}")
    private Integer chunkSize;

    @Override
    protected EntityLookupResponse lookupFromService(String lookupRequestId, DataSourceLookupRequest request) {
        EntityLookupRequest lookupRequest = (EntityLookupRequest) request.getInputData();
        EntityLookupEntry entry = EntityLookupEntryConverter.fromMatchKeyTuple(
                lookupRequest.getEntity(), lookupRequest.getTuple());
        String entityId = entityMatchInternalService.getId(lookupRequest.getTenant(), entry);
        return new EntityLookupResponse(
                lookupRequest.getTenant(), lookupRequest.getEntity(), lookupRequest.getTuple(), entityId);
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

    @Override
    protected void handleRequests(List<String> requestIds) {
        // Retrieve requests and group them by tenant. Handle all requests for the same tenant afterwards.
        Map<Long, List<Pair<String, EntityLookupRequest>>> params = requestIds
                .stream()
                .map(id -> Pair.of(id, getReq(id)))
                .filter(pair -> pair.getValue() != null)
                .map(pair -> Pair.of(pair.getKey(), (EntityLookupRequest) pair.getValue().getInputData()))
                // group by tenant PID, put all lookupRequests in this tenant into a list
                .collect(groupingBy(pair -> pair.getValue().getTenant().getPid(), mapping(pair -> pair, toList())));
        params.values().forEach(this::handleRequestForTenant);
    }

    /*
     * Lookup seed IDs for all requests in one tenant
     */
    private void handleRequestForTenant(List<Pair<String, EntityLookupRequest>> pairs) {
        if (CollectionUtils.isEmpty(pairs)) {
            return;
        }

        Tenant tenant = pairs.get(0).getRight().getTenant(); // should all have the same tenant
        try {
            List<EntityLookupEntry> entries = pairs
                    .stream()
                    .map(Pair::getValue)
                    .map(entry -> EntityLookupEntryConverter.fromMatchKeyTuple(entry.getEntity(), entry.getTuple()))
                    .collect(toList());
            List<String> seedIds = entityMatchInternalService.getIds(tenant, entries);
            Preconditions.checkNotNull(seedIds);
            Preconditions.checkArgument(seedIds.size() == pairs.size());
            IntStream.range(0, seedIds.size()).forEach(idx -> {
                String requestId = pairs.get(idx).getKey();
                EntityLookupRequest lookupRequest = pairs.get(idx).getValue();
                EntityLookupResponse lookupResponse = new EntityLookupResponse(
                        lookupRequest.getTenant(), lookupRequest.getEntity(),
                        lookupRequest.getTuple(), seedIds.get(idx));
                String returnAddress = getReqReturnAddr(requestId);

                // remove request and send response
                removeReq(requestId);
                sendResponse(requestId, lookupResponse, returnAddress);
            });
            log.info("Lookup entity for tenant (PID={}), size = {} successfully", tenant.getPid(), pairs.size());
        } catch (Exception e) {
            log.error("Lookup entity for tenant (PID={}), size = {} failed, error = {}",
                    tenant.getPid(), pairs.size(), e.getMessage());
            // consider all requests failed
            sendFailureResponses(pairs.stream().map(Pair::getKey).collect(toList()), e);
        }
    }
}
