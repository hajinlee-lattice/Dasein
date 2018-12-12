package com.latticeengines.datacloud.match.actors.visitor.impl;

import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityAssociationRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityAssociationResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Associate all the lookup entry for a single record to one entity.
 * The entity to associate to is decide by the result of lookups. If no entity found or there are conflicts
 * between the entity seed and lookup entries, a new entity will be created (and a new ID allocated).
 *
 * Input data: type={@link EntityAssociationRequest}
 * Output: {@link EntityAssociationResponse}
 */
@Component("entityAssociateService")
public class EntityAssociateServiceImpl extends DataSourceMicroBatchLookupServiceBase {

    private static final String THREAD_POOL_NAME = "entity-associate-worker";

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
        // TODO implement
    }

    @Override
    protected EntityAssociationResponse lookupFromService(String lookupRequestId, DataSourceLookupRequest request) {
        // TODO implement
        return null;
    }
}
