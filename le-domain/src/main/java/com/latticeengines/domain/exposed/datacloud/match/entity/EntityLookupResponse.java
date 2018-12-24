package com.latticeengines.domain.exposed.datacloud.match.entity;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Response class for entity lookup.
 */
public class EntityLookupResponse {
    private final Tenant tenant;
    private final String entity;
    private final MatchKeyTuple tuple;
    private final List<String> entityIds;

    public EntityLookupResponse(Tenant tenant, String entity, MatchKeyTuple tuple, List<String> entityIds) {
        this.tenant = tenant;
        this.entity = entity;
        this.tuple = tuple;
        this.entityIds = entityIds;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public String getEntity() {
        return entity;
    }

    public MatchKeyTuple getTuple() {
        return tuple;
    }

    public List<String> getEntityIds() {
        return entityIds;
    }
}
