package com.latticeengines.domain.exposed.datacloud.match.entity;

import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Response class for entity lookup.
 */
public class EntityLookupResponse {
    private final Tenant tenant;
    private final String entity;
    private final MatchKeyTuple tuple;
    private final String entityId;

    public EntityLookupResponse(Tenant tenant, String entity, MatchKeyTuple tuple, String entityId) {
        this.tenant = tenant;
        this.entity = entity;
        this.tuple = tuple;
        this.entityId = entityId;
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

    public String getEntityId() {
        return entityId;
    }
}
