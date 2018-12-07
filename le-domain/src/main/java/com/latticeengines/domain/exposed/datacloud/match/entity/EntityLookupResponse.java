package com.latticeengines.domain.exposed.datacloud.match.entity;

import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.query.BusinessEntity;

/**
 * Response class for entity lookup.
 */
public class EntityLookupResponse {
    private final String entity;
    private final MatchKeyTuple tuple;
    private final String entityId;

    public EntityLookupResponse(String entity, MatchKeyTuple tuple, String entityId) {
        this.entity = entity;
        this.tuple = tuple;
        this.entityId = entityId;
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
