package com.latticeengines.domain.exposed.datacloud.match.entity;

import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.query.BusinessEntity;

/**
 * Request class for entity lookup.
 */
public class EntityLookupRequest {
    private final String entity;
    private final MatchKeyTuple tuple;

    public EntityLookupRequest(String entity, MatchKeyTuple tuple) {
        this.entity = entity;
        this.tuple = tuple;
    }

    public String getEntity() {
        return entity;
    }

    public MatchKeyTuple getTuple() {
        return tuple;
    }
}
