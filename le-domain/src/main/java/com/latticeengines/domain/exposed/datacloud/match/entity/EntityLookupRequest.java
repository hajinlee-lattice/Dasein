package com.latticeengines.domain.exposed.datacloud.match.entity;

import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Request class for entity lookup.
 */
public class EntityLookupRequest {
    /*
     * all fields must be already standardized
     */
    private final Tenant tenant;
    private final String entity;
    private final MatchKeyTuple tuple;

    public EntityLookupRequest(Tenant tenant, String entity, MatchKeyTuple tuple) {
        this.tenant = tenant;
        this.entity = entity;
        this.tuple = tuple;
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
}
