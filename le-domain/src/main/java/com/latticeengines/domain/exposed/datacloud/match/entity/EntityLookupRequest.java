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
    private final Integer servingVersion;
    private final MatchKeyTuple tuple;

    public EntityLookupRequest(Tenant tenant, String entity, Integer servingVersion, MatchKeyTuple tuple) {
        this.tenant = tenant;
        this.entity = entity;
        this.servingVersion = servingVersion;
        this.tuple = tuple;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public String getEntity() {
        return entity;
    }

    public Integer getServingVersion() {
        return servingVersion;
    }

    public MatchKeyTuple getTuple() {
        return tuple;
    }
}
