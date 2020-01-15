package com.latticeengines.domain.exposed.datacloud.match.entity;

import java.util.Map;

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
    private final Map<EntityMatchEnvironment, Integer> versionMap;
    private final MatchKeyTuple tuple;

    public EntityLookupRequest(Tenant tenant, String entity, Map<EntityMatchEnvironment, Integer> versionMap,
            MatchKeyTuple tuple) {
        this.tenant = tenant;
        this.entity = entity;
        this.versionMap = versionMap;
        this.tuple = tuple;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public String getEntity() {
        return entity;
    }

    public Map<EntityMatchEnvironment, Integer> getVersionMap() {
        return versionMap;
    }

    public MatchKeyTuple getTuple() {
        return tuple;
    }
}
