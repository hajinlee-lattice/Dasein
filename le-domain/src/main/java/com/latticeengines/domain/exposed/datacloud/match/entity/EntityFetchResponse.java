package com.latticeengines.domain.exposed.datacloud.match.entity;

import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Response class for fetching entity seed
 */
public class EntityFetchResponse {
    private final Tenant tenant;
    private final EntityRawSeed seed;

    public EntityFetchResponse(Tenant tenant, EntityRawSeed seed) {
        this.tenant = tenant;
        this.seed = seed;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public EntityRawSeed getSeed() {
        return seed;
    }

    @Override
    public String toString() {
        return "EntityFetchResponse{" + "tenant=" + tenant + ", seed=" + seed + '}';
    }
}
