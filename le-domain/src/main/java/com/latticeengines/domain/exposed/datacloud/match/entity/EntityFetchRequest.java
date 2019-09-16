package com.latticeengines.domain.exposed.datacloud.match.entity;

import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Request class for fetching entity seed
 */
public class EntityFetchRequest {
    /*
     * all fields must be already standardized
     */
    private final Tenant tenant;
    private final Integer servingVersion;
    private final String entity;
    private final String entityId;

    public EntityFetchRequest(Tenant tenant, Integer servingVersion, String entity, String entityId) {
        this.tenant = tenant;
        this.servingVersion = servingVersion;
        this.entity = entity;
        this.entityId = entityId;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public Integer getServingVersion() {
        return servingVersion;
    }

    public String getEntity() {
        return entity;
    }

    public String getEntityId() {
        return entityId;
    }

    @Override
    public String toString() {
        return "EntityFetchRequest{" + "tenant=" + tenant + ", servingVersion=" + servingVersion + ", entity='" + entity
                + '\'' + ", entityId='" + entityId + '\'' + '}';
    }
}
