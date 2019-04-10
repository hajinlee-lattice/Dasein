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
    private final String entity;
    private final String entityId;

    public EntityFetchRequest(Tenant tenant, String entity, String entityId) {
        this.tenant = tenant;
        this.entity = entity;
        this.entityId = entityId;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public String getEntity() {
        return entity;
    }

    public String getEntityId() {
        return entityId;
    }

    @Override
    public String toString() {
        return "EntityFetchRequest{" + "tenant=" + tenant + ", entity='" + entity + '\'' + ", entityId='" + entityId
                + '\'' + '}';
    }
}
