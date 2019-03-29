package com.latticeengines.domain.exposed.datacloud.match.entity;

import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Response class for entity association.
 */
public class EntityAssociationResponse {
    private final Tenant tenant;
    private final String entity;
    private final boolean isNewlyAllocated; // should be false if associatedEntityId is null
    private final String associatedEntityId;
    private final List<String> associationErrors;

    public EntityAssociationResponse(@NotNull Tenant tenant, @NotNull String entity, String associatedEntityId,
            boolean isNewlyAllocated) {
        this(tenant, entity, isNewlyAllocated, associatedEntityId, Collections.emptyList());
    }

    public EntityAssociationResponse(Tenant tenant, String entity, boolean isNewlyAllocated, String associatedEntityId,
            List<String> associationErrors) {
        Preconditions.checkNotNull(tenant);
        Preconditions.checkNotNull(entity);
        Preconditions.checkNotNull(associationErrors);
        this.tenant = tenant;
        this.entity = entity;
        this.isNewlyAllocated = isNewlyAllocated;
        this.associatedEntityId = associatedEntityId;
        this.associationErrors = associationErrors;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public String getEntity() {
        return entity;
    }

    public boolean isNewlyAllocated() {
        return isNewlyAllocated;
    }

    public String getAssociatedEntityId() {
        return associatedEntityId;
    }

    public List<String> getAssociationErrors() {
        return associationErrors;
    }
}
