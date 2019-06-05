package com.latticeengines.domain.exposed.datacloud.match.entity;

import java.util.Collections;
import java.util.List;
import java.util.Set;

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
    // snapshot of target seed before update
    private final EntityRawSeed seedBeforeAssociation;
    // state of target seed after update
    private final EntityRawSeed seedAfterAssociation;
    private final Set<EntityLookupEntry> conflictEntries;
    private final List<String> associationErrors;

    public EntityAssociationResponse(@NotNull Tenant tenant, @NotNull String entity, String associatedEntityId,
            EntityRawSeed seedBeforeAssociation, EntityRawSeed seedAfterAssociation,
            boolean isNewlyAllocated) {
        this(tenant, entity, isNewlyAllocated, associatedEntityId, seedBeforeAssociation, seedAfterAssociation,
                null, Collections.emptyList());
    }

    public EntityAssociationResponse(Tenant tenant, String entity, boolean isNewlyAllocated, String associatedEntityId,
            EntityRawSeed seedBeforeAssociation, EntityRawSeed seedAfterAssociation,
            Set<EntityLookupEntry> conflictEntries, List<String> associationErrors) {
        Preconditions.checkNotNull(tenant);
        Preconditions.checkNotNull(entity);
        Preconditions.checkNotNull(associationErrors);
        this.tenant = tenant;
        this.entity = entity;
        this.isNewlyAllocated = isNewlyAllocated;
        this.associatedEntityId = associatedEntityId;
        this.seedBeforeAssociation = seedBeforeAssociation;
        this.seedAfterAssociation = seedAfterAssociation;
        this.conflictEntries = conflictEntries == null ? Collections.emptySet() : conflictEntries;
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

    public EntityRawSeed getSeedBeforeAssociation() {
        return seedBeforeAssociation;
    }

    public Set<EntityLookupEntry> getConflictEntries() {
        return conflictEntries;
    }

    public EntityRawSeed getSeedAfterAssociation() {
        return seedAfterAssociation;
    }

    public List<String> getAssociationErrors() {
        return associationErrors;
    }
}
