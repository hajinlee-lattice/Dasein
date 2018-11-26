package com.latticeengines.domain.exposed.datacloud.match.cdl;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.query.BusinessEntity;

/**
 * Response class for CDL entity association.
 */
public class CDLAssociationResponse {
    private final BusinessEntity entity;
    private final String associatedEntityId;
    // TODO add conflict errors

    public CDLAssociationResponse(@NotNull BusinessEntity entity, String associatedEntityId) {
        Preconditions.checkNotNull(entity);
        this.entity = entity;
        this.associatedEntityId = associatedEntityId;
    }

    public BusinessEntity getEntity() {
        return entity;
    }

    public String getAssociatedEntityId() {
        return associatedEntityId;
    }
}
