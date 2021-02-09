package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.EntityType;

public class DataDeleteOperationConfiguration extends DataOperationConfiguration{

    @JsonProperty("deleteEntityType")
    private EntityType deleteEntityType;

    @JsonProperty("deleteType")
    private DeleteType deleteType;

    public EntityType getDeleteEntityType() {
        return deleteEntityType;
    }

    public void setDeleteEntityType(EntityType deleteEntityType) {
        this.deleteEntityType = deleteEntityType;
    }

    public DeleteType getDeleteType() {
        return deleteType;
    }

    public void setDeleteType(DeleteType deleteType) {
        this.deleteType = deleteType;
    }

    public enum DeleteType{
        SOFT,
        HARD;
    }
}
