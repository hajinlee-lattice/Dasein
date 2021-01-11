package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DataDeleteOperationConfiguration extends DataOperationConfiguration{

    @JsonProperty("deleteType")
    private DeleteType deleteType;

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
