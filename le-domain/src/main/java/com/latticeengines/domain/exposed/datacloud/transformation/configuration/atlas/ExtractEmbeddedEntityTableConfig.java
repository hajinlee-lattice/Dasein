package com.latticeengines.domain.exposed.datacloud.transformation.configuration.atlas;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

public class ExtractEmbeddedEntityTableConfig extends TransformerConfig {
    @JsonProperty("Entity")
    private String entity;

    @JsonProperty("SysmteIdFields")
    private List<String> systemIdFlds;

    // EntityId field name in embedded entity table
    @JsonProperty("EntityIdFields")
    private String entityIdFld;

    public String getEntity() {
        return entity;
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }

    public List<String> getSystemIdFlds() {
        return systemIdFlds;
    }

    public void setSystemIdFlds(List<String> systemIdFlds) {
        this.systemIdFlds = systemIdFlds;
    }

    public String getEntityIdFld() {
        return entityIdFld;
    }

    public void setEntityIdFld(String entityIdFld) {
        this.entityIdFld = entityIdFld;
    }
}
