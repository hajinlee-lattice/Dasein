package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
        @JsonSubTypes.Type(value = DataDeleteOperationConfiguration.class, name = "DataDeleteOperationConfiguration")})
public class DataOperationConfiguration {

    @JsonProperty("idEntity")
    private BusinessEntity idEntity;

    @JsonProperty("systemName")
    private String systemName;

    public BusinessEntity getIdEntity() {
        return idEntity;
    }

    public void setIdEntity(BusinessEntity idEntity) {
        this.idEntity = idEntity;
    }

    public String getSystemName() {
        return systemName;
    }

    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }
}
