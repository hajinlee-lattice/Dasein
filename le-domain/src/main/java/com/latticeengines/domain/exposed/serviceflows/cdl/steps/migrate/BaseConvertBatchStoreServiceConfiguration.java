package com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.rematch.RematchConvertServiceConfiguration;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({
        @JsonSubTypes.Type(value = MigrateImportServiceConfiguration.class, name = "MigrateImportServiceConfiguration"),
        @JsonSubTypes.Type(value = ConvertBatchStoreToImportServiceConfiguration.class, name =
                "ConvertBatchStoreToImportServiceConfiguration"),
        @JsonSubTypes.Type(value = RematchConvertServiceConfiguration.class, name =
                "RematchConvertServiceConfiguration")})
public class BaseConvertBatchStoreServiceConfiguration {

    @JsonProperty("entity")
    private BusinessEntity entity;

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }
}
