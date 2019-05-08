package com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.AccountFileValidationServiceConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.ContactFileValidationServiceConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.ProductFileValidationServiceConfiguration;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
        @JsonSubTypes.Type(value = AccountFileValidationServiceConfiguration.class, name = "AccountFileValidationServiceConfiguration"),
        @JsonSubTypes.Type(value = ContactFileValidationServiceConfiguration.class, name = "ContactFileValidationServiceConfiguration"),
        @JsonSubTypes.Type(value = ProductFileValidationServiceConfiguration.class, name = "ProductFileValidationServiceConfiguration")
})
public class InputFileValidationServiceConfiguration {
    private BusinessEntity entity;
    private List<String> pathList;

    @JsonProperty("entity")
    public BusinessEntity getEntity() {
        return entity;
    }

    @JsonProperty("entity")
    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    @JsonProperty("path_list")
    public List<String> getPathList() {
        return pathList;
    }

    @JsonProperty("path_list")
    public void setPathList(List<String> pathList) {
        this.pathList = pathList;
    }

}
