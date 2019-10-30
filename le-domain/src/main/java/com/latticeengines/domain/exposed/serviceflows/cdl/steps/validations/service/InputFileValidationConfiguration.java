package com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.AccountFileValidationConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.ContactFileValidationConfiguration;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
        @JsonSubTypes.Type(value = AccountFileValidationConfiguration.class, name = "AccountFileValidationConfiguration"),
        @JsonSubTypes.Type(value = ContactFileValidationConfiguration.class, name = "ContactFileValidationConfiguration"),
        })
public class InputFileValidationConfiguration {
    @JsonProperty("entity")
    private BusinessEntity entity;

    @JsonProperty("path_list")
    private List<String> pathList;

    @JsonProperty("enable_entity_match_ga")
    private boolean enableEntityMatchGA;

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    public List<String> getPathList() {
        return pathList;
    }

    public void setPathList(List<String> pathList) {
        this.pathList = pathList;
    }

    public boolean isEnableEntityMatchGA() {
        return enableEntityMatchGA;
    }

    public void setEnableEntityMatchGA(boolean enableEntityMatchGA) {
        this.enableEntityMatchGA = enableEntityMatchGA;
    }
}
