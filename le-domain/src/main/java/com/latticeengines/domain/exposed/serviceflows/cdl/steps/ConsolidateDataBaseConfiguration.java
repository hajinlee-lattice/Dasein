package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ConsolidateDataBaseConfiguration extends BaseWrapperStepConfiguration {

    public ConsolidateDataBaseConfiguration() {
        this.setSkipStep(true);
    }

    @JsonProperty("id_field")
    @NotEmptyString
    @NotNull
    private String idField;

    @JsonProperty("is_bucketing")
    private boolean isBucketing;

    @JsonIgnore
    protected BusinessEntity businessEntity;

    public BusinessEntity getBusinessEntity() {
        return businessEntity;
    }

    public String getIdField() {
        return idField;
    }

    public void setIdField(String idField) {
        this.idField = idField;
    }

    public boolean isBucketing() {
        return isBucketing;
    }

    public void setBucketing(boolean isBucketing) {
        this.isBucketing = isBucketing;
    }
}
