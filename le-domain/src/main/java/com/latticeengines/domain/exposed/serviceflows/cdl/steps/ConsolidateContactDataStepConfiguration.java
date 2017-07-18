package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ConsolidateContactDataStepConfiguration extends ConsolidateDataBaseConfiguration {

    @JsonProperty("id_field")
    @NotEmptyString
    @NotNull
    private String idField;
    public ConsolidateContactDataStepConfiguration() {
        super();
        this.businessEntity = BusinessEntity.Contact;
    }

    public String getIdField() {
        return idField;
    }

    public void setIdField(String idField) {
        this.idField = idField;
    }
}
