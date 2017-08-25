package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ConsolidateTransactionDataStepConfiguration extends ConsolidateDataBaseConfiguration {

    @JsonProperty("id_field")
    @NotEmptyString
    @NotNull
    private String idField;
    public ConsolidateTransactionDataStepConfiguration() {
        super();
        this.businessEntity = BusinessEntity.Transaction;
    }

    public String getIdField() {
        return idField;
    }

    public void setIdField(String idField) {
        this.idField = idField;
    }
}
