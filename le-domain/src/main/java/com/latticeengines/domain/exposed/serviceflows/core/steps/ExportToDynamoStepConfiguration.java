package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

public class ExportToDynamoStepConfiguration extends MicroserviceStepConfiguration {

    @NotNull
    @JsonProperty("dynamoSignature")
    private String dynamoSignature;

    @JsonProperty("migrateTable")
    private Boolean migrateTable;

    public String getDynamoSignature() {
        return dynamoSignature;
    }

    public void setDynamoSignature(String dynamoSignature) {
        this.dynamoSignature = dynamoSignature;
    }

    public Boolean getMigrateTable() {
        return migrateTable;
    }

    public void setMigrateTable(Boolean migrateTable) {
        this.migrateTable = migrateTable;
    }
}
