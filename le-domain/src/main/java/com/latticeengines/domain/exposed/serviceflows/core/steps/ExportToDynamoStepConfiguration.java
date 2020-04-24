package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datafabric.GenericTableEntity;

public class ExportToDynamoStepConfiguration extends BaseExportToDynamoConfiguration {

    @JsonProperty("migrateTable")
    private Boolean migrateTable;

    public Boolean getMigrateTable() {
        return migrateTable;
    }

    public void setMigrateTable(Boolean migrateTable) {
        this.migrateTable = migrateTable;
    }

    @Override
    public Class<?> getEntityClass() {
        return GenericTableEntity.class;
    }

    @Override
    public String getRepoName() {
        return "GenericTable";
    }
}
