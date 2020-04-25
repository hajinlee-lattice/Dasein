package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ExportToDynamoStepConfiguration.class, name = "ExportToDynamoStepConfiguration"),
        @JsonSubTypes.Type(value = ExportTimelineRawTableToDynamoStepConfiguration.class, name = "ExportTimelineRawTableToDynamoStepConfiguration") })
public abstract class BaseExportToDynamoConfiguration extends MicroserviceStepConfiguration {

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

    public abstract Class<?> getEntityClass();

    public abstract String getRepoName();

    public abstract String getContextKey();

    public abstract boolean needKeyPrefix();
}
